use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll, Wake, Waker};

use async_channel::Receiver;
use winit::application::ApplicationHandler;
use winit::event::{DeviceEvent, Event as WinitEvent, StartCause, WindowEvent};
use winit::event_loop::{ActiveEventLoop, ControlFlow, EventLoop};
use winit::platform::run_on_demand::EventLoopExtRunOnDemand;
use winit::window::WindowId;

use crate::BlockOnError;

struct EventLoopWake {
    proxy: winit::event_loop::EventLoopProxy<()>,
    notified: AtomicBool,
}

impl Wake for EventLoopWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.notified.swap(true, Ordering::AcqRel) {
            if let Err(error) = self.proxy.send_event(()) {
                #[cfg(feature = "tracing")]
                tracing::trace!(target: "taao::block_on", %error, "winit wake notification was rejected");
                #[cfg(not(feature = "tracing"))]
                let _ = error;
            }
        }
    }
}

struct Driver<F: Future> {
    future: Pin<Box<F>>,
    output: Option<F::Output>,
    wake: Arc<EventLoopWake>,
    waker: Waker,
    #[cfg(feature = "event")]
    events: Option<crate::event::EventQueueSender<crate::event::EventKind>>,
    #[cfg(feature = "event")]
    operations: Option<Receiver<crate::event::winit::Operation>>,
}

impl<F: Future> Driver<F> {
    #[cfg(feature = "event")]
    fn send(&self, event: WinitEvent<()>) {
        if let Some(sender) = &self.events {
            sender.send(crate::event::EventKind::Winit(event));
        }
    }

    #[cfg(feature = "event")]
    fn process_operations(&self, event_loop: &ActiveEventLoop) {
        if let Some(operations) = &self.operations {
            while let Ok(operation) = operations.try_recv() {
                operation(event_loop);
            }
        }
    }

    fn poll(&mut self, event_loop: &ActiveEventLoop) {
        self.wake.notified.store(false, Ordering::Release);
        let mut context = Context::from_waker(&self.waker);
        if let Poll::Ready(output) = self.future.as_mut().poll(&mut context) {
            self.output = Some(output);
            event_loop.exit();
            #[cfg(feature = "tracing")]
            tracing::debug!(target: "taao::block_on", backend = "winit", "future completed");
        }
    }

    fn after_callback(&mut self, event_loop: &ActiveEventLoop) {
        #[cfg(feature = "event")]
        self.process_operations(event_loop);
        self.poll(event_loop);
    }
}

impl<F: Future> ApplicationHandler<()> for Driver<F> {
    fn new_events(&mut self, event_loop: &ActiveEventLoop, cause: StartCause) {
        #[cfg(feature = "event")]
        self.send(WinitEvent::NewEvents(cause));
        self.after_callback(event_loop);
    }

    fn window_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        window_id: WindowId,
        event: WindowEvent,
    ) {
        #[cfg(feature = "event")]
        self.send(WinitEvent::WindowEvent { window_id, event });
        self.after_callback(event_loop);
    }

    fn device_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        device_id: winit::event::DeviceId,
        event: DeviceEvent,
    ) {
        #[cfg(feature = "event")]
        self.send(WinitEvent::DeviceEvent { device_id, event });
        self.after_callback(event_loop);
    }

    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        #[cfg(feature = "event")]
        self.send(WinitEvent::Resumed);
        self.after_callback(event_loop);
    }

    fn user_event(&mut self, event_loop: &ActiveEventLoop, (): ()) {
        self.after_callback(event_loop);
    }

    fn about_to_wait(&mut self, event_loop: &ActiveEventLoop) {
        #[cfg(feature = "event")]
        self.send(WinitEvent::AboutToWait);
        self.after_callback(event_loop);
        if self.output.is_none() {
            event_loop.set_control_flow(ControlFlow::Wait);
        }
    }

    fn suspended(&mut self, event_loop: &ActiveEventLoop) {
        #[cfg(feature = "event")]
        self.send(WinitEvent::Suspended);
        self.after_callback(event_loop);
    }

    fn memory_warning(&mut self, event_loop: &ActiveEventLoop) {
        #[cfg(feature = "event")]
        self.send(WinitEvent::MemoryWarning);
        self.after_callback(event_loop);
    }
}

#[cfg(feature = "event")]
type EventSink = Option<crate::event::EventQueueSender<crate::event::EventKind>>;
#[cfg(not(feature = "event"))]
type EventSink = ();

#[cfg(feature = "event")]
type Operations = Option<Receiver<crate::event::winit::Operation>>;
#[cfg(not(feature = "event"))]
type Operations = ();

fn empty_event_sink() -> EventSink {
    #[cfg(feature = "event")]
    {
        None
    }
    #[cfg(not(feature = "event"))]
    {
        ()
    }
}

fn empty_operations() -> Operations {
    #[cfg(feature = "event")]
    {
        None
    }
    #[cfg(not(feature = "event"))]
    {
        ()
    }
}

pub(crate) fn block_on<F>(
    event_loop: &mut EventLoop<()>,
    future: F,
) -> Result<F::Output, BlockOnError>
where
    F: Future,
{
    run(event_loop, future, empty_event_sink(), empty_operations())
}

#[cfg(feature = "event")]
pub(crate) fn block_on_with_events<F, Output>(
    event_loop: &mut EventLoop<()>,
    handler: F,
) -> Result<Output, BlockOnError>
where
    F: std::ops::AsyncFnOnce(crate::event::Event) -> Output,
{
    let proxy = event_loop.create_proxy();
    let (event, events, operations) = crate::event::winit::Event::new(proxy);
    let future = async move { handler(event).await };
    run(event_loop, future, Some(events), Some(operations))
}

fn run<F>(
    event_loop: &mut EventLoop<()>,
    future: F,
    events: EventSink,
    operations: Operations,
) -> Result<F::Output, BlockOnError>
where
    F: Future,
{
    #[cfg(feature = "tracing")]
    tracing::debug!(target: "taao::block_on", backend = "winit", "starting event-loop future");
    let wake = Arc::new(EventLoopWake {
        proxy: event_loop.create_proxy(),
        notified: AtomicBool::new(true),
    });
    let waker = Waker::from(Arc::clone(&wake));
    let mut driver = Driver {
        future: Box::pin(future),
        output: None,
        wake,
        waker,
        #[cfg(feature = "event")]
        events,
        #[cfg(feature = "event")]
        operations,
    };

    if let Err(error) = event_loop.run_app_on_demand(&mut driver) {
        #[cfg(feature = "tracing")]
        tracing::error!(target: "taao::block_on", backend = "winit", %error, "failed to run event loop");
        return Err(BlockOnError::Backend(error.to_string()));
    }

    match driver.output {
        Some(output) => Ok(output),
        None => {
            #[cfg(feature = "tracing")]
            tracing::error!(target: "taao::block_on", backend = "winit", "event loop exited before the future completed");
            Err(BlockOnError::EventLoopExited)
        }
    }
}
