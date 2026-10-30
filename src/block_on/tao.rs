use std::future::Future;
use std::pin::pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll, Wake, Waker};

#[cfg(feature = "event")]
use async_channel::Receiver;
use tao::event::Event;
use tao::event_loop::{ControlFlow, EventLoop};
use tao::platform::run_return::EventLoopExtRunReturn;

use crate::BlockOnError;

struct EventLoopWake {
    proxy: tao::event_loop::EventLoopProxy<()>,
    notified: AtomicBool,
}

impl Wake for EventLoopWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.notified.swap(true, Ordering::AcqRel)
            && let Err(error) = self.proxy.send_event(())
        {
            #[cfg(feature = "tracing")]
            tracing::trace!(target: "taao::block_on", %error, "Tao wake notification was rejected");
            #[cfg(not(feature = "tracing"))]
            let _ = error;
        }
    }
}

#[cfg(feature = "event")]
type EventSink = Option<crate::event::EventQueueSender<crate::event::EventKind>>;
#[cfg(not(feature = "event"))]
type EventSink = EventFeatureDisabled;

#[cfg(not(feature = "event"))]
struct EventFeatureDisabled;

fn empty_event_sink() -> EventSink {
    #[cfg(feature = "event")]
    {
        None
    }
    #[cfg(not(feature = "event"))]
    {
        EventFeatureDisabled
    }
}

#[cfg(feature = "event")]
type Operations = Option<Receiver<crate::event::tao::Operation>>;
#[cfg(not(feature = "event"))]
type Operations = EventFeatureDisabled;

fn empty_operations() -> Operations {
    #[cfg(feature = "event")]
    {
        None
    }
    #[cfg(not(feature = "event"))]
    {
        EventFeatureDisabled
    }
}

pub(crate) fn block_on<F>(
    event_loop: &mut EventLoop<()>,
    future: F,
) -> Result<F::Output, BlockOnError>
where
    F: Future,
{
    let proxy = event_loop.create_proxy();
    run(
        event_loop,
        future,
        empty_event_sink(),
        empty_operations(),
        proxy,
    )
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
    let (event, events, operations) = crate::event::tao::Event::new(proxy.clone());
    let future = async move { handler(event).await };
    run(event_loop, future, Some(events), Some(operations), proxy)
}

fn run<F>(
    event_loop: &mut EventLoop<()>,
    future: F,
    _events: EventSink,
    _operations: Operations,
    proxy: tao::event_loop::EventLoopProxy<()>,
) -> Result<F::Output, BlockOnError>
where
    F: Future,
{
    #[cfg(feature = "tracing")]
    tracing::debug!(target: "taao::block_on", backend = "tao", "starting event-loop future");
    let wake = Arc::new(EventLoopWake {
        proxy,
        notified: AtomicBool::new(true),
    });
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    let mut output = None;

    event_loop.run_return(|event, _target, _control_flow| {
        let is_user_event = matches!(&event, Event::UserEvent(()));
        let should_poll =
            matches!(&event, Event::NewEvents(_) | Event::MainEventsCleared) || is_user_event;

        #[cfg(feature = "event")]
        if let Some(operations) = &_operations {
            while let Ok(operation) = operations.try_recv() {
                operation(_target, _control_flow);
            }
        }

        #[cfg(feature = "event")]
        if let Some(sender) = &_events
            && let Some(event) = crate::event::tao::from_tao(event)
        {
            sender.send(event);
        }

        if is_user_event {
            wake.notified.store(false, Ordering::Release);
        }

        if output.is_some() {
            *_control_flow = ControlFlow::Exit;
            return;
        }

        if should_poll {
            wake.notified.store(false, Ordering::Release);
            if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
                output = Some(result);
                *_control_flow = ControlFlow::Exit;
                #[cfg(feature = "tracing")]
                tracing::debug!(target: "taao::block_on", backend = "tao", "future completed");
            } else if *_control_flow != ControlFlow::Exit {
                *_control_flow = ControlFlow::Wait;
            }
        }
    });

    match output {
        Some(output) => Ok(output),
        None => {
            #[cfg(feature = "tracing")]
            tracing::error!(target: "taao::block_on", backend = "tao", "event loop exited before the future completed");
            Err(BlockOnError::EventLoopExited)
        }
    }
}
