use std::pin::Pin;
use std::task::{Context, Poll};

use async_channel::{Receiver, Sender};
use futures_lite::Stream;
use tao::dpi::PhysicalSize;
use tao::event_loop::{ControlFlow, EventLoopProxy, EventLoopWindowTarget};

use super::{DEFAULT_EVENT_CAPACITY, EventQueue, EventQueueSender, SubmitError};

/// An owned Tao event delivered by [`Event`].
#[derive(Debug)]
pub enum EventKind {
    /// An event that does not borrow data from its Tao callback.
    Tao(tao::event::Event<'static, ()>),
    /// A scale-factor event copied out of its callback-scoped size reference.
    TaoScaleFactorChanged {
        window_id: tao::window::WindowId,
        scale_factor: f64,
        suggested_inner_size: PhysicalSize<u32>,
    },
}

pub(crate) type Operation = Box<dyn FnOnce(&EventLoopWindowTarget<()>, &mut ControlFlow)>;

/// A bounded event stream and handle for submitting work to the Tao loop.
///
/// Use `futures_lite::StreamExt::next` to receive events. The event queue has a
/// capacity of 256; the oldest queued event is replaced when it is full.
pub struct Event {
    events: EventQueue<EventKind>,
    operations: Sender<Operation>,
    proxy: EventLoopProxy<()>,
}

impl Event {
    pub(crate) fn new(
        proxy: EventLoopProxy<()>,
    ) -> (Self, EventQueueSender<EventKind>, Receiver<Operation>) {
        let (events, event_sender) = EventQueue::bounded(DEFAULT_EVENT_CAPACITY);
        let (operation_sender, operation_receiver) = async_channel::bounded(64);
        (
            Self {
                events,
                operations: operation_sender,
                proxy,
            },
            event_sender,
            operation_receiver,
        )
    }

    /// Returns how many events were dropped because the bounded queue was full.
    pub fn dropped_events(&self) -> usize {
        self.events.dropped_events()
    }

    /// Runs an operation on Tao's event-loop thread and awaits its result.
    pub async fn submit<R: 'static>(
        &self,
        operation: impl FnOnce(&EventLoopWindowTarget<()>, &mut ControlFlow) -> R + 'static,
    ) -> Result<R, SubmitError> {
        let (reply_sender, reply_receiver) = async_channel::bounded(1);
        if self
            .operations
            .send(Box::new(move |target, control_flow| {
                #[cfg(feature = "tracing")]
                tracing::trace!(target: "taao::event", backend = "tao", "executing submitted operation");
                let _ = reply_sender.try_send(operation(target, control_flow));
            }))
            .await
            .is_err()
        {
            #[cfg(feature = "tracing")]
            tracing::error!(target: "taao::event", backend = "tao", "failed to enqueue submitted operation");
            return Err(SubmitError);
        }
        #[cfg(feature = "tracing")]
        tracing::trace!(target: "taao::event", backend = "tao", "submitted operation queued");
        if let Err(error) = self.proxy.send_event(()) {
            #[cfg(feature = "tracing")]
            tracing::error!(target: "taao::event", backend = "tao", %error, "failed to wake event loop for submitted operation");
            return Err(SubmitError);
        }
        reply_receiver.recv().await.map_err(|_| SubmitError)
    }
}

impl Stream for Event {
    type Item = EventKind;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.events).poll_next(context)
    }
}

pub(crate) fn from_tao(event: tao::event::Event<'_, ()>) -> Option<EventKind> {
    use tao::event::{Event as TaoEvent, WindowEvent};

    match event {
        TaoEvent::UserEvent(())
        | TaoEvent::NewEvents(_)
        | TaoEvent::MainEventsCleared
        | TaoEvent::RedrawEventsCleared
        | TaoEvent::LoopDestroyed => None,
        TaoEvent::WindowEvent {
            window_id,
            event:
                WindowEvent::ScaleFactorChanged {
                    scale_factor,
                    new_inner_size,
                    ..
                },
            ..
        } => Some(EventKind::TaoScaleFactorChanged {
            window_id,
            scale_factor,
            suggested_inner_size: *new_inner_size,
        }),
        event => event.to_static().map(EventKind::Tao),
    }
}
