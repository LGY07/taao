use std::pin::Pin;
use std::task::{Context, Poll};

use async_channel::{Receiver, Sender};
use futures_lite::Stream;
use winit::event_loop::{ActiveEventLoop, EventLoopProxy};

use super::{DEFAULT_EVENT_CAPACITY, EventQueue, EventQueueSender, SubmitError};

/// An owned winit event delivered by [`Event`].
#[derive(Debug)]
pub enum EventKind {
    /// An event delivered by winit's application handler.
    Winit(winit::event::Event<()>),
}

pub(crate) type Operation = Box<dyn FnOnce(&ActiveEventLoop)>;

/// A bounded event stream and handle for submitting work to the winit loop.
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

    /// Runs an operation on winit's event-loop thread and awaits its result.
    pub async fn submit<R: 'static>(
        &self,
        operation: impl FnOnce(&ActiveEventLoop) -> R + 'static,
    ) -> Result<R, SubmitError> {
        let (reply_sender, reply_receiver) = async_channel::bounded(1);
        if self
            .operations
            .send(Box::new(move |event_loop| {
                #[cfg(feature = "tracing")]
                tracing::trace!(target: "taao::event", backend = "winit", "executing submitted operation");
                let _ = reply_sender.try_send(operation(event_loop));
            }))
            .await
            .is_err()
        {
            #[cfg(feature = "tracing")]
            tracing::error!(target: "taao::event", backend = "winit", "failed to enqueue submitted operation");
            return Err(SubmitError);
        }
        #[cfg(feature = "tracing")]
        tracing::trace!(target: "taao::event", backend = "winit", "submitted operation queued");
        if let Err(error) = self.proxy.send_event(()) {
            #[cfg(feature = "tracing")]
            tracing::error!(target: "taao::event", backend = "winit", %error, "failed to wake event loop for submitted operation");
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
