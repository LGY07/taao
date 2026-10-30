use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_channel::{Receiver, Sender};
use futures_lite::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(feature = "tao")]
pub(crate) mod tao;
#[cfg(feature = "winit")]
pub(crate) mod winit;

#[cfg(feature = "tao")]
pub use tao::{Event, EventKind};
#[cfg(feature = "winit")]
pub use winit::{Event, EventKind};

/// The event loop stopped before a submitted operation could complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubmitError;

impl std::fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the event loop stopped before the operation completed")
    }
}

impl std::error::Error for SubmitError {}

pub(crate) const DEFAULT_EVENT_CAPACITY: usize = 256;

pub(crate) struct EventQueue<T> {
    receiver: Pin<Box<Receiver<T>>>,
    dropped: Arc<AtomicUsize>,
}

pub(crate) struct EventQueueSender<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
    dropped: Arc<AtomicUsize>,
}

impl<T> EventQueue<T> {
    pub(crate) fn bounded(capacity: usize) -> (Self, EventQueueSender<T>) {
        let (sender, receiver) = async_channel::bounded(capacity.max(1));
        let dropped = Arc::new(AtomicUsize::new(0));
        let event_sender = EventQueueSender {
            sender,
            receiver: receiver.clone(),
            dropped: Arc::clone(&dropped),
        };
        (
            Self {
                receiver: Box::pin(receiver),
                dropped: Arc::clone(&dropped),
            },
            event_sender,
        )
    }

    pub(crate) fn dropped_events(&self) -> usize {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl<T> Stream for EventQueue<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.as_mut().poll_next(context)
    }
}

impl<T> EventQueueSender<T> {
    pub(crate) fn send(&self, event: T) {
        let mut pending = event;
        loop {
            match self.sender.try_send(pending) {
                Ok(()) => return,
                Err(async_channel::TrySendError::Closed(_)) => return,
                Err(async_channel::TrySendError::Full(event)) => {
                    pending = event;
                    if self.receiver.try_recv().is_ok() {
                        let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                        #[cfg(feature = "tracing")]
                        if dropped == 1 || dropped.is_power_of_two() {
                            tracing::warn!(target: "taao::event", dropped, "overwrote the oldest queued event");
                        }
                        #[cfg(not(feature = "tracing"))]
                        let _ = dropped;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_lite::StreamExt;

    use super::EventQueue;

    #[test]
    fn bounded_queue_overwrites_oldest_event_when_full() {
        let (mut events, sender) = EventQueue::bounded(2);
        sender.send(1);
        sender.send(2);
        sender.send(3);

        assert_eq!(events.dropped_events(), 1);
        assert_eq!(futures_lite::future::block_on(events.next()), Some(2));
        assert_eq!(futures_lite::future::block_on(events.next()), Some(3));
    }

    #[test]
    fn zero_capacity_is_clamped_to_one_and_keeps_the_newest_event() {
        let (mut events, sender) = EventQueue::bounded(0);
        sender.send(1);
        sender.send(2);

        assert_eq!(events.dropped_events(), 1);
        assert_eq!(futures_lite::future::block_on(events.next()), Some(2));
    }

    #[test]
    fn submit_error_has_a_stable_display_message() {
        assert_eq!(
            super::SubmitError.to_string(),
            "the event loop stopped before the operation completed"
        );
    }
}
