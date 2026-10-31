//! [`Event`] and [`EventKind`] for the winit backend.
use std::pin::Pin;
use std::task::{Context, Poll};

use async_channel::{Receiver, Sender};
use futures_lite::Stream;
use winit::event_loop::{ActiveEventLoop, EventLoopProxy};

use super::{DEFAULT_EVENT_CAPACITY, EventQueue, EventQueueSender, SubmitError};

/// An event delivered by the winit backend's event stream.
///
/// User events that taao uses internally to wake the future are consumed by the driver
/// and never delivered.
#[derive(Debug)]
pub enum EventKind {
    /// A regular winit application event.
    Winit(winit::event::Event<()>),
}

pub(crate) type Operation = Box<dyn FnOnce(&ActiveEventLoop) + Send>;

/// Event stream and event-loop operation handle for the winit backend.
///
/// This is the value handed to the future by
/// [`block_on_with_events`](crate::EventLoopBlockOn::block_on_with_events). It implements
/// [`futures_lite::Stream`] with item type [`EventKind`], yielding backend events as they
/// arrive, and its [`submit`](Submitter::submit) method runs closures on the event-loop
/// thread. Use [`Event::submitter`] to submit from separate tasks.
///
/// The stream is bounded; when the queue is full, a new event overwrites the oldest queued
/// one. See [`dropped_events`](Event::dropped_events).
pub struct Event {
    events: EventQueue<EventKind>,
    submitter: EventSubmitter,
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
                submitter: EventSubmitter {
                    operations: operation_sender,
                    proxy,
                },
            },
            event_sender,
            operation_receiver,
        )
    }

    /// Returns an owned, cloneable handle for submitting event-loop operations.
    ///
    /// The handle does not borrow this stream. Move it into other tasks or threads while
    /// continuing to receive events here. Dropping the stream does not invalidate the
    /// handle; submissions remain available until this event-loop run ends.
    pub fn submitter(&self) -> EventSubmitter {
        self.submitter.clone()
    }

    /// Returns the number of events overwritten so far because the queue was full.
    pub fn dropped_events(&self) -> usize {
        self.events.dropped_events()
    }
}

/// Submits operations to the event-loop thread.
///
/// Implemented by both [`Event`] and the cloneable [`EventSubmitter`].
/// Import this trait to call `.submit(...)` on either type. Generic helpers can accept
/// `&impl Submitter`; the returned submission future is `Send`.
pub trait Submitter {
    /// Runs `operation` on the event-loop thread and asynchronously returns its result.
    ///
    /// The closure receives the [`ActiveEventLoop`], which makes this the supported way to
    /// create windows or change loop behaviour from the future.
    ///
    /// The operation and its return value must be `Send` so the handle can be moved
    /// between threads while operations are queued for the event-loop thread.
    ///
    /// Returns [`SubmitError`] when the event loop stops before the operation runs.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use futures_lite::StreamExt;
    /// use taao::EventLoopBlockOn;
    /// use taao::event::Submitter;
    ///
    /// let mut event_loop = winit::event_loop::EventLoop::new()?;
    /// event_loop.block_on_with_events(async |mut events| -> Result<(), Box<dyn std::error::Error>> {
    ///     let submitter = events.submitter();
    ///     // The stream and submitter can be moved into independent futures.
    ///     let receive_event = async move { events.next().await };
    ///     let create_window = submitter
    ///         .submit(|event_loop| {
    ///             event_loop.create_window(winit::window::WindowAttributes::default())
    ///         });
    ///     let (_, window) = futures_lite::future::zip(receive_event, create_window).await;
    ///     let window = window??;
    ///     # let _ = window;
    ///     Ok(())
    /// })?;
    /// # Ok(())
    /// # }
    /// # fn main() {
    /// #     let _ = run();
    /// # }
    /// ```
    fn submit<R: Send + 'static>(
        &self,
        operation: impl FnOnce(&ActiveEventLoop) -> R + Send + 'static,
    ) -> impl std::future::Future<Output = Result<R, SubmitError>> + Send;
}

impl Submitter for Event {
    async fn submit<R: Send + 'static>(
        &self,
        operation: impl FnOnce(&ActiveEventLoop) -> R + Send + 'static,
    ) -> Result<R, SubmitError> {
        self.submitter.submit(operation).await
    }
}

/// An owned, cloneable handle for submitting operations to the event-loop thread.
///
/// Obtain one with [`Event::submitter`]. Clones share the same operation queue and can
/// be moved into separate tasks or threads independently of the event stream.
/// A handle belongs to the event-loop run that created it; later runs do not revive it.
#[derive(Clone)]
pub struct EventSubmitter {
    operations: Sender<Operation>,
    proxy: EventLoopProxy<()>,
}

impl Submitter for EventSubmitter {
    /// Runs `operation` on the event-loop thread and asynchronously returns its result.
    ///
    /// The closure receives the [`ActiveEventLoop`].
    /// The operation and its result must be `Send + 'static`.
    ///
    /// Returns [`SubmitError`] if the event-loop run ends before the operation completes.
    async fn submit<R: Send + 'static>(
        &self,
        operation: impl FnOnce(&ActiveEventLoop) -> R + Send + 'static,
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
            #[cfg(not(feature = "tracing"))]
            let _ = error;
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
