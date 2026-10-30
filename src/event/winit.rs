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

pub(crate) type Operation = Box<dyn FnOnce(&ActiveEventLoop)>;

/// Event stream and event-loop operation handle for the winit backend.
///
/// This is the value handed to the future by
/// [`block_on_with_events`](crate::EventLoopBlockOn::block_on_with_events). It implements
/// [`futures_lite::Stream`] with item type [`EventKind`], yielding backend events as they
/// arrive, and its [`submit`](Event::submit) method runs closures on the event-loop
/// thread.
///
/// The stream is bounded; when the queue is full, a new event overwrites the oldest queued
/// one. See [`dropped_events`](Event::dropped_events).
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

    /// Returns the number of events overwritten so far because the queue was full.
    pub fn dropped_events(&self) -> usize {
        self.events.dropped_events()
    }

    /// Runs `operation` on the event-loop thread and asynchronously returns its result.
    ///
    /// The closure receives the [`ActiveEventLoop`], which makes this the supported way to
    /// create windows or change loop behaviour from the future.
    ///
    /// Returns [`SubmitError`] when the event loop stops before the operation runs.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use futures_lite::StreamExt;
    /// use taao::EventLoopBlockOn;
    ///
    /// let mut event_loop = winit::event_loop::EventLoop::new()?;
    /// event_loop.block_on_with_events(async |mut events| -> Result<(), Box<dyn std::error::Error>> {
    ///     // Wait for the first event, then create a window on the event-loop thread.
    ///     events.next().await;
    ///     let window = events
    ///         .submit(|event_loop| {
    ///             event_loop.create_window(winit::window::WindowAttributes::default())
    ///         })
    ///         .await??;
    ///     # let _ = window;
    ///     Ok(())
    /// })?;
    /// # Ok(())
    /// # }
    /// # fn main() {
    /// #     let _ = run();
    /// # }
    /// ```
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
