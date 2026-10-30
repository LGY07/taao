//! [`Event`] and [`EventKind`] for the Tao backend.
use std::pin::Pin;
use std::task::{Context, Poll};

use async_channel::{Receiver, Sender};
use futures_lite::Stream;
use tao::dpi::PhysicalSize;
use tao::event_loop::{ControlFlow, EventLoopProxy, EventLoopWindowTarget};

use super::{DEFAULT_EVENT_CAPACITY, EventQueue, EventQueueSender, SubmitError};

/// An event delivered by the Tao backend's event stream.
///
/// Events that taao uses internally to wake the future are consumed by the driver and
/// never delivered. Loop-phase events without a useful payload for applications
/// (`NewEvents`, `MainEventsCleared`, `RedrawEventsCleared`, `LoopDestroyed`) are excluded
/// as well.
#[derive(Debug)]
pub enum EventKind {
    /// A regular Tao event, converted to an owned `'static` value.
    Tao(tao::event::Event<'static, ()>),
    /// A window's scale factor changed.
    ///
    /// Tao reports this through [`tao::event::WindowEvent::ScaleFactorChanged`] with a
    /// borrowed inner size, so taao surfaces it as a dedicated variant carrying an owned
    /// suggested size.
    TaoScaleFactorChanged {
        /// The window whose scale factor changed.
        window_id: tao::window::WindowId,
        /// The new scale factor.
        scale_factor: f64,
        /// The inner size Tao suggests for the window after the change.
        suggested_inner_size: PhysicalSize<u32>,
    },
}

pub(crate) type Operation = Box<dyn FnOnce(&EventLoopWindowTarget<()>, &mut ControlFlow)>;

/// Event stream and event-loop operation handle for the Tao backend.
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
    /// The closure receives the [`EventLoopWindowTarget`] and the current [`ControlFlow`],
    /// which makes this the supported way to create windows or change loop behaviour from
    /// the future.
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
    /// let mut event_loop = tao::event_loop::EventLoop::new();
    /// event_loop.block_on_with_events(async |mut events| -> Result<(), Box<dyn std::error::Error>> {
    ///     // Wait for the first event, then create a window on the event-loop thread.
    ///     events.next().await;
    ///     let window = events
    ///         .submit(|target, _control_flow| tao::window::WindowBuilder::new().build(target))
    ///         .await??;
    ///     # let _ = window;
    ///     Ok(())
    /// })??;
    /// # Ok(())
    /// # }
    /// # fn main() {
    /// #     let _ = run();
    /// # }
    /// ```
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
