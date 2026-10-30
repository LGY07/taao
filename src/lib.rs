//! Run futures on a Tao or winit event-loop thread.
//!
//! taao bridges the [Tao](https://github.com/tauri-apps/tao) or [winit](https://docs.rs/winit)
//! event loop and async Rust: it drives the event loop on the current thread while polling a
//! future, and notifies the event-loop thread through the event-loop proxy whenever the future
//! is woken.
//!
//! Exactly one backend feature must be enabled: `tao` (default) or `winit`.
//!
//! # Running a future
//!
//! Call [`EventLoopBlockOn::block_on`] on the backend event loop; it returns the future's
//! output once the future completes:
//!
//! ```no_run
//! # #[cfg(feature = "tao")]
//! # fn run() -> Result<(), taao::BlockOnError> {
//! use taao::EventLoopBlockOn;
//!
//! let mut event_loop = tao::event_loop::EventLoop::new();
//! let answer = event_loop.block_on(async { 42 })?;
//! assert_eq!(answer, 42);
//! # Ok(())
//! # }
//! # fn main() {
//! #     #[cfg(feature = "tao")]
//! #     let _ = run();
//! # }
//! ```
//!
//! With the `winit` feature, the same call works on the winit event loop.
//!
//! # Events and event-loop operations
//!
//! With the `event` feature, [`EventLoopBlockOn::block_on_with_events`] additionally hands
//! the future an [`event::Event`]: a stream of backend events plus [`event::Event::submit`]
//! for running closures on the event-loop thread. See the [`event`] module.
//!
//! # Features
//!
//! | Feature   | Description                                                         | Default |
//! |-----------|---------------------------------------------------------------------|---------|
//! | `tao`     | Use the Tao backend                                                 | Yes     |
//! | `winit`   | Use the winit backend                                               | No      |
//! | `event`   | Enable the event stream and [`Event::submit`](event::Event::submit) | No      |
//! | `tracing` | Enable diagnostic logging                                           | No      |

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(all(feature = "tao", feature = "winit", not(docsrs)))]
compile_error!("features `tao` and `winit` are mutually exclusive");

#[cfg(not(any(feature = "tao", feature = "winit")))]
compile_error!("enable exactly one backend feature: `tao` or `winit`");

mod block_on;
#[cfg(feature = "event")]
pub mod event;

use std::fmt;
use std::future::Future;
#[cfg(feature = "event")]
use std::ops::AsyncFnOnce;

#[cfg(any(
    all(feature = "tao", not(target_os = "ios")),
    all(feature = "winit", not(any(target_os = "ios", target_arch = "wasm32")))
))]
/// Extension trait that runs a future to completion on the backend's event-loop thread.
///
/// Implemented for [`tao::event_loop::EventLoop<()>`](tao::event_loop::EventLoop) with the `tao` feature and for
/// [`winit::event_loop::EventLoop<()>`](winit::event_loop::EventLoop) with the `winit` feature. Both implementations are
/// unavailable on iOS, where the platform must drive the event loop, and the winit one is
/// additionally unavailable for WebAssembly.
pub trait EventLoopBlockOn {
    /// Runs `future` on the event-loop thread and returns its output.
    ///
    /// While the future is pending, the event loop keeps dispatching backend events.
    /// Whenever the future wakes its task, taao sends a user event through the event-loop
    /// proxy so the future is polled again on the event-loop thread.
    ///
    /// Returns [`BlockOnError::EventLoopExited`] if the event loop exits before the future
    /// completes, or [`BlockOnError::Backend`] if the backend rejects the request to run
    /// the loop.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[cfg(feature = "tao")]
    /// # fn run() -> Result<(), taao::BlockOnError> {
    /// use taao::EventLoopBlockOn;
    ///
    /// let mut event_loop = tao::event_loop::EventLoop::new();
    /// let answer = event_loop.block_on(async { 42 })?;
    /// assert_eq!(answer, 42);
    /// # Ok(())
    /// # }
    /// # fn main() {
    /// #     #[cfg(feature = "tao")]
    /// #     let _ = run();
    /// # }
    /// ```
    fn block_on<F>(&mut self, future: F) -> Result<F::Output, BlockOnError>
    where
        F: Future;

    /// Runs `future` on the event-loop thread like [`block_on`](EventLoopBlockOn::block_on),
    /// and additionally passes it an [`event::Event`] handle.
    ///
    /// The handle is a stream of backend events and provides
    /// [`submit`](event::Event::submit) for running operations on the event-loop thread.
    /// See the [`event`] module.
    ///
    /// Requires the `event` feature.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[cfg(all(feature = "event", feature = "tao"))]
    /// # fn run() -> Result<(), taao::BlockOnError> {
    /// use futures_lite::StreamExt;
    /// use taao::EventLoopBlockOn;
    ///
    /// let mut event_loop = tao::event_loop::EventLoop::new();
    /// event_loop.block_on_with_events(async |mut events| {
    ///     while let Some(kind) = events.next().await {
    ///         // Handle backend events here.
    ///         # let _ = kind;
    ///     }
    /// })
    /// # }
    /// # fn main() {
    /// #     #[cfg(all(feature = "event", feature = "tao"))]
    /// #     let _ = run();
    /// # }
    /// ```
    #[cfg(feature = "event")]
    fn block_on_with_events<F, Output>(&mut self, handler: F) -> Result<Output, BlockOnError>
    where
        F: AsyncFnOnce(event::Event) -> Output;
}

/// Error returned when driving a future on the event-loop thread fails.
#[derive(Debug, Eq, PartialEq)]
pub enum BlockOnError {
    /// The event loop exited before the future completed.
    EventLoopExited,
    /// The backend reported an error while running the event loop.
    Backend(String),
}

impl fmt::Display for BlockOnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EventLoopExited => {
                formatter.write_str("event loop exited before the future completed")
            }
            Self::Backend(message) => write!(formatter, "event loop error: {message}"),
        }
    }
}

impl std::error::Error for BlockOnError {}

#[cfg(all(feature = "tao", not(target_os = "ios")))]
impl EventLoopBlockOn for tao::event_loop::EventLoop<()> {
    fn block_on<F>(&mut self, future: F) -> Result<F::Output, BlockOnError>
    where
        F: Future,
    {
        block_on::tao::block_on(self, future)
    }

    #[cfg(feature = "event")]
    fn block_on_with_events<F, Output>(&mut self, handler: F) -> Result<Output, BlockOnError>
    where
        F: AsyncFnOnce(event::Event) -> Output,
    {
        block_on::tao::block_on_with_events(self, handler)
    }
}

#[cfg(all(feature = "winit", not(any(target_os = "ios", target_arch = "wasm32"))))]
impl EventLoopBlockOn for winit::event_loop::EventLoop<()> {
    fn block_on<F>(&mut self, future: F) -> Result<F::Output, BlockOnError>
    where
        F: Future,
    {
        block_on::winit::block_on(self, future)
    }

    #[cfg(feature = "event")]
    fn block_on_with_events<F, Output>(&mut self, handler: F) -> Result<Output, BlockOnError>
    where
        F: AsyncFnOnce(event::Event) -> Output,
    {
        block_on::winit::block_on_with_events(self, handler)
    }
}

#[cfg(test)]
mod tests {
    use super::BlockOnError;

    #[test]
    fn block_on_errors_have_stable_display_messages() {
        assert_eq!(
            BlockOnError::EventLoopExited.to_string(),
            "event loop exited before the future completed"
        );
        assert_eq!(
            BlockOnError::Backend("unavailable".to_owned()).to_string(),
            "event loop error: unavailable"
        );
    }
}
