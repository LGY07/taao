#[cfg(all(feature = "tao", feature = "winit"))]
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

/// Extension methods for running futures on the selected windowing backend's event loop.
///
/// Import this trait to call [`EventLoopBlockOn::block_on`] on an event loop.
///
/// This trait is unavailable on iOS and Web, where the selected backend cannot
/// synchronously return from its event loop.
#[cfg(any(
    all(feature = "tao", not(target_os = "ios")),
    all(feature = "winit", not(any(target_os = "ios", target_arch = "wasm32")))
))]
pub trait EventLoopBlockOn {
    /// Runs a future on the event loop and returns its output when it completes.
    ///
    /// The event loop must use `()` as its user event type, since user events are
    /// used to wake the future after it returns `Poll::Pending`.
    ///
    /// This method runs its own event loop driver and does not forward events.
    /// Returns [`BlockOnError::EventLoopExited`] if the loop exits first.
    fn block_on<F>(&mut self, future: F) -> Result<F::Output, BlockOnError>
    where
        F: Future;

    /// Runs an async handler with an event stream and event-loop submission handle.
    /// Returns [`BlockOnError::EventLoopExited`] if the loop exits before the handler completes.
    #[cfg(feature = "event")]
    fn block_on_with_events<F, Output>(&mut self, handler: F) -> Result<Output, BlockOnError>
    where
        F: AsyncFnOnce(event::Event) -> Output;
}

/// Why an event-loop-driven future did not produce its output.
#[derive(Debug, Eq, PartialEq)]
pub enum BlockOnError {
    /// The backend event loop exited before the future completed.
    EventLoopExited,
    /// The backend rejected the request to run its event loop.
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
