use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(feature = "event")]
use futures_lite::StreamExt;
use taao::EventLoopBlockOn;

struct WakeOnce(bool);

impl Future for WakeOnce {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(42)
        } else {
            self.0 = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[cfg(all(feature = "tao", not(target_os = "ios")))]
#[test]
fn tao_event_loop_wakes_futures_and_runs_submitted_operations() {
    let mut event_loop = tao::event_loop::EventLoop::new();

    assert_eq!(event_loop.block_on(WakeOnce(false)).unwrap(), 42);

    #[cfg(feature = "event")]
    assert_eq!(
        event_loop
            .block_on_with_events(async |mut event| {
                let window = event
                    .submit(|target, _control_flow| {
                        tao::window::WindowBuilder::new().build(target).unwrap()
                    })
                    .await
                    .unwrap();
                let received = event.next().await;
                drop(window);
                received.is_some()
            })
            .unwrap(),
        true
    );

    #[cfg(feature = "event")]
    assert_eq!(
        event_loop.block_on_with_events(async |event| {
            let _ = event
                .submit(|_target, control_flow| {
                    *control_flow = tao::event_loop::ControlFlow::Exit;
                })
                .await;
            std::future::pending::<()>().await;
        }),
        Err(taao::BlockOnError::EventLoopExited)
    );
}

#[cfg(all(feature = "winit", not(any(target_os = "ios", target_arch = "wasm32"))))]
#[test]
fn winit_event_loop_wakes_futures_and_runs_submitted_operations() {
    let mut event_loop = winit::event_loop::EventLoop::new().unwrap();

    assert_eq!(event_loop.block_on(WakeOnce(false)).unwrap(), 42);

    #[cfg(feature = "event")]
    assert_eq!(
        event_loop
            .block_on_with_events(async |mut event| {
                let window = event
                    .submit(|event_loop| {
                        event_loop
                            .create_window(winit::window::Window::default_attributes())
                            .unwrap()
                    })
                    .await
                    .unwrap();
                let received = event.next().await;
                drop(window);
                received.is_some()
            })
            .unwrap(),
        true
    );

    #[cfg(feature = "event")]
    assert_eq!(
        event_loop.block_on_with_events(async |event| {
            let _ = event.submit(|event_loop| event_loop.exit()).await;
            std::future::pending::<()>().await;
        }),
        Err(taao::BlockOnError::EventLoopExited)
    );
}
