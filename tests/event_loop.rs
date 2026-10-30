use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(feature = "event")]
use futures_lite::StreamExt;
use taao::EventLoopBlockOn;

struct WakeFromThread(bool);

impl Future for WakeFromThread {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(42)
        } else {
            self.0 = true;
            let waker = context.waker().clone();
            drop(std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(50));
                waker.wake();
            }));
            Poll::Pending
        }
    }
}

#[cfg(all(feature = "tao", any(target_os = "linux", target_os = "windows")))]
fn tao_event_loop() -> tao::event_loop::EventLoop<()> {
    #[cfg(target_os = "windows")]
    {
        use tao::platform::windows::EventLoopBuilderExtWindows;

        let mut builder = tao::event_loop::EventLoopBuilder::new();
        builder.with_any_thread(true);
        builder.build()
    }
    #[cfg(target_os = "linux")]
    {
        use tao::platform::unix::EventLoopBuilderExtUnix;

        let mut builder = tao::event_loop::EventLoopBuilder::new();
        builder.with_any_thread(true);
        builder.build()
    }
}

#[cfg(all(feature = "winit", any(target_os = "linux", target_os = "windows")))]
fn winit_event_loop() -> winit::event_loop::EventLoop<()> {
    #[cfg(target_os = "windows")]
    {
        use winit::platform::windows::EventLoopBuilderExtWindows;

        let mut builder = winit::event_loop::EventLoop::builder();
        builder.with_any_thread(true);
        builder.build().unwrap()
    }
    #[cfg(target_os = "linux")]
    {
        use winit::platform::x11::EventLoopBuilderExtX11;

        let mut builder = winit::event_loop::EventLoop::builder();
        builder.with_x11().with_any_thread(true);
        builder.build().unwrap()
    }
}

#[cfg(all(feature = "tao", any(target_os = "linux", target_os = "windows")))]
#[test]
fn tao_event_loop_wakes_futures_and_runs_submitted_operations() {
    let mut event_loop = tao_event_loop();

    assert_eq!(event_loop.block_on(WakeFromThread(false)).unwrap(), 42);

    #[cfg(feature = "event")]
    assert!(
        event_loop
            .block_on_with_events(async |mut event| {
                let caller_thread = std::thread::current().id();
                let (window, ran_on_event_loop_thread) = event
                    .submit(move |target, _control_flow| {
                        (
                            tao::window::WindowBuilder::new().build(target).unwrap(),
                            std::thread::current().id() == caller_thread,
                        )
                    })
                    .await
                    .unwrap();
                let got_window_event = loop {
                    match event.next().await {
                        Some(taao::event::EventKind::Tao(tao::event::Event::WindowEvent {
                            ..
                        }))
                        | Some(taao::event::EventKind::TaoScaleFactorChanged { .. }) => break true,
                        Some(_) => continue,
                        None => break false,
                    }
                };
                drop(window);
                ran_on_event_loop_thread && got_window_event
            })
            .unwrap()
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

#[cfg(all(feature = "winit", any(target_os = "linux", target_os = "windows")))]
#[test]
fn winit_event_loop_wakes_futures_and_runs_submitted_operations() {
    let mut event_loop = winit_event_loop();

    assert_eq!(event_loop.block_on(WakeFromThread(false)).unwrap(), 42);

    #[cfg(feature = "event")]
    assert!(
        event_loop
            .block_on_with_events(async |mut event| {
                let caller_thread = std::thread::current().id();
                let (window, ran_on_event_loop_thread) = event
                    .submit(move |event_loop| {
                        (
                            event_loop
                                .create_window(winit::window::Window::default_attributes())
                                .unwrap(),
                            std::thread::current().id() == caller_thread,
                        )
                    })
                    .await
                    .unwrap();
                let got_window_event = loop {
                    match event.next().await {
                        Some(taao::event::EventKind::Winit(winit::event::Event::WindowEvent {
                            ..
                        })) => break true,
                        Some(_) => continue,
                        None => break false,
                    }
                };
                drop(window);
                ran_on_event_loop_thread && got_window_event
            })
            .unwrap()
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
