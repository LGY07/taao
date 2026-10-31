use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(feature = "event")]
use futures_lite::StreamExt;
use taao::EventLoopBlockOn;
#[cfg(feature = "event")]
use taao::event::Submitter;

#[cfg(feature = "event")]
async fn submit_value(submitter: &impl Submitter) -> Result<usize, taao::event::SubmitError> {
    #[cfg(feature = "tao")]
    return submitter.submit(|_, _| 42).await;
    #[cfg(feature = "winit")]
    return submitter.submit(|_| 42).await;
}

struct WakeFromThread(bool);

#[cfg(feature = "event")]
fn assert_pending_submissions_resolve_when_run_ends(event_loop: &mut impl EventLoopBlockOn) {
    let pending = event_loop
        .block_on_with_events(async |event| {
            let mut pending = Vec::new();
            // Fill the 64-operation queue and leave one submission waiting for capacity.
            // Backends may still execute queued operations during their final callbacks.
            for _ in 0..65 {
                let submitter = event.submitter();
                let mut submission = Box::pin(async move { submit_value(&submitter).await });
                assert!(
                    futures_lite::future::poll_once(submission.as_mut())
                        .await
                        .is_none()
                );
                pending.push(submission);
            }
            pending
        })
        .unwrap();
    for (index, submission) in pending.into_iter().enumerate() {
        let result = futures_lite::future::block_on(submission);
        if index == 64 {
            // This submission was waiting for capacity and was never enqueued.
            assert_eq!(result, Err(taao::event::SubmitError));
        } else {
            assert!(matches!(result, Ok(42) | Err(taao::event::SubmitError)));
        }
    }
}

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
    assert_pending_submissions_resolve_when_run_ends(&mut event_loop);

    #[cfg(feature = "event")]
    assert!(
        event_loop
            .block_on_with_events(async |mut event| {
                let caller_thread = std::thread::current().id();
                let submitter = event.submitter();
                let worker_submitter = submitter.clone();
                drop(submitter);
                let (result_sender, result_receiver) = async_channel::bounded(1);
                let worker = std::thread::spawn(move || {
                    let result = futures_lite::future::block_on(worker_submitter.submit(
                        move |target, _control_flow| {
                            (
                                tao::window::WindowBuilder::new().build(target).unwrap(),
                                std::thread::current().id() == caller_thread,
                            )
                        },
                    ));
                    result_sender.send_blocking(result).unwrap();
                });
                let receive_events = async move {
                    loop {
                        match event.next().await {
                            Some(taao::event::EventKind::Tao(tao::event::Event::WindowEvent {
                                ..
                            }))
                            | Some(taao::event::EventKind::TaoScaleFactorChanged { .. }) => {
                                break true;
                            }
                            Some(_) => continue,
                            None => break false,
                        }
                    }
                };
                let (result, got_window_event) =
                    futures_lite::future::zip(result_receiver.recv(), receive_events).await;
                let (window, ran_on_event_loop_thread) = result.unwrap().unwrap();
                worker.join().unwrap();
                drop(window);
                ran_on_event_loop_thread && got_window_event
            })
            .unwrap()
    );

    #[cfg(feature = "event")]
    {
        let submitter = event_loop
            .block_on_with_events(async |event| {
                let submitter = event.submitter();
                assert_eq!(submit_value(&event).await, Ok(42));
                drop(event);
                assert_eq!(submit_value(&submitter).await, Ok(42));
                submitter
            })
            .unwrap();
        assert_eq!(
            futures_lite::future::block_on(submitter.submit(|_, _| ())),
            Err(taao::event::SubmitError)
        );
    }

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
    assert_pending_submissions_resolve_when_run_ends(&mut event_loop);

    #[cfg(feature = "event")]
    assert!(
        event_loop
            .block_on_with_events(async |mut event| {
                let caller_thread = std::thread::current().id();
                let submitter = event.submitter();
                let worker_submitter = submitter.clone();
                drop(submitter);
                let (result_sender, result_receiver) = async_channel::bounded(1);
                let worker = std::thread::spawn(move || {
                    let result = futures_lite::future::block_on(worker_submitter.submit(
                        move |event_loop| {
                            (
                                event_loop
                                    .create_window(winit::window::Window::default_attributes())
                                    .unwrap(),
                                std::thread::current().id() == caller_thread,
                            )
                        },
                    ));
                    result_sender.send_blocking(result).unwrap();
                });
                let receive_events = async move {
                    loop {
                        match event.next().await {
                            Some(taao::event::EventKind::Winit(
                                winit::event::Event::WindowEvent { .. },
                            )) => break true,
                            Some(_) => continue,
                            None => break false,
                        }
                    }
                };
                let (result, got_window_event) =
                    futures_lite::future::zip(result_receiver.recv(), receive_events).await;
                let (window, ran_on_event_loop_thread) = result.unwrap().unwrap();
                worker.join().unwrap();
                drop(window);
                ran_on_event_loop_thread && got_window_event
            })
            .unwrap()
    );

    #[cfg(feature = "event")]
    {
        let submitter = event_loop
            .block_on_with_events(async |event| {
                let submitter = event.submitter();
                assert_eq!(submit_value(&event).await, Ok(42));
                drop(event);
                assert_eq!(submit_value(&submitter).await, Ok(42));
                submitter
            })
            .unwrap();
        assert_eq!(
            futures_lite::future::block_on(submitter.submit(|_| ())),
            Err(taao::event::SubmitError)
        );
    }

    #[cfg(feature = "event")]
    assert_eq!(
        event_loop.block_on_with_events(async |event| {
            let _ = event.submit(|event_loop| event_loop.exit()).await;
            std::future::pending::<()>().await;
        }),
        Err(taao::BlockOnError::EventLoopExited)
    );
}
