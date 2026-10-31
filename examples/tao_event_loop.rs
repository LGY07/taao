use async_executor::Executor;
use futures_lite::StreamExt;
use futures_lite::io::AsyncWriteExt;
use taao::EventLoopBlockOn;
use taao::event::EventKind;
use tao::dpi::LogicalSize;
use tao::event::{Event as TaoEvent, WindowEvent};
use tao::event_loop::EventLoop;

fn main() -> Result<(), taao::BlockOnError> {
    let mut event_loop = EventLoop::new();

    event_loop.block_on_with_events(async |mut events| {
        // Window creation must happen on the event-loop thread, so submit it to the driver.
        let window = events
            .submit(|target, _control_flow| {
                tao::window::WindowBuilder::new()
                    .with_title("taao example")
                    .with_inner_size(LogicalSize::new(800.0, 600.0))
                    .build(target)
                    .expect("failed to create window")
            })
            .await
            .expect("event loop stopped before creating the window");
        let window_id = window.id();

        let executor = Executor::new();

        // Detached tasks run independently of the window event handling below.
        executor
            .spawn(async {
                println!("async task is running");
            })
            .detach();

        executor
            .spawn(async {
                let mut file = async_fs::File::create("test.log")
                    .await
                    .expect("failed to create test.log");
                file.write_all(b"test")
                    .await
                    .expect("failed to write test.log");
            })
            .detach();

        executor
            .run(async {
                // Keep processing events until this window receives a close request.
                while let Some(event) = events.next().await {
                    if matches!(
                        event,
                        EventKind::Tao(TaoEvent::WindowEvent {
                            window_id: id,
                            event: WindowEvent::CloseRequested,
                            ..
                        }) if id == window_id
                    ) {
                        // Change ControlFlow on the event-loop thread before exiting.
                        let _ = events
                            .submit(|_, control_flow| {
                                *control_flow = tao::event_loop::ControlFlow::Exit;
                            })
                            .await;
                        break;
                    }
                }
            })
            .await
    })
}
