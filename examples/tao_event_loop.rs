use async_executor::Executor;
use futures_lite::io::AsyncWriteExt;
use futures_lite::StreamExt;
use taao::event::EventKind;
use taao::EventLoopBlockOn;
use tao::dpi::LogicalSize;
use tao::event::{Event as TaoEvent, WindowEvent};
use tao::event_loop::EventLoop;
use tao::window::WindowBuilder;

fn main() -> Result<(), taao::BlockOnError> {
    let mut event_loop = EventLoop::new();
    let window = WindowBuilder::new()
        .with_title("taao example")
        .with_inner_size(LogicalSize::new(800.0, 600.0))
        .build(&event_loop)
        .expect("failed to create window");
    let window_id = window.id();

    event_loop.block_on_with_events(async |mut events| {
        let executor = Executor::new();

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
                while let Some(event) = events.next().await {
                    if matches!(
                        event,
                        EventKind::Tao(TaoEvent::WindowEvent {
                            window_id: id,
                            event: WindowEvent::CloseRequested,
                            ..
                        }) if id == window_id
                    ) {
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
