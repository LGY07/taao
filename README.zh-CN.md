# taao

![Crates.io Version](https://img.shields.io/crates/v/taao) ![docs.rs](https://img.shields.io/docsrs/taao) ![Crates.io License](https://img.shields.io/crates/l/taao) [![Badge showing this repository's Rust code analysis](https://ferris.love/badge/LGY07/taao?variant=mini)](https://ferris.love/LGY07/taao)

一个将 Rust 异步任务接入 tao 或 winit 事件循环的库。

## 功能

- 通过 `EventLoopBlockOn` trait，在事件循环线程上运行一个 `Future`。
- 可选的 `event` feature 提供事件流和事件循环操作提交接口。
- 支持 `winit` 后端，需要关闭默认 feature 启用 `winit` feature。
- 可选的 `tracing` feature 输出唤醒、操作提交、事件覆盖和错误日志。

## 安装

默认启用 Tao：

```toml
[dependencies]
taao = "0.1"
```

启用 Tao 后端的事件 API：

```toml
[dependencies]
taao = { version = "0.1", features = ["event"] }
```

选择 winit 后端并启用事件 API：

```toml
[dependencies]
taao = { version = "0.1", default-features = false, features = ["winit", "event"] }
```

`tao` 与 `winit` 必须且只能选择一个。若要收集 `tracing` 日志，在依赖配置中加入 `tracing` feature，并在应用中安装 tracing subscriber。

## 运行 Future

```rust,ignore
use tao::event_loop::EventLoop;
use taao::EventLoopBlockOn;

let mut event_loop = EventLoop::new();
let value = event_loop.block_on(async { 42 })?;
assert_eq!(value, 42);
```

`block_on` 驱动事件循环，并在 Future 完成后返回。Future 被唤醒时，库会通过事件循环代理通知事件循环线程继续 poll。`block_on` 不会将后端事件转发给调用方。

## 接收事件和提交操作

启用 `event` feature 后，可用 `block_on_with_events` 接收事件流。事件通过 `futures_lite::StreamExt` 提供的接口读取；调用方负责处理接收到的事件。

```rust,ignore
use futures_lite::StreamExt;
use taao::EventLoopBlockOn;

event_loop.block_on_with_events(async |mut event| {
    while let Some(kind) = event.next().await {
        // 处理后端事件
    }
})?;
```

事件流包含后端各自提供的事件，Tao 和 winit 暴露的事件集合并不相同。驱动 Future 所用的内部 user event 不会传给调用方；Tao 还会过滤 `NewEvents`、`MainEventsCleared`、`RedrawEventsCleared` 和 `LoopDestroyed` 等循环阶段事件。

`Event::submit` 可在事件循环线程上执行操作并异步取得结果。Tao 的闭包接收 `EventLoopWindowTarget` 和可变的 `ControlFlow`；winit 的闭包接收 `ActiveEventLoop`。例如，Tao 后端可以这样创建窗口：

```rust,ignore
let window = event
    .submit(|target, _control_flow| {
        tao::window::WindowBuilder::new().build(target)
    })
    .await??;
```

事件流使用容量为 256 的有界队列。队列满时，新事件会覆盖最早的事件；`Event::dropped_events()` 返回累计覆盖数量。

若事件循环先于 Future 结束，`block_on` 或 `block_on_with_events` 返回 `BlockOnError::EventLoopExited`。winit 拒绝运行事件循环时会返回
`BlockOnError::Backend`。若提交操作时事件循环已经停止，`Event::submit` 返回 `SubmitError`。

## Feature

| Feature   | 说明                     | 默认 |
|-----------|------------------------|----|
| `tao`     | 使用 Tao 后端              | 是  |
| `winit`   | 使用 winit 后端            | 否  |
| `event`   | 启用事件流和 `Event::submit` | 否  |
| `tracing` | 启用诊断日志                 | 否  |

## 许可证

本项目采用 MIT 或 Apache-2.0 双许可证，可任选其一，详见 [MIT](LICENSE-MIT) 和 [Apache-2.0](LICENSE-APACHE)。
