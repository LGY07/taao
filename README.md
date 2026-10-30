# taao

[![Crates.io Version](https://img.shields.io/crates/v/taao)](https://crates.io/crates/taao)
[![docs.rs](https://img.shields.io/docsrs/taao)](https://docs.rs/taao)
![Crates.io License](https://img.shields.io/crates/l/taao)
[![Badge showing this repository's Rust code analysis](https://ferris.love/badge/LGY07/taao?variant=mini)](https://ferris.love/LGY07/taao)

A crate for running Rust futures on a Tao or winit event-loop thread.

[简体中文](README.zh-CN.md)

## Features

- Run a `Future` on the selected backend's event-loop thread with the `EventLoopBlockOn` trait.
- Enable the optional `event` feature to receive backend events as a stream and submit operations to the event-loop
  thread.
- Select Tao or winit with Cargo features. Tao is the default backend; the two backends are mutually exclusive.
- Enable the optional `tracing` feature for wakeup, submitted-operation, event-overwrite, and error diagnostics.

## Installation

Use the default Tao backend:

```toml
[dependencies]
taao = "0.1"
```

Enable the event API with Tao:

```toml
[dependencies]
taao = { version = "0.1", features = ["event"] }
```

Select winit and enable the event API:

```toml
[dependencies]
taao = { version = "0.1", default-features = false, features = ["winit", "event"] }
```

Exactly one of `tao` and `winit` must be enabled. To collect `tracing` output, the application must install a tracing
subscriber.

## Running a Future

```rust,ignore
use tao::event_loop::EventLoop;
use taao::EventLoopBlockOn;

let mut event_loop = EventLoop::new();
let value = event_loop.block_on(async { 42 })?;
assert_eq!(value, 42);
```

`block_on` drives the event loop and returns when the future completes. When the future is woken, taao uses the
event-loop proxy to notify the event-loop thread to poll it again.

## Receiving Events and Submitting Operations

With the `event` feature enabled, `block_on_with_events` provides a stream of events. Use `futures_lite::StreamExt` to
read them; the caller is responsible for handling received events.

```rust,ignore
use futures_lite::StreamExt;
use taao::EventLoopBlockOn;

event_loop.block_on_with_events(async |mut event| {
    while let Some(kind) = event.next().await {
        // Handle backend events.
    }
})?;
```

The stream carries backend-specific event values; Tao and winit do not expose identical event sets. Internal user events
used to wake the future are consumed by the driver and are not included. Tao also omits loop-phase events such as
`NewEvents`, `MainEventsCleared`, `RedrawEventsCleared`, and `LoopDestroyed`.

`Event::submit` runs an operation on the event-loop thread and asynchronously returns its result. Tao passes an
`EventLoopWindowTarget` and mutable `ControlFlow` to the closure; winit passes an `ActiveEventLoop`. For example, create
a window with the Tao backend like this:

```rust,ignore
let window = event
    .submit(|target, _control_flow| {
        tao::window::WindowBuilder::new().build(target)
    })
    .await??;
```

The event stream has a bounded capacity of 256. When full, a new event replaces the oldest queued event.
`Event::dropped_events()` returns the number of events overwritten so far.

If the event loop exits before the future completes, `block_on` and `block_on_with_events` return
`BlockOnError::EventLoopExited`. If winit rejects the request to run its event loop, the method returns
`BlockOnError::Backend`. If the event loop has stopped before a submitted operation can complete, `Event::submit`
returns `SubmitError`.

## Features

| Feature   | Description                                 | Default |
|-----------|---------------------------------------------|---------|
| `tao`     | Use the Tao backend                         | Yes     |
| `winit`   | Use the winit backend                       | No      |
| `event`   | Enable the event stream and `Event::submit` | No      |
| `tracing` | Enable diagnostic logging                   | No      |

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
