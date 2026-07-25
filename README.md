# async-runtime

A minimal async runtime for macOS, written from scratch in Rust with no dependencies beyond `libc`.

Built to understand how async actually works — `Future`, `Waker`, the executor, the reactor, and the syscall boundary. Every kernel call is a raw `unsafe` FFI call; nothing is wrapped by `mio`, `nix`, or `tokio`. It runs real `async`/`await` code on real `std::future::Future`s driven by a hand-rolled `Waker`.

## What it does

Runs multiple `async` tasks concurrently on a single thread — including a TCP echo server — sleeping in the kernel while they wait instead of busy-polling.

```rust
let mut executor = Executor::new();

// An async TCP echo server: one spawned task per connection.
executor.spawn(accept_loop("127.0.0.1:8080".to_string()));

// A task that sleeps on a real kqueue timer, then prints.
executor.spawn(async {
    TimerFuture::new(Duration::from_secs(2)).await;
    println!("2s elapsed");
});

executor.run();
```

## How it works

- **`Future` + `Waker`** — tasks are real `std::future::Future`s. A `Waker` is hand-built from a `RawWakerVTable` (`waker.rs`); its only job is `wake()` → push this task's id back onto the ready queue. That single line is what `async`/`await` ultimately bottoms out on.
- **Executor** (`executor.rs`) — drives `Pin<Box<dyn Future>>` with a `Context`, drops tasks that return `Ready`, and blocks in the reactor when everything left is `Pending`. Supports `spawn()` (e.g. one echo task per accepted socket) and `yield_now()` for cooperative CPU tasks.
- **Reactor** (`reactor.rs`) — owns one `kqueue` fd. Registers `EVFILT_TIMER` / `EVFILT_READ` interest without blocking, stores an `ident → Waker` map, and `wait()` blocks in `kevent` until events fire, then wakes exactly those tasks.
- **Leaf futures** — `TimerFuture` (`timer.rs`) and `Accept`/`ReadFut` (`net.rs`). On a would-block, each stashes its `Waker` with the reactor, returns `Pending`, and gets re-polled only when its kernel event fires.

The kernel never runs any of this code. It stores an integer (`ident`) and hands it back when the event fires. Translating that integer into "wake this task" is the reactor + waker's job.

## Why blocking matters

The whole point of a reactor: while tasks wait, the process should use **zero CPU**, not spin.

| Executor | User CPU while idle |
|---|---|
| Busy-poll loop (old design) | ~1 full core |
| Blocking `kevent` (this) | **0.0%** |

Same work — the spinning version burns a core to accomplish nothing.

## Run it

```sh
cargo run
# in another terminal:
printf 'hello' | nc 127.0.0.1 8080   # -> echoes "hello" back
```

## Status

Working: real `Future`/`Waker`, `async`/`await`, kqueue reactor, timer futures, async TCP accept + echo, task spawning, cooperative yielding.

Not yet:
- Async `write` (currently best-effort synchronous).
- `JoinHandle` / awaiting a task's return value.
- Combinators (`select`, `timeout`, `join`).
- Multi-threading (work-stealing executor), cancellation.

macOS/BSD only — `kqueue` is the backend. Linux would need `epoll`.
