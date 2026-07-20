# async-runtime

A minimal async runtime for macOS, written from scratch in Rust with no dependencies beyond `libc`.

Built to understand how async actually works like futures, executors, and the syscall boundary . Every kernel call is a raw `unsafe` FFI call; nothing is wrapped by `mio`, `nix`, or `tokio`.

## What it does

Runs multiple futures concurrently on a single thread, sleeping in the kernel while they wait instead of busy-polling.

```rust
let mut reactor = Reactor::new();
let mut tasks: Vec<Box<dyn MyFuture>> = vec![
    Box::new(Timer::new(1, Duration::from_secs(2))),
    Box::new(Timer::new(2, Duration::from_secs(5))),
];
run(tasks, &mut reactor);
```

## How it works

- **`MyFuture`** : one method, `poll(&mut self, &mut Reactor) -> Poll`. A future
  computes its own readiness from its own state; nothing external sets it.
- **Executor** (`run`) : polls every task, drops the ones that return `Ready`,
  and blocks in the reactor when everything left is pending.
- **`Reactor`** : owns one `kqueue` fd for the process. `register()` adds an
  event to the kernel's watch list without blocking; `wait()` blocks in
  `kevent` until something fires and returns the idents that did.
- **`Timer`** : a future that completes at a wall-clock deadline. On first poll
  it registers an `EVFILT_TIMER` with the reactor, then returns `Pending`.

The kernel never runs any of this code. It stores an integer (`ident`) and a
duration, and hands the integer back when the timer fires. Translating that
integer into "poll this task" is the reactor's job.

## Why blocking matters

Three countdown tasks plus three timers, ~8.5s of real waiting:

| Executor | Wall clock | User CPU |
|---|---|---|
| Busy-poll loop | 10.22s | 9.97s |
| Blocking `kevent` | 8.55s | 0.00s |

Same work. The spinning version burns a full core to accomplish nothing.

## Status

Working: executor, kqueue reactor, timer futures, trait-object task list.

Not yet:
- **Wakers.** The executor sleeps whenever every task returns `Pending`, even
  if a task is pending only because it needs another turn rather than because
  it's waiting on an event. Such tasks currently advance only when an unrelated
  event wakes the loop.
- Waking only the task whose `ident` fired, instead of re-polling everything.
- `std::future::Future` support (`Pin`, `Context`, `async`/`await`).
- Sockets, multi-threading, task spawning, cancellation.

macOS/BSD only : `kqueue` has no Linux equivalent here. Linux would need `epoll`.
