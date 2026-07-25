use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::executor::with_reactor;

// Timer kqueue idents are allocated from a high range so they can never collide
// with a file descriptor (which are small integers) in the reactor's waker map.
thread_local! {
    static NEXT_TIMER_IDENT: Cell<usize> = const { Cell::new(1_000_000) };
}

fn next_timer_ident() -> usize {
    NEXT_TIMER_IDENT.with(|c| {
        let v = c.get();
        c.set(v + 1);
        v
    })
}

pub struct TimerFuture {
    deadline: Instant,
    ident: usize,
    registered: bool,
}

impl TimerFuture {
    pub fn new(duration: Duration) -> Self {
        Self {
            deadline: Instant::now() + duration,
            ident: next_timer_ident(),
            registered: false,
        }
    }
}

impl Future for TimerFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut(); // TimerFuture is Unpin, so this is safe

        if Instant::now() >= this.deadline {
            return Poll::Ready(());
        }

        // Not yet due: arm a one-shot kernel timer and park until it fires.
        if !this.registered {
            let millis = (this.deadline - Instant::now()).as_millis();
            let waker = cx.waker().clone();
            let _ = with_reactor(|r| r.register_timer(this.ident, millis, waker));
            this.registered = true;
        }

        Poll::Pending
    }
}
