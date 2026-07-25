use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::executor::Executor;
use crate::timer::TimerFuture;

// A timer that ONLY checks the clock — no reactor, no waker registration. A
// naive executor has no way to park on it, so it must spin-poll (burning CPU)
// until the deadline passes. This is the "wrong" way.
struct SpinTimer {
    deadline: Instant,
}

impl Future for SpinTimer {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

// Cumulative user+system CPU time this process has consumed so far. Taking a
// delta around each phase isolates that phase's CPU cost.
fn process_cpu_time() -> Duration {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
    }
    let secs = (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as u64;
    let micros = (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as u64;
    Duration::from_secs(secs) + Duration::from_micros(micros)
}

pub fn run() {
    let deadlines_secs = [1u64, 2, 3];
    let longest = deadlines_secs.iter().max().copied().unwrap_or(0);
    println!(
        "workload: {} timers, longest {}s of waiting\n",
        deadlines_secs.len(),
        longest
    );

    // ---- naive busy-poll executor: never sleeps, re-polls in a tight loop ----
    let (busy_wall, busy_cpu) = {
        let mut futs: Vec<Pin<Box<SpinTimer>>> = deadlines_secs
            .iter()
            .map(|s| {
                Box::pin(SpinTimer {
                    deadline: Instant::now() + Duration::from_secs(*s),
                })
            })
            .collect();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let cpu0 = process_cpu_time();
        let wall0 = Instant::now();
        while !futs.is_empty() {
            // Poll every pending future each pass; drop the ones that finish.
            futs.retain_mut(|f| f.as_mut().poll(&mut cx).is_pending());
        }
        (wall0.elapsed(), process_cpu_time() - cpu0)
    };

    // ---- real runtime: parks in the kernel via kqueue + wakers ----
    let (block_wall, block_cpu) = {
        let mut executor = Executor::new(); // installs the reactor for this thread
        for secs in deadlines_secs {
            executor.spawn(async move {
                TimerFuture::new(Duration::from_secs(secs)).await;
            });
        }
        let cpu0 = process_cpu_time();
        let wall0 = Instant::now();
        executor.run();
        (wall0.elapsed(), process_cpu_time() - cpu0)
    };

    // ---- report ----
    let row = |name: &str, wall: Duration, cpu: Duration| {
        let cores = cpu.as_secs_f64() / wall.as_secs_f64().max(1e-9);
        println!(
            "{:<22} wall {:>6.2}s   cpu {:>6.2}s   ({:.2} cores)",
            name,
            wall.as_secs_f64(),
            cpu.as_secs_f64(),
            cores
        );
    };

    println!("{:<22} {:>11}   {:>10}", "executor", "wall", "cpu");
    row("busy-poll (naive)", busy_wall, busy_cpu);
    row("blocking (kqueue)", block_wall, block_cpu);
    println!("\nSame work, same wall time — the busy-poll version burns a full core doing nothing.");
}
