use std::time::Duration;

use rand::Rng;

use crate::executor::{Executor, yield_now};
use crate::net::accept_loop;
use crate::timer::TimerFuture;

pub mod executor;
pub mod net;
pub mod reactor;
pub mod timer;
pub mod waker;

// A CPU-bound task that cooperatively yields between rounds instead of hogging
// the executor thread.
async fn compute(name: String, target: u64) {
    let mut rounds = 0;
    while rounds < target {
        println!("{} is not completed, {} rounds done", name, rounds);
        rounds += 1;
        yield_now().await;
    }
    println!("{} is completed after {} rounds", name, rounds);
}

// Sleeps `secs` seconds by awaiting a real kqueue timer, then prints.
async fn sleeper(id: usize, secs: u64) {
    TimerFuture::new(Duration::from_secs(secs)).await;
    println!("timer {} fired.", id);
}

fn main() {
    let mut rng = rand::thread_rng();
    let n = rng.gen_range(1..11);

    let mut executor = Executor::new();

    // The TCP echo server runs forever, so the runtime never exits on its own.
    executor.spawn(accept_loop("127.0.0.1:8080".to_string()));

    for i in 1..=n {
        executor.spawn(compute(format!("Task {}", i + 100), rng.gen_range(1..11)));
        executor.spawn(sleeper((i + 1000) as usize, rng.gen_range(1..11)));
    }

    executor.run();
}
