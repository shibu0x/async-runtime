use std::time::Duration;

use crate::{executor::{MyFuture, Task, run}, reactor::Reactor, timer::Timer};
use rand::Rng;

pub mod executor;
pub mod reactor;
pub mod timer;

pub struct TaskList {
    pub tasks: Vec<Box<dyn MyFuture>>,
}

pub fn main() {
    let mut rng = rand::thread_rng();
    let tasks = rng.gen_range(1..11);

    let mut reactor = Reactor::new();
    let mut task_list = TaskList { tasks:vec![] };

    for i in 1..=tasks {
        let new_task = Task {
            id:i,
            task_name : format!("Task {}",i),
            rounds: 0,
            target : rng.gen_range(1..11)
        };

        task_list.tasks.push(Box::new(new_task));
        task_list.tasks.push(Box::new(Timer::new(i.try_into().unwrap(), Duration::from_secs(rng.gen_range(1..11)))));
    }

    let _ = run(task_list, &mut reactor);
}