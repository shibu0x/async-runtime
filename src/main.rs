use crate::future::{Task, run};
use rand::Rng;

pub mod future;

pub struct TaskList {
    pub tasks: Vec<Task>,
}

pub fn main() {
    let mut rng = rand::thread_rng();
    let tasks = rng.gen_range(1..11);

    let mut task_list = TaskList { tasks:vec![] };

    for i in 1..=tasks {
        let new_task = Task {
            task_name : format!("Task {}",i),
            rounds: 0,
            target : rng.gen_range(1..11)
        };

        task_list.tasks.push(new_task);
    }

    let _ = run(task_list);
}