use std::{cell::RefCell, rc::Rc};

use crate::{TaskList, reactor::Reactor};

//a poll enum and here we are storing if the task will be ready or not
pub enum Poll {
    Ready,
    Pending,
}

pub type ReadyQueue = Rc<RefCell<Vec<usize>>>;

//the struct future to store the value of per task
pub struct Task {
    pub id: u64,
    pub task_name: String,
    pub rounds: u64,
    pub target: u64,
}

//a trait to keep calling poll
pub trait MyFuture {
    fn poll(&mut self, reactor: &mut Reactor, ready: &ReadyQueue) -> Poll;
}

impl MyFuture for Task {
    fn poll(&mut self, _reactor: &mut Reactor, ready: &ReadyQueue) -> Poll {
        if self.rounds < self.target {
            println!(
                "{} is not completed, {} rounds done",
                self.task_name, self.rounds
            );
            self.rounds += 1;
            ready.borrow_mut().push(self.id as usize);
            Poll::Pending
        } else {
            println!(
                "{} is completed after {} rounds",
                self.task_name, self.rounds
            );
            Poll::Ready
        }
    }
}

pub fn run(mut task_list: TaskList, reactor: &mut Reactor) -> Result<(), ()> {
    while !task_list.pending_tasks.is_empty() {
        let batch: Vec<usize> = task_list.ready_tasks.borrow_mut().drain(..).collect();

        for id in batch {
            if let Some(task) = task_list.pending_tasks.get_mut(&id) {
                if let Poll::Ready = task.poll(reactor, &task_list.ready_tasks) {
                    task_list.pending_tasks.remove(&id);
                }
            }
        }

        if task_list.ready_tasks.borrow().is_empty() && !task_list.pending_tasks.is_empty() {
            if let Ok(fired) = reactor.wait() {
                task_list.ready_tasks.borrow_mut().extend(fired);
            }
        }
    }

    Ok(())
}
