use std::{cell::RefCell, collections::HashMap, net::TcpStream, rc::Rc, time::Duration, vec};

use crate::{
    executor::{MyFuture, Task, run},
    listener::TcpListenerFuture,
    reactor::Reactor,
    timer::Timer,
};
use rand::Rng;

pub mod executor;
pub mod listener;
pub mod reactor;
pub mod timer;

pub struct TaskList {
    pub pending_tasks: HashMap<usize, Box<dyn MyFuture>>,
    pub ready_tasks: Rc<RefCell<Vec<usize>>>,
}

pub type Connections = Rc<RefCell<Vec<TcpStream>>>;

pub fn main() {
    let mut rng = rand::thread_rng();
    let tasks = rng.gen_range(1..11);
    let connections: Connections = Rc::new(RefCell::new(vec![]));

    let mut reactor = Reactor::new();
    let mut task_list = TaskList {
        pending_tasks: HashMap::new(),
        ready_tasks: Rc::new(RefCell::new(vec![])),
    };

    let listener = TcpListenerFuture::new("127.0.0.1:8080", connections);
    let listener_id = listener.id;
    task_list
        .pending_tasks
        .insert(listener_id, Box::new(listener));
    task_list.ready_tasks.borrow_mut().push(listener_id);

    for i in 1..=tasks {
        let task_id = i+100;
        let time_id = (i + 1000) as usize;

        let new_task = {
            Task {
                id: task_id,
                task_name: format!("Task {}", task_id),
                rounds: 0,
                target: rng.gen_range(1..11),
            }
        };
        task_list
            .pending_tasks
            .insert(task_id as usize, Box::new(new_task));
        task_list.ready_tasks.borrow_mut().push(task_id as usize);

        let new_timer = {
            Timer {
                id: time_id,
                deadline: std::time::Instant::now() + Duration::from_secs(rng.gen_range(1..11)),
                registered: false,
            }
        };
        task_list.pending_tasks.insert(time_id, Box::new(new_timer));
        task_list.ready_tasks.borrow_mut().push(time_id);
    }

    let _ = run(task_list, &mut reactor);
}
