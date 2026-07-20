use std::time::{Duration, Instant};

use crate::{
    executor::{MyFuture, Poll},
    reactor::Reactor,
};

pub struct Timer {
    pub id: usize,
    pub deadline: Instant,
    pub registered: bool,
}

impl Timer {
    pub fn new(id: usize, duration: Duration) -> Self {
        Self {
            id,
            deadline: Instant::now() + duration,
            registered: false,
        }
    }
}

impl MyFuture for Timer {
    fn poll(&mut self, reactor: &mut Reactor) -> Poll {
        if Instant::now() >= self.deadline {
            println!("timer {} fired.", self.id);
            return Poll::Ready;
        }

        if !self.registered {
            let millis = (self.deadline - Instant::now()).as_millis();
            let _ = reactor.register(self.id, millis);
            self.registered = true;
        }

        Poll::Pending
    }
}
