use std::{io::ErrorKind, net::TcpListener, os::fd::AsRawFd};

use crate::{
    Connections,
    executor::{MyFuture, Poll},
};

pub struct TcpListenerFuture {
    pub listener: TcpListener,
    pub id: usize,
    pub registered: bool,
    pub connections: Connections,
}

impl TcpListenerFuture {
    pub fn new(addr: &str, connections: Connections) -> Self {
        let listener = TcpListener::bind(addr).expect("failed to bind");
        let _ = listener.set_nonblocking(true);
        let id = listener.as_raw_fd() as usize;

        return Self {
            listener,
            id,
            registered: false,
            connections,
        };
    }
}

impl MyFuture for TcpListenerFuture {
    fn poll(
        &mut self,
        reactor: &mut crate::reactor::Reactor,
        ready: &crate::executor::ReadyQueue,
    ) -> crate::executor::Poll {
        match self.listener.accept() {
            Ok((stream, addr)) => {
                println!("accepted : {}", addr);
                self.connections.borrow_mut().push(stream);
                self.registered = false;
                ready.borrow_mut().push(self.id as usize);
                return Poll::Pending;
            }
            Err(e) => {
                if e.kind() == ErrorKind::WouldBlock {
                    if !self.registered {
                        let _ = reactor.register_read(self.id as usize);
                        self.registered = true;
                    }
                    return Poll::Pending;
                } else {
                    println!("accept error : {}", e);
                    return Poll::Pending;
                }
            }
        }
    }
}
