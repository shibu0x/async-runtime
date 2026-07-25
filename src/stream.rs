use std::{
    io::{ErrorKind::WouldBlock, Read, Write},
    net::TcpStream,
    os::fd::AsRawFd,
};

use crate::executor::{MyFuture, Poll};

pub struct TcpStreamFuture {
    pub stream: TcpStream,
    pub id: usize,
    pub registered: bool,
}

impl TcpStreamFuture {
    pub fn new(stream: TcpStream) -> Self {
        let _ = stream.set_nonblocking(true);
        let id = stream.as_raw_fd() as usize;

        return Self {
            stream,
            id,
            registered: false,
        };
    }
}

impl MyFuture for TcpStreamFuture {
    fn poll(
        &mut self,
        reactor: &mut crate::reactor::Reactor,
        ready: &crate::executor::ReadyQueue,
    ) -> crate::executor::Poll {
        let mut buf = [0u8; 512];

        match self.stream.read(&mut buf) {
            Ok(0) => {
                println!("connections closed");
                return Poll::Ready;
            }

            Ok(n) => {
                println!(
                    "read {} bytes : {:?}",
                    n,
                    String::from_utf8_lossy(&buf[..n])
                );
                // Echo the bytes straight back to the client.
                let _ = self.stream.write_all(&buf[..n]);
                self.registered = false;
                ready.borrow_mut().push(self.id);
                return Poll::Pending;
            }

            Err(e) => {
                if e.kind() == WouldBlock {
                    if !self.registered {
                        let _ = reactor.register_read(self.id);
                        self.registered = true;
                    }
                    return Poll::Pending;
                } else {
                    println!("read error: {}", e);
                    return Poll::Ready;
                }
            }
        }
    }
}
