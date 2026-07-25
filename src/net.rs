use std::future::Future;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::executor::{spawn, with_reactor};

// ---------------------------------------------------------------------------
// Listener
// ---------------------------------------------------------------------------

pub struct AsyncTcpListener {
    listener: TcpListener,
    fd: usize,
}

impl AsyncTcpListener {
    pub fn bind(addr: &str) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let fd = listener.as_raw_fd() as usize;
        Ok(Self { listener, fd })
    }

    // Returns a future that resolves to the next incoming connection.
    pub fn accept(&self) -> Accept<'_> {
        Accept {
            listener: &self.listener,
            fd: self.fd,
            registered: false,
        }
    }
}

pub struct Accept<'a> {
    listener: &'a TcpListener,
    fd: usize,
    registered: bool,
}

impl Future for Accept<'_> {
    type Output = std::io::Result<AsyncTcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.listener.accept() {
            Ok((stream, addr)) => {
                println!("accepted : {}", addr);
                Poll::Ready(Ok(AsyncTcpStream::new(stream)))
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                this.arm(cx);
                Poll::Pending
            }
            Err(e) => {
                // Transient errors (e.g. ECONNABORTED): log and re-arm rather
                // than tearing the listener down.
                println!("accept error : {}", e);
                this.arm(cx);
                Poll::Pending
            }
        }
    }
}

impl Accept<'_> {
    fn arm(&mut self, cx: &mut Context<'_>) {
        if !self.registered {
            let waker = cx.waker().clone();
            let _ = with_reactor(|r| r.register_read(self.fd, waker));
            self.registered = true;
        }
    }
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

pub struct AsyncTcpStream {
    stream: TcpStream,
    fd: usize,
}

impl AsyncTcpStream {
    pub fn new(stream: TcpStream) -> Self {
        let _ = stream.set_nonblocking(true);
        let fd = stream.as_raw_fd() as usize;
        Self { stream, fd }
    }

    // Returns a future that resolves once `buf` has been filled with some bytes
    // (or 0 bytes, meaning the peer closed the connection).
    pub fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadFut<'a> {
        ReadFut {
            stream: self,
            buf,
            registered: false,
        }
    }

    // Best-effort synchronous echo write. Fine for the small payloads here; a
    // production runtime would model this as its own WouldBlock-aware future.
    pub fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(data)
    }
}

pub struct ReadFut<'a> {
    stream: &'a mut AsyncTcpStream,
    buf: &'a mut [u8],
    registered: bool,
}

impl Future for ReadFut<'_> {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.stream.stream.read(this.buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                if !this.registered {
                    let waker = cx.waker().clone();
                    let _ = with_reactor(|r| r.register_read(this.stream.fd, waker));
                    this.registered = true;
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// High-level async tasks
// ---------------------------------------------------------------------------

// Accept connections forever, spawning one echo task per client.
pub async fn accept_loop(addr: String) {
    let listener = AsyncTcpListener::bind(&addr).expect("failed to bind");
    println!("listening on {}", addr);
    loop {
        match listener.accept().await {
            Ok(stream) => spawn(echo(stream)),
            Err(e) => println!("accept failed: {}", e),
        }
    }
}

// Read from the client and echo every chunk back until it disconnects.
pub async fn echo(mut stream: AsyncTcpStream) {
    let mut buf = [0u8; 512];
    loop {
        let n = match stream.read(&mut buf).await {
            Ok(0) => {
                println!("connection closed");
                return;
            }
            Ok(n) => n,
            Err(e) => {
                println!("read error: {}", e);
                return;
            }
        };
        println!("read {} bytes : {:?}", n, String::from_utf8_lossy(&buf[..n]));
        let _ = stream.write_all(&buf[..n]);
    }
}
