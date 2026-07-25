use std::collections::HashMap;
use std::task::Waker;

pub struct Reactor {
    pub kq: i32,
    // kqueue ident -> the Waker to fire when that event becomes ready.
    // For timers the ident is a synthetic id (see timer.rs), for I/O it's the fd.
    wakers: HashMap<usize, Waker>,
}

impl Default for Reactor {
    fn default() -> Self {
        Self::new()
    }
}

impl Reactor {
    pub fn new() -> Self {
        let kq = get_kq().expect("kq registration failed");
        Self {
            kq,
            wakers: HashMap::new(),
        }
    }

    // Register a one-shot timer. When it fires, `waker` is woken.
    pub fn register_timer(&mut self, ident: usize, millis: u128, waker: Waker) -> std::io::Result<()> {
        let event = libc::kevent {
            ident,
            filter: libc::EVFILT_TIMER,
            // EV_ONESHOT: fire exactly once, then the kernel removes the
            // registration itself. Without it EVFILT_TIMER is periodic.
            flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT,
            fflags: 0,
            data: millis as isize,
            udata: std::ptr::null_mut(),
        };
        self.apply(event)?;
        self.wakers.insert(ident, waker);
        Ok(())
    }

    // Register interest in `ident` (an fd) becoming readable. When it does,
    // `waker` is woken. EV_CLEAR makes it edge-triggered.
    pub fn register_read(&mut self, ident: usize, waker: Waker) -> std::io::Result<()> {
        let event = libc::kevent {
            ident,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        self.apply(event)?;
        self.wakers.insert(ident, waker);
        Ok(())
    }

    // Submit a single kevent change to the kernel (no events read back).
    fn apply(&self, event: libc::kevent) -> std::io::Result<()> {
        let changes = [event];
        let rc = unsafe {
            libc::kevent(
                self.kq,
                changes.as_ptr(),
                changes.len() as i32,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
            )
        };
        if rc == -1 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    // Block until at least one registered event fires, then wake the
    // corresponding tasks. This is the runtime's only blocking point.
    pub fn wait(&mut self) -> std::io::Result<()> {
        let mut event_list: [libc::kevent; 32] = unsafe { std::mem::zeroed() };

        let nevents = unsafe {
            libc::kevent(
                self.kq,
                std::ptr::null(),
                0,
                event_list.as_mut_ptr(),
                event_list.len() as i32,
                std::ptr::null(),
            )
        };

        if nevents == -1 {
            return Err(std::io::Error::last_os_error());
        }

        for event in event_list.iter().take(nevents as usize) {
            let ident = event.ident;
            if let Some(waker) = self.wakers.remove(&ident) {
                waker.wake();
            }
        }
        Ok(())
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.kq);
        }
    }
}

// kqueue() asks the kernel for a fresh event queue and returns its fd. kevent()
// then registers/removes interest in events on it and blocks until they fire.
pub fn get_kq() -> std::io::Result<i32> {
    let fd = unsafe { libc::kqueue() };
    if fd == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(fd)
}
