pub struct Reactor {
    pub kq: i32,
}

impl Reactor {
    pub fn new() -> Self {
        let kq = get_kq().expect("kq registration failed");
        return Self { kq };
    }

    pub fn register(&mut self, ident: usize, millis: u128) -> std::io::Result<()> {
        let event = libc::kevent {
            ident: ident,
            filter: libc::EVFILT_TIMER,
            // EV_ONESHOT: fire exactly once, then the kernel removes the
            // registration itself. Without it EVFILT_TIMER is periodic and
            // keeps firing after the timer task is already gone.
            flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT,
            fflags: 0,
            data: millis as isize,
            udata: std::ptr::null_mut(),
        };
        let changes = [event];

        unsafe {
            let register = libc::kevent(
                self.kq,
                changes.as_ptr(),
                changes.len() as i32,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
            );
            if register == -1 {
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(())
    }

    pub fn register_read(&mut self, ident: usize) -> std::io::Result<()> {
        let event = libc::kevent {
            ident: ident,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        let changes = [event];

        unsafe {
            let register = libc::kevent(
                self.kq,
                changes.as_ptr(),
                changes.len() as i32,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
            );
            if register == -1 {
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(())
    }

    pub fn wait(&mut self) -> std::io::Result<Vec<usize>> {
        unsafe {
            let mut event_list: [libc::kevent; 32] = [std::mem::zeroed(); 32];

            let nevents = libc::kevent(
                self.kq,
                std::ptr::null(),
                0,
                event_list.as_mut_ptr(),
                event_list.len() as i32,
                std::ptr::null(),
            );

            if nevents == -1 {
                return Err(std::io::Error::last_os_error());
            }

            let mut idents = Vec::new();

            for event in event_list.iter().take(nevents as usize) {
                idents.push(event.ident as usize);
            }
            Ok(idents)
        }
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.kq);
        }
    }
}

//this function is used to register a queue in kernel so that we can register an event polling in it
//kqueue is a function using which we can directly interact with the kernel and can create an empty list
//   which can be used anytime by kevent to register and and blocking the events and polling
pub fn get_kq() -> std::io::Result<i32> {
    let fd = unsafe { libc::kqueue() };

    if fd == -1 {
        let error = std::io::Error::last_os_error();
        return Err(error);
    }

    Ok(fd)
}
