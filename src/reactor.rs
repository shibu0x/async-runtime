pub fn reactor() -> std::io::Result<()> {
    unsafe {
        let kq = get_kq().expect("kqueue failed");

        let event = libc::kevent {
            ident: 1,
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_ADD | libc::EV_ENABLE,
            fflags: 0,
            data: 2000,
            udata: std::ptr::null_mut(),
        };
        let changes = [event];

        let register = libc::kevent(
            kq,
            changes.as_ptr(),
            changes.len() as i32,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
        );

        if register == -1 {
            return Err(std::io::Error::last_os_error());
        }

        println!("Listening for kq {}...", kq);

        let mut event_list: [libc::kevent; 1] = [std::mem::zeroed()];

        loop {
            let nevents = libc::kevent(
                kq,
                std::ptr::null(),
                0,
                event_list.as_mut_ptr(),
                event_list.len() as i32,
                std::ptr::null(),
            );

            if nevents == -1 {
                return Err(std::io::Error::last_os_error());
            }

            if nevents > 0 {
                println!("event amount fired : {}", nevents);
                let ident = event_list[0].ident;
                println!("event which is fired : {:?}", ident);
                break;
            }
        }

        Ok(())
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
