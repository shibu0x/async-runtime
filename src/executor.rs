use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::reactor::Reactor;
use crate::waker::{make_waker, ReadyQueue};

// A boxed, pinned future is our unit of work. `Output = ()` — top-level tasks
// run for their side effects.
type BoxFuture = Pin<Box<dyn Future<Output = ()>>>;

// Thread-locals give leaf futures (timers, sockets) access to the runtime without
// threading a `&mut Reactor` through every `poll` signature — the real
// `Future::poll` has no room for one. This is how tokio exposes its reactor too.
thread_local! {
    static REACTOR: RefCell<Option<Rc<RefCell<Reactor>>>> = const { RefCell::new(None) };
    static SPAWN_QUEUE: RefCell<Vec<BoxFuture>> = const { RefCell::new(Vec::new()) };
}

// Run `f` with mutable access to the current thread's reactor. Leaf futures call
// this from inside `poll` to register their fd/timer.
pub fn with_reactor<F, R>(f: F) -> R
where
    F: FnOnce(&mut Reactor) -> R,
{
    REACTOR.with(|slot| {
        let handle = slot.borrow();
        let rc = handle.as_ref().expect("no reactor installed on this thread");
        let mut reactor = rc.borrow_mut();
        f(&mut reactor)
    })
}

// Spawn a new task from *inside* another task (e.g. one echo task per accepted
// connection). It's queued and adopted by the executor on the next loop turn.
pub fn spawn<F: Future<Output = ()> + 'static>(fut: F) {
    SPAWN_QUEUE.with(|q| q.borrow_mut().push(Box::pin(fut)));
}

struct Task {
    future: BoxFuture,
    waker: Waker,
}

pub struct Executor {
    tasks: HashMap<usize, Task>,
    ready: ReadyQueue,
    reactor: Rc<RefCell<Reactor>>,
    next_id: usize,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    pub fn new() -> Self {
        let reactor = Rc::new(RefCell::new(Reactor::new()));
        // Install the reactor so `with_reactor` works from any future on this thread.
        REACTOR.with(|slot| *slot.borrow_mut() = Some(reactor.clone()));
        Self {
            tasks: HashMap::new(),
            ready: Rc::new(RefCell::new(VecDeque::new())),
            reactor,
            next_id: 0,
        }
    }

    // Register a top-level task with the executor.
    pub fn spawn<F: Future<Output = ()> + 'static>(&mut self, fut: F) {
        self.add_task(Box::pin(fut));
    }

    fn add_task(&mut self, future: BoxFuture) {
        let id = self.next_id;
        self.next_id += 1;
        // Each task gets its own waker carrying its id, so waking re-queues *it*.
        let waker = make_waker(id, self.ready.clone());
        self.tasks.insert(id, Task { future, waker });
        self.ready.borrow_mut().push_back(id);
    }

    // Adopt any tasks spawned via the free `spawn()` since the last check.
    fn drain_spawned(&mut self) {
        let new: Vec<BoxFuture> = SPAWN_QUEUE.with(|q| q.borrow_mut().drain(..).collect());
        for future in new {
            self.add_task(future);
        }
    }

    pub fn run(&mut self) {
        while !self.tasks.is_empty() {
            self.drain_spawned();

            // Take a snapshot of what's ready and poll each one.
            let batch: Vec<usize> = self.ready.borrow_mut().drain(..).collect();
            for id in batch {
                // Clone the task's waker to build its Context.
                let waker = match self.tasks.get(&id) {
                    Some(task) => task.waker.clone(),
                    None => continue, // task already completed this turn
                };
                let mut cx = Context::from_waker(&waker);

                let completed = match self.tasks.get_mut(&id) {
                    Some(task) => task.future.as_mut().poll(&mut cx).is_ready(),
                    None => false,
                };
                if completed {
                    self.tasks.remove(&id);
                }

                // A poll may have spawned children (e.g. accept -> echo).
                self.drain_spawned();
            }

            // Nothing ready but tasks remain -> everyone is parked on I/O or a
            // timer. Block in the kernel until an event wakes someone.
            if self.ready.borrow().is_empty() && !self.tasks.is_empty() {
                let _ = self.reactor.borrow_mut().wait();
            }
        }
    }
}

// A future that yields once: returns Pending after re-scheduling itself, then
// Ready on the next poll. This is how a CPU-bound task cooperatively hands the
// executor back control instead of hogging the thread.
pub struct Yield {
    yielded: bool,
}

pub fn yield_now() -> Yield {
    Yield { yielded: false }
}

impl Future for Yield {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref(); // re-queue ourselves for another turn
            Poll::Pending
        }
    }
}
