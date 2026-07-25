use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::task::{RawWaker, RawWakerVTable, Waker};

// The executor's run-queue: a list of task ids that are ready to be polled.
pub type ReadyQueue = Rc<RefCell<VecDeque<usize>>>;

// The data every Waker carries: which task to wake, and where the run-queue is.
// When `wake()` is called, we push `id` onto `ready` — that is the entire job of
// a waker. `async`/`await` and every leaf future ultimately bottom out here.
struct WakerData {
    id: usize,
    ready: ReadyQueue,
}

// A `Waker` is a type-erased `*const ()` + a vtable of 4 fn pointers. We store an
// `Rc<WakerData>` as that pointer (via `Rc::into_raw`) and manage its refcount by
// hand in the vtable below.
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, wake_fn, wake_by_ref_fn, drop_fn);

// clone: hand out another owner of the same WakerData (refcount +1).
unsafe fn clone_fn(ptr: *const ()) -> RawWaker {
    let rc = unsafe { Rc::from_raw(ptr as *const WakerData) };
    let cloned = rc.clone(); // refcount +1
    std::mem::forget(rc); // don't run the original's destructor (we only borrowed it)
    RawWaker::new(Rc::into_raw(cloned) as *const (), &VTABLE)
}

// wake: schedule the task, then consume this waker (refcount -1).
unsafe fn wake_fn(ptr: *const ()) {
    let rc = unsafe { Rc::from_raw(ptr as *const WakerData) };
    rc.ready.borrow_mut().push_back(rc.id);
    // `rc` drops here -> refcount -1, since `wake(self)` consumes the waker.
}

// wake_by_ref: schedule the task without consuming the waker (refcount unchanged).
unsafe fn wake_by_ref_fn(ptr: *const ()) {
    let rc = unsafe { Rc::from_raw(ptr as *const WakerData) };
    rc.ready.borrow_mut().push_back(rc.id);
    std::mem::forget(rc); // caller still owns this waker
}

// drop: release one owner (refcount -1).
unsafe fn drop_fn(ptr: *const ()) {
    unsafe {
        drop(Rc::from_raw(ptr as *const WakerData));
    }
}

// Build a real `std::task::Waker` bound to task `id` and the run-queue `ready`.
pub fn make_waker(id: usize, ready: ReadyQueue) -> Waker {
    let data = Rc::new(WakerData { id, ready });
    let raw = RawWaker::new(Rc::into_raw(data) as *const (), &VTABLE);
    // Safe because our vtable upholds the RawWaker contract (balanced refcounts).
    unsafe { Waker::from_raw(raw) }
}
