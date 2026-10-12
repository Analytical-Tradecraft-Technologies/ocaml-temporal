//! SIGPIPE containment for bridge-owned I/O (#1041).
//!
//! A Rust *executable* ignores SIGPIPE because Rust's startup code sets its
//! disposition to `SIG_IGN` before `main`. That code never runs when this
//! crate is a static library linked into an OCaml executable, and OCaml native
//! programs keep the default action, which terminates the process. Rust std's
//! scalar socket writes pass `MSG_NOSIGNAL`, but vectored writes are a plain
//! `writev`, and h2, hyper and tokio-rustls all write vectored. On Linux a
//! write to a socket the peer has reset therefore raises SIGPIPE and kills the
//! worker or client with status 141. macOS sockets created by std and mio set
//! `SO_NOSIGPIPE`, and Windows has no SIGPIPE.
//!
//! The bridge does **not** change the process-wide disposition: that is the
//! application's setting, and `SIG_IGN` would also be inherited by every
//! child process the application later executes. Instead, a SIGPIPE raised by
//! a write is directed at the writing thread, so blocking it in that thread's
//! signal mask is enough for the write to fail with `EPIPE` and be reported
//! through the ordinary error path:
//!
//! - Threads the bridge owns block SIGPIPE for their whole life with
//!   [`block_current_thread`]: every Tokio worker and blocking-pool thread of
//!   Core's runtime (through Core's `lang_on_thread_start` hook), the runtime
//!   cleanup threads, and the Core log writer. A signal left pending on such a
//!   thread is never delivered and is harmless.
//! - A thread the application owns (the OCaml supervisor Domain) enters the
//!   bridge through the shared ABI wrapper, which holds a [`CallerGuard`] for
//!   the duration of the call. `block_on` can drive connection setup, such as
//!   a TLS handshake, on that thread. The guard blocks SIGPIPE, consumes one
//!   that became pending during the call, and restores the caller's exact
//!   mask, so the application observes no change. Threads that Core spawns
//!   during a guarded call inherit the blocked mask.
//!
//! A process-directed SIGPIPE (for example `kill -PIPE`) that arrives while
//! every unblocked thread is busy elsewhere can be the one a guard consumes;
//! this matches the containment used by libcurl and Git and is preferable to
//! letting an internal `EPIPE` terminate the process.

/// Blocks SIGPIPE in the calling thread's signal mask for the rest of the
/// thread's life.
///
/// Only call this on a thread the bridge (or Core's Tokio runtime) owns; the
/// mask of an application thread is restored by [`CallerGuard`] instead.
/// Failure of `pthread_sigmask` (only possible with an invalid argument) is
/// ignored: the thread then keeps the process default, as before #1041.
#[cfg(unix)]
pub fn block_current_thread() {
    let set = sigpipe_set();
    // SAFETY: `set` is an initialized signal set and a null old-mask pointer
    // is permitted.
    unsafe {
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}

/// Windows has no SIGPIPE; nothing to block.
#[cfg(not(unix))]
pub fn block_current_thread() {}

/// Whether SIGPIPE is blocked in the calling thread's signal mask. Exposed so
/// integration tests can check the threads the bridge creates; always `false`
/// on platforms without SIGPIPE.
#[doc(hidden)]
#[cfg(unix)]
pub fn blocked_in_current_thread() -> bool {
    let mut current = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: A null new-mask pointer only queries; `current` is writable.
    let rc =
        unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, std::ptr::null(), current.as_mut_ptr()) };
    // SAFETY: A successful query initialized `current`.
    rc == 0 && unsafe { libc::sigismember(current.as_ptr(), libc::SIGPIPE) } == 1
}

/// See the Unix variant.
#[doc(hidden)]
#[cfg(not(unix))]
pub fn blocked_in_current_thread() -> bool {
    false
}

/// Whether SIGPIPE is pending for the calling thread or the process. Always
/// `false` on platforms without SIGPIPE.
#[doc(hidden)]
#[cfg(unix)]
pub fn pending() -> bool {
    let mut set = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: `set` is writable; success initializes it.
    let rc = unsafe { libc::sigpending(set.as_mut_ptr()) };
    // SAFETY: A successful call initialized `set`.
    rc == 0 && unsafe { libc::sigismember(set.as_ptr(), libc::SIGPIPE) } == 1
}

/// See the Unix variant.
#[doc(hidden)]
#[cfg(not(unix))]
pub fn pending() -> bool {
    false
}

/// A signal set containing only SIGPIPE.
#[cfg(unix)]
fn sigpipe_set() -> libc::sigset_t {
    let mut set = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: `sigemptyset` initializes the set and `sigaddset` adds a valid
    // signal number to it; neither can fail for these arguments.
    unsafe {
        libc::sigemptyset(set.as_mut_ptr());
        libc::sigaddset(set.as_mut_ptr(), libc::SIGPIPE);
        set.assume_init()
    }
}

/// Removes one pending SIGPIPE without delivering it.
///
/// Linux uses a zero-timeout `sigtimedwait` so the call cannot block even if
/// another thread accepted the signal after [`pending`] reported it. Other
/// Unix platforms lack `sigtimedwait`; there `sigwait` returns immediately
/// because the signal was just observed pending and is blocked here. (On
/// macOS, the other supported Unix, bridge sockets never raise SIGPIPE.)
#[cfg(unix)]
fn consume_pending() {
    let set = sigpipe_set();
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let timeout = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `set` and `timeout` are initialized; a null info pointer
        // is permitted.
        unsafe {
            libc::sigtimedwait(&set, std::ptr::null_mut(), &timeout);
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let mut signal: libc::c_int = 0;
        // SAFETY: `set` is initialized and `signal` is writable.
        unsafe {
            libc::sigwait(&set, &mut signal);
        }
    }
}

/// Scoped SIGPIPE block for one bridge call on an application-owned thread.
///
/// [`CallerGuard::enter`] blocks SIGPIPE and remembers the caller's previous
/// mask. Dropping the guard consumes a SIGPIPE that became pending during the
/// call, then restores that exact mask. A caller that already blocked SIGPIPE
/// is left entirely alone, including any signal it has pending. The guard
/// must be dropped on the thread that created it, because a saved mask is
/// meaningless on another thread; the guard is `!Send` to enforce that.
#[must_use = "SIGPIPE is unblocked again when the guard is dropped"]
pub struct CallerGuard {
    /// The caller's mask to restore, or `None` when nothing was changed.
    #[cfg(unix)]
    previous: Option<libc::sigset_t>,
    /// Whether SIGPIPE was already pending on entry; such a signal belongs
    /// to someone else and is not consumed.
    #[cfg(unix)]
    was_pending: bool,
    /// Keeps the guard `!Send` so it is always dropped on its own thread.
    _thread_bound: std::marker::PhantomData<*const ()>,
}

impl CallerGuard {
    /// Blocks SIGPIPE on the calling thread until the guard is dropped.
    #[cfg(unix)]
    pub fn enter() -> Self {
        let set = sigpipe_set();
        let mut previous = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
        // SAFETY: `set` is initialized and `previous` is writable.
        let rc = unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &set, previous.as_mut_ptr()) };
        let previous = if rc == 0 {
            // SAFETY: A successful call initialized the old mask.
            let previous = unsafe { previous.assume_init() };
            // SAFETY: `previous` is initialized.
            let already_blocked = unsafe { libc::sigismember(&previous, libc::SIGPIPE) } == 1;
            (!already_blocked).then_some(previous)
        } else {
            None
        };
        Self {
            was_pending: previous.is_some() && pending(),
            previous,
            _thread_bound: std::marker::PhantomData,
        }
    }

    /// No SIGPIPE exists on this platform; the guard does nothing.
    #[cfg(not(unix))]
    pub fn enter() -> Self {
        Self {
            _thread_bound: std::marker::PhantomData,
        }
    }
}

impl Drop for CallerGuard {
    /// Consumes a SIGPIPE raised during the call, then restores the caller's
    /// mask. Consuming first matters: unblocking with the signal pending
    /// would deliver it immediately with the default (terminating) action.
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(previous) = self.previous.take() {
            if !self.was_pending && pending() {
                consume_pending();
            }
            // SAFETY: `previous` is the initialized mask saved on entry.
            unsafe {
                libc::pthread_sigmask(libc::SIG_SETMASK, &previous, std::ptr::null_mut());
            }
        }
    }
}

/// Runs `operation` under a [`CallerGuard`]. Integration tests use this to
/// exercise the guard exactly as the ABI wrapper does.
#[doc(hidden)]
pub fn contain<T>(operation: impl FnOnce() -> T) -> T {
    let _guard = CallerGuard::enter();
    operation()
}
