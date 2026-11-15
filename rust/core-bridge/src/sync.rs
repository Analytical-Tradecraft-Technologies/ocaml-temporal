//! Crate-private synchronisation helpers shared by the bridge modules.

use std::sync::{Mutex, MutexGuard, PoisonError};

/// Locks `mutex`, recovering the guard if a previous holder panicked.
///
/// Every bridge mutex locked through here guards state whose critical
/// sections cannot leave it logically inconsistent part-way (a single
/// assignment, a counter or map edit, or an `Option` swap), so the data a
/// poisoned lock exposes is as valid as after any completed section. Each
/// caller module states why that holds for its own state. Recovering is also
/// what keeps the bridge's panic containment useful: a panic caught at the
/// ABI boundary must not turn every later lock into a second panic, because
/// shutdown, disposal and awaiting callers still need the state to finish
/// deterministically.
///
/// Poisoning is the only error [`Mutex::lock`] reports, so this never fails
/// and blocks exactly as `lock` does.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
