//! One Core slot pool shared by remote and local activity tasks.
//!
//! The OCaml worker decodes, invokes, and completes one activity callback
//! before admitting another, whether the task is a remote or a local activity.
//! Core normally keeps a separate permit pool per slot kind, so even one slot
//! of each kind lets the activity lane hold a remote task in the bridge queue
//! while a local callback runs. The server has already started that remote
//! task's timeouts, so it could expire before OCaml reaches it. Handing both
//! kinds permits from one semaphore bounds the outstanding activity work to
//! what the serial executor can actually run.
//!
//! Core reserves a permit before it polls and releases it when the task's
//! completion is reported, including `WillCompleteAsync` for an asynchronously
//! completed activity. Cancellation tasks for an outstanding activity need no
//! new permit, so a running activity can still observe cancellation.

use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use temporalio_sdk_core::{
    SlotKind, SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext, SlotSupplier,
    SlotSupplierPermit,
};
use tokio::sync::Semaphore;

/// A fixed-size supplier for one slot kind whose permits come from a
/// semaphore that other kinds may share.
///
/// Each permit owns its semaphore permit as Core user data, so dropping Core's
/// permit returns capacity to the shared pool exactly once.
pub(crate) struct SharedSlotSupplier<SK> {
    semaphore: Arc<Semaphore>,
    kind: PhantomData<fn() -> SK>,
}

impl<SK> SharedSlotSupplier<SK> {
    /// Creates a supplier that draws permits from `semaphore`.
    pub(crate) fn new(semaphore: Arc<Semaphore>) -> Self {
        Self {
            semaphore,
            kind: PhantomData,
        }
    }
}

// `SlotSupplier` is declared with `#[async_trait]`. Its desugared form is
// spelled out here rather than adding the `async-trait` crate as a direct
// dependency for one method.
impl<SK> SlotSupplier for SharedSlotSupplier<SK>
where
    SK: SlotKind + Send + Sync,
{
    type SlotKind = SK;

    fn reserve_slot<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _ctx: &'life1 dyn SlotReservationContext,
    ) -> Pin<Box<dyn Future<Output = SlotSupplierPermit> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let semaphore = self.semaphore.clone();
        Box::pin(async move {
            let permit = semaphore
                .acquire_owned()
                .await
                .expect("shared activity slot semaphore is never closed");
            SlotSupplierPermit::with_user_data(permit)
        })
    }

    fn try_reserve_slot(&self, _ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        self.semaphore
            .clone()
            .try_acquire_owned()
            .ok()
            .map(SlotSupplierPermit::with_user_data)
    }

    fn mark_slot_used(&self, _ctx: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {}

    fn release_slot(&self, _ctx: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {}

    fn available_slots(&self) -> Option<usize> {
        Some(self.semaphore.available_permits())
    }

    fn slot_supplier_kind(&self) -> String {
        "SharedFixedSize".to_owned()
    }
}
