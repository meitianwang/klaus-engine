//! Explicit ownership of one entry in the machine loopback exemption table.
//!
//! The caller holds one exclusive transaction guard across each acquire or
//! release call. The production guard serializes participating Klaus processes
//! across Windows sessions; this interface cannot lock out third-party writers.
//! Reads must return complete snapshots with canonical SID strings, never a
//! lossy or partially decoded table. Existing entries are not normalized.
//!
//! A lease is not cloneable and has no Drop cleanup. Its caller must stop the
//! owned target before releasing the exemption, and retain recovery evidence
//! whenever cleanup cannot establish that the owned entry is gone.

#[cfg(windows)]
mod native;
#[cfg(windows)]
pub use native::read_configuration;

use serde::Serialize;
use serde_json::{json, Value};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ExemptionEntry {
    pub sid: String,
    pub attributes: u32,
}

/// A complete-table interface used while the caller holds its exclusive guard.
pub trait ExemptionTransaction {
    /// Reject incomplete enumeration, invalid SIDs, or failed decoding instead
    /// of returning an empty or shortened successful snapshot.
    fn read(&mut self) -> Result<Vec<ExemptionEntry>, LoopbackError>;

    /// Replace the table without changing entry order, attributes, or duplicates.
    /// An error does not establish whether the write took effect.
    fn replace(&mut self, entries: &[ExemptionEntry]) -> Result<(), LoopbackError>;

    /// Complete this transaction, including any owned lock release. An error
    /// retains its resources for an explicit retry on this same transaction.
    /// Implementations whose guard remains caller-owned may use the default.
    fn finish(&mut self) -> Result<(), LoopbackError> {
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LoopbackError {
    #[error("{operation} failed: Win32 error {code}")]
    Native { operation: &'static str, code: u32 },
    #[error("invalid loopback exemption snapshot: {0}")]
    InvalidSnapshot(String),
    #[error("the loopback exemption transaction lock was abandoned")]
    LockAbandoned,
    #[error("timed out acquiring the loopback exemption transaction lock")]
    LockTimeout,
    #[error("loopback exemption ownership conflict: {0}")]
    Conflict(String),
    #[error("the loopback exemption entry cannot be attributed to this lease")]
    UncertainOwnership,
    #[error("{operation}; transaction completion also failed: {finish}")]
    OperationAndFinish {
        #[source]
        operation: Box<LoopbackError>,
        finish: Box<LoopbackError>,
    },
}

impl LoopbackError {
    pub fn native_code(&self) -> Option<u32> {
        match self {
            Self::Native { code, .. } => Some(*code),
            Self::OperationAndFinish { operation, finish } => {
                operation.native_code().or_else(|| finish.native_code())
            }
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum LoopbackOwnership {
    /// Only transaction cleanup is owned; no complete initial snapshot or
    /// exemption ownership was established.
    NotAcquired,
    Borrowed,
    Added,
    Uncertain,
    Released,
}

#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct LoopbackAcquireError {
    #[source]
    error: LoopbackError,
    recovery: Option<Box<LoopbackLease>>,
}

impl LoopbackAcquireError {
    /// Retain any recovery value until explicit cleanup completes. A native
    /// initial-read failure may return NotAcquired: it owns only a pending
    /// transaction, without claiming that an exemption was added or observed.
    pub fn into_parts(self) -> (LoopbackError, Option<LoopbackLease>) {
        (self.error, self.recovery.map(|owner| *owner))
    }
}

impl From<LoopbackError> for LoopbackAcquireError {
    fn from(error: LoopbackError) -> Self {
        Self {
            error,
            recovery: None,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WriteAttempt {
    operation: &'static str,
    expected_snapshot: Vec<ExemptionEntry>,
    succeeded: bool,
}

#[derive(Debug)]
pub struct LoopbackLease {
    entry: ExemptionEntry,
    ownership: LoopbackOwnership,
    initial_snapshot: Option<Vec<ExemptionEntry>>,
    last_observed_snapshot: Option<Vec<ExemptionEntry>>,
    last_write: Option<WriteAttempt>,
    add_write_succeeded: bool,
    remove_write_succeeded: bool,
    transaction_finish_pending: bool,
    last_operation_error: Option<LoopbackError>,
    last_finish_error: Option<LoopbackError>,
    // A pending Windows mutex can only be retried on its acquiring thread.
    #[cfg(windows)]
    pending_transaction: Option<native::NativeTransaction>,
}

impl LoopbackLease {
    /// Borrow an existing exemption or append one entry with zero attributes.
    /// The native caller validates and canonicalizes `sid` before entering this
    /// portable algorithm so textual equality denotes native SID equality.
    /// Always finish the transaction, retaining both errors if operation and
    /// completion fail. The caller continues to own the supplied transaction.
    pub fn acquire_with(
        transaction: &mut impl ExemptionTransaction,
        sid: &str,
    ) -> Result<Self, LoopbackAcquireError> {
        let operation = Self::acquire_in_transaction(transaction, sid);
        let finish = transaction.finish();
        match (operation, finish) {
            (outcome, Ok(())) => outcome,
            (Ok(mut lease), Err(error)) => {
                lease.record_finish_failure(&error);
                Err(LoopbackAcquireError {
                    error,
                    recovery: Some(Box::new(lease)),
                })
            }
            (Err(mut failure), Err(finish)) => {
                if let Some(lease) = &mut failure.recovery {
                    lease.record_finish_failure(&finish);
                }
                failure.error = combine_errors(failure.error, finish);
                Err(failure)
            }
        }
    }

    fn acquire_in_transaction(
        transaction: &mut impl ExemptionTransaction,
        sid: &str,
    ) -> Result<Self, LoopbackAcquireError> {
        validate_sid_text(sid)?;
        let before = read_snapshot(transaction)?;
        let borrowed = before.iter().any(|entry| entry.sid == sid);
        let mut lease = Self {
            entry: ExemptionEntry {
                sid: sid.to_owned(),
                attributes: 0,
            },
            ownership: if borrowed {
                LoopbackOwnership::Borrowed
            } else {
                LoopbackOwnership::Uncertain
            },
            initial_snapshot: Some(before.clone()),
            last_observed_snapshot: Some(before.clone()),
            last_write: None,
            add_write_succeeded: false,
            remove_write_succeeded: false,
            transaction_finish_pending: false,
            last_operation_error: None,
            last_finish_error: None,
            #[cfg(windows)]
            pending_transaction: None,
        };
        if borrowed {
            return Ok(lease);
        }

        let mut expected = before;
        expected.push(lease.entry.clone());
        if let Err(error) = lease.write(transaction, "add", &expected) {
            return Err(lease.acquire_error(error));
        }
        lease.add_write_succeeded = true;
        let observed = match read_snapshot(transaction) {
            Ok(observed) => observed,
            Err(error) => return Err(lease.acquire_error(error)),
        };
        lease.last_observed_snapshot = Some(observed.clone());
        if observed != expected {
            return Err(lease.acquire_error(LoopbackError::Conflict(
                "the complete table did not match the successful addition".into(),
            )));
        }
        lease.ownership = LoopbackOwnership::Added;
        Ok(lease)
    }

    /// Retry an unfinished completion on the same supplied transaction before
    /// attempting further reads or writes. Completed borrowed/released leases
    /// perform no transaction operations.
    pub fn release_with(
        &mut self,
        transaction: &mut impl ExemptionTransaction,
    ) -> Result<(), LoopbackError> {
        #[cfg(windows)]
        if self.pending_transaction.is_some() {
            return Err(LoopbackError::Conflict(
                "retry the pending native transaction through release on its original thread"
                    .into(),
            ));
        }
        if self.transaction_finish_pending {
            if let Err(error) = transaction.finish() {
                return Err(self.finish_retry_error(error));
            }
            self.transaction_finish_pending = false;
        }
        if matches!(
            self.ownership,
            LoopbackOwnership::NotAcquired
                | LoopbackOwnership::Borrowed
                | LoopbackOwnership::Released
        ) {
            self.ownership = LoopbackOwnership::Released;
            return Ok(());
        }

        let operation = self.release_in_transaction(transaction);
        self.last_operation_error = operation.as_ref().err().cloned();
        match (operation, transaction.finish()) {
            (outcome, Ok(())) => outcome,
            (outcome, Err(finish)) => {
                self.record_finish_failure(&finish);
                Err(match outcome {
                    Ok(()) => finish,
                    Err(operation) => combine_errors(operation, finish),
                })
            }
        }
    }

    fn release_in_transaction(
        &mut self,
        transaction: &mut impl ExemptionTransaction,
    ) -> Result<(), LoopbackError> {
        let current = read_snapshot(transaction)?;
        self.last_observed_snapshot = Some(current.clone());
        let mut matches = current
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.sid == self.entry.sid);
        let Some((index, entry)) = matches.next() else {
            self.ownership = LoopbackOwnership::Released;
            return Ok(());
        };
        if matches.next().is_some() || entry != &self.entry {
            return Err(LoopbackError::Conflict(
                "the owned SID has changed attributes or duplicate entries".into(),
            ));
        }
        if self.remove_write_succeeded {
            // A successful removal write ends our authority to delete. A later
            // same-SID entry may belong to a third party, even when its fields
            // are identical and the preceding read-back failed.
            return Err(LoopbackError::Conflict(
                "the SID is present after a successful removal write".into(),
            ));
        }
        if !self.add_write_succeeded {
            return Err(LoopbackError::UncertainOwnership);
        }

        let mut expected = current;
        expected.remove(index);
        self.ownership = LoopbackOwnership::Uncertain;
        self.write(transaction, "remove", &expected)?;
        self.remove_write_succeeded = true;
        let observed = read_snapshot(transaction)?;
        self.last_observed_snapshot = Some(observed.clone());
        if !observed.iter().any(|entry| entry.sid == self.entry.sid) {
            // Retire ownership even if another part of the table changed. An
            // observed absence must never authorize deleting a later addition.
            self.ownership = LoopbackOwnership::Released;
        }
        if observed != expected {
            return Err(LoopbackError::Conflict(
                "the complete table did not match the successful removal".into(),
            ));
        }
        Ok(())
    }

    pub fn ownership(&self) -> LoopbackOwnership {
        self.ownership
    }

    /// A final cleanup receipt requires both retired exemption authority and
    /// successful completion of any retained transaction.
    pub fn is_released(&self) -> bool {
        self.ownership == LoopbackOwnership::Released && !self.transaction_finish_pending
    }

    pub fn has_pending_finish(&self) -> bool {
        self.transaction_finish_pending
    }

    /// Diagnostic evidence only; deserializing a record cannot recreate the
    /// process's ownership or establish that its target has stopped.
    pub fn recovery_record(&self) -> Value {
        #[cfg(windows)]
        let pending_native_transaction = self
            .pending_transaction
            .as_ref()
            .map(native::NativeTransaction::recovery_record);
        #[cfg(not(windows))]
        let pending_native_transaction: Option<Value> = None;
        json!({
            "version": 1,
            "entry": self.entry,
            "ownership": self.ownership,
            "released": self.is_released(),
            "addWriteSucceeded": self.add_write_succeeded,
            "removeWriteSucceeded": self.remove_write_succeeded,
            "initialSnapshot": self.initial_snapshot,
            "lastObservedSnapshot": self.last_observed_snapshot,
            "lastWrite": self.last_write,
            "transactionFinishPending": self.transaction_finish_pending,
            "operationError": self.last_operation_error.as_ref().map(ToString::to_string),
            "finishError": self.last_finish_error.as_ref().map(ToString::to_string),
            "pendingNativeTransaction": pending_native_transaction,
        })
    }

    fn write(
        &mut self,
        transaction: &mut impl ExemptionTransaction,
        operation: &'static str,
        expected: &[ExemptionEntry],
    ) -> Result<(), LoopbackError> {
        self.last_write = Some(WriteAttempt {
            operation,
            expected_snapshot: expected.to_vec(),
            succeeded: false,
        });
        transaction.replace(expected)?;
        if let Some(attempt) = &mut self.last_write {
            attempt.succeeded = true;
        }
        Ok(())
    }

    fn acquire_error(mut self, error: LoopbackError) -> LoopbackAcquireError {
        self.last_operation_error = Some(error.clone());
        LoopbackAcquireError {
            error,
            recovery: Some(Box::new(self)),
        }
    }

    fn record_finish_failure(&mut self, error: &LoopbackError) {
        self.transaction_finish_pending = true;
        self.last_finish_error = Some(error.clone());
    }

    fn finish_retry_error(&mut self, finish: LoopbackError) -> LoopbackError {
        self.record_finish_failure(&finish);
        match &self.last_operation_error {
            Some(operation) => combine_errors(operation.clone(), finish),
            None => finish,
        }
    }
}

fn combine_errors(operation: LoopbackError, finish: LoopbackError) -> LoopbackError {
    LoopbackError::OperationAndFinish {
        operation: Box::new(operation),
        finish: Box::new(finish),
    }
}

fn read_snapshot(
    transaction: &mut impl ExemptionTransaction,
) -> Result<Vec<ExemptionEntry>, LoopbackError> {
    let entries = transaction.read()?;
    for entry in &entries {
        validate_sid_text(&entry.sid)?;
    }
    Ok(entries)
}

fn validate_sid_text(sid: &str) -> Result<(), LoopbackError> {
    // Native readers validate SID structure and return canonical text. Reject
    // obvious lossy/invalid values here as well; this is not a SID parser or a
    // substitute for validating the native enumeration's bounds and pointers.
    if !sid.starts_with("S-1-")
        || sid.len() <= 4
        || !sid.is_ascii()
        || sid
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(LoopbackError::InvalidSnapshot(format!(
            "SID text is invalid: {sid:?}"
        )));
    }
    Ok(())
}
