//! Defect: loopback cleanup could remove a preexisting exemption or overwrite
//! another execution's entry while rebuilding the machine-wide table.
//! Root cause: exemption writes did not carry explicit, recoverable ownership.
//! Regression for the 2026-09-09 Windows loopback lease work item.
//!
//! Exercise the public portable lease API using an in-memory transaction. These
//! tests perform no Windows calls and do not prove native configuration writes,
//! native enumeration completeness, or cross-process mutex behavior.
//! Transaction-finish failures exercise the public operation/cleanup composition;
//! the memory transaction retains its own pending resource until explicitly retried.

use std::collections::VecDeque;

use klaus_sandbox_win::loopback::{
    ExemptionEntry, ExemptionTransaction, LoopbackAcquireError, LoopbackError, LoopbackLease,
    LoopbackOwnership,
};

const FIRST: &str = "S-1-15-2-1-2-3-4-5-6-7";
const SECOND: &str = "S-1-15-2-11-12-13-14-15-16-17";
const OTHER: &str = "S-1-15-2-21-22-23-24-25-26-27";

fn entry(sid: &str, attributes: u32) -> ExemptionEntry {
    ExemptionEntry {
        sid: sid.into(),
        attributes,
    }
}

fn read_error() -> LoopbackError {
    LoopbackError::Native {
        operation: "memory read",
        code: 1117,
    }
}

fn write_error() -> LoopbackError {
    LoopbackError::Native {
        operation: "memory replace",
        code: 5,
    }
}

fn finish_error() -> LoopbackError {
    LoopbackError::Native {
        operation: "memory finish",
        code: 288,
    }
}

fn operation_and_finish(operation: LoopbackError) -> LoopbackError {
    LoopbackError::OperationAndFinish {
        operation: Box::new(operation),
        finish: Box::new(finish_error()),
    }
}

#[derive(Default)]
enum WriteEffect {
    #[default]
    Commit,
    Reject,
    CommitThenError,
    CommitThenChange(Vec<ExemptionEntry>),
}

#[derive(Default)]
struct MemoryTransaction {
    entries: Vec<ExemptionEntry>,
    read_results: VecDeque<Result<(), LoopbackError>>,
    write_effects: VecDeque<WriteEffect>,
    read_count: usize,
    write_images: Vec<Vec<ExemptionEntry>>,
    finish_results: VecDeque<Result<(), LoopbackError>>,
    finish_count: usize,
    transaction_held: bool,
    calls: Vec<&'static str>,
}

impl MemoryTransaction {
    fn with_entries(entries: Vec<ExemptionEntry>) -> Self {
        Self {
            entries,
            ..Self::default()
        }
    }
}

impl ExemptionTransaction for MemoryTransaction {
    fn read(&mut self) -> Result<Vec<ExemptionEntry>, LoopbackError> {
        self.transaction_held = true;
        self.calls.push("read");
        self.read_count += 1;
        self.read_results.pop_front().unwrap_or(Ok(()))?;
        Ok(self.entries.clone())
    }

    fn replace(&mut self, entries: &[ExemptionEntry]) -> Result<(), LoopbackError> {
        self.transaction_held = true;
        self.calls.push("replace");
        self.write_images.push(entries.to_vec());
        match self.write_effects.pop_front().unwrap_or_default() {
            WriteEffect::Commit => self.entries = entries.to_vec(),
            WriteEffect::Reject => return Err(write_error()),
            WriteEffect::CommitThenError => {
                self.entries = entries.to_vec();
                return Err(write_error());
            }
            WriteEffect::CommitThenChange(changed) => self.entries = changed,
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), LoopbackError> {
        self.finish_count += 1;
        self.calls.push("finish");
        if self.transaction_held {
            self.finish_results.pop_front().unwrap_or(Ok(()))?;
            self.transaction_held = false;
        }
        Ok(())
    }
}

#[test]
fn borrowed_entries_are_never_written_or_revoked() {
    let original = vec![entry(OTHER, 7), entry(FIRST, 4), entry(FIRST, 0)];
    let mut transaction = MemoryTransaction::with_entries(original.clone());
    let mut lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
    assert_eq!(lease.ownership(), LoopbackOwnership::Borrowed);
    assert!(!lease.is_released());
    assert_eq!(transaction.read_count, 1);
    transaction.read_results.push_back(Err(read_error()));

    lease.release_with(&mut transaction).unwrap();
    lease.release_with(&mut transaction).unwrap();
    assert!(lease.is_released());
    assert_eq!(transaction.read_count, 1, "borrowed release must not read");
    assert!(transaction.write_images.is_empty());
    assert_eq!(transaction.entries, original);
}

#[test]
fn distinct_leases_preserve_each_other_in_both_release_orders() {
    for release_first_before_second in [true, false] {
        let original = vec![entry(OTHER, 0x8000_0004), entry(OTHER, 0), entry(OTHER, 0)];
        let mut transaction = MemoryTransaction::with_entries(original.clone());
        let mut first = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
        let mut second = LoopbackLease::acquire_with(&mut transaction, SECOND).unwrap();
        assert_eq!(first.ownership(), LoopbackOwnership::Added);
        assert_eq!(second.ownership(), LoopbackOwnership::Added);
        let mut both = original.clone();
        both.extend([entry(FIRST, 0), entry(SECOND, 0)]);
        assert_eq!(transaction.entries, both);

        let remaining = if release_first_before_second {
            first.release_with(&mut transaction).unwrap();
            SECOND
        } else {
            second.release_with(&mut transaction).unwrap();
            FIRST
        };
        let mut expected = original.clone();
        expected.push(entry(remaining, 0));
        assert_eq!(transaction.entries, expected);
        if release_first_before_second {
            second.release_with(&mut transaction).unwrap();
        } else {
            first.release_with(&mut transaction).unwrap();
        }
        assert_eq!(transaction.entries, original);
        assert!(first.is_released() && second.is_released());
    }
}

#[test]
fn release_preserves_current_foreign_entries_instead_of_restoring_an_old_table() {
    let mut transaction = MemoryTransaction::with_entries(vec![entry(OTHER, 1)]);
    let mut lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
    transaction.entries = vec![
        entry(SECOND, 9),
        entry(FIRST, 0),
        entry(OTHER, 0xFFFF_FFFF),
        entry(SECOND, 9),
    ];

    lease.release_with(&mut transaction).unwrap();
    assert_eq!(
        transaction.entries,
        vec![
            entry(SECOND, 9),
            entry(OTHER, 0xFFFF_FFFF),
            entry(SECOND, 9)
        ]
    );
}

#[test]
fn failed_or_incomplete_initial_reads_never_create_an_owner_or_write() {
    for error in [
        read_error(),
        LoopbackError::InvalidSnapshot("enumeration was truncated".into()),
        LoopbackError::LockAbandoned,
        LoopbackError::LockTimeout,
    ] {
        let mut transaction = MemoryTransaction::with_entries(vec![entry(OTHER, 7)]);
        transaction.read_results.push_back(Err(error.clone()));
        let (actual, recovery) = LoopbackLease::acquire_with(&mut transaction, FIRST)
            .unwrap_err()
            .into_parts();
        assert_eq!(actual, error);
        assert!(recovery.is_none());
        assert!(transaction.write_images.is_empty());
        assert_eq!(transaction.entries, vec![entry(OTHER, 7)]);
    }
    let (error, recovery) = LoopbackAcquireError::from(LoopbackError::LockTimeout).into_parts();
    assert_eq!(error, LoopbackError::LockTimeout);
    assert!(recovery.is_none());
    assert_eq!(error.native_code(), None);
}

#[test]
fn invalid_sid_text_is_not_used_to_replace_a_snapshot() {
    for invalid in ["", "not a SID", "S-1-15-2\0", " S-1-15-2-1"] {
        let mut transaction = MemoryTransaction::with_entries(vec![entry(OTHER, 7)]);
        let (error, recovery) = LoopbackLease::acquire_with(&mut transaction, invalid)
            .unwrap_err()
            .into_parts();
        assert!(matches!(error, LoopbackError::InvalidSnapshot(_)));
        assert!(recovery.is_none());
        assert_eq!(transaction.read_count, 0);
        assert!(transaction.write_images.is_empty());

        transaction.entries.push(entry(invalid, 0));
        let (error, recovery) = LoopbackLease::acquire_with(&mut transaction, FIRST)
            .unwrap_err()
            .into_parts();
        assert!(matches!(error, LoopbackError::InvalidSnapshot(_)));
        assert!(recovery.is_none());
        assert!(transaction.write_images.is_empty());
    }
}

#[test]
fn failed_add_write_never_claims_a_subsequently_present_entry() {
    for effect in [WriteEffect::Reject, WriteEffect::CommitThenError] {
        let mut transaction = MemoryTransaction::default();
        transaction.write_effects.push_back(effect);
        let (error, recovery) = LoopbackLease::acquire_with(&mut transaction, FIRST)
            .unwrap_err()
            .into_parts();
        assert_eq!(error, write_error());
        assert_eq!(error.native_code(), Some(5));
        let mut lease = recovery.expect("every attempted write retains an owner");
        assert_eq!(lease.ownership(), LoopbackOwnership::Uncertain);
        let record = lease.recovery_record();
        assert_eq!(record["addWriteSucceeded"], false);
        assert_eq!(record["lastWrite"]["operation"], "add");
        assert_eq!(record["lastWrite"]["expectedSnapshot"][0]["sid"], FIRST);

        transaction.entries = vec![entry(FIRST, 0)];
        assert_eq!(
            lease.release_with(&mut transaction),
            Err(LoopbackError::UncertainOwnership)
        );
        assert_eq!(transaction.write_images.len(), 1);
        transaction.entries.clear();
        lease.release_with(&mut transaction).unwrap();
        assert!(lease.is_released());
        assert_eq!(transaction.write_images.len(), 1);
    }
}

#[test]
fn successful_add_with_failed_readback_remains_recoverable() {
    let mut transaction = MemoryTransaction::with_entries(vec![entry(OTHER, 8)]);
    transaction.read_results.extend([Ok(()), Err(read_error())]);
    let (error, recovery) = LoopbackLease::acquire_with(&mut transaction, FIRST)
        .unwrap_err()
        .into_parts();
    assert_eq!(error, read_error());
    let mut lease = recovery.unwrap();
    assert_eq!(lease.ownership(), LoopbackOwnership::Uncertain);
    assert_eq!(lease.recovery_record()["addWriteSucceeded"], true);
    assert_eq!(transaction.entries, vec![entry(OTHER, 8), entry(FIRST, 0)]);

    transaction.entries.insert(0, entry(SECOND, 6));
    lease.release_with(&mut transaction).unwrap();
    assert!(lease.is_released());
    assert_eq!(transaction.entries, vec![entry(SECOND, 6), entry(OTHER, 8)]);
}

#[test]
fn conflicting_add_readback_does_not_report_added_or_rewrite_the_table() {
    let changed = vec![entry(FIRST, 0), entry(OTHER, 9)];
    let mut transaction = MemoryTransaction::with_entries(vec![entry(OTHER, 8)]);
    transaction
        .write_effects
        .push_back(WriteEffect::CommitThenChange(changed.clone()));
    let (error, recovery) = LoopbackLease::acquire_with(&mut transaction, FIRST)
        .unwrap_err()
        .into_parts();
    assert!(matches!(error, LoopbackError::Conflict(_)));
    let lease = recovery.unwrap();
    assert_eq!(lease.ownership(), LoopbackOwnership::Uncertain);
    assert_eq!(lease.recovery_record()["addWriteSucceeded"], true);
    assert_eq!(transaction.write_images.len(), 1);
    assert_eq!(transaction.entries, changed);
}

#[test]
fn changed_owned_attributes_duplicates_and_invalid_snapshots_are_not_written() {
    for changed in [
        vec![entry(OTHER, 8), entry(FIRST, 4)],
        vec![entry(FIRST, 0), entry(OTHER, 8), entry(FIRST, 0)],
        vec![entry(FIRST, 0), entry("", 0)],
    ] {
        let mut transaction = MemoryTransaction::with_entries(vec![entry(OTHER, 8)]);
        let mut lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
        transaction.entries = changed.clone();
        assert!(matches!(
            lease.release_with(&mut transaction),
            Err(LoopbackError::Conflict(_) | LoopbackError::InvalidSnapshot(_))
        ));
        assert!(!lease.is_released());
        assert_eq!(transaction.write_images.len(), 1);
        assert_eq!(transaction.entries, changed);
    }
}

#[test]
fn incomplete_release_read_retains_ownership_without_a_write() {
    let mut transaction = MemoryTransaction::default();
    let mut lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
    transaction
        .read_results
        .push_back(Err(LoopbackError::InvalidSnapshot(
            "enumeration ended before its declared count".into(),
        )));
    assert!(matches!(
        lease.release_with(&mut transaction),
        Err(LoopbackError::InvalidSnapshot(_))
    ));
    assert!(!lease.is_released());
    assert_eq!(transaction.write_images.len(), 1);
    assert_eq!(transaction.entries, vec![entry(FIRST, 0)]);

    lease.release_with(&mut transaction).unwrap();
    assert!(lease.is_released());
}

#[test]
fn failed_remove_write_can_retry_without_overwriting_a_peer_addition() {
    let mut transaction = MemoryTransaction::with_entries(vec![entry(OTHER, 8)]);
    let mut lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
    transaction.write_effects.push_back(WriteEffect::Reject);
    assert_eq!(lease.release_with(&mut transaction), Err(write_error()));
    assert_eq!(lease.ownership(), LoopbackOwnership::Uncertain);
    assert_eq!(lease.recovery_record()["removeWriteSucceeded"], false);

    transaction.entries.insert(0, entry(SECOND, 6));
    lease.release_with(&mut transaction).unwrap();
    assert!(lease.is_released());
    assert_eq!(transaction.entries, vec![entry(SECOND, 6), entry(OTHER, 8)]);
    assert_eq!(transaction.write_images.len(), 3);
}

#[test]
fn failed_remove_that_took_effect_converges_without_another_write() {
    let mut transaction = MemoryTransaction::default();
    let mut lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
    transaction
        .write_effects
        .push_back(WriteEffect::CommitThenError);
    assert_eq!(lease.release_with(&mut transaction), Err(write_error()));
    assert!(!lease.is_released());
    assert!(transaction.entries.is_empty());

    lease.release_with(&mut transaction).unwrap();
    assert!(lease.is_released());
    assert_eq!(transaction.write_images.len(), 2);
}

#[test]
fn successful_remove_with_failed_readback_cannot_delete_a_reappearing_entry() {
    let mut transaction = MemoryTransaction::default();
    let mut lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
    transaction.read_results.extend([Ok(()), Err(read_error())]);
    assert_eq!(lease.release_with(&mut transaction), Err(read_error()));
    assert!(!lease.is_released());
    assert_eq!(lease.recovery_record()["removeWriteSucceeded"], true);
    assert!(transaction.entries.is_empty());

    transaction.entries.push(entry(FIRST, 0));
    assert!(matches!(
        lease.release_with(&mut transaction),
        Err(LoopbackError::Conflict(_))
    ));
    assert_eq!(transaction.write_images.len(), 2);
    assert_eq!(transaction.entries, vec![entry(FIRST, 0)]);
    transaction.entries.clear();
    lease.release_with(&mut transaction).unwrap();
    assert!(lease.is_released());
}

#[test]
fn confirmed_release_never_touches_later_same_sid_entries() {
    let mut transaction = MemoryTransaction::default();
    let mut lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
    lease.release_with(&mut transaction).unwrap();
    assert!(lease.is_released());
    let reads = transaction.read_count;
    let writes = transaction.write_images.len();

    transaction.entries.push(entry(FIRST, 0));
    lease.release_with(&mut transaction).unwrap();
    assert_eq!(transaction.read_count, reads);
    assert_eq!(transaction.write_images.len(), writes);
    assert_eq!(transaction.entries, vec![entry(FIRST, 0)]);
}

#[test]
fn observed_removal_retires_ownership_even_when_foreign_entries_conflict() {
    let mut transaction = MemoryTransaction::with_entries(vec![entry(OTHER, 8)]);
    let mut lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
    transaction
        .write_effects
        .push_back(WriteEffect::CommitThenChange(vec![entry(OTHER, 9)]));
    assert!(matches!(
        lease.release_with(&mut transaction),
        Err(LoopbackError::Conflict(_))
    ));
    assert!(lease.is_released());
    transaction.entries.push(entry(FIRST, 0));
    lease.release_with(&mut transaction).unwrap();
    assert_eq!(transaction.write_images.len(), 2);
    assert_eq!(transaction.entries, vec![entry(OTHER, 9), entry(FIRST, 0)]);
}

#[test]
fn dropping_a_lease_does_not_modify_the_transaction() {
    let mut transaction = MemoryTransaction::default();
    let lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
    let reads = transaction.read_count;
    let writes = transaction.write_images.len();
    let finishes = transaction.finish_count;
    drop(lease);
    assert_eq!(transaction.entries, vec![entry(FIRST, 0)]);
    assert_eq!(transaction.read_count, reads);
    assert_eq!(transaction.write_images.len(), writes);
    assert_eq!(transaction.finish_count, finishes);
}

#[test]
fn successful_acquire_finish_failure_retains_the_added_lease() {
    let original = vec![entry(OTHER, 7)];
    let mut transaction = MemoryTransaction::with_entries(original.clone());
    transaction.finish_results.push_back(Err(finish_error()));
    let (error, recovery) = LoopbackLease::acquire_with(&mut transaction, FIRST)
        .unwrap_err()
        .into_parts();
    assert_eq!(error, finish_error());
    let mut lease = recovery.expect("a completed addition remains owned after finish fails");
    assert_eq!(lease.ownership(), LoopbackOwnership::Added);
    assert!(lease.has_pending_finish() && transaction.transaction_held);
    assert_eq!(transaction.entries, vec![entry(OTHER, 7), entry(FIRST, 0)]);
    assert_eq!(
        lease.recovery_record()["operationError"],
        serde_json::Value::Null
    );
    assert_eq!(
        lease.recovery_record()["finishError"],
        finish_error().to_string()
    );

    let first_attempt_calls = transaction.calls.len();
    lease.release_with(&mut transaction).unwrap();
    assert_eq!(
        &transaction.calls[first_attempt_calls..],
        &["finish", "read", "replace", "read", "finish"]
    );
    assert!(lease.is_released());
    assert!(!lease.has_pending_finish());
    assert!(!transaction.transaction_held);
    assert_eq!(transaction.entries, original);
}

#[test]
fn failed_initial_read_and_finish_preserve_both_errors_without_an_exemption_owner() {
    let original = vec![entry(OTHER, 7)];
    let mut transaction = MemoryTransaction::with_entries(original.clone());
    transaction.read_results.push_back(Err(read_error()));
    transaction.finish_results.push_back(Err(finish_error()));
    let (error, recovery) = LoopbackLease::acquire_with(&mut transaction, FIRST)
        .unwrap_err()
        .into_parts();
    assert_eq!(error, operation_and_finish(read_error()));
    assert_eq!(error.native_code(), Some(1117));
    assert!(
        recovery.is_none(),
        "a failed initial read established no exemption owner"
    );
    assert_eq!(transaction.calls, vec!["read", "finish"]);
    assert!(
        transaction.transaction_held,
        "the caller still owns the failed transaction"
    );
    assert!(transaction.write_images.is_empty());
    assert_eq!(transaction.entries, original);

    transaction.finish().unwrap();
    assert!(!transaction.transaction_held);
    assert_eq!(transaction.entries, original);
    assert!(transaction.write_images.is_empty());
}

#[test]
fn failed_write_and_finish_keep_the_uncertain_owner_and_original_error() {
    let mut transaction = MemoryTransaction::default();
    transaction
        .write_effects
        .push_back(WriteEffect::CommitThenError);
    transaction.finish_results.push_back(Err(finish_error()));
    let (error, recovery) = LoopbackLease::acquire_with(&mut transaction, FIRST)
        .unwrap_err()
        .into_parts();
    assert_eq!(error, operation_and_finish(write_error()));
    let mut lease = recovery.unwrap();
    assert_eq!(lease.ownership(), LoopbackOwnership::Uncertain);
    assert_eq!(lease.recovery_record()["addWriteSucceeded"], false);
    assert_eq!(
        lease.recovery_record()["operationError"],
        write_error().to_string()
    );
    assert!(lease.has_pending_finish() && transaction.transaction_held);

    assert_eq!(
        lease.release_with(&mut transaction),
        Err(LoopbackError::UncertainOwnership)
    );
    assert!(!lease.has_pending_finish());
    assert_eq!(transaction.entries, vec![entry(FIRST, 0)]);
    assert_eq!(
        transaction.write_images.len(),
        1,
        "finish success cannot authorize adoption"
    );
}

#[test]
fn confirmed_removal_waits_for_finish_without_deleting_a_reappearing_entry() {
    let mut transaction = MemoryTransaction::default();
    let mut lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
    transaction.finish_results.push_back(Err(finish_error()));
    assert_eq!(lease.release_with(&mut transaction), Err(finish_error()));
    assert_eq!(lease.ownership(), LoopbackOwnership::Released);
    assert!(
        !lease.is_released(),
        "pending transaction cleanup prevents a final receipt"
    );
    assert!(lease.has_pending_finish() && transaction.transaction_held);
    assert!(transaction.entries.is_empty());

    // An unrelated writer can ignore our lock. Confirmed removal has already
    // retired deletion authority, even if that SID reappears before unlock retry.
    transaction.entries.push(entry(FIRST, 0));
    let reads = transaction.read_count;
    let writes = transaction.write_images.len();
    let calls = transaction.calls.len();
    lease.release_with(&mut transaction).unwrap();
    assert_eq!(&transaction.calls[calls..], &["finish"]);
    assert_eq!(transaction.read_count, reads);
    assert_eq!(transaction.write_images.len(), writes);
    assert_eq!(transaction.entries, vec![entry(FIRST, 0)]);
    assert!(lease.is_released());
}

#[test]
fn repeated_finish_failure_retains_the_original_release_operation_error() {
    let mut transaction = MemoryTransaction::default();
    let mut lease = LoopbackLease::acquire_with(&mut transaction, FIRST).unwrap();
    transaction.read_results.push_back(Err(read_error()));
    transaction
        .finish_results
        .extend([Err(finish_error()), Err(finish_error())]);
    assert_eq!(
        lease.release_with(&mut transaction),
        Err(operation_and_finish(read_error()))
    );
    let calls = transaction.calls.len();
    assert_eq!(
        lease.release_with(&mut transaction),
        Err(operation_and_finish(read_error()))
    );
    assert_eq!(&transaction.calls[calls..], &["finish"]);
    assert_eq!(lease.ownership(), LoopbackOwnership::Added);
    assert!(lease.has_pending_finish() && transaction.transaction_held);
    assert_eq!(transaction.entries, vec![entry(FIRST, 0)]);

    lease.release_with(&mut transaction).unwrap();
    assert!(lease.is_released());
    assert!(transaction.entries.is_empty());
}

#[test]
fn borrowed_acquire_finish_retry_only_completes_the_owned_transaction() {
    let original = vec![entry(FIRST, 9), entry(OTHER, 7)];
    let mut transaction = MemoryTransaction::with_entries(original.clone());
    transaction.finish_results.push_back(Err(finish_error()));
    let (error, recovery) = LoopbackLease::acquire_with(&mut transaction, FIRST)
        .unwrap_err()
        .into_parts();
    assert_eq!(error, finish_error());
    let mut lease = recovery.unwrap();
    assert_eq!(lease.ownership(), LoopbackOwnership::Borrowed);
    let calls = transaction.calls.len();
    lease.release_with(&mut transaction).unwrap();
    assert_eq!(&transaction.calls[calls..], &["finish"]);
    assert!(lease.is_released());
    let calls = transaction.calls.len();
    lease.release_with(&mut transaction).unwrap();
    assert_eq!(
        transaction.calls.len(),
        calls,
        "completed borrowed release performs no operations"
    );
    assert_eq!(transaction.entries, original);
    assert!(transaction.write_images.is_empty());
}
