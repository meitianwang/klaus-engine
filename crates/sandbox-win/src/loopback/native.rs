//! Windows storage for the machine-wide loopback lease transaction.
//!
//! Every Klaus writer holds one cross-session mutex across read, merge, write,
//! and readback. The Windows API has no compare-and-swap operation: unrelated
//! software that does not take this mutex can still race with the transaction.

use std::io::Write as _;
use std::marker::PhantomData;
use std::ptr;
use std::rc::Rc;

use serde_json::{json, Value};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, LocalFree, ERROR_SUCCESS, HANDLE, HLOCAL, PSID, WAIT_ABANDONED,
    WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::NetworkManagement::WindowsFirewall::{
    NetworkIsolationGetAppContainerConfig, NetworkIsolationSetAppContainerConfig,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
    ConvertStringSidToSidW,
};
use windows_sys::Win32::Security::{IsValidSid, SECURITY_ATTRIBUTES, SID_AND_ATTRIBUTES};
use windows_sys::Win32::System::Memory::{GetProcessHeap, HeapFree};
use windows_sys::Win32::System::Threading::{
    CreateMutexExW, GetCurrentThreadId, ReleaseMutex, WaitForSingleObject, MUTEX_MODIFY_STATE,
};

use super::{
    ExemptionEntry, ExemptionTransaction, LoopbackAcquireError, LoopbackError, LoopbackLease,
    LoopbackOwnership,
};
use crate::identity::AppContainerSid;

const ENTRY_LIMIT: usize = 65_536;
const MUTEX_NAME: &str = "Global\\Klaus.AppContainerLoopbackConfig.v1";
const LOCK_TIMEOUT_MS: u32 = 5_000;
const SYNCHRONIZE: u32 = 0x0010_0000;

fn native_error(operation: &'static str) -> LoopbackError {
    LoopbackError::Native {
        operation,
        code: unsafe { GetLastError() },
    }
}

fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(Some(0)).collect()
}

/// Conversion APIs allocate independent LocalAlloc buffers, unlike the config API.
struct LocalAllocation(HLOCAL);

impl Drop for LocalAllocation {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { LocalFree(self.0) };
        }
    }
}

fn sid_text(sid: PSID) -> Result<String, LoopbackError> {
    if sid.is_null() || unsafe { IsValidSid(sid) } == 0 {
        return Err(LoopbackError::InvalidSnapshot("invalid or null SID".into()));
    }
    let mut text = ptr::null_mut();
    let ok = unsafe { ConvertSidToStringSidW(sid, &mut text) };
    let _allocation = LocalAllocation(text.cast());
    if ok == 0 {
        return Err(native_error("ConvertSidToStringSidW"));
    }
    if text.is_null() {
        return Err(LoopbackError::InvalidSnapshot("null SID string".into()));
    }
    let length = (0..256)
        .find(|index| unsafe { *text.add(*index) == 0 })
        .ok_or_else(|| LoopbackError::InvalidSnapshot("SID string exceeds its limit".into()))?;
    String::from_utf16(unsafe { std::slice::from_raw_parts(text, length) })
        .map_err(|error| LoopbackError::InvalidSnapshot(error.to_string()))
}

fn parse_sid(text: &str) -> Result<LocalAllocation, LoopbackError> {
    if text.len() > 256 || text.contains('\0') {
        return Err(LoopbackError::InvalidSnapshot(
            "invalid SID string length or terminator".into(),
        ));
    }
    let mut sid = ptr::null_mut();
    let ok = unsafe { ConvertStringSidToSidW(wide(text).as_ptr(), &mut sid) };
    let allocation = LocalAllocation(sid);
    if ok == 0 {
        return Err(native_error("ConvertStringSidToSidW"));
    }
    // Reject aliases and noncanonical forms before a replacement. Native reads
    // always return this canonical form, so a merge preserves SID identity.
    if sid_text(sid)? != text {
        return Err(LoopbackError::InvalidSnapshot(
            "noncanonical SID string".into(),
        ));
    }
    Ok(allocation)
}

/// Both each SID and the outer array use the process heap. Install the owner
/// before calling Get so that every return path releases any returned allocation.
/// https://learn.microsoft.com/en-us/windows/win32/api/netfw/nf-netfw-networkisolationgetappcontainerconfig
struct ExemptionArray {
    entries: *mut SID_AND_ATTRIBUTES,
    count: u32,
}

impl Drop for ExemptionArray {
    fn drop(&mut self) {
        if self.entries.is_null() {
            return;
        }
        unsafe {
            let heap = GetProcessHeap();
            for index in 0..self.count as usize {
                let sid = (*self.entries.add(index)).Sid;
                if !sid.is_null() {
                    HeapFree(heap, 0, sid);
                }
            }
            HeapFree(heap, 0, self.entries.cast());
        }
    }
}

/// Read one complete native snapshot without creating a lock or changing config.
/// A malformed entry rejects the entire snapshot; no partial list can be written.
pub fn read_configuration() -> Result<Vec<ExemptionEntry>, LoopbackError> {
    let mut array = ExemptionArray {
        entries: ptr::null_mut(),
        count: 0,
    };
    let code =
        unsafe { NetworkIsolationGetAppContainerConfig(&mut array.count, &mut array.entries) };
    if code != ERROR_SUCCESS {
        return Err(LoopbackError::Native {
            operation: "NetworkIsolationGetAppContainerConfig",
            code,
        });
    }
    if (array.count != 0 && array.entries.is_null()) || array.count as usize > ENTRY_LIMIT {
        return Err(LoopbackError::InvalidSnapshot(
            "invalid exemption array storage or length".into(),
        ));
    }
    (0..array.count as usize)
        .map(|index| {
            let entry = unsafe { &*array.entries.add(index) };
            Ok(ExemptionEntry {
                sid: sid_text(entry.Sid)?,
                attributes: entry.Attributes,
            })
        })
        .collect()
}

/// Thread-affine ownership matters: Windows mutex release must happen on the
/// acquiring thread. The marker prevents moving a live guard to another thread.
#[derive(Debug)]
struct ConfigurationGuard {
    handle: HANDLE,
    acquired: bool,
    thread_id: u32,
    _thread_affine: PhantomData<Rc<()>>,
}

impl ConfigurationGuard {
    fn open() -> Result<Self, LoopbackError> {
        // The object is shared across Klaus processes in different logon sessions.
        // Only wait/release rights are granted; this creates a private coordination
        // object and does not alter any existing kernel object's descriptor.
        let descriptor_text = wide("D:(A;;0x00100001;;;AU)");
        let mut descriptor = ptr::null_mut();
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                descriptor_text.as_ptr(),
                1,
                &mut descriptor,
                ptr::null_mut(),
            )
        };
        let _allocation = LocalAllocation(descriptor);
        if ok == 0 {
            return Err(native_error(
                "ConvertStringSecurityDescriptorToSecurityDescriptorW",
            ));
        }
        if descriptor.is_null() {
            return Err(LoopbackError::InvalidSnapshot(
                "null mutex security descriptor".into(),
            ));
        }
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor,
            bInheritHandle: 0,
        };
        let handle = unsafe {
            CreateMutexExW(
                &attributes,
                wide(MUTEX_NAME).as_ptr(),
                0,
                SYNCHRONIZE | MUTEX_MODIFY_STATE,
            )
        };
        if handle == 0 {
            return Err(native_error("CreateMutexExW"));
        }
        Ok(Self {
            handle,
            acquired: false,
            thread_id: unsafe { GetCurrentThreadId() },
            _thread_affine: PhantomData,
        })
    }

    fn wait(&mut self) -> Result<(), LoopbackError> {
        self.check_thread()?;
        match unsafe { WaitForSingleObject(self.handle, LOCK_TIMEOUT_MS) } {
            WAIT_OBJECT_0 => {
                self.acquired = true;
                Ok(())
            }
            WAIT_ABANDONED => {
                // Ownership is retained in NativeTransaction even when this
                // acquisition fails. Its explicit finish must release the mutex.
                self.acquired = true;
                Err(LoopbackError::LockAbandoned)
            }
            WAIT_TIMEOUT => Err(LoopbackError::LockTimeout),
            WAIT_FAILED => Err(native_error("WaitForSingleObject(loopback mutex)")),
            result => Err(LoopbackError::InvalidSnapshot(format!(
                "unexpected mutex wait result {result}"
            ))),
        }
    }

    fn check_thread(&self) -> Result<(), LoopbackError> {
        let current = unsafe { GetCurrentThreadId() };
        if current != self.thread_id {
            return Err(LoopbackError::Conflict(format!(
                "loopback transaction belongs to thread {}; retry attempted on thread {current}",
                self.thread_id
            )));
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), LoopbackError> {
        if self.handle == 0 {
            return Ok(());
        }
        self.check_thread()?;
        if self.acquired {
            if unsafe { ReleaseMutex(self.handle) } == 0 {
                // Keep both the handle and acquisition state for this same
                // thread to retry. Closing a handle is not a release receipt.
                return Err(native_error("ReleaseMutex(loopback transaction)"));
            }
            self.acquired = false;
        }
        if unsafe { CloseHandle(self.handle) } == 0 {
            return Err(native_error("CloseHandle(loopback transaction)"));
        }
        self.handle = 0;
        Ok(())
    }
}

impl Drop for ConfigurationGuard {
    fn drop(&mut self) {
        if self.handle != 0 {
            // The caller discarded an unfinished transaction. Preserve the
            // native owner until this process exits; Drop cannot report failure
            // or stand in for an explicit retry on the original thread.
            let _ = writeln!(
                std::io::stderr().lock(),
                "KLAUS_LOOPBACK_TRANSACTION_RETAINED mutex={MUTEX_NAME} handle={} thread_id={} acquired={}",
                self.handle, self.thread_id, self.acquired
            );
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct NativeTransaction {
    guard: Option<ConfigurationGuard>,
    lock_error: Option<LoopbackError>,
}

impl NativeTransaction {
    fn lock(&mut self) -> Result<(), LoopbackError> {
        if self.guard.is_none() {
            // Install the handle owner before waiting: WAIT_ABANDONED acquires
            // the mutex while reporting an operation error.
            let guard = self.guard.insert(ConfigurationGuard::open()?);
            if let Err(error) = guard.wait() {
                self.lock_error = Some(error.clone());
                return Err(error);
            }
        }
        if let Some(error) = &self.lock_error {
            return Err(error.clone());
        }
        Ok(())
    }

    fn is_pending(&self) -> bool {
        self.guard.is_some()
    }

    pub(super) fn recovery_record(&self) -> Value {
        json!({
            "mutexName": MUTEX_NAME,
            "handle": self.guard.as_ref().map(|guard| guard.handle),
            "threadId": self.guard.as_ref().map(|guard| guard.thread_id),
            "acquired": self.guard.as_ref().is_some_and(|guard| guard.acquired),
            "acquisitionError": self.lock_error.as_ref().map(ToString::to_string),
        })
    }
}

impl ExemptionTransaction for NativeTransaction {
    fn read(&mut self) -> Result<Vec<ExemptionEntry>, LoopbackError> {
        self.lock()?;
        read_configuration()
    }

    fn replace(&mut self, entries: &[ExemptionEntry]) -> Result<(), LoopbackError> {
        self.lock()?;
        if entries.len() > ENTRY_LIMIT {
            return Err(LoopbackError::InvalidSnapshot(
                "exemption count exceeds its limit".into(),
            ));
        }
        let sids = entries
            .iter()
            .map(|entry| parse_sid(&entry.sid))
            .collect::<Result<Vec<_>, _>>()?;
        let native = entries
            .iter()
            .zip(&sids)
            .map(|(entry, sid)| SID_AND_ATTRIBUTES {
                Sid: sid.0,
                Attributes: entry.attributes,
            })
            .collect::<Vec<_>>();
        let code = unsafe {
            NetworkIsolationSetAppContainerConfig(
                native.len() as u32,
                if native.is_empty() {
                    ptr::null()
                } else {
                    native.as_ptr()
                },
            )
        };
        if code != ERROR_SUCCESS {
            return Err(LoopbackError::Native {
                operation: "NetworkIsolationSetAppContainerConfig",
                code,
            });
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), LoopbackError> {
        if let Some(guard) = &mut self.guard {
            guard.finish()?;
            self.guard = None;
        }
        self.lock_error = None;
        Ok(())
    }
}

impl LoopbackLease {
    /// Merge this execution's SID under the shared Klaus writer lock.
    pub fn acquire(sid: &AppContainerSid) -> Result<Self, LoopbackAcquireError> {
        let sid = sid_text(sid.as_psid())?;
        let mut transaction = NativeTransaction::default();
        let outcome = Self::acquire_with(&mut transaction, &sid);
        if !transaction.is_pending() {
            return outcome;
        }
        let mut failure = match outcome {
            Err(failure) => failure,
            Ok(lease) => lease.acquire_error(LoopbackError::Conflict(
                "native transaction returned success with an unfinished guard".into(),
            )),
        };
        if failure.recovery.is_none() {
            let (operation, finish) = match &failure.error {
                LoopbackError::OperationAndFinish { operation, finish } => {
                    (Some((**operation).clone()), Some((**finish).clone()))
                }
                error => (Some(error.clone()), None),
            };
            // Initial read/lock failure did not establish an exemption owner.
            // This recovery value owns only the actual pending transaction;
            // absent snapshot evidence must not be represented as an empty table.
            failure.recovery = Some(Box::new(Self {
                entry: ExemptionEntry { sid, attributes: 0 },
                ownership: LoopbackOwnership::NotAcquired,
                initial_snapshot: None,
                last_observed_snapshot: None,
                last_write: None,
                add_write_succeeded: false,
                remove_write_succeeded: false,
                transaction_finish_pending: true,
                last_operation_error: operation,
                last_finish_error: finish,
                pending_transaction: None,
            }));
        }
        if let Some(owner) = &mut failure.recovery {
            owner.transaction_finish_pending = true;
            owner.pending_transaction = Some(transaction);
        }
        Err(failure)
    }

    /// Explicit, retryable cleanup on the original thread. Borrowed/released
    /// leases without a pending transaction make no native calls. A retained
    /// transaction is finished before any new read/write operation can begin.
    pub fn release(&mut self) -> Result<(), LoopbackError> {
        if self.transaction_finish_pending && self.pending_transaction.is_none() {
            return Err(LoopbackError::Conflict(
                "retry the caller-owned transaction through release_with on that same transaction"
                    .into(),
            ));
        }
        if let Some(transaction) = &mut self.pending_transaction {
            if let Err(error) = transaction.finish() {
                return Err(self.finish_retry_error(error));
            }
            self.pending_transaction = None;
            self.transaction_finish_pending = false;
        }
        let mut transaction = NativeTransaction::default();
        let outcome = self.release_with(&mut transaction);
        if transaction.is_pending() {
            self.transaction_finish_pending = true;
            self.pending_transaction = Some(transaction);
        }
        outcome
    }
}
