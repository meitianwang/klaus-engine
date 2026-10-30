//! Private, process-coordinated byte storage for the Windows ACL ledger.
//!
//! This module never applies execution policy or changes an existing object's
//! security descriptor. A fixed per-user directory contains a stable lock file
//! and a replaceable ledger. New objects receive an explicit private descriptor;
//! existing objects must already have that descriptor. All participants must use
//! this protocol. A malicious process running as the same Windows user can bypass
//! the protocol and is not a security boundary this store can establish.

use std::ffi::{c_void, OsString};
use std::fmt;
use std::io::Write;
use std::mem;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::{Component, Path, PathBuf, Prefix};
use std::ptr;
use std::time::{Duration, Instant};

use windows_sys::core::GUID;
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, LocalFree, ERROR_ALREADY_EXISTS, ERROR_FILE_NOT_FOUND,
    ERROR_INSUFFICIENT_BUFFER, ERROR_LOCK_VIOLATION, ERROR_NO_TOKEN, GENERIC_READ, GENERIC_WRITE,
    HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::{
    AclSizeInformation, GetAce, GetAclInformation, GetLengthSid, GetSecurityDescriptorControl,
    GetTokenInformation, IsValidAcl, IsValidSid, TokenUser, ACCESS_ALLOWED_ACE, ACE_HEADER, ACL,
    ACL_SIZE_INFORMATION, DACL_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION,
    SECURITY_ATTRIBUTES, SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateDirectoryW, CreateFileW, FileIdInfo, FlushFileBuffers, GetFileInformationByHandle,
    GetFileInformationByHandleEx, GetFileSizeEx, LockFileEx, MoveFileExW, ReadFile, SetEndOfFile,
    SetFilePointerEx, UnlockFileEx, WriteFile, BY_HANDLE_FILE_INFORMATION, CREATE_NEW, DELETE,
    FILE_ALL_ACCESS, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT,
    FILE_BEGIN, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_FLAG_WRITE_THROUGH,
    FILE_ID_INFO, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, MOVEFILE_REPLACE_EXISTING,
    MOVEFILE_WRITE_THROUGH, OPEN_ALWAYS, OPEN_EXISTING, READ_CONTROL,
};
use windows_sys::Win32::System::Com::{CoCreateGuid, CoTaskMemFree};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetCurrentThread, OpenProcessToken, OpenThreadToken,
};
use windows_sys::Win32::System::IO::OVERLAPPED;
use windows_sys::Win32::UI::Shell::{FOLDERID_LocalAppData, SHGetKnownFolderPath};

const DIRECTORY: &str = "KlausSandboxAcl-v1";
const LEDGER: &str = "ledger.bin";
const LOCK: &str = "ledger.lock";
const MAX_BYTES: usize = 16 * 1024 * 1024;
const LOCK_TIMEOUT: Duration = Duration::from_secs(3);
const SHARED: u32 = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

#[derive(Debug, Clone)]
struct Failure {
    operation: &'static str,
    code: Option<u32>,
    detail: String,
}

/// An operation failure, optionally retaining an incompletely acquired store.
///
/// `read`, `write`, retry and `finish` retain resources in the borrowed Store.
/// Only `acquire` needs to transfer a partial owner through this error.
#[derive(Debug)]
pub struct StoreError {
    failure: Failure,
    recovery: Option<Box<Store>>,
}

impl StoreError {
    fn win(operation: &'static str) -> Self {
        Self::code(operation, unsafe { GetLastError() })
    }

    fn code(operation: &'static str, code: u32) -> Self {
        Self {
            failure: Failure {
                operation,
                code: Some(code),
                detail: String::new(),
            },
            recovery: None,
        }
    }

    fn invalid(operation: &'static str, detail: impl Into<String>) -> Self {
        Self {
            failure: Failure {
                operation,
                code: None,
                detail: detail.into(),
            },
            recovery: None,
        }
    }

    /// Extract the actual partial owner without discarding the original error.
    pub fn take_recovery(&mut self) -> Option<Store> {
        self.recovery.take().map(|store| *store)
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ACL store {} failed", self.failure.operation)?;
        if let Some(code) = self.failure.code {
            write!(f, ": Windows error {code}")?;
        }
        if !self.failure.detail.is_empty() {
            write!(f, ": {}", self.failure.detail)?;
        }
        Ok(())
    }
}

impl std::error::Error for StoreError {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileIdentity {
    volume: u64,
    id: [u8; 16],
}

impl FileIdentity {
    fn record(&self) -> serde_json::Value {
        serde_json::json!({"volume": self.volume, "fileId": self.id})
    }
}

#[derive(Debug)]
struct FileRecord {
    path: PathBuf,
    directory: bool,
    private: bool,
    identity: Option<FileIdentity>,
}

#[derive(Debug)]
struct RetainedHandle {
    raw: HANDLE,
    label: &'static str,
    file: Option<FileRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WritePhase {
    Staging,
    ReadyToRename,
    RenameAttempted,
    Renamed,
    Confirmed,
}

struct PendingWrite {
    temporary: usize,
    path: PathBuf,
    expected: Vec<u8>,
    before: Option<FileIdentity>,
    phase: WritePhase,
}

impl fmt::Debug for PendingWrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingWrite")
            .field("temporary", &self.temporary)
            .field("path", &self.path)
            .field("expected_bytes", &self.expected.len())
            .field("before", &self.before)
            .field("phase", &self.phase)
            .finish()
    }
}

/// A short transaction owning a file lock, pinned path components and open files.
///
/// A file lock belongs to its retained handle, so this owner may move to another
/// unimpersonated thread. `finish` is mandatory and retryable. Drop intentionally
/// retains unfinished handles; closing one there would silently release its lock.
#[derive(Debug, Default)]
pub struct Store {
    user_sid: String,
    directory: Option<PathBuf>,
    handles: Vec<RetainedHandle>,
    anchors: Vec<usize>,
    lock: Option<usize>,
    locked: bool,
    ready: bool,
    ledger: Option<usize>,
    pending: Option<PendingWrite>,
    first_error: Option<Failure>,
    last_error: Option<Failure>,
}

impl Store {
    /// Acquire the fixed LocalAppData store for the unimpersonated process user.
    /// The stable lock file is never replaced or deleted by this component.
    pub fn acquire() -> Result<Self, StoreError> {
        let mut store = Self::default();
        match store.initialize() {
            Ok(()) => Ok(store),
            Err(mut error) => {
                store.remember(&error);
                if store.has_handles() {
                    error.recovery = Some(Box::new(store));
                }
                Err(error)
            }
        }
    }

    /// Read one bounded ledger image while keeping the transaction lock held.
    pub fn read(&mut self) -> Result<Option<Vec<u8>>, StoreError> {
        let result = self.read_inner();
        if let Err(error) = &result {
            self.remember(error);
        }
        result
    }

    /// Persist and read back an image. An uncertain write retains these exact
    /// bytes; a different image is refused until the first write is resolved.
    pub fn write(&mut self, bytes: &[u8]) -> Result<(), StoreError> {
        let result = self.write_inner(bytes);
        if let Err(error) = &result {
            self.remember(error);
        }
        result
    }

    /// Complete the previously requested bytes without releasing the lock.
    /// A typed coordinator calls this before persisting a subsequent transition.
    pub(crate) fn retry_pending_write(&mut self) -> Result<(), StoreError> {
        let result = self.retry_inner();
        if let Err(error) = &result {
            self.remember(error);
        }
        result
    }

    /// Complete any requested write, then explicitly unlock and close handles.
    /// Failed unlock or close retains the corresponding handle for another call.
    pub fn finish(&mut self) -> Result<(), StoreError> {
        let result = self.finish_inner();
        if let Err(error) = &result {
            self.remember(error);
        }
        result
    }

    /// Ownership evidence only; it does not authorize adopting a file by name.
    pub fn recovery_record(&self) -> serde_json::Value {
        let error_record = |failure: &Failure| serde_json::json!({"operation": failure.operation, "code": failure.code, "detail": failure.detail});
        serde_json::json!({
            "version": 1,
            "creatorUserSid": self.user_sid,
            "directory": self.directory.as_ref().map(|path| path.to_string_lossy()),
            "lockHeld": self.locked,
            "ready": self.ready,
            "handles": self.handles.iter().filter(|owner| owner.raw != 0).map(|owner| {
                serde_json::json!({
                    "handle": owner.raw,
                    "label": owner.label,
                    "path": owner.file.as_ref().map(|file| file.path.to_string_lossy()),
                    "identity": owner.file.as_ref().and_then(|file| file.identity.as_ref()).map(FileIdentity::record),
                })
            }).collect::<Vec<_>>(),
            "pendingWrite": self.pending.as_ref().map(|pending| serde_json::json!({
                "temporaryPath": pending.path.to_string_lossy(),
                "phase": format!("{:?}", pending.phase),
                "expectedBytes": pending.expected.len(),
                "previousLedger": pending.before.as_ref().map(FileIdentity::record),
            })),
            "firstError": self.first_error.as_ref().map(error_record),
            "lastError": self.last_error.as_ref().map(error_record),
        })
    }

    fn initialize(&mut self) -> Result<(), StoreError> {
        self.user_sid = self.current_user()?;
        let local = local_app_data()?;
        let mut path = PathBuf::new();
        for component in local.components() {
            match component {
                Component::Prefix(prefix)
                    if matches!(prefix.kind(), Prefix::Disk(_) | Prefix::VerbatimDisk(_)) =>
                {
                    path.push(component.as_os_str());
                    continue;
                }
                Component::RootDir | Component::Normal(_) => path.push(component.as_os_str()),
                _ => {
                    return Err(StoreError::invalid(
                        "LocalAppData",
                        "expected a local absolute path",
                    ))
                }
            }
            if !path.is_absolute() {
                return Err(StoreError::invalid(
                    "LocalAppData",
                    "relative directory component",
                ));
            }
            let index = self.open(
                &path,
                true,
                false,
                READ_CONTROL | FILE_READ_ATTRIBUTES,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                OPEN_EXISTING,
            )?;
            self.anchors.push(index);
            self.validate_file(index)?;
        }
        if self.anchors.is_empty() {
            return Err(StoreError::invalid(
                "LocalAppData",
                "the known folder had no anchored directory",
            ));
        }
        let directory = local.join(DIRECTORY);
        self.directory = Some(directory.clone());
        let security = private_descriptor(&self.user_sid)?;
        let attributes = security.attributes();
        if unsafe { CreateDirectoryW(wide_path(&directory).as_ptr(), &attributes) } == 0 {
            let code = unsafe { GetLastError() };
            if code != ERROR_ALREADY_EXISTS {
                return Err(StoreError::code("CreateDirectoryW(private store)", code));
            }
        }
        let index = self.open(
            &directory,
            true,
            true,
            READ_CONTROL | FILE_READ_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            OPEN_EXISTING,
        )?;
        self.anchors.push(index);
        self.validate_file(index)?;
        let lock = self.open(
            &directory.join(LOCK),
            false,
            true,
            GENERIC_READ | GENERIC_WRITE | READ_CONTROL,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            OPEN_ALWAYS,
        )?;
        self.lock = Some(lock);
        self.validate_file(lock)?;
        let started = Instant::now();
        loop {
            let mut overlapped: OVERLAPPED = unsafe { mem::zeroed() };
            if unsafe {
                LockFileEx(
                    self.handles[lock].raw,
                    LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                    0,
                    1,
                    0,
                    &mut overlapped,
                )
            } != 0
            {
                self.locked = true;
                break;
            }
            let code = unsafe { GetLastError() };
            if code != ERROR_LOCK_VIOLATION || started.elapsed() >= LOCK_TIMEOUT {
                return Err(StoreError::code("LockFileEx(private store)", code));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        self.validate_all()?;
        self.ledger = self.open_ledger(FILE_SHARE_READ)?;
        self.ready = true;
        Ok(())
    }

    fn read_inner(&mut self) -> Result<Option<Vec<u8>>, StoreError> {
        self.check_ready()?;
        if self.pending.is_some() {
            return Err(StoreError::invalid(
                "read",
                "a pending write must be resolved first",
            ));
        }
        self.validate_all()?;
        if let Some(index) = self.ledger {
            return read_bytes(self.handles[index].raw).map(Some);
        }
        if self.probe(&self.ledger_path()?)?.is_some() {
            return Err(StoreError::invalid(
                "read",
                "the missing ledger was replaced outside the transaction",
            ));
        }
        Ok(None)
    }

    fn write_inner(&mut self, bytes: &[u8]) -> Result<(), StoreError> {
        self.check_ready()?;
        if bytes.len() > MAX_BYTES {
            return Err(StoreError::invalid(
                "write",
                "ledger exceeds the 16 MiB limit",
            ));
        }
        if let Some(pending) = &self.pending {
            if pending.expected != bytes {
                return Err(StoreError::invalid(
                    "write",
                    "different bytes cannot replace an unresolved write",
                ));
            }
        } else {
            self.validate_all()?;
            let before = self.ledger.map(|index| self.identity(index)).transpose()?;
            let directory = self
                .directory
                .as_ref()
                .ok_or_else(|| StoreError::invalid("write", "no private directory"))?;
            let path = directory.join(temporary_name()?);
            let index = self.open(
                &path,
                false,
                true,
                GENERIC_READ | GENERIC_WRITE | DELETE | READ_CONTROL,
                FILE_SHARE_READ | FILE_SHARE_DELETE,
                CREATE_NEW,
            )?;
            self.pending = Some(PendingWrite {
                temporary: index,
                path,
                expected: bytes.to_vec(),
                before,
                phase: WritePhase::Staging,
            });
            self.validate_file(index)?;
        }
        self.retry_inner()
    }

    fn retry_inner(&mut self) -> Result<(), StoreError> {
        self.check_ready()?;
        while self.pending.is_some() {
            self.validate_all()?;
            let pending = self
                .pending
                .as_ref()
                .ok_or_else(|| StoreError::invalid("retry", "pending write disappeared"))?;
            let index = pending.temporary;
            let phase = pending.phase;
            let expected_id = self.identity(index)?;
            match phase {
                WritePhase::Staging => {
                    write_bytes(self.handles[index].raw, &pending.expected)?;
                    if read_bytes(self.handles[index].raw)? != pending.expected {
                        return Err(StoreError::invalid(
                            "staging readback",
                            "written bytes did not match",
                        ));
                    }
                    self.set_phase(WritePhase::ReadyToRename)?;
                }
                WritePhase::ReadyToRename => {
                    let previous = pending.before.clone();
                    let temporary_path = pending.path.clone();
                    if let Some(old) = self.ledger {
                        self.close(old)?;
                        self.ledger = None;
                    }
                    let ledger_path = self.ledger_path()?;
                    if self.probe(&ledger_path)? != previous
                        || self.probe(&temporary_path)? != Some(expected_id)
                    {
                        return Err(StoreError::invalid(
                            "rename validation",
                            "ledger or temporary identity changed",
                        ));
                    }
                    self.set_phase(WritePhase::RenameAttempted)?;
                    let flags = MOVEFILE_WRITE_THROUGH
                        | if previous.is_some() {
                            MOVEFILE_REPLACE_EXISTING
                        } else {
                            0
                        };
                    if unsafe {
                        MoveFileExW(
                            wide_path(&temporary_path).as_ptr(),
                            wide_path(&ledger_path).as_ptr(),
                            flags,
                        )
                    } == 0
                    {
                        return Err(StoreError::win("MoveFileExW(ledger commit)"));
                    }
                    self.set_phase(WritePhase::Renamed)?;
                }
                WritePhase::RenameAttempted => {
                    let previous = pending.before.clone();
                    let temporary_path = pending.path.clone();
                    let current = self.probe(&self.ledger_path()?)?;
                    if current == Some(expected_id.clone()) {
                        self.set_phase(WritePhase::Renamed)?;
                    } else if current == previous
                        && self.probe(&temporary_path)? == Some(expected_id)
                    {
                        self.set_phase(WritePhase::ReadyToRename)?;
                    } else {
                        return Err(StoreError::invalid(
                            "rename recovery",
                            "neither the original nor requested ledger identity is confirmed",
                        ));
                    }
                }
                WritePhase::Renamed => {
                    if self.handles[index].raw != 0 {
                        if unsafe { FlushFileBuffers(self.handles[index].raw) } == 0 {
                            return Err(StoreError::win("FlushFileBuffers(committed ledger)"));
                        }
                        self.close(index)?;
                    }
                    if self.ledger.is_none() {
                        self.ledger = self.open_ledger(FILE_SHARE_READ)?;
                    }
                    let ledger = self.ledger.ok_or_else(|| {
                        StoreError::invalid("commit readback", "committed ledger is missing")
                    })?;
                    if self.identity(ledger)? != expected_id {
                        return Err(StoreError::invalid(
                            "commit readback",
                            "committed ledger was replaced",
                        ));
                    }
                    let pending = self.pending.as_ref().ok_or_else(|| {
                        StoreError::invalid("commit readback", "pending write disappeared")
                    })?;
                    if read_bytes(self.handles[ledger].raw)? != pending.expected {
                        return Err(StoreError::invalid(
                            "commit readback",
                            "committed bytes did not match",
                        ));
                    }
                    self.set_phase(WritePhase::Confirmed)?;
                }
                WritePhase::Confirmed => {
                    self.close_data_handles(self.ledger)?;
                    self.pending = None;
                }
            }
        }
        Ok(())
    }

    fn finish_inner(&mut self) -> Result<(), StoreError> {
        if self.pending.is_some() {
            self.retry_inner()?;
        }
        self.ready = false;
        self.close_data_handles(None)?;
        self.ledger = None;
        if self.locked {
            let index = self
                .lock
                .ok_or_else(|| StoreError::invalid("finish", "held lock has no handle"))?;
            let mut overlapped: OVERLAPPED = unsafe { mem::zeroed() };
            if unsafe { UnlockFileEx(self.handles[index].raw, 0, 1, 0, &mut overlapped) } == 0 {
                return Err(StoreError::win("UnlockFileEx(private store)"));
            }
            self.locked = false;
        }
        if let Some(index) = self.lock {
            self.close(index)?;
        }
        for index in (0..self.handles.len()).rev() {
            self.close(index)?;
        }
        Ok(())
    }

    fn check_ready(&mut self) -> Result<(), StoreError> {
        if !self.ready || !self.locked {
            return Err(StoreError::invalid(
                "transaction",
                "store is not holding an active transaction",
            ));
        }
        if self.current_user()? != self.user_sid {
            return Err(StoreError::invalid(
                "transaction",
                "current process user differs from the creator",
            ));
        }
        Ok(())
    }

    fn current_user(&mut self) -> Result<String, StoreError> {
        let mut token = 0;
        if unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut token) } != 0 {
            self.retain(token, "impersonation token", None);
            return Err(StoreError::invalid(
                "identity",
                "thread impersonation is not supported",
            ));
        }
        let code = unsafe { GetLastError() };
        if code != ERROR_NO_TOKEN {
            return Err(StoreError::code("OpenThreadToken", code));
        }
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(StoreError::win("OpenProcessToken"));
        }
        let index = self.retain(token, "process token", None);
        let mut bytes = 0;
        let probe =
            unsafe { GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut bytes) };
        let code = unsafe { GetLastError() };
        if probe != 0
            || code != ERROR_INSUFFICIENT_BUFFER
            || bytes < mem::size_of::<TOKEN_USER>() as u32
            || bytes > 65_536
        {
            return Err(StoreError::invalid(
                "TokenUser size",
                format!("result={probe}, code={code}, bytes={bytes}"),
            ));
        }
        let capacity = bytes;
        let mut storage = vec![0usize; (bytes as usize).div_ceil(mem::size_of::<usize>())];
        if unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                storage.as_mut_ptr().cast(),
                capacity,
                &mut bytes,
            )
        } == 0
        {
            return Err(StoreError::win("GetTokenInformation(TokenUser)"));
        }
        if bytes < mem::size_of::<TOKEN_USER>() as u32 || bytes > capacity {
            return Err(StoreError::invalid(
                "TokenUser",
                "invalid returned buffer length",
            ));
        }
        let sid = unsafe { (*storage.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        let text = sid_text(sid)?;
        self.close(index)?;
        Ok(text)
    }

    fn ledger_path(&self) -> Result<PathBuf, StoreError> {
        self.directory
            .as_ref()
            .map(|path| path.join(LEDGER))
            .ok_or_else(|| {
                StoreError::invalid("ledger path", "private directory is not initialized")
            })
    }

    fn open_ledger(&mut self, sharing: u32) -> Result<Option<usize>, StoreError> {
        self.open_optional(&self.ledger_path()?, sharing)
    }

    fn open_optional(&mut self, path: &Path, sharing: u32) -> Result<Option<usize>, StoreError> {
        match self.open(
            path,
            false,
            true,
            GENERIC_READ | READ_CONTROL,
            sharing,
            OPEN_EXISTING,
        ) {
            Ok(index) => {
                self.validate_file(index)?;
                bounded_size(self.handles[index].raw)?;
                Ok(Some(index))
            }
            Err(error) if error.failure.code == Some(ERROR_FILE_NOT_FOUND) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn probe(&mut self, path: &Path) -> Result<Option<FileIdentity>, StoreError> {
        let Some(index) = self.open_optional(path, SHARED)? else {
            return Ok(None);
        };
        let identity = self.identity(index)?;
        self.close(index)?;
        Ok(Some(identity))
    }

    fn open(
        &mut self,
        path: &Path,
        directory: bool,
        private: bool,
        access: u32,
        sharing: u32,
        disposition: u32,
    ) -> Result<usize, StoreError> {
        let security = if disposition == OPEN_EXISTING {
            None
        } else {
            Some(private_descriptor(&self.user_sid)?)
        };
        let attributes = security.as_ref().map(LocalAllocation::attributes);
        let flags = FILE_FLAG_OPEN_REPARSE_POINT
            | FILE_FLAG_BACKUP_SEMANTICS
            | FILE_ATTRIBUTE_NORMAL
            | if disposition == CREATE_NEW {
                FILE_FLAG_WRITE_THROUGH
            } else {
                0
            };
        let raw = unsafe {
            CreateFileW(
                wide_path(path).as_ptr(),
                access,
                sharing,
                attributes
                    .as_ref()
                    .map_or(ptr::null(), |attributes| attributes),
                disposition,
                flags,
                0,
            )
        };
        if raw == INVALID_HANDLE_VALUE {
            return Err(StoreError::win("CreateFileW(private store)"));
        }
        Ok(self.retain(
            raw,
            "file",
            Some(FileRecord {
                path: path.to_owned(),
                directory,
                private,
                identity: None,
            }),
        ))
    }

    fn retain(&mut self, raw: HANDLE, label: &'static str, file: Option<FileRecord>) -> usize {
        let index = self.handles.len();
        self.handles.push(RetainedHandle { raw, label, file });
        index
    }

    fn validate_file(&mut self, index: usize) -> Result<(), StoreError> {
        let owner = &self.handles[index];
        let Some(file) = &owner.file else {
            return Ok(());
        };
        if owner.raw == 0 {
            return Ok(());
        }
        let mut information: BY_HANDLE_FILE_INFORMATION = unsafe { mem::zeroed() };
        if unsafe { GetFileInformationByHandle(owner.raw, &mut information) } == 0 {
            return Err(StoreError::win("GetFileInformationByHandle"));
        }
        if information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
            || (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0) != file.directory
            || (!file.directory && information.nNumberOfLinks != 1)
        {
            return Err(StoreError::invalid(
                "file identity",
                format!(
                    "reparse point, wrong kind or hard link: {}",
                    file.path.display()
                ),
            ));
        }
        let mut id: FILE_ID_INFO = unsafe { mem::zeroed() };
        if unsafe {
            GetFileInformationByHandleEx(
                owner.raw,
                FileIdInfo,
                (&mut id as *mut FILE_ID_INFO).cast(),
                mem::size_of::<FILE_ID_INFO>() as u32,
            )
        } == 0
        {
            return Err(StoreError::win("GetFileInformationByHandleEx(FileIdInfo)"));
        }
        let identity = FileIdentity {
            volume: id.VolumeSerialNumber,
            id: id.FileId.Identifier,
        };
        if file
            .identity
            .as_ref()
            .is_some_and(|previous| *previous != identity)
        {
            return Err(StoreError::invalid(
                "file identity",
                "retained file identity changed",
            ));
        }
        if file.private {
            validate_private(owner.raw, &self.user_sid)?;
        }
        if let Some(file) = &mut self.handles[index].file {
            file.identity = Some(identity);
        }
        Ok(())
    }

    fn validate_all(&mut self) -> Result<(), StoreError> {
        for index in 0..self.handles.len() {
            self.validate_file(index)?;
        }
        Ok(())
    }

    fn identity(&self, index: usize) -> Result<FileIdentity, StoreError> {
        self.handles[index]
            .file
            .as_ref()
            .and_then(|file| file.identity.clone())
            .ok_or_else(|| StoreError::invalid("file identity", "handle has no verified identity"))
    }

    fn set_phase(&mut self, phase: WritePhase) -> Result<(), StoreError> {
        let pending = self
            .pending
            .as_mut()
            .ok_or_else(|| StoreError::invalid("write phase", "no pending write"))?;
        pending.phase = phase;
        Ok(())
    }

    fn close_data_handles(&mut self, keep: Option<usize>) -> Result<(), StoreError> {
        for index in (0..self.handles.len()).rev() {
            if Some(index) != keep && Some(index) != self.lock && !self.anchors.contains(&index) {
                self.close(index)?;
            }
        }
        Ok(())
    }

    fn close(&mut self, index: usize) -> Result<(), StoreError> {
        let owner = &mut self.handles[index];
        if owner.raw != 0 {
            if unsafe { CloseHandle(owner.raw) } == 0 {
                return Err(StoreError::win("CloseHandle(private store)"));
            }
            owner.raw = 0;
        }
        Ok(())
    }

    fn has_handles(&self) -> bool {
        self.handles.iter().any(|owner| owner.raw != 0)
    }

    fn remember(&mut self, error: &StoreError) {
        if self.first_error.is_none() {
            self.first_error = Some(error.failure.clone());
        }
        self.last_error = Some(error.failure.clone());
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if self.has_handles() || self.pending.is_some() {
            let _ = writeln!(
                std::io::stderr().lock(),
                "KLAUS_ACL_STORE_RETAINED={}",
                self.recovery_record()
            );
        }
    }
}

struct LocalAllocation(*mut c_void);

impl LocalAllocation {
    fn attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: self.0,
            bInheritHandle: 0,
        }
    }
}

impl Drop for LocalAllocation {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                LocalFree(self.0);
            }
        }
    }
}

struct ComAllocation(*mut u16);

impl Drop for ComAllocation {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                CoTaskMemFree(self.0.cast());
            }
        }
    }
}

fn sid_text(sid: *mut c_void) -> Result<String, StoreError> {
    if sid.is_null() || unsafe { IsValidSid(sid) } == 0 {
        return Err(StoreError::invalid("SID", "invalid SID"));
    }
    let mut raw = ptr::null_mut();
    let ok = unsafe { ConvertSidToStringSidW(sid, &mut raw) };
    let allocation = LocalAllocation(raw.cast());
    if ok == 0 {
        return Err(StoreError::win("ConvertSidToStringSidW"));
    }
    if allocation.0.is_null() {
        return Err(StoreError::invalid("SID", "null converted SID"));
    }
    let length = (0..256)
        .find(|index| unsafe { *raw.add(*index) == 0 })
        .ok_or_else(|| StoreError::invalid("SID", "unterminated converted SID"))?;
    String::from_utf16(unsafe { std::slice::from_raw_parts(raw, length) })
        .map_err(|error| StoreError::invalid("SID", error.to_string()))
}

fn private_descriptor(user_sid: &str) -> Result<LocalAllocation, StoreError> {
    let text: Vec<u16> = format!("O:{user_sid}D:P(A;;FA;;;{user_sid})")
        .encode_utf16()
        .chain(Some(0))
        .collect();
    let mut raw = ptr::null_mut();
    let ok = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            text.as_ptr(),
            1,
            &mut raw,
            ptr::null_mut(),
        )
    };
    let allocation = LocalAllocation(raw);
    if ok == 0 {
        return Err(StoreError::win(
            "ConvertStringSecurityDescriptorToSecurityDescriptorW",
        ));
    }
    if allocation.0.is_null() {
        return Err(StoreError::invalid(
            "private descriptor",
            "null security descriptor",
        ));
    }
    Ok(allocation)
}

fn validate_private(handle: HANDLE, user_sid: &str) -> Result<(), StoreError> {
    let mut owner = ptr::null_mut();
    let mut dacl: *mut ACL = ptr::null_mut();
    let mut descriptor = ptr::null_mut();
    let code = unsafe {
        GetSecurityInfo(
            handle,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            ptr::null_mut(),
            &mut dacl,
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    let allocation = LocalAllocation(descriptor);
    if code != 0 {
        return Err(StoreError::code("GetSecurityInfo(private store)", code));
    }
    if descriptor.is_null()
        || sid_text(owner)? != user_sid
        || dacl.is_null()
        || unsafe { IsValidAcl(dacl) } == 0
    {
        return Err(StoreError::invalid(
            "private descriptor",
            "owner is not the current user or DACL is missing/invalid",
        ));
    }
    let mut control = 0;
    let mut revision = 0;
    if unsafe { GetSecurityDescriptorControl(allocation.0, &mut control, &mut revision) } == 0 {
        return Err(StoreError::win("GetSecurityDescriptorControl"));
    }
    if control & SE_DACL_PROTECTED == 0 {
        return Err(StoreError::invalid(
            "private descriptor",
            "DACL is not protected",
        ));
    }
    let mut information: ACL_SIZE_INFORMATION = unsafe { mem::zeroed() };
    if unsafe {
        GetAclInformation(
            dacl,
            (&mut information as *mut ACL_SIZE_INFORMATION).cast(),
            mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        )
    } == 0
    {
        return Err(StoreError::win("GetAclInformation"));
    }
    if information.AceCount != 1 {
        return Err(StoreError::invalid(
            "private descriptor",
            "expected exactly one current-user ACE",
        ));
    }
    let mut raw = ptr::null_mut();
    if unsafe { GetAce(dacl, 0, &mut raw) } == 0 {
        return Err(StoreError::win("GetAce"));
    }
    if raw.is_null() {
        return Err(StoreError::invalid("private descriptor", "null ACE"));
    }
    let header = unsafe { &*raw.cast::<ACE_HEADER>() };
    if header.AceType != 0
        || header.AceFlags != 0
        || header.AceSize < mem::size_of::<ACCESS_ALLOWED_ACE>() as u16
    {
        return Err(StoreError::invalid(
            "private descriptor",
            "unexpected ACE type, flags or rights",
        ));
    }
    let ace = unsafe { &*raw.cast::<ACCESS_ALLOWED_ACE>() };
    if ace.Mask != FILE_ALL_ACCESS {
        return Err(StoreError::invalid(
            "private descriptor",
            "unexpected current-user rights",
        ));
    }
    let sid = ptr::addr_of!(ace.SidStart).cast_mut().cast();
    if sid_text(sid)? != user_sid || ace.Header.AceSize as u32 != 8 + unsafe { GetLengthSid(sid) } {
        return Err(StoreError::invalid(
            "private descriptor",
            "ACE names another identity or has unexpected data",
        ));
    }
    Ok(())
}

fn local_app_data() -> Result<PathBuf, StoreError> {
    let mut raw = ptr::null_mut();
    let code = unsafe { SHGetKnownFolderPath(&FOLDERID_LocalAppData, 0, 0, &mut raw) };
    let allocation = ComAllocation(raw);
    if code != 0 {
        return Err(StoreError::code(
            "SHGetKnownFolderPath(LocalAppData)",
            code as u32,
        ));
    }
    if allocation.0.is_null() {
        return Err(StoreError::invalid(
            "LocalAppData",
            "null known-folder path",
        ));
    }
    let length = (0..32_768)
        .find(|index| unsafe { *raw.add(*index) == 0 })
        .ok_or_else(|| StoreError::invalid("LocalAppData", "unterminated known-folder path"))?;
    Ok(PathBuf::from(OsString::from_wide(unsafe {
        std::slice::from_raw_parts(raw, length)
    })))
}

fn wide_path(path: &Path) -> Vec<u16> {
    let mut text = OsString::new();
    if matches!(path.components().next(), Some(Component::Prefix(prefix)) if matches!(prefix.kind(), Prefix::Disk(_)))
    {
        text.push("\\\\?\\");
    }
    text.push(path);
    text.encode_wide().chain(Some(0)).collect()
}

fn temporary_name() -> Result<String, StoreError> {
    let mut id: GUID = unsafe { mem::zeroed() };
    let code = unsafe { CoCreateGuid(&mut id) };
    if code != 0 {
        return Err(StoreError::code("CoCreateGuid(staging file)", code as u32));
    }
    let tail = id
        .data4
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!(
        "ledger-{:08x}{:04x}{:04x}{tail}.pending",
        id.data1, id.data2, id.data3
    ))
}

fn bounded_size(handle: HANDLE) -> Result<usize, StoreError> {
    let mut length = 0;
    if unsafe { GetFileSizeEx(handle, &mut length) } == 0 {
        return Err(StoreError::win("GetFileSizeEx"));
    }
    if !(0..=MAX_BYTES as i64).contains(&length) {
        return Err(StoreError::invalid(
            "ledger size",
            "ledger exceeds the 16 MiB limit",
        ));
    }
    Ok(length as usize)
}

fn seek_start(handle: HANDLE) -> Result<(), StoreError> {
    if unsafe { SetFilePointerEx(handle, 0, ptr::null_mut(), FILE_BEGIN) } == 0 {
        return Err(StoreError::win("SetFilePointerEx"));
    }
    Ok(())
}

fn read_bytes(handle: HANDLE) -> Result<Vec<u8>, StoreError> {
    let length = bounded_size(handle)?;
    seek_start(handle)?;
    let mut bytes = vec![0; length];
    let mut offset = 0;
    while offset < length {
        let mut count = 0;
        let request = (length - offset).min(1024 * 1024) as u32;
        if unsafe {
            ReadFile(
                handle,
                bytes[offset..].as_mut_ptr(),
                request,
                &mut count,
                ptr::null_mut(),
            )
        } == 0
        {
            return Err(StoreError::win("ReadFile(ledger)"));
        }
        if count == 0 || count > request {
            return Err(StoreError::invalid("ledger read", "unexpected read length"));
        }
        offset += count as usize;
    }
    if bounded_size(handle)? != length {
        return Err(StoreError::invalid(
            "ledger read",
            "file length changed during read",
        ));
    }
    Ok(bytes)
}

fn write_bytes(handle: HANDLE, bytes: &[u8]) -> Result<(), StoreError> {
    seek_start(handle)?;
    if unsafe { SetEndOfFile(handle) } == 0 {
        return Err(StoreError::win("SetEndOfFile(staging)"));
    }
    let mut offset = 0;
    while offset < bytes.len() {
        let mut count = 0;
        let request = (bytes.len() - offset).min(1024 * 1024) as u32;
        if unsafe {
            WriteFile(
                handle,
                bytes[offset..].as_ptr(),
                request,
                &mut count,
                ptr::null_mut(),
            )
        } == 0
        {
            return Err(StoreError::win("WriteFile(staging)"));
        }
        if count == 0 || count > request {
            return Err(StoreError::invalid(
                "staging write",
                "unexpected write length",
            ));
        }
        offset += count as usize;
    }
    if unsafe { FlushFileBuffers(handle) } == 0 {
        return Err(StoreError::win("FlushFileBuffers(staging)"));
    }
    Ok(())
}
