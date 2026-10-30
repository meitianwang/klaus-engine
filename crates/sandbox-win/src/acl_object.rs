//! Handle-bound filesystem identity and DACL access for the native ACL ledger.
//!
//! This component does not authorize a path, lock other writers, journal a lease,
//! or roll back a write. The coordinator must retain its recovery owner before
//! calling `write`, including when that call later returns an error. A handle
//! continues to name the opened object after a rename; its original path is only
//! diagnostic. Ancestor reparse points and propagation footprints remain the
//! coordinator's responsibility. The final opened object must not be a reparse
//! point. No path-based security setter is used.
//!
//! Local writes use the documented user-mode `NtSetSecurityObject` entry on the
//! retained filesystem handle. Tree writes use `SetSecurityInfo`; its inheritance
//! conversion has a separate, fully specified AUTO_INHERITED intermediate image.
//! The coordinator journals that phase before restoring the exact local image.
//! https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/nf-ntifs-ntsetsecurityobject
//! https://learn.microsoft.com/en-us/windows/win32/secauthz/automatic-propagation-of-inheritable-aces
//! This object's read-back alone cannot certify propagation to every child.

use std::mem::size_of;
use std::ptr;

use serde::{Deserialize, Serialize};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, LocalFree, ERROR_FILE_NOT_FOUND, ERROR_PATH_NOT_FOUND,
    ERROR_SUCCESS, HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Security::Authorization::{
    GetSecurityInfo, SetSecurityInfo, SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::{
    AddAce, GetAce, GetSecurityDescriptorControl, GetSecurityDescriptorDacl,
    GetSecurityDescriptorLength, InitializeAcl, InitializeSecurityDescriptor, IsValidAcl,
    IsValidSecurityDescriptor, SetSecurityDescriptorControl, SetSecurityDescriptorDacl, ACE_HEADER,
    ACL, ACL_REVISION, ACL_REVISION_DS, DACL_SECURITY_INFORMATION,
    PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, SECURITY_DESCRIPTOR,
    SE_DACL_AUTO_INHERITED, SE_DACL_DEFAULTED, SE_DACL_PRESENT, SE_DACL_PROTECTED,
    SE_SELF_RELATIVE, UNPROTECTED_DACL_SECURITY_INFORMATION,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FileAttributeTagInfo, FileIdInfo, GetFileInformationByHandleEx, GetFileType,
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_ATTRIBUTE_TAG_INFO,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO, FILE_READ_ATTRIBUTES,
    FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TYPE_DISK, OPEN_EXISTING,
    READ_CONTROL, WRITE_DAC,
};
use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress};
use windows_sys::Win32::System::SystemServices::SECURITY_DESCRIPTOR_REVISION;

const SHARE: u32 = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
const OPEN_FLAGS: u32 = FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT;
const MAX_DESCRIPTOR_BYTES: usize = 1_048_576;
const SUPPORTED_CONTROL: u16 =
    SE_SELF_RELATIVE | SE_DACL_PRESENT | SE_DACL_AUTO_INHERITED | SE_DACL_PROTECTED;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectId {
    pub(crate) volume_serial: u64,
    pub(crate) file_id: [u8; 16],
}

/// A null DACL and an absent DACL must never become an empty present ACL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Dacl {
    Absent,
    Null,
    Present { revision: u8, aces: Vec<Vec<u8>> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DaclSnapshot {
    /// Raw descriptor control, including bits this writer cannot restore.
    /// A requested image must keep this consistent with `protected`/`defaulted`.
    pub(crate) control: u16,
    pub(crate) descriptor_revision: u32,
    pub(crate) protected: bool,
    pub(crate) defaulted: bool,
    pub(crate) dacl: Dacl,
}

impl DaclSnapshot {
    /// Win32 inheritance conversion may set this one declared metadata bit.
    /// ACE bytes, protection, presence and defaulted state must still match.
    pub(crate) fn propagation_image(&self) -> Self {
        let mut image = self.clone();
        image.control |= SE_DACL_AUTO_INHERITED;
        image
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Propagation {
    Local,
    Tree,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize)]
pub(crate) enum ObjectError {
    #[error("{operation} failed for {path}: Win32 error {code}")]
    Native {
        path: String,
        operation: &'static str,
        code: u32,
    },
    #[error("unsupported ACL object or state at {path}: {detail}")]
    Unsupported { path: String, detail: String },
    #[error("invalid DACL snapshot at {path}: {detail}")]
    InvalidSnapshot { path: String, detail: String },
    #[error("filesystem identity changed at {path}: expected {expected:?}, observed {observed:?}")]
    IdentityChanged {
        path: String,
        expected: ObjectId,
        observed: ObjectId,
    },
    #[error("DACL changed before writing {path}; the requested native phase did not run")]
    BeforeImageChanged {
        path: String,
        expected: Box<DaclSnapshot>,
        observed: Box<DaclSnapshot>,
    },
    #[error("{operation} failed for {path}: {code}; recovery remains required")]
    WriteFailed {
        path: String,
        operation: &'static str,
        code: String,
        observed: Option<Box<DaclSnapshot>>,
        readback_error: Option<Box<ObjectError>>,
    },
    #[error("{operation} at {path} succeeded but read-back failed: {source}")]
    ReadbackFailed {
        path: String,
        operation: &'static str,
        #[source]
        source: Box<ObjectError>,
    },
    #[error(
        "{operation} at {path} did not read back as requested (control {expected_control:#06x} -> {observed_control:#06x}); recovery remains required"
    )]
    ReadbackMismatch {
        path: String,
        operation: &'static str,
        expected_control: u16,
        observed_control: u16,
        expected: Box<DaclSnapshot>,
        observed: Box<DaclSnapshot>,
    },
}

impl ObjectError {
    /// False proves only that this call did not reach the security setter.
    pub(crate) fn write_attempted(&self) -> bool {
        matches!(
            self,
            Self::WriteFailed { .. } | Self::ReadbackFailed { .. } | Self::ReadbackMismatch { .. }
        )
    }
}

fn native_error(path: &str, operation: &'static str) -> ObjectError {
    let code = unsafe { GetLastError() };
    ObjectError::Native {
        path: path.to_owned(),
        operation,
        code,
    }
}

fn invalid(path: &str, detail: impl Into<String>) -> ObjectError {
    ObjectError::InvalidSnapshot {
        path: path.to_owned(),
        detail: detail.into(),
    }
}

fn unsupported(path: &str, detail: impl Into<String>) -> ObjectError {
    ObjectError::Unsupported {
        path: path.to_owned(),
        detail: detail.into(),
    }
}

#[derive(Debug)]
struct FileHandle(HANDLE);

impl Drop for FileHandle {
    fn drop(&mut self) {
        // These are ordinary file handles: closing them performs no ACL cleanup.
        // Never panic during destruction or retry a possibly recycled handle.
        let _ = unsafe { CloseHandle(self.0) };
    }
}

struct Descriptor(PSECURITY_DESCRIPTOR);

impl Drop for Descriptor {
    fn drop(&mut self) {
        if !self.0.is_null() {
            let _ = unsafe { LocalFree(self.0) };
        }
    }
}

/// Keeps the original READ_CONTROL/WRITE_DAC grant for the object's lifetime.
#[derive(Debug)]
pub(crate) struct Object {
    path: String,
    id: ObjectId,
    is_directory: bool,
    primary: FileHandle,
}

impl Object {
    pub(crate) fn open(path: &str) -> Result<Option<Self>, ObjectError> {
        if !std::path::Path::new(path).is_absolute() || path.contains('\0') {
            return Err(unsupported(
                path,
                "an absolute path without NUL is required",
            ));
        }
        let wide: Vec<_> = path.encode_utf16().chain(std::iter::once(0)).collect();
        let raw = unsafe {
            CreateFileW(
                wide.as_ptr(),
                READ_CONTROL | WRITE_DAC | FILE_READ_ATTRIBUTES,
                SHARE,
                ptr::null(),
                OPEN_EXISTING,
                OPEN_FLAGS,
                0,
            )
        };
        if raw == INVALID_HANDLE_VALUE {
            let code = unsafe { GetLastError() };
            return if matches!(code, ERROR_FILE_NOT_FOUND | ERROR_PATH_NOT_FOUND) {
                Ok(None)
            } else {
                Err(ObjectError::Native {
                    path: path.to_owned(),
                    operation: "CreateFileW(READ_CONTROL|WRITE_DAC|FILE_READ_ATTRIBUTES)",
                    code,
                })
            };
        }
        let primary = FileHandle(raw);
        let (id, is_directory) = inspect_handle(primary.0, path)?;
        Ok(Some(Self {
            path: path.to_owned(),
            id,
            is_directory,
            primary,
        }))
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn identity(&self) -> ObjectId {
        self.id
    }

    pub(crate) fn is_directory(&self) -> bool {
        self.is_directory
    }

    pub(crate) fn snapshot(&self) -> Result<DaclSnapshot, ObjectError> {
        self.snapshot_handle(self.primary.0)
    }

    fn snapshot_handle(&self, handle: HANDLE) -> Result<DaclSnapshot, ObjectError> {
        let (observed, _) = inspect_handle(handle, &self.path)?;
        if observed != self.id {
            return Err(ObjectError::IdentityChanged {
                path: self.path.clone(),
                expected: self.id,
                observed,
            });
        }
        read_snapshot(handle, &self.path)
    }

    /// Read, write and read back through one identity-checked filesystem handle.
    ///
    /// No compare-and-swap or tree-wide success guarantee is implied. The caller
    /// must serialize its ledger transaction and check outside changes. Even a
    /// failed native setter is followed by read-back and is a write attempt.
    pub(crate) fn write(
        &mut self,
        expected_before: &DaclSnapshot,
        image: &DaclSnapshot,
        propagation: Propagation,
    ) -> Result<DaclSnapshot, ObjectError> {
        let handle = self.primary.0;
        let before = self.snapshot_handle(handle)?;
        // This is not a compare-and-swap against unrelated writers. It ensures
        // that we never discard an outside change this handle already observed.
        if before != *expected_before {
            return Err(ObjectError::BeforeImageChanged {
                path: self.path.clone(),
                expected: Box::new(expected_before.clone()),
                observed: Box::new(before),
            });
        }
        validate_write_image(&self.path, &before)?;
        validate_write_image(&self.path, image)?;
        // Refuse before changing an ACL whose original ordered entries could
        // not be rebuilt by the same native API for a later recovery attempt.
        let _ = build_acl(&self.path, &before)?;
        let acl = build_acl(&self.path, image)?;
        // Resolve the already-loaded system module before either phase writes.
        // This does not search a workspace or load an arbitrary native library.
        type SetSecurity = unsafe extern "system" fn(HANDLE, u32, PSECURITY_DESCRIPTOR) -> i32;
        let name: Vec<u16> = "ntdll.dll\0".encode_utf16().collect();
        let module = unsafe { GetModuleHandleW(name.as_ptr()) };
        if module == 0 {
            return Err(native_error(&self.path, "GetModuleHandleW(ntdll.dll)"));
        }
        let entry = unsafe { GetProcAddress(module, c"NtSetSecurityObject".as_ptr().cast()) }
            .ok_or_else(|| native_error(&self.path, "GetProcAddress(NtSetSecurityObject)"))?;
        let set_security = unsafe {
            std::mem::transmute::<unsafe extern "system" fn() -> isize, SetSecurity>(entry)
        };
        let mut descriptor: SECURITY_DESCRIPTOR = unsafe { std::mem::zeroed() };
        let pointer = (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast();
        let control = SE_DACL_PROTECTED | SE_DACL_AUTO_INHERITED;
        if unsafe {
            InitializeSecurityDescriptor(pointer, SECURITY_DESCRIPTOR_REVISION) == 0
                || SetSecurityDescriptorDacl(pointer, 1, acl.as_ptr().cast(), 0) == 0
                || SetSecurityDescriptorControl(pointer, control, image.control & control) == 0
        } {
            return Err(native_error(
                &self.path,
                "constructing the exact DACL descriptor",
            ));
        }
        let (operation, failure) = match propagation {
            Propagation::Local => {
                // The absolute descriptor above deliberately excludes SELF_RELATIVE.
                // Only DACL information is set; owner, group and SACL are untouched.
                let status = unsafe { set_security(handle, DACL_SECURITY_INFORMATION, pointer) };
                (
                    "NtSetSecurityObject",
                    (status < 0).then(|| format!("NTSTATUS {status:#010x}")),
                )
            }
            Propagation::Tree => {
                if image.control & SE_DACL_AUTO_INHERITED == 0 {
                    return Err(invalid(
                        &self.path,
                        "tree write must declare inheritance conversion",
                    ));
                }
                let mut information = DACL_SECURITY_INFORMATION;
                if before.protected != image.protected {
                    information |= if image.protected {
                        PROTECTED_DACL_SECURITY_INFORMATION
                    } else {
                        UNPROTECTED_DACL_SECURITY_INFORMATION
                    };
                }
                let code = unsafe {
                    SetSecurityInfo(
                        handle,
                        SE_FILE_OBJECT,
                        information,
                        ptr::null_mut(),
                        ptr::null_mut(),
                        acl.as_ptr().cast(),
                        ptr::null(),
                    )
                };
                (
                    "SetSecurityInfo",
                    (code != ERROR_SUCCESS).then(|| format!("Win32 error {code}")),
                )
            }
        };
        let readback = self.snapshot_handle(handle);
        if let Some(code) = failure {
            let (observed, readback_error) = match readback {
                Ok(observed) => (Some(Box::new(observed)), None),
                Err(error) => (None, Some(Box::new(error))),
            };
            return Err(ObjectError::WriteFailed {
                path: self.path.clone(),
                operation,
                code,
                observed,
                readback_error,
            });
        }
        let observed = readback.map_err(|source| ObjectError::ReadbackFailed {
            path: self.path.clone(),
            operation,
            source: Box::new(source),
        })?;
        // Control and defaulted metadata were constrained before the write. The
        // actual grant, null/absent distinction, ACE order and protection must all
        // match; normalization is not permission to discard an unexpected ACE.
        if observed != *image {
            return Err(ObjectError::ReadbackMismatch {
                path: self.path.clone(),
                operation,
                expected_control: image.control,
                observed_control: observed.control,
                expected: Box::new(image.clone()),
                observed: Box::new(observed),
            });
        }
        Ok(observed)
    }
}

fn inspect_handle(handle: HANDLE, path: &str) -> Result<(ObjectId, bool), ObjectError> {
    let kind = unsafe { GetFileType(handle) };
    if kind != FILE_TYPE_DISK {
        return Err(unsupported(
            path,
            format!("filesystem disk handle required; type={kind}"),
        ));
    }
    let mut tag: FILE_ATTRIBUTE_TAG_INFO = unsafe { std::mem::zeroed() };
    if unsafe {
        GetFileInformationByHandleEx(
            handle,
            FileAttributeTagInfo,
            (&mut tag as *mut FILE_ATTRIBUTE_TAG_INFO).cast(),
            size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
        )
    } == 0
    {
        return Err(native_error(
            path,
            "GetFileInformationByHandleEx(FileAttributeTagInfo)",
        ));
    }
    if tag.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(unsupported(
            path,
            format!("reparse object rejected; tag={:#x}", tag.ReparseTag),
        ));
    }
    let mut info: FILE_ID_INFO = unsafe { std::mem::zeroed() };
    if unsafe {
        GetFileInformationByHandleEx(
            handle,
            FileIdInfo,
            (&mut info as *mut FILE_ID_INFO).cast(),
            size_of::<FILE_ID_INFO>() as u32,
        )
    } == 0
    {
        return Err(native_error(
            path,
            "GetFileInformationByHandleEx(FileIdInfo)",
        ));
    }
    Ok((
        ObjectId {
            volume_serial: info.VolumeSerialNumber,
            file_id: info.FileId.Identifier,
        },
        tag.FileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0,
    ))
}

fn read_snapshot(handle: HANDLE, path: &str) -> Result<DaclSnapshot, ObjectError> {
    let mut raw = ptr::null_mut();
    let code = unsafe {
        GetSecurityInfo(
            handle,
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut raw,
        )
    };
    if code != ERROR_SUCCESS {
        return Err(ObjectError::Native {
            path: path.to_owned(),
            operation: "GetSecurityInfo(DACL_SECURITY_INFORMATION)",
            code,
        });
    }
    let descriptor = Descriptor(raw);
    if descriptor.0.is_null() || unsafe { IsValidSecurityDescriptor(descriptor.0) } == 0 {
        return Err(invalid(
            path,
            "GetSecurityInfo returned an invalid descriptor",
        ));
    }
    let mut control = 0;
    let mut descriptor_revision = 0;
    if unsafe { GetSecurityDescriptorControl(descriptor.0, &mut control, &mut descriptor_revision) }
        == 0
    {
        return Err(native_error(path, "GetSecurityDescriptorControl"));
    }
    let mut present = 0;
    let mut defaulted = 0;
    let mut acl = ptr::null_mut();
    if unsafe { GetSecurityDescriptorDacl(descriptor.0, &mut present, &mut acl, &mut defaulted) }
        == 0
    {
        return Err(native_error(path, "GetSecurityDescriptorDacl"));
    }
    if (present != 0) != (control & SE_DACL_PRESENT != 0)
        || (present != 0 && (defaulted != 0) != (control & SE_DACL_DEFAULTED != 0))
    {
        return Err(invalid(
            path,
            "descriptor control disagrees with DACL metadata",
        ));
    }
    // The ACL/defaulted outputs are unspecified when present is false. Preserve
    // the raw defaulted control bit without reading those unspecified outputs.
    let dacl = if present == 0 {
        Dacl::Absent
    } else if acl.is_null() {
        Dacl::Null
    } else {
        let descriptor_len = unsafe { GetSecurityDescriptorLength(descriptor.0) } as usize;
        if descriptor_len == 0 || descriptor_len > MAX_DESCRIPTOR_BYTES {
            return Err(invalid(
                path,
                "descriptor allocation length is outside the supported bound",
            ));
        }
        let start = descriptor.0 as usize;
        let end = start
            .checked_add(descriptor_len)
            .ok_or_else(|| invalid(path, "descriptor address overflow"))?;
        let acl_start = acl as usize;
        if acl_start < start
            || acl_start
                .checked_add(size_of::<ACL>())
                .is_none_or(|v| v > end)
        {
            return Err(invalid(
                path,
                "DACL header is outside the returned descriptor",
            ));
        }
        let header = unsafe { ptr::read_unaligned(acl) };
        let acl_end = acl_start
            .checked_add(usize::from(header.AclSize))
            .ok_or_else(|| invalid(path, "ACL address overflow"))?;
        if usize::from(header.AclSize) < size_of::<ACL>()
            || acl_end > end
            || unsafe { IsValidAcl(acl) } == 0
        {
            return Err(invalid(
                path,
                "DACL layout is invalid or outside the descriptor",
            ));
        }
        let mut aces = Vec::with_capacity(usize::from(header.AceCount));
        for index in 0..header.AceCount {
            let mut raw_ace = ptr::null_mut();
            if unsafe { GetAce(acl, u32::from(index), &mut raw_ace) } == 0 {
                return Err(native_error(path, "GetAce"));
            }
            let ace_start = raw_ace as usize;
            if ace_start < acl_start + size_of::<ACL>()
                || ace_start
                    .checked_add(size_of::<ACE_HEADER>())
                    .is_none_or(|v| v > acl_end)
            {
                return Err(invalid(path, "ACE header is outside the DACL"));
            }
            let ace = unsafe { ptr::read_unaligned(raw_ace.cast::<ACE_HEADER>()) };
            let size = usize::from(ace.AceSize);
            if size < size_of::<ACE_HEADER>()
                || size % 4 != 0
                || ace_start.checked_add(size).is_none_or(|v| v > acl_end)
            {
                return Err(invalid(path, "ACE size is invalid or outside the DACL"));
            }
            // Preserve every byte and the original order, including unknown ACEs.
            aces.push(unsafe { std::slice::from_raw_parts(raw_ace.cast::<u8>(), size) }.to_vec());
        }
        Dacl::Present {
            revision: header.AclRevision,
            aces,
        }
    };
    Ok(DaclSnapshot {
        control,
        descriptor_revision,
        protected: control & SE_DACL_PROTECTED != 0,
        defaulted: control & SE_DACL_DEFAULTED != 0,
        dacl,
    })
}

fn validate_write_image(path: &str, image: &DaclSnapshot) -> Result<(), ObjectError> {
    if image.protected != (image.control & SE_DACL_PROTECTED != 0)
        || image.defaulted != (image.control & SE_DACL_DEFAULTED != 0)
        || !matches!(image.dacl, Dacl::Absent) != (image.control & SE_DACL_PRESENT != 0)
    {
        return Err(invalid(
            path,
            "requested fields disagree with descriptor control",
        ));
    }
    if !matches!(image.dacl, Dacl::Present { .. }) {
        return Err(unsupported(
            path,
            "absent/null DACL round-trip writes are not supported",
        ));
    }
    if image.descriptor_revision != SECURITY_DESCRIPTOR_REVISION || image.defaulted {
        return Err(unsupported(
            path,
            "descriptor revision/defaulted state cannot be restored by this writer",
        ));
    }
    // Ordinary NTFS objects can report control 0x8004 through GetSecurityInfo.
    // AUTO_INHERITED is not a prerequisite for that documented file API. Keep
    // its actual value in the requested image and require exact read-back; do
    // not assume that the setter preserved or normalized it successfully.
    if image.control & !SUPPORTED_CONTROL != 0 || image.control & SE_SELF_RELATIVE == 0 {
        return Err(unsupported(
            path,
            format!(
                "descriptor control {:#06x} is not safely round-trippable",
                image.control
            ),
        ));
    }
    Ok(())
}

fn build_acl(path: &str, image: &DaclSnapshot) -> Result<Vec<usize>, ObjectError> {
    let Dacl::Present { revision, aces } = &image.dacl else {
        return Err(unsupported(path, "only present non-null ACLs can be built"));
    };
    if !matches!(u32::from(*revision), ACL_REVISION | ACL_REVISION_DS) {
        return Err(unsupported(
            path,
            format!("ACL revision {revision} is not supported"),
        ));
    }
    let mut length = size_of::<ACL>();
    for ace in aces {
        if ace.len() < size_of::<ACE_HEADER>()
            || ace.len() % 4 != 0
            || usize::from(u16::from_le_bytes([ace[2], ace[3]])) != ace.len()
        {
            return Err(invalid(
                path,
                "ACE bytes do not match the declared aligned length",
            ));
        }
        length = length
            .checked_add(ace.len())
            .ok_or_else(|| invalid(path, "ACL length overflow"))?;
    }
    if length > usize::from(u16::MAX) || aces.len() > usize::from(u16::MAX) {
        return Err(invalid(
            path,
            "ACL length or ACE count exceeds the native limit",
        ));
    }
    let mut storage = vec![0usize; length.div_ceil(size_of::<usize>())];
    let acl = storage.as_mut_ptr().cast::<ACL>();
    if unsafe { InitializeAcl(acl, length as u32, u32::from(*revision)) } == 0 {
        return Err(native_error(path, "InitializeAcl"));
    }
    for ace in aces {
        // Append each complete entry without normalizing or reordering it.
        // Reject entries the native API cannot represent; never omit an unknown
        // ACE to make the resulting ACL writable.
        let mut aligned = vec![0usize; ace.len().div_ceil(size_of::<usize>())];
        unsafe {
            ptr::copy_nonoverlapping(ace.as_ptr(), aligned.as_mut_ptr().cast::<u8>(), ace.len())
        };
        if unsafe {
            AddAce(
                acl,
                u32::from(*revision),
                u32::MAX,
                aligned.as_ptr().cast(),
                ace.len() as u32,
            )
        } == 0
        {
            return Err(native_error(path, "AddAce"));
        }
    }
    if unsafe { IsValidAcl(acl) } == 0 {
        return Err(invalid(
            path,
            "ordered ACE bytes do not form a valid native ACL",
        ));
    }
    let header = unsafe { ptr::read(acl) };
    if header.AclRevision != *revision || usize::from(header.AceCount) != aces.len() {
        return Err(invalid(
            path,
            "native ACL construction changed revision or ACE count",
        ));
    }
    for (index, expected) in aces.iter().enumerate() {
        let mut entry = ptr::null_mut();
        if unsafe { GetAce(acl, index as u32, &mut entry) } == 0 {
            return Err(native_error(path, "GetAce(rebuilt ACL)"));
        }
        let header = unsafe { ptr::read_unaligned(entry.cast::<ACE_HEADER>()) };
        if usize::from(header.AceSize) != expected.len()
            || unsafe { std::slice::from_raw_parts(entry.cast::<u8>(), expected.len()) }
                != expected.as_slice()
        {
            return Err(invalid(
                path,
                "native ACL construction changed ordered ACE bytes",
            ));
        }
    }
    Ok(storage)
}
