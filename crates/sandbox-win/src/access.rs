//! Read-only DACL checks using the same AppContainer SID and capabilities as launch.
//!
//! A successful host access check is insufficient: AppContainer access also needs
//! the package/capability grant. Create a real LowBox token rather than synthesizing
//! groups on the host token. This checks discretionary access, not every condition
//! of a future object open (such as mandatory integrity or a replaced path).

use std::ffi::c_void;
use std::mem::{size_of, transmute};
use std::ptr;

use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, LocalFree, ERROR_FILE_NOT_FOUND, ERROR_INSUFFICIENT_BUFFER,
    ERROR_INVALID_SECURITY_DESCR, ERROR_PATH_NOT_FOUND, ERROR_SUCCESS, FALSE, HANDLE, HLOCAL, PSID,
};
use windows_sys::Win32::Security::Authorization::{
    GetNamedSecurityInfoW, GetSecurityInfo, SE_FILE_OBJECT, SE_OBJECT_TYPE,
};
use windows_sys::Win32::Security::{
    AccessCheck, DuplicateTokenEx, IsValidSecurityDescriptor, MapGenericMask,
    SecurityIdentification, TokenImpersonation, DACL_SECURITY_INFORMATION, GENERIC_MAPPING,
    GROUP_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION, PRIVILEGE_SET, PSECURITY_DESCRIPTOR,
    SID_AND_ATTRIBUTES, TOKEN_DUPLICATE, TOKEN_QUERY,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ALL_ACCESS, FILE_GENERIC_EXECUTE, FILE_GENERIC_READ, FILE_GENERIC_WRITE, WRITE_DAC,
};
use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

use crate::capability::CapabilitySid;
use crate::identity::AppContainerSid;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AccessError {
    #[error("{operation} for an AppContainer access check failed: Win32 error {code}")]
    Win32 { operation: &'static str, code: u32 },
    #[error("creating an AppContainer access-check token failed: NTSTATUS {0:#010x}")]
    LowBox(i32),
    #[error("reading access-check security for {path} failed: Win32 error {code}")]
    Read { path: String, code: u32 },
    #[error("the AppContainer access-check privilege buffer exceeded its limit")]
    PrivilegeBuffer,
    #[error("the AppContainer access-check capability count is out of range")]
    CapabilityCount,
}

fn last_error(operation: &'static str) -> AccessError {
    AccessError::Win32 {
        operation,
        code: unsafe { GetLastError() },
    }
}

struct Token(HANDLE);

impl Drop for Token {
    fn drop(&mut self) {
        if self.0 != 0 {
            unsafe { CloseHandle(self.0) };
        }
    }
}

pub(crate) struct Descriptor(pub(crate) PSECURITY_DESCRIPTOR);

impl Drop for Descriptor {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { LocalFree(self.0 as HLOCAL) };
        }
    }
}

// Microsoft documents this ABI but supplies no import library. ObjectAttributes
// is optional; no named object handles or inheritable handles are supplied.
type CreateLowBoxToken = unsafe extern "system" fn(
    *mut HANDLE,
    HANDLE,
    u32,
    *const c_void,
    PSID,
    u32,
    *const SID_AND_ATTRIBUTES,
    u32,
    *const HANDLE,
) -> i32;

/// Query-only impersonation token; never assigned to the caller's thread.
pub(crate) struct TargetAccess(Token);

/// The ordinary caller's query-only token, separate from the AppContainer token.
pub(crate) struct HostAccess(Token);

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccessDecision {
    pub(crate) allowed: bool,
    pub(crate) granted: u32,
}

impl HostAccess {
    pub(crate) fn new() -> Result<Self, AccessError> {
        let mut host = Token(0);
        if unsafe {
            OpenProcessToken(
                GetCurrentProcess(),
                TOKEN_QUERY | TOKEN_DUPLICATE,
                &mut host.0,
            )
        } == FALSE
        {
            return Err(last_error("opening the host token"));
        }
        let mut query = Token(0);
        if unsafe {
            DuplicateTokenEx(
                host.0,
                TOKEN_QUERY,
                ptr::null(),
                SecurityIdentification,
                TokenImpersonation,
                &mut query.0,
            )
        } == FALSE
        {
            return Err(last_error("duplicating the host query token"));
        }
        Ok(Self(query))
    }

    /// Check a borrowed descriptor using the ordinary host token.
    ///
    /// # Safety
    /// The descriptor must remain valid throughout this call.
    pub(crate) unsafe fn check_descriptor(
        &self,
        descriptor: PSECURITY_DESCRIPTOR,
        desired: u32,
        mapping: GENERIC_MAPPING,
    ) -> Result<AccessDecision, AccessError> {
        check_descriptor(self.0 .0, descriptor, desired, mapping)
    }

    /// Whether this ordinary caller holds WRITE_DAC on `path` and can therefore
    /// install an ACE there. A missing path answers `true` so callers keep their
    /// own missing-path behavior.
    pub(crate) fn can_write_dacl(&self, path: &str) -> Result<bool, AccessError> {
        let descriptor = match read_path_descriptor(path) {
            Ok(descriptor) => descriptor,
            Err(AccessError::Read {
                code: ERROR_FILE_NOT_FOUND | ERROR_PATH_NOT_FOUND,
                ..
            }) => return Ok(true),
            Err(error) => return Err(error),
        };
        let decision = unsafe { self.check_descriptor(descriptor.0, WRITE_DAC, file_mapping()) }?;
        Ok(decision.allowed)
    }
}

pub(crate) fn file_mapping() -> GENERIC_MAPPING {
    GENERIC_MAPPING {
        GenericRead: FILE_GENERIC_READ,
        GenericWrite: FILE_GENERIC_WRITE,
        GenericExecute: FILE_GENERIC_EXECUTE,
        GenericAll: FILE_ALL_ACCESS,
    }
}

pub(crate) fn read_path_descriptor(path: &str) -> Result<Descriptor, AccessError> {
    let wide: Vec<u16> = path.encode_utf16().chain(std::iter::once(0)).collect();
    let mut descriptor = Descriptor(ptr::null_mut());
    let code = unsafe {
        GetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut descriptor.0,
        )
    };
    if code != ERROR_SUCCESS {
        return Err(AccessError::Read {
            path: path.to_owned(),
            code,
        });
    }
    Ok(descriptor)
}

pub(crate) fn read_handle_descriptor(
    handle: HANDLE,
    kind: SE_OBJECT_TYPE,
) -> Result<Descriptor, AccessError> {
    let mut descriptor = Descriptor(ptr::null_mut());
    let code = unsafe {
        GetSecurityInfo(
            handle,
            kind,
            OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut descriptor.0,
        )
    };
    if code != ERROR_SUCCESS {
        return Err(AccessError::Win32 {
            operation: "reading object security",
            code,
        });
    }
    Ok(descriptor)
}

impl TargetAccess {
    pub(crate) fn new(
        container: &AppContainerSid,
        capabilities: &[CapabilitySid],
    ) -> Result<Self, AccessError> {
        let mut host = Token(0);
        // Creating a restricted derivative needs duplication access to this
        // process's token. The final access-check handle remains query-only.
        if unsafe {
            OpenProcessToken(
                GetCurrentProcess(),
                TOKEN_QUERY | TOKEN_DUPLICATE,
                &mut host.0,
            )
        } == FALSE
        {
            return Err(last_error("opening the host token"));
        }

        // Ntdll is already loaded for the process lifetime. Looking up that exact
        // module does not search the workspace for a loadable library.
        let name: Vec<u16> = "ntdll.dll\0".encode_utf16().collect();
        let module = unsafe { GetModuleHandleW(name.as_ptr()) };
        if module == 0 {
            return Err(last_error("finding ntdll"));
        }
        let entry = unsafe { GetProcAddress(module, c"NtCreateLowBoxToken".as_ptr().cast()) }
            .ok_or_else(|| last_error("finding NtCreateLowBoxToken"))?;
        let create =
            unsafe { transmute::<unsafe extern "system" fn() -> isize, CreateLowBoxToken>(entry) };

        let entries: Vec<SID_AND_ATTRIBUTES> = capabilities
            .iter()
            .map(|capability| SID_AND_ATTRIBUTES {
                Sid: capability.as_psid(),
                // SE_GROUP_ENABLED, identical to the launch attributes.
                Attributes: 0x0000_0004,
            })
            .collect();
        let count = u32::try_from(entries.len()).map_err(|_| AccessError::CapabilityCount)?;
        let mut lowbox = Token(0);
        let status = unsafe {
            create(
                &mut lowbox.0,
                host.0,
                TOKEN_QUERY | TOKEN_DUPLICATE,
                ptr::null(),
                container.as_psid(),
                count,
                if entries.is_empty() {
                    ptr::null()
                } else {
                    entries.as_ptr()
                },
                0,
                ptr::null(),
            )
        };
        if status < 0 {
            return Err(AccessError::LowBox(status));
        }

        let mut query = Token(0);
        if unsafe {
            DuplicateTokenEx(
                lowbox.0,
                TOKEN_QUERY,
                ptr::null(),
                SecurityIdentification,
                TokenImpersonation,
                &mut query.0,
            )
        } == FALSE
        {
            return Err(last_error("duplicating the AppContainer token"));
        }
        Ok(Self(query))
    }

    /// None denotes a missing path, matching the ACL planner's missing-path rule.
    pub(crate) fn allows_path(
        &self,
        path: &str,
        desired: u32,
    ) -> Result<Option<bool>, AccessError> {
        match read_path_descriptor(path) {
            Ok(descriptor) => unsafe { self.check_descriptor(descriptor.0, desired) }
                .map(|result| Some(result.allowed)),
            Err(AccessError::Read {
                code: ERROR_FILE_NOT_FOUND | ERROR_PATH_NOT_FOUND,
                ..
            }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Check a borrowed descriptor using the real AppContainer token.
    ///
    /// # Safety
    /// The descriptor must remain valid throughout this call.
    pub(crate) unsafe fn check_descriptor(
        &self,
        descriptor: PSECURITY_DESCRIPTOR,
        desired: u32,
    ) -> Result<AccessDecision, AccessError> {
        check_descriptor(self.0 .0, descriptor, desired, file_mapping())
    }
}

unsafe fn check_descriptor(
    token: HANDLE,
    descriptor: PSECURITY_DESCRIPTOR,
    mut desired: u32,
    mapping: GENERIC_MAPPING,
) -> Result<AccessDecision, AccessError> {
    if descriptor.is_null() || unsafe { IsValidSecurityDescriptor(descriptor) } == FALSE {
        return Err(AccessError::Win32 {
            operation: "validating the queried security descriptor",
            code: ERROR_INVALID_SECURITY_DESCR,
        });
    }
    unsafe { MapGenericMask(&mut desired, &mapping) };
    // usize storage supplies alignment for PRIVILEGE_SET and its variable
    // LUID_AND_ATTRIBUTES array. The OS cannot cause an unbounded allocation.
    let mut storage = vec![0usize; size_of::<PRIVILEGE_SET>().div_ceil(size_of::<usize>())];
    loop {
        let mut bytes = (storage.len() * size_of::<usize>()) as u32;
        let mut granted = 0;
        let mut allowed = FALSE;
        let ok = unsafe {
            AccessCheck(
                descriptor,
                token,
                desired,
                &mapping,
                storage.as_mut_ptr().cast(),
                &mut bytes,
                &mut granted,
                &mut allowed,
            )
        };
        if ok != FALSE {
            return Ok(AccessDecision {
                allowed: allowed != FALSE && granted & desired == desired,
                granted,
            });
        }
        let code = unsafe { GetLastError() };
        if code != ERROR_INSUFFICIENT_BUFFER {
            return Err(AccessError::Win32 {
                operation: "checking the target DACL",
                code,
            });
        }
        if bytes as usize <= storage.len() * size_of::<usize>() || bytes > 65_536 {
            return Err(AccessError::PrivilegeBuffer);
        }
        storage.resize((bytes as usize).div_ceil(size_of::<usize>()), 0);
    }
}
