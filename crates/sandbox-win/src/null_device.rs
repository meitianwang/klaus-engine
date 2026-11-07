//! Access to `NUL` for the sandboxed process.
//!
//! # Why this exists at all
//!
//! An AppContainer cannot open the null device. Measured on a real machine: every
//! spelling of it — `NUL`, `\\.\NUL`, `\\?\NUL`, and `\\?\GLOBALROOT\Device\Null`,
//! which bypasses the DOS device map entirely — fails with `ERROR_ACCESS_DENIED`,
//! while `CONOUT$` opens fine. So it is the device object's own DACL: it names
//! `Everyone`, and an AppContainer's access is additionally gated on an ACE for an
//! app-package or capability SID, which `\Device\Null` has none of.
//!
//! That would be a curiosity if `NUL` were rare. It is not: `>nul` and `2>nul` are
//! everywhere in Windows shell usage, including the `.cmd` shims that front `npm`,
//! `yarn` and most Node tooling. A sandbox where those fail is a sandbox where most
//! real commands fail, and they fail as "access denied" with no indication that the
//! bit bucket was the problem.
//!
//! # Why granting it is not a hole
//!
//! The access granted here is read and write on the null device: reads return
//! end-of-file and writes are discarded. It carries no information in either
//! direction, which is why Windows already grants it to `Everyone`. The ACE names
//! this one container rather than ALL APPLICATION PACKAGES, so nothing else on the
//! machine gains anything.
//!
//! The device object's security is in-memory, so it resets when the machine reboots.
//! That makes this idempotent setup work rather than a one-time install step, and it
//! is why teardown removes the ACE: otherwise a machine that runs many workspaces
//! accumulates one ACE per container until the next boot.

#[cfg(windows)]
mod imp {
    use std::ptr;

    use windows_sys::Win32::Foundation::{
        CloseHandle, GetLastError, LocalFree, ERROR_ACCESS_DENIED, ERROR_SUCCESS, GENERIC_WRITE,
        HANDLE, HLOCAL, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::Security::Authorization::{
        GetSecurityInfo, SetEntriesInAclW, SetSecurityInfo, ACCESS_MODE, EXPLICIT_ACCESS_W,
        GRANT_ACCESS, NO_MULTIPLE_TRUSTEE, REVOKE_ACCESS, SE_KERNEL_OBJECT, TRUSTEE_IS_SID,
        TRUSTEE_IS_UNKNOWN, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{ACL, DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR};
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };

    use crate::identity::AppContainerSid;

    /// Why the null device's ACL could not be changed.
    #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
    pub enum NullDeviceError {
        #[error("opening the null device failed: Win32 error {0}")]
        Open(u32),
        #[error("reading the null device's security failed: Win32 error {0}")]
        Read(u32),
        #[error("building the null device's ACL failed: Win32 error {0}")]
        Build(u32),
        #[error("writing the null device's security failed: Win32 error {0}")]
        Write(u32),
    }

    impl NullDeviceError {
        /// Whether this is the expected ordinary-user outcome: the caller holds
        /// no WRITE_DAC on the device object, so the ACE cannot exist. Callers
        /// may degrade instead of failing setup; every other error is not.
        pub fn is_dacl_write_denied(&self) -> bool {
            matches!(self, Self::Open(ERROR_ACCESS_DENIED))
        }
    }

    /// From winnt.h; these standard rights are not re-exported by this binding.
    const READ_CONTROL: u32 = 0x0002_0000;
    const WRITE_DAC: u32 = 0x0004_0000;

    /// Read and write on the bit bucket, plus the synchronize bit every handle needs.
    const READ_WRITE: u32 = 0x0012_019f;

    /// Owned handle to the device, closed on drop.
    struct Device(HANDLE);

    impl Drop for Device {
        fn drop(&mut self) {
            if self.0 != INVALID_HANDLE_VALUE && self.0 != 0 {
                unsafe { CloseHandle(self.0) };
            }
        }
    }

    /// Open `\Device\Null` with enough access to read and rewrite its DACL.
    ///
    /// The `GLOBALROOT` form is used rather than `NUL` because it names the device
    /// object directly, without going through the caller's DOS device map.
    fn open() -> Result<Device, NullDeviceError> {
        let path: Vec<u16> = "\\\\?\\GLOBALROOT\\Device\\Null"
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                READ_CONTROL | WRITE_DAC | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                ptr::null(),
                OPEN_EXISTING,
                0,
                0,
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(NullDeviceError::Open(unsafe { GetLastError() }));
        }
        Ok(Device(handle))
    }

    /// Add or remove one ACE for `sid` on the null device.
    fn change(sid: &AppContainerSid, mode: ACCESS_MODE) -> Result<(), NullDeviceError> {
        let device = open()?;

        let mut existing: *mut ACL = ptr::null_mut();
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        let code = unsafe {
            GetSecurityInfo(
                device.0,
                SE_KERNEL_OBJECT,
                DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                &mut existing,
                ptr::null_mut(),
                &mut descriptor,
            )
        };
        if code != ERROR_SUCCESS {
            return Err(NullDeviceError::Read(code));
        }
        // Freed however this function exits.
        struct Owned(PSECURITY_DESCRIPTOR);
        impl Drop for Owned {
            fn drop(&mut self) {
                if !self.0.is_null() {
                    unsafe { LocalFree(self.0 as HLOCAL) };
                }
            }
        }
        let _descriptor = Owned(descriptor);

        let access = EXPLICIT_ACCESS_W {
            grfAccessPermissions: if mode == REVOKE_ACCESS { 0 } else { READ_WRITE },
            grfAccessMode: mode,
            // A device object has no children to inherit anything.
            grfInheritance: 0,
            Trustee: TRUSTEE_W {
                pMultipleTrustee: ptr::null_mut(),
                MultipleTrusteeOperation: NO_MULTIPLE_TRUSTEE,
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_UNKNOWN,
                ptstrName: sid.as_psid() as *mut u16,
            },
        };
        let mut merged: *mut ACL = ptr::null_mut();
        let code = unsafe { SetEntriesInAclW(1, &access, existing, &mut merged) };
        if code != ERROR_SUCCESS {
            return Err(NullDeviceError::Build(code));
        }
        struct OwnedAcl(*mut ACL);
        impl Drop for OwnedAcl {
            fn drop(&mut self) {
                if !self.0.is_null() {
                    unsafe { LocalFree(self.0 as HLOCAL) };
                }
            }
        }
        let merged = OwnedAcl(merged);

        let code = unsafe {
            SetSecurityInfo(
                device.0,
                SE_KERNEL_OBJECT,
                DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                merged.0,
                ptr::null_mut(),
            )
        };
        if code != ERROR_SUCCESS {
            return Err(NullDeviceError::Write(code));
        }
        Ok(())
    }

    /// Let this container open `NUL`.
    pub fn grant(sid: &AppContainerSid) -> Result<(), NullDeviceError> {
        change(sid, GRANT_ACCESS)
    }

    /// Take this container's access to `NUL` back off.
    pub fn revoke(sid: &AppContainerSid) -> Result<(), NullDeviceError> {
        change(sid, REVOKE_ACCESS)
    }
}

#[cfg(windows)]
pub use imp::{grant, revoke, NullDeviceError};
