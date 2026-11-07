// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0

use native_windows_sys::Win32::System::JobObjects::CreateJobObjectW;
use native_windows_sys::Win32::System::JobObjects::JobObjectExtendedLimitInformation;
use native_windows_sys::Win32::System::JobObjects::SetInformationJobObject;
use native_windows_sys::Win32::System::JobObjects::TerminateJobObject;
use native_windows_sys::Win32::System::JobObjects::JOBOBJECT_EXTENDED_LIMIT_INFORMATION;
use native_windows_sys::Win32::System::JobObjects::JOB_OBJECT_LIMIT_BREAKAWAY_OK;
use native_windows_sys::Win32::System::JobObjects::JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
use std::io;
use std::os::windows::io::AsRawHandle;
use std::os::windows::io::FromRawHandle;
use std::os::windows::io::OwnedHandle;
use std::os::windows::io::RawHandle;
use std::sync::Mutex;

/// Owns the Job Object containing a launched process tree.
#[derive(Debug)]
pub struct JobObject {
    handle: OwnedHandle,
    // Serialize the state check and native operation so preservation and
    // termination cannot race past one another.
    preserve_descendants: Mutex<bool>,
}

impl JobObject {
    /// Configures the job to terminate its members when the last handle closes.
    pub fn create() -> io::Result<Self> {
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateJobObjectW returned a new handle owned by this object.
        let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
        Self::set_limit_flags(
            &handle,
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_BREAKAWAY_OK,
        )?;
        Ok(Self {
            handle,
            preserve_descendants: Mutex::new(false),
        })
    }

    fn set_limit_flags(handle: &OwnedHandle, flags: u32) -> io::Result<()> {
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = flags;
        let configured = unsafe {
            SetInformationJobObject(
                handle.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                std::ptr::addr_of_mut!(limits).cast(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if configured == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Keeps descendants alive after a normal root-process exit.
    ///
    /// The first operation to acquire the lock determines whether descendants
    /// are preserved or terminated. Preservation disables explicit termination
    /// and kill-on-close for this object.
    pub fn preserve_descendants(&self) -> io::Result<()> {
        let mut preserve_descendants = self
            .preserve_descendants
            .lock()
            .map_err(|_| io::Error::other("job state lock poisoned"))?;
        if *preserve_descendants {
            return Ok(());
        }
        Self::set_limit_flags(&self.handle, JOB_OBJECT_LIMIT_BREAKAWAY_OK)?;
        *preserve_descendants = true;
        Ok(())
    }

    /// Terminates every process assigned to the job unless it was preserved.
    pub fn terminate(&self) -> io::Result<()> {
        let preserve_descendants = self
            .preserve_descendants
            .lock()
            .map_err(|_| io::Error::other("job state lock poisoned"))?;
        if *preserve_descendants {
            return Ok(());
        }
        let terminated = unsafe { TerminateJobObject(self.handle.as_raw_handle(), 1) };
        if terminated == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

impl AsRawHandle for JobObject {
    fn as_raw_handle(&self) -> RawHandle {
        self.handle.as_raw_handle()
    }
}
