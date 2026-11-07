// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to query native package identity independently of provisioning.

use anyhow::{ensure, Context, Result};
use native_windows_sys::Win32::Foundation::{
    APPMODEL_ERROR_NO_PACKAGE, ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS,
};
use native_windows_sys::Win32::Storage::Packaging::Appx::GetPackageFullName;
use native_windows_sys::Win32::System::Threading::GetCurrentProcess;

pub(crate) fn current_process_has_package_identity() -> Result<bool> {
    let mut length = 0;
    let process = unsafe { GetCurrentProcess() };
    let status = unsafe { GetPackageFullName(process, &mut length, std::ptr::null_mut()) };
    if status == APPMODEL_ERROR_NO_PACKAGE {
        return Ok(false);
    }
    ensure!(
        status == ERROR_INSUFFICIENT_BUFFER && length > 1 && length <= 32768,
        "Package name query failed: {status}"
    );
    let mut name = vec![0u16; length as usize];
    let status = unsafe { GetPackageFullName(process, &mut length, name.as_mut_ptr()) };
    ensure!(
        status == ERROR_SUCCESS && length > 1 && length as usize <= name.len(),
        "Package name read failed: {status}"
    );
    let name = name[..length as usize]
        .strip_suffix(&[0])
        .context("Unterminated package name")?;
    ensure!(!name.contains(&0), "Invalid package name");
    String::from_utf16(name)?;
    Ok(true)
}
