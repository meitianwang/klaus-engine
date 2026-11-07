// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0 AND MIT
//
// Portions of this file are derived from https://github.com/wezterm/wezterm.
// Copyright (c) 2018-Present Wez Furlong
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! ConPTY creation and restricted-token process launch.

use crate::elevated::desktop::LaunchDesktop;
use crate::elevated::job::JobObject;
use crate::elevated::proc_thread_attr::ProcThreadAttributeList;
use crate::elevated::process::make_env_block;
use crate::elevated::winutil::format_last_error;
use crate::elevated::winutil::quote_windows_arg;
use crate::elevated::winutil::to_wide;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use native_windows_sys::Win32::Foundation::FreeLibrary;
use native_windows_sys::Win32::Foundation::GetLastError;
use native_windows_sys::Win32::Foundation::HANDLE;
use native_windows_sys::Win32::Foundation::HMODULE;
use native_windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
use native_windows_sys::Win32::Foundation::S_OK;
use native_windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
use native_windows_sys::Win32::System::Console::COORD;
use native_windows_sys::Win32::System::Console::HPCON;
use native_windows_sys::Win32::System::LibraryLoader::GetProcAddress;
use native_windows_sys::Win32::System::LibraryLoader::LoadLibraryW;
use native_windows_sys::Win32::System::Pipes::CreatePipe;
use native_windows_sys::Win32::System::Threading::CreateProcessAsUserW;
use native_windows_sys::Win32::System::Threading::CREATE_UNICODE_ENVIRONMENT;
use native_windows_sys::Win32::System::Threading::EXTENDED_STARTUPINFO_PRESENT;
use native_windows_sys::Win32::System::Threading::PROCESS_INFORMATION;
use native_windows_sys::Win32::System::Threading::STARTF_USESTDHANDLES;
use native_windows_sys::Win32::System::Threading::STARTUPINFOEXW;
use std::collections::HashMap;
use std::ffi::c_void;
use std::os::windows::io::AsRawHandle;
use std::os::windows::io::BorrowedHandle;
use std::os::windows::io::FromRawHandle;
use std::os::windows::io::IntoRawHandle;
use std::os::windows::io::OwnedHandle;
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;

const PSEUDOCONSOLE_RESIZE_QUIRK: u32 = 0x2;

type CreatePseudoConsoleFn =
    unsafe extern "system" fn(COORD, HANDLE, HANDLE, u32, *mut HPCON) -> i32;
type ClosePseudoConsoleFn = unsafe extern "system" fn(HPCON);

struct ConptyFunctions {
    module: HMODULE,
    create: CreatePseudoConsoleFn,
    close: ClosePseudoConsoleFn,
}

// The module and its immutable function addresses can be shared between threads.
unsafe impl Send for ConptyFunctions {}
unsafe impl Sync for ConptyFunctions {}

impl Drop for ConptyFunctions {
    fn drop(&mut self) {
        unsafe { FreeLibrary(self.module) };
    }
}

impl ConptyFunctions {
    fn load() -> Result<Self> {
        let module = unsafe { LoadLibraryW(to_wide("kernel32.dll").as_ptr()) };
        if module.is_null() {
            return Err(std::io::Error::last_os_error()).context("load console library");
        }
        let functions = (|| {
            let create = unsafe { GetProcAddress(module, c"CreatePseudoConsole".as_ptr().cast()) }
                .context("CreatePseudoConsole is unavailable")?;
            // The console API is loaded as one supported function group.
            unsafe { GetProcAddress(module, c"ResizePseudoConsole".as_ptr().cast()) }
                .context("ResizePseudoConsole is unavailable")?;
            let close = unsafe { GetProcAddress(module, c"ClosePseudoConsole".as_ptr().cast()) }
                .context("ClosePseudoConsole is unavailable")?;
            // SAFETY: these exports have the documented Windows console signatures.
            Ok(Self {
                module,
                create: unsafe {
                    std::mem::transmute::<unsafe extern "system" fn() -> isize, CreatePseudoConsoleFn>(
                        create,
                    )
                },
                close: unsafe {
                    std::mem::transmute::<unsafe extern "system" fn() -> isize, ClosePseudoConsoleFn>(
                        close,
                    )
                },
            })
        })();
        if functions.is_err() {
            unsafe { FreeLibrary(module) };
        }
        functions
    }
}

fn conpty_functions() -> &'static ConptyFunctions {
    static FUNCTIONS: LazyLock<ConptyFunctions> = LazyLock::new(|| {
        ConptyFunctions::load().unwrap_or_else(|error| {
            panic!(
                "this system does not support conpty. Windows 10 October 2018 or newer is required: {error}"
            )
        })
    });
    &FUNCTIONS
}

struct Pseudoconsole {
    handle: HPCON,
    // Retain both creation handles for the entire externally attached lifetime.
    _creation_handles: (OwnedHandle, OwnedHandle),
}

impl Drop for Pseudoconsole {
    fn drop(&mut self) {
        unsafe { (conpty_functions().close)(self.handle) };
    }
}

fn create_pipe() -> Result<(OwnedHandle, OwnedHandle)> {
    let attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: std::ptr::null_mut(),
        bInheritHandle: 0,
    };
    let mut read = INVALID_HANDLE_VALUE;
    let mut write = INVALID_HANDLE_VALUE;
    let created = unsafe { CreatePipe(&mut read, &mut write, &attributes, 0) };
    if created == 0 {
        return Err(std::io::Error::last_os_error()).context("create console pipe");
    }
    // SAFETY: successful CreatePipe transfers two new handles to this caller.
    Ok(unsafe {
        (
            OwnedHandle::from_raw_handle(read),
            OwnedHandle::from_raw_handle(write),
        )
    })
}

fn create_console() -> Result<(Pseudoconsole, OwnedHandle, OwnedHandle)> {
    let (input_read, input_write) = create_pipe()?;
    let (output_read, output_write) = create_pipe()?;
    let mut handle = INVALID_HANDLE_VALUE as HPCON;
    let result = unsafe {
        (conpty_functions().create)(
            COORD { X: 80, Y: 24 },
            input_read.as_raw_handle(),
            output_write.as_raw_handle(),
            PSEUDOCONSOLE_RESIZE_QUIRK,
            &mut handle,
        )
    };
    ensure!(
        result == S_OK,
        "failed to create psuedo console: HRESULT {result}"
    );
    Ok((
        Pseudoconsole {
            handle,
            _creation_handles: (input_read, output_write),
        },
        input_write,
        output_read,
    ))
}

/// Owns the pseudoconsole, its pipe ends, and the launched process job.
pub struct ConptyInstance {
    pseudoconsole: Option<Pseudoconsole>,
    input_write: Option<OwnedHandle>,
    output_read: Option<OwnedHandle>,
    job: Option<Arc<JobObject>>,
    _desktop: Option<LaunchDesktop>,
}

impl Drop for ConptyInstance {
    fn drop(&mut self) {
        drop(self.input_write.take());
        drop(self.output_read.take());
        drop(self.pseudoconsole.take());
    }
}

impl ConptyInstance {
    pub fn raw_handle(&self) -> Option<HPCON> {
        self.pseudoconsole.as_ref().map(|console| console.handle)
    }

    pub fn take_input_write(&mut self) -> HANDLE {
        self.input_write
            .take()
            .map_or(std::ptr::null_mut(), IntoRawHandle::into_raw_handle)
    }

    pub fn take_output_read(&mut self) -> HANDLE {
        self.output_read
            .take()
            .map_or(std::ptr::null_mut(), IntoRawHandle::into_raw_handle)
    }

    pub fn job(&self) -> Option<Arc<JobObject>> {
        self.job.as_ref().map(Arc::clone)
    }
}

/// Starts a process under the supplied primary token with ConPTY attached.
pub fn spawn_conpty_process_as_user(
    h_token: BorrowedHandle<'_>,
    argv: &[String],
    cwd: &Path,
    env_map: &HashMap<String, String>,
    desktop: LaunchDesktop,
) -> Result<(PROCESS_INFORMATION, ConptyInstance)> {
    let cmdline_str = argv
        .iter()
        .map(|arg| quote_windows_arg(arg))
        .collect::<Vec<_>>()
        .join(" ");
    let mut cmdline = to_wide(&cmdline_str);
    let env_block = make_env_block(env_map);
    let mut startup: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
    startup.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = INVALID_HANDLE_VALUE;
    startup.StartupInfo.hStdOutput = INVALID_HANDLE_VALUE;
    startup.StartupInfo.hStdError = INVALID_HANDLE_VALUE;
    startup.StartupInfo.lpDesktop = desktop.startup_info_desktop();
    let job = Arc::new(JobObject::create().context("create process job")?);
    let (pseudoconsole, input_write, output_read) = create_console()?;
    let hpc = pseudoconsole.handle;
    let conpty = ConptyInstance {
        pseudoconsole: Some(pseudoconsole),
        input_write: Some(input_write),
        output_read: Some(output_read),
        job: Some(Arc::clone(&job)),
        _desktop: Some(desktop),
    };
    let preserve_app_context =
        crate::elevated::app_identity::current_process_has_package_identity()?;
    let mut attributes = ProcThreadAttributeList::new(2 + u32::from(preserve_app_context))?;
    attributes.set_pseudoconsole(hpc)?;
    attributes.set_job(job.as_raw_handle())?;
    if preserve_app_context {
        attributes.preserve_desktop_app_context()?;
    }
    startup.lpAttributeList = attributes.as_mut_ptr();

    let mut process: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
    let created = unsafe {
        CreateProcessAsUserW(
            h_token.as_raw_handle(),
            std::ptr::null(),
            cmdline.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT,
            env_block.as_ptr() as *mut c_void,
            to_wide(cwd).as_ptr(),
            &startup.StartupInfo,
            &mut process,
        )
    };
    if created == 0 {
        let error = unsafe { GetLastError() } as i32;
        let message = format!(
            "CreateProcessAsUserW failed: {} ({}) | cwd={} | cmd={} | env_u16_len={}",
            error,
            format_last_error(error),
            cwd.display(),
            cmdline_str,
            env_block.len()
        );
        return Err(std::io::Error::from_raw_os_error(error)).context(message);
    }
    Ok((process, conpty))
}
