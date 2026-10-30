// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for captured requests, streaming host I/O, and native preparation.

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::desktop::DesktopPolicy;
use super::identity::{refresh_logon_sandbox_creds, require_logon_sandbox_creds, SandboxCreds};
use super::ipc_framed::{
    self, EmptyPayload, FramedMessage, Message, OutputStream, ResizePayload, SpawnRequest,
    StdinPayload, IPC_PROTOCOL_VERSION,
};
use super::resolved_permissions::ResolvedWindowsSandboxPermissions;
use super::runner_client::{retry_runner_spawn_once, spawn_runner_transport};
use crate::boundary::NetworkBoundary;
use crate::protocol::{ControlMessage, SandboxRequest};

pub fn validate_request(request: &SandboxRequest) -> Result<()> {
    ResolvedWindowsSandboxPermissions::from_request(request).map(|_| ())
}

pub fn setup_is_complete(config_home: &Path) -> bool {
    super::identity::sandbox_setup_is_complete(config_home)
}

pub fn setup_matches_network(config_home: &Path, network: &NetworkBoundary) -> bool {
    let (proxy_ports, allow_local_binding) = match network {
        NetworkBoundary::LoopbackPorts {
            ports,
            allow_local_binding,
        } => (ports.clone(), *allow_local_binding),
        NetworkBoundary::Denied | NetworkBoundary::Allowed => (Vec::new(), false),
    };
    super::identity::sandbox_setup_is_complete_with_settings(
        config_home,
        &super::WindowsSandboxProvisioningSettings {
            proxy_ports,
            allow_local_binding,
        },
    )
}

/// Prepares native accounts and permissions without launching the requested command.
pub async fn prepare(request: SandboxRequest, config_home: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || Prepared::new(&request, &config_home).map(|_| ()))
        .await
        .context("native sandbox preparation worker failed")?
}

struct Prepared {
    permissions: ResolvedWindowsSandboxPermissions,
    cwd: PathBuf,
    env: HashMap<String, String>,
    read_roots: Vec<PathBuf>,
    deny_read: Vec<PathBuf>,
    deny_write: Vec<PathBuf>,
    proxy_enforced: bool,
    creds: SandboxCreds,
    cap_sids: Vec<String>,
}

impl Prepared {
    fn new(request: &SandboxRequest, config_home: &Path) -> Result<Self> {
        if !config_home.is_absolute() {
            bail!("Native sandbox configuration home must be absolute");
        }
        let permissions = ResolvedWindowsSandboxPermissions::from_request(request)?;
        let cwd = PathBuf::from(request.cwd.as_str());
        let mut env = request
            .env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        super::env::normalize_null_device_env(&mut env);
        super::env::ensure_non_interactive_pager(&mut env);
        super::env::inherit_path_env(&mut env);
        super::sandbox_utils::inject_git_safe_directory(&mut env, &cwd);
        let read_roots = super::setup::gather_read_roots(&cwd, &permissions, config_home);
        let deny_read = request
            .filesystem
            .deny_read
            .iter()
            .map(|path| PathBuf::from(path.as_str()))
            .collect::<Vec<_>>();
        let deny_write = request
            .filesystem
            .deny_write
            .iter()
            .map(|path| PathBuf::from(path.as_str()))
            .collect::<Vec<_>>();
        let proxy_enforced = matches!(request.network, NetworkBoundary::LoopbackPorts { .. });
        let creds = require_logon_sandbox_creds(
            &permissions,
            &cwd,
            &env,
            config_home,
            Some(&read_roots),
            false,
            None,
            &deny_read,
            &deny_write,
            proxy_enforced,
        )?;
        let caps = super::cap::load_or_create_cap_sids(config_home)?;
        let cap_sids = if permissions.uses_write_capabilities() {
            let roots = super::setup::effective_write_roots_for_permissions(
                &permissions,
                config_home,
                None,
            );
            let sids = roots
                .iter()
                .map(|root| super::cap::workspace_write_cap_sid_for_root(config_home, &cwd, root))
                .collect::<Result<Vec<_>>>()?;
            if sids.is_empty() {
                bail!("Workspace-write sandbox has no writable root capability SIDs");
            }
            sids
        } else {
            vec![caps.readonly]
        };
        let sid = super::token::LocalSid::from_string(&cap_sids[0])?;
        unsafe {
            super::acl::allow_null_device(sid.as_ptr());
        }
        Ok(Self {
            permissions,
            cwd,
            env,
            read_roots,
            deny_read,
            deny_write,
            proxy_enforced,
            creds,
            cap_sids,
        })
    }
}

/// The channel is borrowed from the wrapper after its initial request frame.
pub fn run(request: &SandboxRequest, config_home: &Path, channel: usize) -> Result<u32> {
    if request.command.is_empty() {
        bail!("Native sandbox request names no command");
    }
    let log_dir = config_home.join(".sandbox");
    super::sandbox_utils::ensure_config_home_exists(&log_dir)?;
    super::logging::log_start(&request.command, Some(&log_dir));
    let prepared = Prepared::new(request, config_home)?;
    let desktop_policy = DesktopPolicy::elevated(
        super::setup::SandboxSetupRequest {
            permissions: &prepared.permissions,
            command_cwd: &prepared.cwd,
            env_map: &prepared.env,
            config_home,
            proxy_enforced: prepared.proxy_enforced,
        },
        super::setup::SetupRootOverrides {
            read_roots: Some(prepared.read_roots.clone()),
            read_roots_include_platform_defaults: false,
            write_roots: None,
            deny_read_paths: Some(prepared.deny_read.clone()),
            deny_write_paths: Some(prepared.deny_write.clone()),
        },
        &prepared.cap_sids,
        request.network_route_capability_sid.as_deref(),
    )?;
    let spawn_request = SpawnRequest {
        command: request.command.clone(),
        cwd: prepared.cwd.clone(),
        env: prepared.env.clone(),
        permissions: prepared.permissions.clone(),
        config_home: log_dir.clone(),
        real_config_home: config_home.to_path_buf(),
        cap_sids: prepared.cap_sids.clone(),
        network_proxy_restricting_sid: request.network_route_capability_sid.clone(),
        timeout_ms: None,
        tty: request.terminal.is_some(),
        stdin_open: true,
        private_desktop_name: None,
    };
    let transport = retry_runner_spawn_once(
        prepared.creds,
        &request.command,
        |creds| {
            spawn_runner_transport(
                config_home,
                &prepared.cwd,
                &creds,
                Some(&log_dir),
                spawn_request.clone(),
                Some(&desktop_policy),
            )
        },
        || {
            refresh_logon_sandbox_creds(
                &prepared.permissions,
                &prepared.cwd,
                &prepared.env,
                config_home,
                Some(&prepared.read_roots),
                false,
                None,
                &prepared.deny_read,
                &prepared.deny_write,
                prepared.proxy_enforced,
            )
        },
    )?;
    let (writer, mut reader) = transport.into_files();
    let writer = Arc::new(Mutex::new(writer));
    if let Some(size) = request.terminal {
        send(
            &writer,
            Message::Resize {
                payload: ResizePayload {
                    rows: size.rows,
                    cols: size.cols,
                },
            },
        )?;
    }
    let tty = request.terminal.is_some();
    let control_writer = writer.clone();
    let control = Relay::spawn(move |done| {
        // Duplicate ownership is avoided: the wrapper retains the original channel.
        let mut pipe = std::mem::ManuallyDrop::new(unsafe { File::from_raw_handle(channel as _) });
        while !done.load(Ordering::Acquire) {
            let message = match crate::protocol::read_frame::<ControlMessage>(&mut *pipe) {
                Ok(Some(message)) => message,
                _ => {
                    if !done.load(Ordering::Acquire) {
                        let _ = send(
                            &control_writer,
                            Message::Terminate {
                                payload: EmptyPayload::default(),
                            },
                        );
                    }
                    break;
                }
            };
            let message = match message {
                ControlMessage::Resize(size) => Message::Resize {
                    payload: ResizePayload {
                        rows: size.rows,
                        cols: size.cols,
                    },
                },
                ControlMessage::Interrupt if tty => Message::Stdin {
                    payload: StdinPayload {
                        data_b64: ipc_framed::encode_bytes(&[3]),
                    },
                },
                ControlMessage::Interrupt | ControlMessage::Terminate => Message::Terminate {
                    payload: EmptyPayload::default(),
                },
            };
            if send(&control_writer, message).is_err() {
                break;
            }
        }
    })?;
    let input_writer = writer.clone();
    let input = Relay::spawn(move |done| {
        let mut input = std::io::stdin().lock();
        let mut buffer = [0u8; 8192];
        while !done.load(Ordering::Acquire) {
            match input.read(&mut buffer) {
                Ok(0) => {
                    let _ = send(
                        &input_writer,
                        Message::CloseStdin {
                            payload: EmptyPayload::default(),
                        },
                    );
                    break;
                }
                Ok(read) => {
                    if send(
                        &input_writer,
                        Message::Stdin {
                            payload: StdinPayload {
                                data_b64: ipc_framed::encode_bytes(&buffer[..read]),
                            },
                        },
                    )
                    .is_err()
                    {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
    })?;
    let result = (|| loop {
        let frame =
            ipc_framed::read_frame(&mut reader)?.context("Runner pipe closed before exit")?;
        match frame.message {
            Message::Output { payload } => {
                let bytes = ipc_framed::decode_bytes(&payload.data_b64)?;
                match payload.stream {
                    OutputStream::Stdout => {
                        let mut stream = std::io::stdout().lock();
                        stream.write_all(&bytes)?;
                        stream.flush()?;
                    }
                    OutputStream::Stderr => {
                        let mut stream = std::io::stderr().lock();
                        stream.write_all(&bytes)?;
                        stream.flush()?;
                    }
                }
            }
            Message::Exit { payload } => return Ok(payload.exit_code as u32),
            Message::Error { payload } => bail!("Runner error: {}", payload.message),
            Message::SpawnReady { .. } => {}
            other => bail!("Unexpected runner message during execution: {other:?}"),
        }
    })();
    Relay::stop_pair(input, control);
    drop(writer);
    match &result {
        Ok(0) => super::logging::log_success(&request.command, Some(&log_dir)),
        Ok(code) => super::logging::log_failure(
            &request.command,
            &format!("exit code {code}"),
            Some(&log_dir),
        ),
        Err(error) => {
            super::logging::log_failure(&request.command, &format!("{error:#}"), Some(&log_dir))
        }
    }
    super::runner_metrics::record_command(result.as_ref().ok().map(|code| (*code as i32, false)));
    result
}

fn send(writer: &Mutex<File>, message: Message) -> Result<()> {
    let mut writer = writer
        .lock()
        .map_err(|_| anyhow::anyhow!("Runner input lock was poisoned"))?;
    ipc_framed::write_frame(
        &mut *writer,
        &FramedMessage {
            version: IPC_PROTOCOL_VERSION,
            message,
        },
    )
}

struct Relay {
    thread: Option<std::thread::JoinHandle<()>>,
    handle: OwnedHandle,
    done: Arc<AtomicBool>,
}

impl Relay {
    fn spawn(run: impl FnOnce(Arc<AtomicBool>) + Send + 'static) -> Result<Self> {
        use native_windows_sys::Win32::{
            Foundation::DUPLICATE_SAME_ACCESS,
            System::Threading::{GetCurrentProcess, GetCurrentThread},
        };
        let done = Arc::new(AtomicBool::new(false));
        let worker_done = done.clone();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::spawn(move || {
            let mut handle = std::ptr::null_mut();
            let process = unsafe { GetCurrentProcess() };
            let result = if unsafe {
                native_windows_sys::Win32::Foundation::DuplicateHandle(
                    process,
                    GetCurrentThread(),
                    process,
                    &mut handle,
                    0,
                    0,
                    DUPLICATE_SAME_ACCESS,
                )
            } == 0
            {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
            };
            let ready = result.is_ok();
            if sender.send(result).is_ok() && ready {
                run(worker_done);
            }
        });
        let handle = receiver
            .recv()
            .context("Native relay exited before registration")??;
        Ok(Self {
            thread: Some(thread),
            handle,
            done,
        })
    }

    fn stop_pair(mut first: Self, mut second: Self) {
        first.done.store(true, Ordering::Release);
        second.done.store(true, Ordering::Release);
        loop {
            // Either relay may hold the writer mutex while the other waits for it.
            // Cancel both I/O operations again before waiting on either thread.
            first.cancel_io();
            second.cancel_io();
            let first_done = first.wait_finished(50);
            let second_done = second.wait_finished(0);
            if first_done && second_done {
                break;
            }
        }
        first.shutdown();
        second.shutdown();
    }

    fn cancel_io(&self) {
        unsafe {
            native_windows_sys::Win32::System::IO::CancelSynchronousIo(self.handle.as_raw_handle());
        }
    }

    fn wait_finished(&self, milliseconds: u32) -> bool {
        unsafe {
            native_windows_sys::Win32::System::Threading::WaitForSingleObject(
                self.handle.as_raw_handle(),
                milliseconds,
            ) != 258
        }
    }

    fn shutdown(&mut self) {
        self.done.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            loop {
                self.cancel_io();
                if self.wait_finished(50) {
                    break;
                }
            }
            let _ = thread.join();
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use native_windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use native_windows_sys::Win32::System::Pipes::CreatePipe;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn relay_shutdown_cancels_both_sides_of_a_blocked_writer() {
        let mut read = INVALID_HANDLE_VALUE;
        let mut write = INVALID_HANDLE_VALUE;
        assert_ne!(
            unsafe { CreatePipe(&mut read, &mut write, std::ptr::null(), 4096) },
            0
        );
        let read = unsafe { OwnedHandle::from_raw_handle(read) };
        let writer = Arc::new(Mutex::new(unsafe { File::from_raw_handle(write) }));
        let control_writer = writer.clone();
        let (locked_tx, locked_rx) = mpsc::sync_channel(1);
        let control = Relay::spawn(move |_| {
            let mut writer = control_writer.lock().unwrap();
            locked_tx.send(()).unwrap();
            let _ = writer.write_all(&[0; 8192]);
        })
        .unwrap();
        locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let input = Relay::spawn(move |_| {
            let mut writer = writer.lock().unwrap();
            let _ = writer.write_all(&[1]);
        })
        .unwrap();
        let (finished_tx, finished_rx) = mpsc::sync_channel(1);
        let shutdown = std::thread::spawn(move || {
            Relay::stop_pair(input, control);
            let _ = finished_tx.send(());
        });
        let result = finished_rx.recv_timeout(Duration::from_secs(5));
        // Release a blocked writer even when the assertion fails, so the fixture cleans up.
        drop(read);
        shutdown.join().unwrap();
        assert!(
            result.is_ok(),
            "relay shutdown waited on an uncanceled writer"
        );
    }
}
