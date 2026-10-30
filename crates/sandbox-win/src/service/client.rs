// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for owned asynchronous pipe I/O and setup-only native service requests.

use std::io;
use std::mem::size_of;
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use std::ptr;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use native_windows_sys::Win32::Foundation::{
    ERROR_BROKEN_PIPE, ERROR_FILE_NOT_FOUND, ERROR_NO_DATA, ERROR_PIPE_BUSY,
    ERROR_PIPE_NOT_CONNECTED, ERROR_SERVICE_DOES_NOT_EXIST, ERROR_SERVICE_MARKED_FOR_DELETE,
    HANDLE,
};
use native_windows_sys::Win32::Storage::FileSystem::SECURITY_IMPERSONATION;
use native_windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;
use native_windows_sys::Win32::System::Services::{self, SC_HANDLE};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
use tokio::time::{sleep, timeout_at, Instant};

use super::protocol::{
    self, FramedMessage, Message, ProvisioningRequest, ProvisioningResponse, ProvisioningSettings,
    ProxyListeners, PIPE_NAME, PROTOCOL_VERSION, SANDBOX_GROUP_CHANGED, SERVICE_NAME,
};
use super::ProvisioningOutcome;

const PROVISIONING_TIMEOUT: Duration = Duration::from_secs(120);
const SERVICE_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_INTERVAL: Duration = Duration::from_millis(25);

/// Attempts installed-service provisioning; it does not install or start a service.
/// Dropping this future closes its owned pipe and cancels pending pipe I/O.
pub async fn provision(
    config_home: &Path,
    settings: ProvisioningSettings,
    listeners: ProxyListeners,
) -> Result<ProvisioningOutcome> {
    let request = FramedMessage {
        version: PROTOCOL_VERSION,
        message: Message::ProvisionSandboxRequest {
            payload: ProvisioningRequest {
                config_home: config_home
                    .to_str()
                    .context("sandbox provisioning home is not valid UTF-8")?
                    .to_owned(),
                settings,
                listeners,
            },
        },
    };
    let deadline = Instant::now() + PROVISIONING_TIMEOUT;
    let Some(mut pipe) = connect(deadline).await? else {
        return Ok(ProvisioningOutcome::Unavailable);
    };
    let response = match exchange_request(&mut pipe, &request, deadline).await {
        Ok(response) => response,
        Err(error) if is_unavailable_transport(&error) => {
            return Ok(ProvisioningOutcome::Unavailable);
        }
        Err(error) => return Err(error),
    };
    let response = if matches!(&response, ProvisioningResponse::Error { message } if message == SANDBOX_GROUP_CHANGED)
    {
        drop(pipe);
        retry_group_change(&request, deadline).await?
    } else {
        response
    };
    outcome(response)
}

fn outcome(response: ProvisioningResponse) -> Result<ProvisioningOutcome> {
    match response {
        ProvisioningResponse::Ok => Ok(ProvisioningOutcome::Provisioned),
        ProvisioningResponse::Unavailable => Ok(ProvisioningOutcome::Unavailable),
        ProvisioningResponse::Error { message } => bail!(message),
    }
}

fn is_unavailable_transport(error: &anyhow::Error) -> bool {
    error.downcast_ref::<io::Error>().is_some_and(|error| {
        matches!(
            error.kind(),
            io::ErrorKind::UnexpectedEof | io::ErrorKind::TimedOut
        ) || matches!(error.raw_os_error(), Some(code)
                if code == ERROR_BROKEN_PIPE as i32
                    || code == ERROR_NO_DATA as i32
                    || code == ERROR_PIPE_NOT_CONNECTED as i32)
    })
}

async fn retry_group_change(
    request: &FramedMessage,
    deadline: Instant,
) -> Result<ProvisioningResponse> {
    loop {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "refreshed sandbox service pipe is unavailable",
            )
            .into());
        }
        if let Some(mut pipe) = connect(deadline).await? {
            // Only the authenticated pre-dispatch refusal permits a retry. A lost
            // retry response may follow mutation and must remain an error.
            return exchange_request(&mut pipe, request, deadline).await;
        }
        sleep(RETRY_INTERVAL.min(deadline.saturating_duration_since(Instant::now()))).await;
    }
}

async fn exchange_request(
    pipe: &mut NamedPipeClient,
    request: &FramedMessage,
    deadline: Instant,
) -> Result<ProvisioningResponse> {
    verify_server(pipe.as_raw_handle() as HANDLE)
        .context("authenticate provisioning pipe server")?;
    if Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "sandbox provisioning deadline expired before sending request",
        )
        .into());
    }
    exchange_frame(pipe, request, deadline).await
}

async fn exchange_frame(
    pipe: &mut (impl AsyncRead + AsyncWrite + Unpin),
    request: &FramedMessage,
    deadline: Instant,
) -> Result<ProvisioningResponse> {
    let frame = protocol::encode_frame(request)?;
    timeout_at(deadline, async {
        pipe.write_all(&frame)
            .await
            .context("send sandbox provisioning request")?;
        pipe.flush()
            .await
            .context("send sandbox provisioning request")?;
        let mut header = [0; 4];
        pipe.read_exact(&mut header)
            .await
            .context("read sandbox provisioning response")?;
        let length = u32::from_le_bytes(header) as usize;
        protocol::check_length(length)?;
        let mut body = vec![0; length];
        pipe.read_exact(&mut body)
            .await
            .context("read sandbox provisioning response")?;
        protocol::response(
            serde_json::from_slice(&body).context("read sandbox provisioning response")?,
        )
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "sandbox provisioning deadline expired",
        )
    })?
}

async fn connect(deadline: Instant) -> Result<Option<NamedPipeClient>> {
    let startup_deadline = deadline.min(Instant::now() + SERVICE_STARTUP_TIMEOUT);
    loop {
        match query_service_status() {
            Ok(status) if status.dwCurrentState == Services::SERVICE_START_PENDING => {
                let remaining = startup_deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Ok(None);
                }
                sleep(remaining.min(RETRY_INTERVAL)).await;
                continue;
            }
            Ok(status) if status.dwCurrentState == Services::SERVICE_RUNNING => {}
            Ok(_) => return Ok(None),
            Err(error) if is_missing_service(&error) => return Ok(None),
            Err(error) => return Err(error),
        }
        match ClientOptions::new()
            .security_qos_flags(SECURITY_IMPERSONATION)
            .open(PIPE_NAME)
        {
            Ok(pipe) => return Ok(Some(pipe)),
            Err(error) if error.raw_os_error() == Some(ERROR_FILE_NOT_FOUND as i32) => {
                return Ok(None)
            }
            Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Ok(None);
                }
                // Reopen after an async wait: no blocking pipe waiter survives
                // cancellation of the setup operation.
                sleep(remaining.min(RETRY_INTERVAL)).await;
            }
            Err(error) => return Err(error).context("open sandbox provisioning pipe"),
        }
    }
}

fn is_missing_service(error: &anyhow::Error) -> bool {
    error.downcast_ref::<io::Error>().is_some_and(|error| {
        matches!(error.raw_os_error(), Some(code)
            if code == ERROR_SERVICE_DOES_NOT_EXIST as i32
                || code == ERROR_SERVICE_MARKED_FOR_DELETE as i32)
    })
}

fn verify_server(pipe: HANDLE) -> Result<()> {
    let mut pipe_process_id = 0;
    if unsafe { GetNamedPipeServerProcessId(pipe, &mut pipe_process_id) } == 0 {
        return Err(io::Error::last_os_error()).context("identify provisioning pipe server");
    }
    let status = query_service_status()?;
    verify_server_identity(status.dwCurrentState, status.dwProcessId, pipe_process_id)
}

fn verify_server_identity(state: u32, service_process_id: u32, pipe_process_id: u32) -> Result<()> {
    if state != Services::SERVICE_RUNNING
        || service_process_id == 0
        || service_process_id != pipe_process_id
    {
        bail!("the provisioning pipe server does not match the running service");
    }
    Ok(())
}

fn query_service_status() -> Result<Services::SERVICE_STATUS_PROCESS> {
    let manager =
        unsafe { Services::OpenSCManagerW(ptr::null(), ptr::null(), Services::SC_MANAGER_CONNECT) };
    if manager.is_null() {
        return Err(io::Error::last_os_error()).context("open service control manager");
    }
    let manager = ServiceHandle(manager);
    let service_name = crate::native::winutil::to_wide(SERVICE_NAME);
    let service = unsafe {
        Services::OpenServiceW(
            manager.0,
            service_name.as_ptr(),
            Services::SERVICE_QUERY_STATUS,
        )
    };
    if service.is_null() {
        return Err(io::Error::last_os_error()).context("open sandbox provisioning service");
    }
    let service = ServiceHandle(service);
    let mut status: Services::SERVICE_STATUS_PROCESS = unsafe { std::mem::zeroed() };
    let mut bytes_needed = 0;
    if unsafe {
        Services::QueryServiceStatusEx(
            service.0,
            Services::SC_STATUS_PROCESS_INFO,
            ptr::from_mut(&mut status).cast(),
            size_of::<Services::SERVICE_STATUS_PROCESS>() as u32,
            &mut bytes_needed,
        )
    } == 0
    {
        return Err(io::Error::last_os_error()).context("query sandbox provisioning service");
    }
    Ok(status)
}

struct ServiceHandle(SC_HANDLE);

impl Drop for ServiceHandle {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { Services::CloseServiceHandle(self.0) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_expected_transport_absence_is_unavailable() {
        for error in [
            io::Error::from(io::ErrorKind::UnexpectedEof),
            io::Error::from(io::ErrorKind::TimedOut),
            io::Error::from_raw_os_error(ERROR_BROKEN_PIPE as i32),
            io::Error::from_raw_os_error(ERROR_NO_DATA as i32),
            io::Error::from_raw_os_error(ERROR_PIPE_NOT_CONNECTED as i32),
        ] {
            assert!(is_unavailable_transport(&error.into()));
        }
        assert!(!is_unavailable_transport(
            &io::Error::from(io::ErrorKind::PermissionDenied).into()
        ));
        assert!(!is_unavailable_transport(&anyhow::anyhow!(
            "identity mismatch"
        )));
        assert!(outcome(ProvisioningResponse::Error {
            message: "authenticated denial".into()
        })
        .is_err());
        assert_eq!(
            outcome(ProvisioningResponse::Unavailable).unwrap(),
            ProvisioningOutcome::Unavailable
        );
    }

    #[test]
    fn pipe_identity_requires_the_running_nonzero_scm_process() {
        assert!(verify_server_identity(Services::SERVICE_RUNNING, 42, 42).is_ok());
        for (state, service_pid, pipe_pid) in [
            (Services::SERVICE_STOPPED, 42, 42),
            (Services::SERVICE_RUNNING, 0, 0),
            (Services::SERVICE_RUNNING, 42, 43),
        ] {
            assert!(verify_server_identity(state, service_pid, pipe_pid).is_err());
        }
    }

    #[tokio::test]
    async fn complete_exchange_preserves_authenticated_response_errors() {
        let request = FramedMessage {
            version: PROTOCOL_VERSION,
            message: Message::ProvisionSandboxResponse {
                payload: ProvisioningResponse::Unavailable,
            },
        };
        let (mut client, mut peer) = tokio::io::duplex(4096);
        let response = FramedMessage {
            version: PROTOCOL_VERSION,
            message: Message::ProvisionSandboxResponse {
                payload: ProvisioningResponse::Error {
                    message: "policy refused".into(),
                },
            },
        };
        let reply = protocol::encode_frame(&response).unwrap();
        let peer_task = tokio::spawn(async move {
            let mut header = [0; 4];
            peer.read_exact(&mut header).await.unwrap();
            let mut body = vec![0; u32::from_le_bytes(header) as usize];
            peer.read_exact(&mut body).await.unwrap();
            peer.write_all(&reply).await.unwrap();
        });
        let response = exchange_frame(
            &mut client,
            &request,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert!(outcome(response)
            .unwrap_err()
            .to_string()
            .contains("policy refused"));
        peer_task.await.unwrap();
    }

    #[tokio::test]
    async fn deadline_cancels_pending_io_and_closes_the_owned_connection() {
        let request = FramedMessage {
            version: PROTOCOL_VERSION,
            message: Message::ProvisionSandboxResponse {
                payload: ProvisioningResponse::Unavailable,
            },
        };
        let (mut client, mut peer) = tokio::io::duplex(1);
        let result = exchange_frame(&mut client, &request, Instant::now()).await;
        assert!(is_unavailable_transport(&result.unwrap_err()));
        drop(client);
        let mut remaining = [0; 2];
        let length = peer.read(&mut remaining).await.unwrap();
        assert!(length <= 1);
        assert_eq!(peer.read(&mut remaining).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn installed_service_probe_reports_actual_machine_state() {
        match query_service_status() {
            Ok(status) if status.dwCurrentState == Services::SERVICE_RUNNING => {
                // A running installation is not proof that it accepts this caller.
                // Authenticate any available pipe without provisioning resources.
                if let Some(pipe) = connect(Instant::now() + SERVICE_STARTUP_TIMEOUT)
                    .await
                    .unwrap()
                {
                    verify_server(pipe.as_raw_handle() as HANDLE).unwrap();
                }
            }
            Ok(status) if status.dwCurrentState != Services::SERVICE_START_PENDING => {
                assert!(connect(Instant::now()).await.unwrap().is_none());
            }
            Ok(_) => {}
            Err(error) if is_missing_service(&error) => {
                assert!(connect(Instant::now()).await.unwrap().is_none());
            }
            Err(error) => panic!("native service status query failed: {error:#}"),
        }
    }
}
