// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for native configuration-home naming and setup-only requests.

//! Bounded, versioned messages exchanged with the native provisioning service.

#[cfg(test)]
use std::io::{self, Read};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u8 = 1;
pub const SERVICE_NAME: &str = "KlausSandboxService";
pub const PIPE_NAME: &str = r"\\.\pipe\Klaus.SandboxProvisioning";
pub const SANDBOX_GROUP_CHANGED: &str = "sandbox group changed before authentication";
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FramedMessage {
    pub version: u8,
    #[serde(flatten)]
    pub message: Message,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    ProvisionSandboxRequest { payload: ProvisioningRequest },
    ProvisionSandboxResponse { payload: ProvisioningResponse },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProvisioningRequest {
    pub config_home: String,
    pub settings: ProvisioningSettings,
    pub listeners: ProxyListeners,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProvisioningSettings {
    pub proxy_ports: Vec<u16>,
    pub allow_local_binding: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProxyListeners {
    pub http_ports: Vec<u16>,
    pub socks_ports: Vec<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProvisioningResponse {
    Ok,
    Unavailable,
    Error { message: String },
}

/// Serializes one bounded message, including its little-endian frame header.
pub fn encode_frame(message: &FramedMessage) -> Result<Vec<u8>> {
    let body = serde_json::to_vec(message)?;
    check_length(body.len())?;
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

#[cfg(test)]
fn read_frame(mut reader: impl Read) -> Result<Option<FramedMessage>> {
    let mut header = [0; 4];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let length = u32::from_le_bytes(header) as usize;
    check_length(length)?;
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    Ok(Some(serde_json::from_slice(&body)?))
}

pub(crate) fn check_length(length: usize) -> Result<()> {
    if length > MAX_FRAME_BYTES {
        bail!("frame length {length} exceeds maximum allowed {MAX_FRAME_BYTES}");
    }
    Ok(())
}

#[cfg(any(windows, test))]
pub(crate) fn response(message: FramedMessage) -> Result<ProvisioningResponse> {
    if message.version != PROTOCOL_VERSION {
        return Ok(ProvisioningResponse::Unavailable);
    }
    match message.message {
        Message::ProvisionSandboxResponse { payload } => Ok(payload),
        Message::ProvisionSandboxRequest { .. } => {
            bail!("unexpected sandbox provisioning response message")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request() -> FramedMessage {
        FramedMessage {
            version: PROTOCOL_VERSION,
            message: Message::ProvisionSandboxRequest {
                payload: ProvisioningRequest {
                    config_home: "C:\\Users\\test\\.klaus".into(),
                    settings: ProvisioningSettings {
                        proxy_ports: vec![3128, 8081],
                        allow_local_binding: false,
                    },
                    listeners: ProxyListeners {
                        http_ports: vec![3128],
                        socks_ports: vec![8081],
                    },
                },
            },
        }
    }

    #[test]
    fn setup_wire_preserves_version_settings_roles_and_native_home() {
        let message = request();
        let encoded = encode_frame(&message).unwrap();
        assert_eq!(
            u32::from_le_bytes(encoded[..4].try_into().unwrap()) as usize,
            encoded.len() - 4
        );
        assert_eq!(
            read_frame(encoded.as_slice()).unwrap(),
            Some(message.clone())
        );
        assert_eq!(
            serde_json::to_value(message).unwrap(),
            json!({
                "version":1,"type":"provision_sandbox_request","payload":{
                    "config_home":"C:\\Users\\test\\.klaus",
                    "settings":{"proxy_ports":[3128,8081],"allow_local_binding":false},
                    "listeners":{"http_ports":[3128],"socks_ports":[8081]}
                }
            })
        );
        assert!(serde_json::from_value::<ProvisioningRequest>(json!({
            "config_home":"C:\\test","unexpected_field":true,
            "settings":{"proxy_ports":[],"allow_local_binding":false},
            "listeners":{"http_ports":[],"socks_ports":[]}
        }))
        .is_err());
    }

    #[test]
    fn mismatched_version_is_unavailable_but_unexpected_message_is_error() {
        let mut message = request();
        assert!(response(message.clone()).is_err());
        message.version += 1;
        assert_eq!(
            response(message).unwrap(),
            ProvisioningResponse::Unavailable
        );
    }

    #[test]
    fn frames_are_bounded_before_allocation_and_mid_body_eof_is_error() {
        let header = ((MAX_FRAME_BYTES + 1) as u32).to_le_bytes();
        assert!(read_frame(header.as_slice())
            .unwrap_err()
            .to_string()
            .contains("exceeds maximum"));
        assert!(read_frame([5, 0, 0, 0, b'{'].as_slice()).is_err());
        assert_eq!(read_frame([].as_slice()).unwrap(), None);
        assert_eq!(read_frame([1, 0].as_slice()).unwrap(), None);
    }
}
