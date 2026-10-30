// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for native request paths, helper packaging, and execution lifecycle.

//! Framed IPC protocol used between the parent (CLI) and the elevated command runner.
//!
//! This module defines the JSON message schema (spawn request/ready, output, stdin,
//! exit, error, terminate) plus length‑prefixed framing helpers for a byte stream.
//! It is **elevated-path only**: the parent uses it to bootstrap the runner and
//! stream unified_exec I/O over named pipes. The legacy restricted‑token path does
//! not use this protocol, and non‑unified exec capture uses it only when running
//! through the elevated runner.

use super::resolved_permissions::ResolvedWindowsSandboxPermissions;
use anyhow::Result;
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;

/// Protocol version shared by the parent process and elevated command runner.
pub const IPC_PROTOCOL_VERSION: u8 = 6;

/// Length-prefixed, JSON-encoded frame.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FramedMessage {
    pub version: u8,
    #[serde(flatten)]
    pub message: Message,
}

/// IPC message variants exchanged between parent and runner.
///
/// `SpawnRequest`, `Stdin`, `CloseStdin`, `Resize`, and `Terminate` are parent->runner commands.
/// `SpawnReady`, `Output`, `Exit`, and `Error` are runner->parent events/results.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    SpawnRequest { payload: Box<SpawnRequest> },
    SpawnReady { payload: SpawnReady },
    Output { payload: OutputPayload },
    Stdin { payload: StdinPayload },
    CloseStdin { payload: EmptyPayload },
    Resize { payload: ResizePayload },
    Exit { payload: ExitPayload },
    Error { payload: ErrorPayload },
    Terminate { payload: EmptyPayload },
}

/// Spawn parameters sent from parent to runner.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpawnRequest {
    pub command: Vec<String>,
    pub cwd: PathBuf,
    pub env: HashMap<String, String>,
    pub permissions: ResolvedWindowsSandboxPermissions,
    pub config_home: PathBuf,
    pub real_config_home: PathBuf,
    pub cap_sids: Vec<String>,
    /// Optional managed-network identity added only to the child's restricting SID set.
    #[serde(default)]
    pub network_proxy_restricting_sid: Option<String>,
    pub timeout_ms: Option<u64>,
    pub tty: bool,
    #[serde(default)]
    pub stdin_open: bool,
    /// Private desktop kept alive by the parent across command runners.
    pub private_desktop_name: Option<String>,
}

/// Ack from runner after it spawns the child process.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpawnReady {
    pub process_id: u32,
}

/// Output data sent from runner to parent.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OutputPayload {
    pub data_b64: String,
    pub stream: OutputStream,
}

/// Output stream identifier for `OutputPayload`.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Stdin bytes sent from parent to runner.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StdinPayload {
    pub data_b64: String,
}

/// PTY resize request sent from parent to runner.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResizePayload {
    pub rows: u16,
    pub cols: u16,
}

/// Exit status sent from runner to parent.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExitPayload {
    pub exit_code: i32,
    pub timed_out: bool,
}

/// Error payload sent when the runner fails to spawn or stream.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorPayload {
    pub message: String,
    pub stage: ErrorStage,
    pub windows_error_code: Option<u32>,
}

/// Runner startup stage that produced an error.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorStage {
    ReadSpawnRequest,
    SpawnChild,
    WriteSpawnReady,
}

/// Empty payload for control messages.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct EmptyPayload {}

/// Base64-encode raw bytes for IPC payloads.
pub fn encode_bytes(data: &[u8]) -> String {
    STANDARD.encode(data)
}

/// Decode base64 payload data into raw bytes.
pub fn decode_bytes(data: &str) -> Result<Vec<u8>> {
    Ok(STANDARD.decode(data.as_bytes())?)
}

/// Write a length-prefixed JSON frame.
pub fn write_frame<W: Write>(writer: W, msg: &FramedMessage) -> Result<()> {
    crate::elevated::framed_io::write_frame(writer, msg)
}

/// Read a length-prefixed JSON frame; returns `Ok(None)` on EOF.
pub fn read_frame<R: Read>(reader: R) -> Result<Option<FramedMessage>> {
    crate::elevated::framed_io::read_frame(reader)
}
