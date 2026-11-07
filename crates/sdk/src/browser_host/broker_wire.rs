use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use klaus_agent_core::runtime_values::{runtime_json, CanonicalRuntimeJsonUtf8, RuntimeJson};

const BOOTSTRAP_RECORD_LEN: usize = 48;
const UNIX_MAGIC: [u8; 4] = *b"KBU1";
const WINDOWS_MAGIC: [u8; 4] = *b"KBB1";

const LENGTH_PREFIX_LEN: usize = 4;
const SYNC_BODY_HEADER_LEN: usize = 20;
const MUX_BODY_HEADER_LEN: usize = 40;
const MAX_SYNC_REMAINING_LENGTH: usize = 16 * 1024 * 1024;
const MAX_MUX_REMAINING_LENGTH: usize = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum BrokerLaneRole {
    SyncClientToHost = 0,
    SyncHostToClient = 1,
    MuxClientToHost = 2,
    MuxHostToClient = 3,
}

impl BrokerLaneRole {
    fn decode(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::SyncClientToHost),
            1 => Some(Self::SyncHostToClient),
            2 => Some(Self::MuxClientToHost),
            3 => Some(Self::MuxHostToClient),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WindowsBrokerLanePair {
    Sync,
    Mux,
}

impl WindowsBrokerLanePair {
    fn roles(self) -> (BrokerLaneRole, BrokerLaneRole) {
        match self {
            Self::Sync => (
                BrokerLaneRole::SyncHostToClient,
                BrokerLaneRole::SyncClientToHost,
            ),
            Self::Mux => (
                BrokerLaneRole::MuxHostToClient,
                BrokerLaneRole::MuxClientToHost,
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum WindowsBootstrapPhase {
    ServerHello = 1,
    ClientHello = 2,
    AdoptionChallenge = 3,
    AdoptionEcho = 4,
    Commit = 5,
    CommitEcho = 6,
    AdoptionAck = 7,
    PackageReady = 8,
}

impl WindowsBootstrapPhase {
    fn roles(self, pair: WindowsBrokerLanePair) -> (BrokerLaneRole, BrokerLaneRole) {
        let (host_to_client, client_to_host) = pair.roles();
        match self {
            Self::ServerHello | Self::AdoptionChallenge | Self::Commit | Self::AdoptionAck => {
                (host_to_client, client_to_host)
            }
            Self::ClientHello | Self::AdoptionEcho | Self::CommitEcho | Self::PackageReady => {
                (client_to_host, host_to_client)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum UnixPackageRecordType {
    PackageLoadPermit = 1,
    PackageReady = 2,
}

impl UnixPackageRecordType {
    fn roles(self) -> (BrokerLaneRole, BrokerLaneRole) {
        match self {
            Self::PackageLoadPermit => (
                BrokerLaneRole::MuxHostToClient,
                BrokerLaneRole::MuxClientToHost,
            ),
            Self::PackageReady => (
                BrokerLaneRole::MuxClientToHost,
                BrokerLaneRole::MuxHostToClient,
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BrowserBootstrapRecord {
    pub record_type: u8,
    pub own_role: BrokerLaneRole,
    pub peer_role: BrokerLaneRole,
    pub generation: u64,
    pub token: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BrowserBootstrapWireError {
    InvalidLength,
    InvalidMagic,
    InvalidType,
    InvalidRole,
    InvalidReserved,
    WrongGeneration,
    WrongToken,
}

impl BrowserBootstrapRecord {
    pub fn encode_unix(
        record_type: UnixPackageRecordType,
        generation: u64,
        token: [u8; 32],
    ) -> [u8; BOOTSTRAP_RECORD_LEN] {
        let (own_role, peer_role) = record_type.roles();
        encode_record(
            UNIX_MAGIC,
            record_type as u8,
            own_role,
            peer_role,
            generation,
            token,
        )
    }

    pub fn decode_unix(
        bytes: &[u8],
        expected_type: UnixPackageRecordType,
        expected_generation: u64,
        expected_token: &[u8; 32],
    ) -> Result<Self, BrowserBootstrapWireError> {
        let record = decode_record(bytes, UNIX_MAGIC)?;
        let (own_role, peer_role) = expected_type.roles();
        if record.record_type != expected_type as u8 {
            return Err(BrowserBootstrapWireError::InvalidType);
        }
        if record.own_role != own_role || record.peer_role != peer_role {
            return Err(BrowserBootstrapWireError::InvalidRole);
        }
        if record.generation != expected_generation {
            return Err(BrowserBootstrapWireError::WrongGeneration);
        }
        if &record.token != expected_token {
            return Err(BrowserBootstrapWireError::WrongToken);
        }
        Ok(record)
    }

    pub fn encode_windows_authenticated(
        phase: WindowsBootstrapPhase,
        pair: WindowsBrokerLanePair,
        generation: u64,
        token: [u8; 32],
    ) -> [u8; BOOTSTRAP_RECORD_LEN] {
        let (own_role, peer_role) = phase.roles(pair);
        encode_record(
            WINDOWS_MAGIC,
            phase as u8,
            own_role,
            peer_role,
            generation,
            token,
        )
    }

    pub fn decode_windows_authenticated(
        bytes: &[u8],
        expected_phase: WindowsBootstrapPhase,
        expected_pair: WindowsBrokerLanePair,
        expected_generation: u64,
        expected_token: &[u8; 32],
    ) -> Result<Self, BrowserBootstrapWireError> {
        let record = decode_record(bytes, WINDOWS_MAGIC)?;
        if record.record_type != expected_phase as u8 {
            return Err(BrowserBootstrapWireError::InvalidType);
        }
        let (own_role, peer_role) = expected_phase.roles(expected_pair);
        if record.own_role != own_role || record.peer_role != peer_role {
            return Err(BrowserBootstrapWireError::InvalidRole);
        }
        if record.generation != expected_generation {
            return Err(BrowserBootstrapWireError::WrongGeneration);
        }
        if &record.token != expected_token {
            return Err(BrowserBootstrapWireError::WrongToken);
        }
        Ok(record)
    }
}

fn encode_record(
    magic: [u8; 4],
    record_type: u8,
    own_role: BrokerLaneRole,
    peer_role: BrokerLaneRole,
    generation: u64,
    token: [u8; 32],
) -> [u8; BOOTSTRAP_RECORD_LEN] {
    let mut bytes = [0_u8; BOOTSTRAP_RECORD_LEN];
    bytes[..4].copy_from_slice(&magic);
    bytes[4] = record_type;
    bytes[5] = own_role as u8;
    bytes[6] = peer_role as u8;
    bytes[7] = 0;
    bytes[8..16].copy_from_slice(&generation.to_be_bytes());
    bytes[16..].copy_from_slice(&token);
    bytes
}

fn decode_record(
    bytes: &[u8],
    expected_magic: [u8; 4],
) -> Result<BrowserBootstrapRecord, BrowserBootstrapWireError> {
    if bytes.len() != BOOTSTRAP_RECORD_LEN {
        return Err(BrowserBootstrapWireError::InvalidLength);
    }
    if bytes[..4] != expected_magic {
        return Err(BrowserBootstrapWireError::InvalidMagic);
    }
    if bytes[7] != 0 {
        return Err(BrowserBootstrapWireError::InvalidReserved);
    }
    let own_role =
        BrokerLaneRole::decode(bytes[5]).ok_or(BrowserBootstrapWireError::InvalidRole)?;
    let peer_role =
        BrokerLaneRole::decode(bytes[6]).ok_or(BrowserBootstrapWireError::InvalidRole)?;
    let generation = u64::from_be_bytes(
        bytes[8..16]
            .try_into()
            .expect("bootstrap generation occupies eight bytes"),
    );
    let token = bytes[16..]
        .try_into()
        .expect("bootstrap token occupies thirty-two bytes");
    Ok(BrowserBootstrapRecord {
        record_type: bytes[4],
        own_role,
        peer_role,
        generation,
        token,
    })
}

#[derive(Clone, Debug)]
pub(super) struct ExactBootstrapRecordReader {
    bytes: [u8; BOOTSTRAP_RECORD_LEN],
    filled: usize,
}

impl Default for ExactBootstrapRecordReader {
    fn default() -> Self {
        Self {
            bytes: [0; BOOTSTRAP_RECORD_LEN],
            filled: 0,
        }
    }
}

impl ExactBootstrapRecordReader {
    pub fn feed(&mut self, input: &[u8]) -> usize {
        let consumed = input.len().min(BOOTSTRAP_RECORD_LEN - self.filled);
        self.bytes[self.filled..self.filled + consumed].copy_from_slice(&input[..consumed]);
        self.filled += consumed;
        consumed
    }

    pub fn is_complete(&self) -> bool {
        self.filled == BOOTSTRAP_RECORD_LEN
    }

    pub fn record(&self) -> Option<&[u8; BOOTSTRAP_RECORD_LEN]> {
        self.is_complete().then_some(&self.bytes)
    }
}

/// Closed operational kinds admitted on the synchronous browser broker lane.
///
/// The numeric values are part of the first-party sidecar contract. Request
/// and response direction is supplied by the directional lane, not by an
/// extensible method name or caller-selected policy identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u16)]
#[allow(clippy::enum_variant_names)]
pub(super) enum BrowserSyncKind {
    PlaywrightLaunch = 1,
    PlaywrightTreeKill = 2,
    PlaywrightMacOsSwVers = 3,
    PlaywrightLinuxLdd = 4,
    PlaywrightLinuxLdconfig = 5,
    PlaywrightWindowsWinldd = 6,
    BrowserFfmpegEncode = 7,
}

impl BrowserSyncKind {
    fn decode(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::PlaywrightLaunch),
            2 => Some(Self::PlaywrightTreeKill),
            3 => Some(Self::PlaywrightMacOsSwVers),
            4 => Some(Self::PlaywrightLinuxLdd),
            5 => Some(Self::PlaywrightLinuxLdconfig),
            6 => Some(Self::PlaywrightWindowsWinldd),
            7 => Some(Self::BrowserFfmpegEncode),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrowserFfmpegEncodeRequest {
    pub list_path: klaus_agent_core::runtime_values::RuntimeString,
    pub file_path: klaus_agent_core::runtime_values::RuntimeString,
    pub fps: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BrowserPlaywrightLaunchMode {
    Ephemeral,
    Persistent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrowserPlaywrightLaunchRequest {
    pub command: klaus_agent_core::runtime_values::RuntimeString,
    pub args: Vec<klaus_agent_core::runtime_values::RuntimeString>,
    pub options: RuntimeJson,
    pub source_options: RuntimeJson,
    pub temp_directories: Vec<klaus_agent_core::runtime_values::RuntimeString>,
    pub mode: BrowserPlaywrightLaunchMode,
    pub planner_trace: RuntimeJson,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub(super) enum BrowserSyncRequestPayload {
    PlaywrightLaunch(BrowserPlaywrightLaunchRequest),
    PlaywrightTreeKill(BrowserTreeKillRequest),
    PlaywrightMacOsSwVers,
    PlaywrightLinuxLdd(BrowserDependencyProbeRequest),
    PlaywrightLinuxLdconfig,
    PlaywrightWindowsWinldd(BrowserWindowsDependencyProbeRequest),
    BrowserFfmpegEncode(BrowserFfmpegEncodeRequest),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrowserTreeKillRequest {
    pub pid: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrowserDependencyProbeRequest {
    pub file_path: klaus_agent_core::runtime_values::RuntimeString,
    pub cwd: klaus_agent_core::runtime_values::RuntimeString,
    pub ld_library_path: klaus_agent_core::runtime_values::RuntimeString,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrowserWindowsDependencyProbeRequest {
    pub executable: klaus_agent_core::runtime_values::RuntimeString,
    pub file_path: klaus_agent_core::runtime_values::RuntimeString,
    pub cwd: klaus_agent_core::runtime_values::RuntimeString,
    pub ld_library_path: klaus_agent_core::runtime_values::RuntimeString,
}

pub(super) fn decode_sync_request_payload(
    kind: BrowserSyncKind,
    payload: &RuntimeJson,
) -> Result<BrowserSyncRequestPayload, BrowserBrokerWireError> {
    match kind {
        BrowserSyncKind::PlaywrightLaunch => {
            decode_playwright_launch(payload).map(BrowserSyncRequestPayload::PlaywrightLaunch)
        }
        BrowserSyncKind::BrowserFfmpegEncode => {
            decode_ffmpeg_encode(payload).map(BrowserSyncRequestPayload::BrowserFfmpegEncode)
        }
        BrowserSyncKind::PlaywrightTreeKill => {
            decode_tree_kill(payload).map(BrowserSyncRequestPayload::PlaywrightTreeKill)
        }
        BrowserSyncKind::PlaywrightMacOsSwVers => {
            decode_macos_sw_vers(payload)?;
            Ok(BrowserSyncRequestPayload::PlaywrightMacOsSwVers)
        }
        BrowserSyncKind::PlaywrightLinuxLdd => {
            decode_dependency_probe(payload, false).map(|request| {
                BrowserSyncRequestPayload::PlaywrightLinuxLdd(BrowserDependencyProbeRequest {
                    file_path: request.file_path,
                    cwd: request.cwd,
                    ld_library_path: request.ld_library_path,
                })
            })
        }
        BrowserSyncKind::PlaywrightLinuxLdconfig => {
            decode_linux_ldconfig(payload)?;
            Ok(BrowserSyncRequestPayload::PlaywrightLinuxLdconfig)
        }
        BrowserSyncKind::PlaywrightWindowsWinldd => {
            decode_dependency_probe(payload, true).map(|request| {
                BrowserSyncRequestPayload::PlaywrightWindowsWinldd(
                    BrowserWindowsDependencyProbeRequest {
                        executable: request.executable.expect("Windows probe has executable"),
                        file_path: request.file_path,
                        cwd: request.cwd,
                        ld_library_path: request.ld_library_path,
                    },
                )
            })
        }
    }
}

struct DecodedDependencyProbe {
    executable: Option<klaus_agent_core::runtime_values::RuntimeString>,
    file_path: klaus_agent_core::runtime_values::RuntimeString,
    cwd: klaus_agent_core::runtime_values::RuntimeString,
    ld_library_path: klaus_agent_core::runtime_values::RuntimeString,
}

fn decode_wrapped_args(payload: &RuntimeJson) -> Result<&[RuntimeJson], BrowserBrokerWireError> {
    let RuntimeJson::Object(object) = payload else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    let mut entries = object.entries();
    let Some((key, RuntimeJson::Array(args))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if key != "args" || entries.next().is_some() {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    Ok(args)
}

fn decode_tree_kill(
    payload: &RuntimeJson,
) -> Result<BrowserTreeKillRequest, BrowserBrokerWireError> {
    let args = decode_wrapped_args(payload)?;
    if args.len() != 2 {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    if cfg!(windows) {
        let [RuntimeJson::String(command), RuntimeJson::Object(options)] = args else {
            return Err(BrowserBrokerWireError::InvalidPayload);
        };
        let command = command
            .try_to_utf8()
            .map_err(|_| BrowserBrokerWireError::InvalidPayload)?;
        let prefix = "taskkill /pid ";
        let suffix = " /T /F";
        let Some(pid) = command
            .strip_prefix(prefix)
            .and_then(|value| value.strip_suffix(suffix))
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|pid| *pid != 0)
        else {
            return Err(BrowserBrokerWireError::InvalidPayload);
        };
        let mut entries = options.entries();
        let Some((shell_key, RuntimeJson::Bool(true))) = entries.next() else {
            return Err(BrowserBrokerWireError::InvalidPayload);
        };
        if shell_key != "shell" || entries.next().is_some() {
            return Err(BrowserBrokerWireError::InvalidPayload);
        }
        Ok(BrowserTreeKillRequest { pid })
    } else {
        let [RuntimeJson::Number(pid), RuntimeJson::String(signal)] = args else {
            return Err(BrowserBrokerWireError::InvalidPayload);
        };
        let pid = pid.as_f64();
        if !pid.is_finite()
            || pid.fract() != 0.0
            || pid >= 0.0
            || pid < -(u32::MAX as f64)
            || signal.try_to_utf8().as_deref() != Ok("SIGKILL")
        {
            return Err(BrowserBrokerWireError::InvalidPayload);
        }
        Ok(BrowserTreeKillRequest { pid: (-pid) as u32 })
    }
}

fn decode_macos_sw_vers(payload: &RuntimeJson) -> Result<(), BrowserBrokerWireError> {
    let args = decode_wrapped_args(payload)?;
    let [RuntimeJson::String(command), RuntimeJson::Object(options)] = args else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if command.try_to_utf8().as_deref() != Ok("sw_vers -productVersion") {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let mut entries = options.entries();
    let Some((stdio_key, RuntimeJson::Array(stdio))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    let expected = ["ignore", "pipe", "ignore"];
    if stdio_key != "stdio"
        || entries.next().is_some()
        || stdio.len() != expected.len()
        || stdio.iter().zip(expected).any(|(actual, expected)| {
            !matches!(actual, RuntimeJson::String(value) if value.try_to_utf8().as_deref() == Ok(expected))
        })
    {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    Ok(())
}

fn decode_dependency_probe(
    payload: &RuntimeJson,
    windows: bool,
) -> Result<DecodedDependencyProbe, BrowserBrokerWireError> {
    let args = decode_wrapped_args(payload)?;
    let [RuntimeJson::String(executable), RuntimeJson::Array(file_args), RuntimeJson::Object(options)] =
        args
    else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    let [RuntimeJson::String(file_path)] = file_args.as_slice() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if !windows && executable.try_to_utf8().as_deref() != Ok("ldd") {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let mut entries = options.entries();
    let Some((cwd_key, RuntimeJson::String(cwd))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    let Some((env_key, RuntimeJson::Object(environment))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if cwd_key != "cwd" || env_key != "env" || entries.next().is_some() {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let mut ld_library_path = None;
    for (key, value) in environment.entries() {
        let RuntimeJson::String(value) = value else {
            return Err(BrowserBrokerWireError::InvalidPayload);
        };
        if key == "LD_LIBRARY_PATH" && ld_library_path.replace(value.clone()).is_some() {
            return Err(BrowserBrokerWireError::InvalidPayload);
        }
    }
    let ld_library_path = ld_library_path.ok_or(BrowserBrokerWireError::InvalidPayload)?;
    Ok(DecodedDependencyProbe {
        executable: windows.then(|| executable.clone()),
        file_path: file_path.clone(),
        cwd: cwd.clone(),
        ld_library_path,
    })
}

fn decode_linux_ldconfig(payload: &RuntimeJson) -> Result<(), BrowserBrokerWireError> {
    let args = decode_wrapped_args(payload)?;
    let [RuntimeJson::String(command), RuntimeJson::Array(command_args), RuntimeJson::Object(options)] =
        args
    else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if command.try_to_utf8().as_deref() != Ok("/sbin/ldconfig")
        || !matches!(command_args.as_slice(), [RuntimeJson::String(value)] if value.try_to_utf8().as_deref() == Ok("-p"))
        || options.entries().next().is_some()
    {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    Ok(())
}

fn decode_ffmpeg_encode(
    payload: &RuntimeJson,
) -> Result<BrowserFfmpegEncodeRequest, BrowserBrokerWireError> {
    let RuntimeJson::Object(object) = payload else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    let mut entries = object.entries();
    let Some((list_path_key, RuntimeJson::String(list_path))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if list_path_key != "listPath" {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let Some((file_path_key, RuntimeJson::String(file_path))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if file_path_key != "filePath" {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let Some((fps_key, RuntimeJson::Number(fps))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if fps_key != "fps" || entries.next().is_some() {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let fps = fps.as_f64();
    if !fps.is_finite() || fps.fract() != 0.0 || !(1.0..=30.0).contains(&fps) {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    Ok(BrowserFfmpegEncodeRequest {
        list_path: list_path.clone(),
        file_path: file_path.clone(),
        fps: fps as u8,
    })
}

fn decode_playwright_launch(
    payload: &RuntimeJson,
) -> Result<BrowserPlaywrightLaunchRequest, BrowserBrokerWireError> {
    let RuntimeJson::Object(object) = payload else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    let mut entries = object.entries();
    let Some((command_key, RuntimeJson::String(command))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if command_key != "command" {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let Some((args_key, RuntimeJson::Array(args))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if args_key != "args" {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let args = args
        .iter()
        .map(|value| match value {
            RuntimeJson::String(value) => Ok(value.clone()),
            _ => Err(BrowserBrokerWireError::InvalidPayload),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some((options_key, options @ RuntimeJson::Object(_))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if options_key != "options" {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    validate_playwright_spawn_options(options)?;
    let Some((source_options_key, source_options @ RuntimeJson::Object(_))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if source_options_key != "sourceOptions" {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let Some((temp_directories_key, RuntimeJson::Array(temp_directories))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if temp_directories_key != "tempDirectories" {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let temp_directories = temp_directories
        .iter()
        .map(|value| match value {
            RuntimeJson::String(value) => Ok(value.clone()),
            _ => Err(BrowserBrokerWireError::InvalidPayload),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some((mode_key, RuntimeJson::String(mode))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if mode_key != "mode" {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let mode = match mode.try_to_utf8().as_deref() {
        Ok("ephemeral") => BrowserPlaywrightLaunchMode::Ephemeral,
        Ok("persistent") => BrowserPlaywrightLaunchMode::Persistent,
        _ => return Err(BrowserBrokerWireError::InvalidPayload),
    };
    let Some((planner_trace_key, planner_trace @ RuntimeJson::Object(_))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if planner_trace_key != "plannerTrace" || entries.next().is_some() {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    Ok(BrowserPlaywrightLaunchRequest {
        command: command.clone(),
        args,
        options: options.clone(),
        source_options: source_options.clone(),
        temp_directories,
        mode,
        planner_trace: planner_trace.clone(),
    })
}

fn validate_playwright_spawn_options(value: &RuntimeJson) -> Result<(), BrowserBrokerWireError> {
    let RuntimeJson::Object(object) = value else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    let mut entries = object.entries();
    let Some((detached_key, RuntimeJson::Bool(_))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if detached_key != "detached" {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let Some((cwd_key, cwd)) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if cwd_key != "cwd" || !matches!(cwd, RuntimeJson::Null | RuntimeJson::String(_)) {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let Some((environment_key, RuntimeJson::Array(environment))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if environment_key != "env" || environment.iter().any(|entry| {
        !matches!(entry, RuntimeJson::Array(pair) if pair.len() == 2 && pair.iter().all(|value| matches!(value, RuntimeJson::String(_))))
    }) {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let Some((shell_key, shell)) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    if shell_key != "shell"
        || !matches!(
            shell,
            RuntimeJson::Null | RuntimeJson::Bool(_) | RuntimeJson::String(_)
        )
    {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    let Some((stdio_key, RuntimeJson::Array(stdio))) = entries.next() else {
        return Err(BrowserBrokerWireError::InvalidPayload);
    };
    let exact_stdio = ["ignore", "pipe", "pipe", "pipe", "pipe"];
    if stdio_key != "stdio"
        || stdio.len() != exact_stdio.len()
        || stdio.iter().zip(exact_stdio).any(|(actual, expected)| {
            !matches!(actual, RuntimeJson::String(value) if value.try_to_utf8().as_deref() == Ok(expected))
        })
        || entries.next().is_some()
    {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BrowserBrokerWireError {
    InvalidLength,
    FrameTooLarge,
    TrailingBytes,
    InvalidGeneration,
    WrongGeneration,
    InvalidRequestId,
    WrongRequestId,
    DuplicateRequestId,
    SyncRequestAlreadyInFlight,
    NoSyncRequestInFlight,
    UnknownSyncKind,
    WrongSyncKind,
    InvalidFlags,
    InvalidCanonicalRuntimeJson,
    InvalidObjectId,
    UnknownObjectId,
    DuplicateObjectId,
    UnknownMuxKind,
    InvalidDirection,
    InvalidStream,
    InvalidSequence,
    SequenceExhausted,
    InvalidCorrelation,
    InvalidCredit,
    CreditNotAvailable,
    InvalidPayload,
    InvalidSpawnOutcome,
    DuplicateFrame,
    StreamHalfClosed,
    StreamClosing,
    StreamClosed,
    ObjectClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrowserSyncFrame {
    pub generation: u64,
    pub request_id: u64,
    pub kind: BrowserSyncKind,
    pub payload: RuntimeJson,
}

impl BrowserSyncFrame {
    pub fn new(
        generation: u64,
        request_id: u64,
        kind: BrowserSyncKind,
        payload: RuntimeJson,
    ) -> Result<Self, BrowserBrokerWireError> {
        if generation == 0 {
            return Err(BrowserBrokerWireError::InvalidGeneration);
        }
        if request_id == 0 {
            return Err(BrowserBrokerWireError::InvalidRequestId);
        }
        Ok(Self {
            generation,
            request_id,
            kind,
            payload,
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, BrowserBrokerWireError> {
        if self.generation == 0 {
            return Err(BrowserBrokerWireError::InvalidGeneration);
        }
        if self.request_id == 0 {
            return Err(BrowserBrokerWireError::InvalidRequestId);
        }
        let payload = runtime_json::stringify(&self.payload).to_node_utf8();
        let remaining = SYNC_BODY_HEADER_LEN
            .checked_add(payload.len())
            .ok_or(BrowserBrokerWireError::FrameTooLarge)?;
        if remaining > MAX_SYNC_REMAINING_LENGTH {
            return Err(BrowserBrokerWireError::FrameTooLarge);
        }
        let mut bytes = Vec::with_capacity(LENGTH_PREFIX_LEN + remaining);
        bytes.extend_from_slice(
            &u32::try_from(remaining)
                .expect("sync frame limit fits u32")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&self.generation.to_be_bytes());
        bytes.extend_from_slice(&self.request_id.to_be_bytes());
        bytes.extend_from_slice(&(self.kind as u16).to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    pub fn decode_exact(bytes: &[u8]) -> Result<Self, BrowserBrokerWireError> {
        let body = exact_frame_body(bytes, SYNC_BODY_HEADER_LEN, MAX_SYNC_REMAINING_LENGTH)?;
        let generation = read_u64(&body[..8]);
        if generation == 0 {
            return Err(BrowserBrokerWireError::InvalidGeneration);
        }
        let request_id = read_u64(&body[8..16]);
        if request_id == 0 {
            return Err(BrowserBrokerWireError::InvalidRequestId);
        }
        let kind = BrowserSyncKind::decode(read_u16(&body[16..18]))
            .ok_or(BrowserBrokerWireError::UnknownSyncKind)?;
        if read_u16(&body[18..20]) != 0 {
            return Err(BrowserBrokerWireError::InvalidFlags);
        }
        let payload = CanonicalRuntimeJsonUtf8::decode(&body[SYNC_BODY_HEADER_LEN..])
            .map_err(|_| BrowserBrokerWireError::InvalidCanonicalRuntimeJson)?;
        Self::new(generation, request_id, kind, payload)
    }
}

#[derive(Debug)]
pub(super) struct BrowserSyncExchange {
    generation: u64,
    in_flight: Option<(u64, BrowserSyncKind)>,
    request_ids: HashSet<u64>,
}

impl BrowserSyncExchange {
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            in_flight: None,
            request_ids: HashSet::new(),
        }
    }

    pub fn admit_request(
        &mut self,
        frame: &BrowserSyncFrame,
    ) -> Result<(), BrowserBrokerWireError> {
        self.check_generation(frame.generation)?;
        if self.in_flight.is_some() {
            return Err(BrowserBrokerWireError::SyncRequestAlreadyInFlight);
        }
        if !self.request_ids.insert(frame.request_id) {
            return Err(BrowserBrokerWireError::DuplicateRequestId);
        }
        self.in_flight = Some((frame.request_id, frame.kind));
        Ok(())
    }

    pub fn accept_response(
        &mut self,
        frame: &BrowserSyncFrame,
    ) -> Result<(), BrowserBrokerWireError> {
        self.check_generation(frame.generation)?;
        let Some((request_id, kind)) = self.in_flight else {
            return Err(BrowserBrokerWireError::NoSyncRequestInFlight);
        };
        if request_id != frame.request_id {
            return Err(BrowserBrokerWireError::WrongRequestId);
        }
        if kind != frame.kind {
            return Err(BrowserBrokerWireError::WrongSyncKind);
        }
        self.in_flight = None;
        Ok(())
    }

    fn check_generation(&self, generation: u64) -> Result<(), BrowserBrokerWireError> {
        if generation != self.generation {
            return Err(BrowserBrokerWireError::WrongGeneration);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct ExactSyncFrameReader {
    inner: ExactLengthPrefixedFrameReader,
}

impl ExactSyncFrameReader {
    pub fn feed(&mut self, input: &[u8]) -> Result<usize, BrowserBrokerWireError> {
        self.inner
            .feed(input, SYNC_BODY_HEADER_LEN, MAX_SYNC_REMAINING_LENGTH)
    }

    pub fn is_complete(&self) -> bool {
        self.inner.is_complete()
    }

    pub fn frame(&self) -> Option<&[u8]> {
        self.inner.frame()
    }
}

/// Physical direction is deliberately not serialized: each descriptor or
/// authenticated named pipe has one immutable direction.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum BrowserMuxDirection {
    HostToJs,
    JsToHost,
}

impl BrowserMuxDirection {
    fn index(self) -> usize {
        match self {
            Self::HostToJs => 0,
            Self::JsToHost => 1,
        }
    }

    fn opposite(self) -> Self {
        match self {
            Self::HostToJs => Self::JsToHost,
            Self::JsToHost => Self::HostToJs,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u16)]
pub(super) enum BrowserMuxKind {
    StdoutData = 1,
    StderrData = 2,
    ExtraData = 3,
    Write = 4,
    WriteAck = 5,
    ReadCredit = 6,
    HalfClose = 7,
    StreamError = 8,
    StreamClose = 9,
    CloseHooksReady = 10,
    Exit = 11,
    Close = 12,
    CleanupComplete = 13,
    BeginGracefulClose = 14,
    GracefulCloseResult = 15,
    BeginForceClose = 16,
    ForceCloseResult = 17,
}

impl BrowserMuxKind {
    fn decode(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::StdoutData),
            2 => Some(Self::StderrData),
            3 => Some(Self::ExtraData),
            4 => Some(Self::Write),
            5 => Some(Self::WriteAck),
            6 => Some(Self::ReadCredit),
            7 => Some(Self::HalfClose),
            8 => Some(Self::StreamError),
            9 => Some(Self::StreamClose),
            10 => Some(Self::CloseHooksReady),
            11 => Some(Self::Exit),
            12 => Some(Self::Close),
            13 => Some(Self::CleanupComplete),
            14 => Some(Self::BeginGracefulClose),
            15 => Some(Self::GracefulCloseResult),
            16 => Some(Self::BeginForceClose),
            17 => Some(Self::ForceCloseResult),
            _ => None,
        }
    }

    pub(super) fn sequence_class(self) -> BrowserMuxSequenceClass {
        match self {
            Self::StdoutData | Self::StderrData | Self::ExtraData => BrowserMuxSequenceClass::Data,
            Self::Write => BrowserMuxSequenceClass::Write,
            Self::ReadCredit => BrowserMuxSequenceClass::Credit,
            Self::WriteAck
            | Self::HalfClose
            | Self::StreamError
            | Self::StreamClose
            | Self::CloseHooksReady
            | Self::Exit
            | Self::Close
            | Self::CleanupComplete
            | Self::BeginGracefulClose
            | Self::GracefulCloseResult
            | Self::BeginForceClose
            | Self::ForceCloseResult => BrowserMuxSequenceClass::Control,
        }
    }

    fn payload_grammar(self) -> MuxPayloadGrammar {
        match self {
            Self::StdoutData | Self::StderrData | Self::ExtraData | Self::Write => {
                MuxPayloadGrammar::Bytes
            }
            Self::StreamError
            | Self::Exit
            | Self::CleanupComplete
            | Self::GracefulCloseResult
            | Self::ForceCloseResult => MuxPayloadGrammar::RuntimeJson,
            Self::WriteAck
            | Self::ReadCredit
            | Self::HalfClose
            | Self::StreamClose
            | Self::CloseHooksReady
            | Self::Close
            | Self::BeginGracefulClose
            | Self::BeginForceClose => MuxPayloadGrammar::Empty,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BrowserMuxSequenceClass {
    Data,
    Write,
    Credit,
    Control,
}

impl BrowserMuxSequenceClass {
    pub(super) fn index(self) -> usize {
        match self {
            Self::Data => 0,
            Self::Write => 1,
            Self::Credit => 2,
            Self::Control => 3,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum BrowserMuxPayload {
    Empty,
    Bytes(Vec<u8>),
    RuntimeJson(RuntimeJson),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrowserMuxFrame {
    pub direction: BrowserMuxDirection,
    pub generation: u64,
    pub object_id: u64,
    pub stream_id: u16,
    pub kind: BrowserMuxKind,
    pub sequence: u64,
    pub correlation: u64,
    pub credit: u32,
    pub payload: BrowserMuxPayload,
}

impl BrowserMuxFrame {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        direction: BrowserMuxDirection,
        generation: u64,
        object_id: u64,
        stream_id: u16,
        kind: BrowserMuxKind,
        sequence: u64,
        correlation: u64,
        credit: u32,
        payload: BrowserMuxPayload,
    ) -> Result<Self, BrowserBrokerWireError> {
        let frame = Self {
            direction,
            generation,
            object_id,
            stream_id,
            kind,
            sequence,
            correlation,
            credit,
            payload,
        };
        validate_mux_static(&frame)?;
        Ok(frame)
    }

    pub fn sequence_class(&self) -> BrowserMuxSequenceClass {
        self.kind.sequence_class()
    }

    pub fn encode(&self) -> Result<Vec<u8>, BrowserBrokerWireError> {
        validate_mux_static(self)?;
        let payload = match &self.payload {
            BrowserMuxPayload::Empty => Vec::new(),
            BrowserMuxPayload::Bytes(value) => value.clone(),
            BrowserMuxPayload::RuntimeJson(value) => runtime_json::stringify(value).to_node_utf8(),
        };
        let remaining = MUX_BODY_HEADER_LEN
            .checked_add(payload.len())
            .ok_or(BrowserBrokerWireError::FrameTooLarge)?;
        if remaining > MAX_MUX_REMAINING_LENGTH {
            return Err(BrowserBrokerWireError::FrameTooLarge);
        }
        let mut bytes = Vec::with_capacity(LENGTH_PREFIX_LEN + remaining);
        bytes.extend_from_slice(
            &u32::try_from(remaining)
                .expect("mux frame limit fits u32")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&self.generation.to_be_bytes());
        bytes.extend_from_slice(&self.object_id.to_be_bytes());
        bytes.extend_from_slice(&self.stream_id.to_be_bytes());
        bytes.extend_from_slice(&(self.kind as u16).to_be_bytes());
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes.extend_from_slice(&self.correlation.to_be_bytes());
        bytes.extend_from_slice(&self.credit.to_be_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    pub fn decode_exact(
        bytes: &[u8],
        direction: BrowserMuxDirection,
    ) -> Result<Self, BrowserBrokerWireError> {
        let body = exact_frame_body(bytes, MUX_BODY_HEADER_LEN, MAX_MUX_REMAINING_LENGTH)?;
        let generation = read_u64(&body[..8]);
        let object_id = read_u64(&body[8..16]);
        let stream_id = read_u16(&body[16..18]);
        let kind = BrowserMuxKind::decode(read_u16(&body[18..20]))
            .ok_or(BrowserBrokerWireError::UnknownMuxKind)?;
        let sequence = read_u64(&body[20..28]);
        let correlation = read_u64(&body[28..36]);
        let credit = read_u32(&body[36..40]);
        let payload_bytes = &body[MUX_BODY_HEADER_LEN..];
        let payload = match kind.payload_grammar() {
            MuxPayloadGrammar::Empty => {
                if !payload_bytes.is_empty() {
                    return Err(BrowserBrokerWireError::InvalidPayload);
                }
                BrowserMuxPayload::Empty
            }
            MuxPayloadGrammar::Bytes => BrowserMuxPayload::Bytes(payload_bytes.to_vec()),
            MuxPayloadGrammar::RuntimeJson => {
                let value = CanonicalRuntimeJsonUtf8::decode(payload_bytes)
                    .map_err(|_| BrowserBrokerWireError::InvalidCanonicalRuntimeJson)?;
                BrowserMuxPayload::RuntimeJson(value)
            }
        };
        Self::new(
            direction,
            generation,
            object_id,
            stream_id,
            kind,
            sequence,
            correlation,
            credit,
            payload,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MuxPayloadGrammar {
    Empty,
    Bytes,
    RuntimeJson,
}

fn validate_mux_static(frame: &BrowserMuxFrame) -> Result<(), BrowserBrokerWireError> {
    if frame.generation == 0 {
        return Err(BrowserBrokerWireError::InvalidGeneration);
    }
    if frame.object_id == 0 {
        return Err(BrowserBrokerWireError::InvalidObjectId);
    }
    if frame.sequence == 0 {
        return Err(BrowserBrokerWireError::InvalidSequence);
    }

    let direction_is_valid = match frame.kind {
        BrowserMuxKind::StdoutData
        | BrowserMuxKind::StderrData
        | BrowserMuxKind::ExtraData
        | BrowserMuxKind::WriteAck
        | BrowserMuxKind::Exit
        | BrowserMuxKind::Close
        | BrowserMuxKind::CleanupComplete
        | BrowserMuxKind::BeginGracefulClose
        | BrowserMuxKind::BeginForceClose => frame.direction == BrowserMuxDirection::HostToJs,
        BrowserMuxKind::Write
        | BrowserMuxKind::ReadCredit
        | BrowserMuxKind::HalfClose
        | BrowserMuxKind::CloseHooksReady
        | BrowserMuxKind::GracefulCloseResult
        | BrowserMuxKind::ForceCloseResult => frame.direction == BrowserMuxDirection::JsToHost,
        BrowserMuxKind::StreamError | BrowserMuxKind::StreamClose => true,
    };
    if !direction_is_valid {
        return Err(BrowserBrokerWireError::InvalidDirection);
    }

    let stream_is_valid = match frame.kind {
        BrowserMuxKind::StdoutData => frame.stream_id == 1,
        BrowserMuxKind::StderrData => frame.stream_id == 2,
        BrowserMuxKind::ExtraData
        | BrowserMuxKind::Write
        | BrowserMuxKind::WriteAck
        | BrowserMuxKind::HalfClose => matches!(frame.stream_id, 3 | 4),
        BrowserMuxKind::ReadCredit | BrowserMuxKind::StreamError | BrowserMuxKind::StreamClose => {
            (1..=4).contains(&frame.stream_id)
        }
        BrowserMuxKind::CloseHooksReady
        | BrowserMuxKind::Exit
        | BrowserMuxKind::Close
        | BrowserMuxKind::CleanupComplete
        | BrowserMuxKind::BeginGracefulClose
        | BrowserMuxKind::GracefulCloseResult
        | BrowserMuxKind::BeginForceClose
        | BrowserMuxKind::ForceCloseResult => frame.stream_id == 0,
    };
    if !stream_is_valid {
        return Err(BrowserBrokerWireError::InvalidStream);
    }

    if frame.kind == BrowserMuxKind::ReadCredit {
        if frame.credit != 1 {
            return Err(BrowserBrokerWireError::InvalidCredit);
        }
    } else if frame.credit != 0 {
        return Err(BrowserBrokerWireError::InvalidCredit);
    }

    let correlation_is_valid = match frame.kind {
        BrowserMuxKind::StdoutData
        | BrowserMuxKind::StderrData
        | BrowserMuxKind::ExtraData
        | BrowserMuxKind::Write
        | BrowserMuxKind::CloseHooksReady
        | BrowserMuxKind::Exit
        | BrowserMuxKind::BeginGracefulClose => frame.correlation == 0,
        BrowserMuxKind::WriteAck
        | BrowserMuxKind::CleanupComplete
        | BrowserMuxKind::GracefulCloseResult
        | BrowserMuxKind::BeginForceClose
        | BrowserMuxKind::ForceCloseResult => frame.correlation != 0,
        BrowserMuxKind::StreamClose if frame.direction == BrowserMuxDirection::JsToHost => {
            frame.correlation == 0
        }
        BrowserMuxKind::ReadCredit
        | BrowserMuxKind::HalfClose
        | BrowserMuxKind::StreamError
        | BrowserMuxKind::StreamClose
        | BrowserMuxKind::Close => true,
    };
    if !correlation_is_valid {
        return Err(BrowserBrokerWireError::InvalidCorrelation);
    }

    let payload_is_valid = match (&frame.payload, frame.kind.payload_grammar()) {
        (BrowserMuxPayload::Empty, MuxPayloadGrammar::Empty) => true,
        (BrowserMuxPayload::Bytes(_), MuxPayloadGrammar::Bytes) => true,
        (BrowserMuxPayload::RuntimeJson(value), MuxPayloadGrammar::RuntimeJson) => {
            validate_mux_json_payload(frame.kind, value)
        }
        _ => false,
    };
    if !payload_is_valid {
        return Err(BrowserBrokerWireError::InvalidPayload);
    }
    Ok(())
}

fn validate_mux_json_payload(kind: BrowserMuxKind, value: &RuntimeJson) -> bool {
    match kind {
        BrowserMuxKind::StreamError => {
            exact_two_members(value, "code", "message").is_some_and(|(code, message)| {
                matches!(code, RuntimeJson::String(_)) && matches!(message, RuntimeJson::String(_))
            })
        }
        BrowserMuxKind::Exit => {
            exact_two_members(value, "code", "signal").is_some_and(|(code, signal)| {
                (matches!(code, RuntimeJson::Null) || is_signed_integer(code))
                    && matches!(signal, RuntimeJson::Null | RuntimeJson::String(_))
            })
        }
        BrowserMuxKind::CleanupComplete
        | BrowserMuxKind::GracefulCloseResult
        | BrowserMuxKind::ForceCloseResult => {
            exact_two_members(value, "ok", "error").is_some_and(|(ok, error)| {
                matches!(ok, RuntimeJson::Bool(_))
                    && matches!(error, RuntimeJson::Null | RuntimeJson::String(_))
            })
        }
        _ => false,
    }
}

fn exact_two_members<'a>(
    value: &'a RuntimeJson,
    first: &str,
    second: &str,
) -> Option<(&'a RuntimeJson, &'a RuntimeJson)> {
    let RuntimeJson::Object(object) = value else {
        return None;
    };
    let mut entries = object.entries();
    let (first_key, first_value) = entries.next()?;
    let (second_key, second_value) = entries.next()?;
    if entries.next().is_some() || first_key != first || second_key != second {
        return None;
    }
    Some((first_value, second_value))
}

fn is_signed_integer(value: &RuntimeJson) -> bool {
    let RuntimeJson::Number(value) = value else {
        return false;
    };
    let value = value.as_f64();
    value.is_finite() && value.fract() == 0.0
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BrowserMuxSpawnOutcome {
    Committed,
    PidlessSpawnFailure,
}

#[derive(Clone, Debug)]
struct BrowserMuxObjectState {
    spawn_outcome: BrowserMuxSpawnOutcome,
    sequences: [[[u64; 4]; 5]; 2],
    read_credit_available: [bool; 5],
    last_data_sequence: [u64; 5],
    last_write_ack: [u64; 5],
    half_closed: [bool; 5],
    stream_close_request: [Option<u64>; 5],
    stream_closed: [bool; 5],
    close_hooks_ready: bool,
    exit_sequence: Option<u64>,
    close_sequence: Option<u64>,
    cleanup_complete: bool,
    begin_graceful_sequence: Option<u64>,
    graceful_result: bool,
    begin_force_sequence: Option<u64>,
    force_result: bool,
}

impl BrowserMuxObjectState {
    fn new(spawn_outcome: BrowserMuxSpawnOutcome) -> Self {
        Self {
            spawn_outcome,
            sequences: [[[0; 4]; 5]; 2],
            read_credit_available: [false; 5],
            last_data_sequence: [0; 5],
            last_write_ack: [0; 5],
            half_closed: [false; 5],
            stream_close_request: [None; 5],
            stream_closed: [false; 5],
            close_hooks_ready: false,
            exit_sequence: None,
            close_sequence: None,
            cleanup_complete: false,
            begin_graceful_sequence: None,
            graceful_result: false,
            begin_force_sequence: None,
            force_result: false,
        }
    }

    fn sequence(
        &self,
        direction: BrowserMuxDirection,
        stream: u16,
        class: BrowserMuxSequenceClass,
    ) -> u64 {
        self.sequences[direction.index()][usize::from(stream)][class.index()]
    }

    fn accept(&mut self, frame: &BrowserMuxFrame) -> Result<(), BrowserBrokerWireError> {
        if self.spawn_outcome == BrowserMuxSpawnOutcome::PidlessSpawnFailure
            && !matches!(
                frame.kind,
                BrowserMuxKind::Close | BrowserMuxKind::CleanupComplete
            )
        {
            return Err(BrowserBrokerWireError::InvalidSpawnOutcome);
        }
        if self.cleanup_complete
            || (self.close_sequence.is_some()
                && !matches!(
                    frame.kind,
                    BrowserMuxKind::CleanupComplete
                        | BrowserMuxKind::GracefulCloseResult
                        | BrowserMuxKind::BeginForceClose
                        | BrowserMuxKind::ForceCloseResult
                ))
        {
            return Err(BrowserBrokerWireError::ObjectClosed);
        }
        let stream = usize::from(frame.stream_id);
        if frame.stream_id != 0 {
            if self.stream_closed[stream] {
                return Err(BrowserBrokerWireError::StreamClosed);
            }
            if self.stream_close_request[stream].is_some()
                && !matches!(
                    (frame.direction, frame.kind),
                    (
                        BrowserMuxDirection::HostToJs,
                        BrowserMuxKind::StreamClose | BrowserMuxKind::StreamError
                    )
                )
            {
                return Err(BrowserBrokerWireError::StreamClosing);
            }
        }

        let class = frame.sequence_class();
        let previous = self.sequence(frame.direction, frame.stream_id, class);
        let expected = previous
            .checked_add(1)
            .ok_or(BrowserBrokerWireError::SequenceExhausted)?;
        if frame.sequence != expected {
            return Err(BrowserBrokerWireError::InvalidSequence);
        }

        match frame.kind {
            BrowserMuxKind::StdoutData | BrowserMuxKind::StderrData | BrowserMuxKind::ExtraData => {
                if !self.read_credit_available[stream] {
                    return Err(BrowserBrokerWireError::CreditNotAvailable);
                }
                self.read_credit_available[stream] = false;
                self.last_data_sequence[stream] = frame.sequence;
            }
            BrowserMuxKind::Write => {
                if self.half_closed[stream] {
                    return Err(BrowserBrokerWireError::StreamHalfClosed);
                }
            }
            BrowserMuxKind::WriteAck => {
                let acknowledged = self.last_write_ack[stream]
                    .checked_add(1)
                    .ok_or(BrowserBrokerWireError::SequenceExhausted)?;
                let last_write = self.sequence(
                    BrowserMuxDirection::JsToHost,
                    frame.stream_id,
                    BrowserMuxSequenceClass::Write,
                );
                if frame.correlation != acknowledged || acknowledged > last_write {
                    return Err(BrowserBrokerWireError::InvalidCorrelation);
                }
                self.last_write_ack[stream] = acknowledged;
            }
            BrowserMuxKind::ReadCredit => {
                if self.read_credit_available[stream]
                    || frame.correlation != self.last_data_sequence[stream]
                {
                    return Err(BrowserBrokerWireError::InvalidCorrelation);
                }
                self.read_credit_available[stream] = true;
            }
            BrowserMuxKind::HalfClose => {
                if self.half_closed[stream] {
                    return Err(BrowserBrokerWireError::DuplicateFrame);
                }
                let last_write = self.sequence(
                    BrowserMuxDirection::JsToHost,
                    frame.stream_id,
                    BrowserMuxSequenceClass::Write,
                );
                if frame.correlation != self.last_write_ack[stream]
                    || self.last_write_ack[stream] != last_write
                {
                    return Err(BrowserBrokerWireError::InvalidCorrelation);
                }
                self.half_closed[stream] = true;
            }
            BrowserMuxKind::StreamError => {
                if frame.correlation != 0
                    && !self.opposite_sequence_exists(
                        frame.direction,
                        frame.stream_id,
                        frame.correlation,
                    )
                {
                    return Err(BrowserBrokerWireError::InvalidCorrelation);
                }
            }
            BrowserMuxKind::StreamClose => match frame.direction {
                BrowserMuxDirection::JsToHost => {
                    if self.stream_close_request[stream].is_some() {
                        return Err(BrowserBrokerWireError::DuplicateFrame);
                    }
                    self.stream_close_request[stream] = Some(frame.sequence);
                }
                BrowserMuxDirection::HostToJs => {
                    if let Some(request) = self.stream_close_request[stream] {
                        if frame.correlation != request {
                            return Err(BrowserBrokerWireError::InvalidCorrelation);
                        }
                    } else if frame.correlation != 0 {
                        return Err(BrowserBrokerWireError::InvalidCorrelation);
                    }
                    self.stream_closed[stream] = true;
                }
            },
            BrowserMuxKind::CloseHooksReady => {
                if self.close_hooks_ready {
                    return Err(BrowserBrokerWireError::DuplicateFrame);
                }
                self.close_hooks_ready = true;
            }
            BrowserMuxKind::Exit => {
                if self.exit_sequence.is_some() {
                    return Err(BrowserBrokerWireError::DuplicateFrame);
                }
                self.exit_sequence = Some(frame.sequence);
            }
            BrowserMuxKind::Close => {
                if self.close_sequence.is_some() {
                    return Err(BrowserBrokerWireError::DuplicateFrame);
                }
                match (self.spawn_outcome, self.exit_sequence, frame.correlation) {
                    (BrowserMuxSpawnOutcome::Committed, Some(exit), correlation)
                        if correlation == exit => {}
                    (BrowserMuxSpawnOutcome::Committed, _, 0) => {
                        return Err(BrowserBrokerWireError::InvalidSpawnOutcome);
                    }
                    (BrowserMuxSpawnOutcome::PidlessSpawnFailure, None, 0) => {}
                    (BrowserMuxSpawnOutcome::PidlessSpawnFailure, _, _) => {
                        return Err(BrowserBrokerWireError::InvalidSpawnOutcome);
                    }
                    _ => return Err(BrowserBrokerWireError::InvalidCorrelation),
                }
                self.close_sequence = Some(frame.sequence);
            }
            BrowserMuxKind::CleanupComplete => {
                if self.cleanup_complete {
                    return Err(BrowserBrokerWireError::DuplicateFrame);
                }
                if self.close_sequence != Some(frame.correlation) {
                    return Err(BrowserBrokerWireError::InvalidCorrelation);
                }
                self.cleanup_complete = true;
            }
            BrowserMuxKind::BeginGracefulClose => {
                if self.begin_graceful_sequence.is_some() {
                    return Err(BrowserBrokerWireError::DuplicateFrame);
                }
                if !self.close_hooks_ready || self.exit_sequence.is_some() {
                    return Err(BrowserBrokerWireError::InvalidCorrelation);
                }
                self.begin_graceful_sequence = Some(frame.sequence);
            }
            BrowserMuxKind::GracefulCloseResult => {
                if self.graceful_result {
                    return Err(BrowserBrokerWireError::DuplicateFrame);
                }
                if self.begin_graceful_sequence != Some(frame.correlation) {
                    return Err(BrowserBrokerWireError::InvalidCorrelation);
                }
                self.graceful_result = true;
            }
            BrowserMuxKind::BeginForceClose => {
                if self.begin_force_sequence.is_some() {
                    return Err(BrowserBrokerWireError::DuplicateFrame);
                }
                if self.begin_graceful_sequence != Some(frame.correlation) {
                    return Err(BrowserBrokerWireError::InvalidCorrelation);
                }
                self.begin_force_sequence = Some(frame.sequence);
            }
            BrowserMuxKind::ForceCloseResult => {
                if self.force_result {
                    return Err(BrowserBrokerWireError::DuplicateFrame);
                }
                if self.begin_force_sequence != Some(frame.correlation) {
                    return Err(BrowserBrokerWireError::InvalidCorrelation);
                }
                self.force_result = true;
            }
        }

        self.sequences[frame.direction.index()][stream][class.index()] = frame.sequence;
        Ok(())
    }

    fn opposite_sequence_exists(
        &self,
        direction: BrowserMuxDirection,
        stream: u16,
        sequence: u64,
    ) -> bool {
        self.sequences[direction.opposite().index()][usize::from(stream)]
            .iter()
            .any(|last| sequence <= *last)
    }
}

#[derive(Debug)]
pub(super) struct BrowserMuxState {
    generation: u64,
    objects: HashMap<u64, BrowserMuxObjectState>,
}

impl BrowserMuxState {
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            objects: HashMap::new(),
        }
    }

    pub fn register_committed_object(
        &mut self,
        object_id: u64,
    ) -> Result<(), BrowserBrokerWireError> {
        self.register_object(object_id, BrowserMuxSpawnOutcome::Committed)
    }

    pub fn register_pidless_spawn_failure(
        &mut self,
        object_id: u64,
    ) -> Result<(), BrowserBrokerWireError> {
        self.register_object(object_id, BrowserMuxSpawnOutcome::PidlessSpawnFailure)
    }

    fn register_object(
        &mut self,
        object_id: u64,
        spawn_outcome: BrowserMuxSpawnOutcome,
    ) -> Result<(), BrowserBrokerWireError> {
        if object_id == 0 {
            return Err(BrowserBrokerWireError::InvalidObjectId);
        }
        match self.objects.entry(object_id) {
            Entry::Vacant(entry) => {
                entry.insert(BrowserMuxObjectState::new(spawn_outcome));
                Ok(())
            }
            Entry::Occupied(_) => Err(BrowserBrokerWireError::DuplicateObjectId),
        }
    }

    pub fn accept(&mut self, frame: &BrowserMuxFrame) -> Result<(), BrowserBrokerWireError> {
        validate_mux_static(frame)?;
        if frame.generation != self.generation {
            return Err(BrowserBrokerWireError::WrongGeneration);
        }
        let object = self
            .objects
            .get_mut(&frame.object_id)
            .ok_or(BrowserBrokerWireError::UnknownObjectId)?;
        let mut candidate = object.clone();
        candidate.accept(frame)?;
        *object = candidate;
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct ExactMuxFrameReader {
    inner: ExactLengthPrefixedFrameReader,
}

impl ExactMuxFrameReader {
    pub fn feed(&mut self, input: &[u8]) -> Result<usize, BrowserBrokerWireError> {
        self.inner
            .feed(input, MUX_BODY_HEADER_LEN, MAX_MUX_REMAINING_LENGTH)
    }

    pub fn is_complete(&self) -> bool {
        self.inner.is_complete()
    }

    pub fn frame(&self) -> Option<&[u8]> {
        self.inner.frame()
    }
}

#[derive(Clone, Debug, Default)]
struct ExactLengthPrefixedFrameReader {
    bytes: Vec<u8>,
    total_length: Option<usize>,
}

impl ExactLengthPrefixedFrameReader {
    fn feed(
        &mut self,
        input: &[u8],
        minimum_remaining: usize,
        maximum_remaining: usize,
    ) -> Result<usize, BrowserBrokerWireError> {
        if self.is_complete() {
            return Ok(0);
        }
        let mut consumed = 0;
        if self.bytes.len() < LENGTH_PREFIX_LEN {
            let take = (LENGTH_PREFIX_LEN - self.bytes.len()).min(input.len());
            self.bytes.extend_from_slice(&input[..take]);
            consumed += take;
            if self.bytes.len() < LENGTH_PREFIX_LEN {
                return Ok(consumed);
            }
            let remaining =
                usize::try_from(read_u32(&self.bytes[..4])).expect("u32 frame length fits usize");
            if remaining > maximum_remaining {
                return Err(BrowserBrokerWireError::FrameTooLarge);
            }
            if remaining < minimum_remaining {
                return Err(BrowserBrokerWireError::InvalidLength);
            }
            self.total_length = Some(LENGTH_PREFIX_LEN + remaining);
        }
        let total = self
            .total_length
            .expect("complete prefix fixes total frame length");
        let take = (total - self.bytes.len()).min(input.len() - consumed);
        self.bytes
            .extend_from_slice(&input[consumed..consumed + take]);
        consumed += take;
        Ok(consumed)
    }

    fn is_complete(&self) -> bool {
        self.total_length == Some(self.bytes.len())
    }

    fn frame(&self) -> Option<&[u8]> {
        self.is_complete().then_some(self.bytes.as_slice())
    }
}

fn exact_frame_body(
    bytes: &[u8],
    minimum_remaining: usize,
    maximum_remaining: usize,
) -> Result<&[u8], BrowserBrokerWireError> {
    if bytes.len() < LENGTH_PREFIX_LEN {
        return Err(BrowserBrokerWireError::InvalidLength);
    }
    let remaining = usize::try_from(read_u32(&bytes[..4])).expect("u32 frame length fits usize");
    if remaining > maximum_remaining {
        return Err(BrowserBrokerWireError::FrameTooLarge);
    }
    if remaining < minimum_remaining {
        return Err(BrowserBrokerWireError::InvalidLength);
    }
    let total = LENGTH_PREFIX_LEN + remaining;
    match bytes.len().cmp(&total) {
        std::cmp::Ordering::Less => Err(BrowserBrokerWireError::InvalidLength),
        std::cmp::Ordering::Greater => Err(BrowserBrokerWireError::TrailingBytes),
        std::cmp::Ordering::Equal => Ok(&bytes[LENGTH_PREFIX_LEN..]),
    }
}

fn read_u16(bytes: &[u8]) -> u16 {
    u16::from_be_bytes(bytes.try_into().expect("two-byte wire integer"))
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("four-byte wire integer"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("eight-byte wire integer"))
}
