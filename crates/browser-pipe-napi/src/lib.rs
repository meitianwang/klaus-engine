//! Sealed Windows Node-API bridge for the four browser broker pipes.
//!
//! The platform-independent portion deliberately contains only the immutable
//! bootstrap, pipe, API, and one-shot state contracts. The Windows module owns
//! every OS handle and is the only place that can open a named pipe.

#![cfg_attr(not(windows), allow(dead_code))]

use sha2::{Digest, Sha256};

const VALUE_PREFIX: &[u8] = b"v1.";
const PIPE_PREFIX: &[u8] = b"\\\\.\\pipe\\klaus-browser-v1-";
const PIPE_LANES: [&[u8]; 4] = [b"sync-c2h", b"sync-h2c", b"mux-c2h", b"mux-h2c"];
const MAX_PIPE_NAME_BYTES: usize = 512;
const MAX_BOOTSTRAP_PAYLOAD_BYTES: usize = 2_096;
const MAX_BOOTSTRAP_BASE64URL_BYTES: usize = 2_795;
const SYNC_FRAME_LIMIT: usize = 16 * 1024 * 1024;
const MUX_FRAME_LIMIT: usize = 256 * 1024 * 1024;
const MUX_READ_CHUNK_BYTES: usize = 64 * 1024;
const BROKER_ERROR: &str = "Browser broker native pipe binding failed.";

const API_KEYS: [&str; 8] = [
    "version",
    "generation",
    "writeSync",
    "readSyncExact",
    "startMux",
    "writeMux",
    "markPackageReady",
    "close",
];
const API_C_KEYS: [&[u8]; 8] = [
    b"version\0",
    b"generation\0",
    b"writeSync\0",
    b"readSyncExact\0",
    b"startMux\0",
    b"writeMux\0",
    b"markPackageReady\0",
    b"close\0",
];
const OPEN_EXPORT_C_KEY: &[u8] = b"openWindowsBrokerPipesV1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PipeAccess {
    Write,
    Read,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PipeContract {
    lane: &'static str,
    access: PipeAccess,
    share_mode: u32,
    open_existing: bool,
    overlapped: bool,
    require_byte_read_mode: bool,
}

const PIPE_CONTRACTS: [PipeContract; 4] = [
    PipeContract {
        lane: "sync-c2h",
        access: PipeAccess::Write,
        share_mode: 0,
        open_existing: true,
        overlapped: false,
        require_byte_read_mode: false,
    },
    PipeContract {
        lane: "sync-h2c",
        access: PipeAccess::Read,
        share_mode: 0,
        open_existing: true,
        overlapped: false,
        require_byte_read_mode: true,
    },
    PipeContract {
        lane: "mux-c2h",
        access: PipeAccess::Write,
        share_mode: 0,
        open_existing: true,
        overlapped: true,
        require_byte_read_mode: false,
    },
    PipeContract {
        lane: "mux-h2c",
        access: PipeAccess::Read,
        share_mode: 0,
        open_existing: true,
        overlapped: true,
        require_byte_read_mode: true,
    },
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct Bootstrap {
    generation: u64,
    nonce: [u8; 32],
    pipe_names: [Vec<u8>; 4],
}

impl Drop for Bootstrap {
    fn drop(&mut self) {
        self.nonce.fill(0);
        for name in &mut self.pipe_names {
            name.fill(0);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContractError {
    InvalidBootstrap,
    InvalidState,
    FrameTooLarge,
}

fn parse_bootstrap(value: &[u8]) -> Result<Bootstrap, ContractError> {
    if !value.is_ascii() {
        return Err(ContractError::InvalidBootstrap);
    }
    let encoded = value
        .strip_prefix(VALUE_PREFIX)
        .ok_or(ContractError::InvalidBootstrap)?;
    if encoded.len() > MAX_BOOTSTRAP_BASE64URL_BYTES {
        return Err(ContractError::InvalidBootstrap);
    }
    let mut payload = decode_base64url(encoded)?;
    let mut canonical = encode_base64url(&payload);
    let canonical_match = canonical == encoded;
    canonical.fill(0);
    if payload.len() > MAX_BOOTSTRAP_PAYLOAD_BYTES || !canonical_match {
        payload.fill(0);
        return Err(ContractError::InvalidBootstrap);
    }
    let parsed = decode_bootstrap_payload(&payload);
    payload.fill(0);
    parsed
}

fn decode_bootstrap_payload(payload: &[u8]) -> Result<Bootstrap, ContractError> {
    if payload.len() < 48 || payload.len() > MAX_BOOTSTRAP_PAYLOAD_BYTES {
        return Err(ContractError::InvalidBootstrap);
    }
    let generation = u64::from_be_bytes(
        payload[..8]
            .try_into()
            .map_err(|_| ContractError::InvalidBootstrap)?,
    );
    if generation == 0 {
        return Err(ContractError::InvalidBootstrap);
    }
    let mut nonce = Sensitive32(
        payload[8..40]
            .try_into()
            .map_err(|_| ContractError::InvalidBootstrap)?,
    );
    let mut offset = 40_usize;
    let mut pipe_names = SensitivePipeNames(std::array::from_fn(|_| Vec::new()));
    for name in &mut pipe_names.0 {
        let length_end = offset
            .checked_add(2)
            .ok_or(ContractError::InvalidBootstrap)?;
        if length_end > payload.len() {
            return Err(ContractError::InvalidBootstrap);
        }
        let length = usize::from(u16::from_be_bytes(
            payload[offset..length_end]
                .try_into()
                .map_err(|_| ContractError::InvalidBootstrap)?,
        ));
        offset = length_end;
        let end = offset
            .checked_add(length)
            .ok_or(ContractError::InvalidBootstrap)?;
        if length == 0
            || length > MAX_PIPE_NAME_BYTES
            || end > payload.len()
            || !payload[offset..end].is_ascii()
        {
            return Err(ContractError::InvalidBootstrap);
        }
        *name = payload[offset..end].to_vec();
        offset = end;
    }
    let mut expected = expected_pipe_names(generation, &nonce.0);
    let names_match = pipe_names.0 == expected;
    for name in &mut expected {
        name.fill(0);
    }
    if offset != payload.len() || !names_match {
        return Err(ContractError::InvalidBootstrap);
    }
    let bootstrap_nonce = nonce.0;
    nonce.0.fill(0);
    Ok(Bootstrap {
        generation,
        nonce: bootstrap_nonce,
        pipe_names: std::array::from_fn(|index| std::mem::take(&mut pipe_names.0[index])),
    })
}

struct SensitivePipeNames([Vec<u8>; 4]);

impl Drop for SensitivePipeNames {
    fn drop(&mut self) {
        for name in &mut self.0 {
            name.fill(0);
        }
    }
}

struct Sensitive32([u8; 32]);

impl Drop for Sensitive32 {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

fn expected_pipe_names(generation: u64, nonce: &[u8; 32]) -> [Vec<u8>; 4] {
    let mut generation_hex = Vec::with_capacity(16);
    append_hex(
        &mut generation_hex,
        &generation.to_be_bytes(),
        b"0123456789abcdef",
    );
    let mut nonce_hex = Vec::with_capacity(64);
    append_hex(&mut nonce_hex, nonce, b"0123456789ABCDEF");
    let names = PIPE_LANES.map(|lane| {
        let mut name = Vec::with_capacity(PIPE_PREFIX.len() + 16 + 1 + 64 + 1 + lane.len());
        name.extend_from_slice(PIPE_PREFIX);
        name.extend_from_slice(&generation_hex);
        name.push(b'-');
        name.extend_from_slice(&nonce_hex);
        name.push(b'-');
        name.extend_from_slice(lane);
        name
    });
    generation_hex.fill(0);
    nonce_hex.fill(0);
    names
}

fn append_hex(output: &mut Vec<u8>, bytes: &[u8], table: &[u8; 16]) {
    for byte in bytes {
        output.push(table[usize::from(byte >> 4)]);
        output.push(table[usize::from(byte & 0x0f)]);
    }
}

fn encode_base64url(bytes: &[u8]) -> Vec<u8> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = Vec::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        output.push(TABLE[usize::from(first >> 2)]);
        if chunk.len() == 1 {
            output.push(TABLE[usize::from((first & 0x03) << 4)]);
            continue;
        }
        let second = chunk[1];
        output.push(TABLE[usize::from(((first & 0x03) << 4) | (second >> 4))]);
        if chunk.len() == 2 {
            output.push(TABLE[usize::from((second & 0x0f) << 2)]);
            continue;
        }
        let third = chunk[2];
        output.push(TABLE[usize::from(((second & 0x0f) << 2) | (third >> 6))]);
        output.push(TABLE[usize::from(third & 0x3f)]);
    }
    output
}

fn decode_base64url(bytes: &[u8]) -> Result<Vec<u8>, ContractError> {
    if bytes.is_empty() || bytes.len() % 4 == 1 || bytes.contains(&b'=') {
        return Err(ContractError::InvalidBootstrap);
    }
    let mut output = Vec::with_capacity(bytes.len() / 4 * 3 + 2);
    for chunk in bytes.chunks(4) {
        let mut values = [0_u8; 4];
        for (index, byte) in chunk.iter().copied().enumerate() {
            let Some(value) = decode_base64url_byte(byte) else {
                values.fill(0);
                output.fill(0);
                return Err(ContractError::InvalidBootstrap);
            };
            values[index] = value;
        }
        output.push((values[0] << 2) | (values[1] >> 4));
        if chunk.len() >= 3 {
            output.push((values[1] << 4) | (values[2] >> 2));
        }
        if chunk.len() == 4 {
            output.push((values[2] << 6) | values[3]);
        }
        values.fill(0);
    }
    Ok(output)
}

fn decode_base64url_byte(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Lifecycle {
    Open,
    Closing,
    Closed,
}

#[derive(Debug)]
struct ContractState {
    lifecycle: Lifecycle,
    mux_started: bool,
    package_ready_marked: bool,
}

impl Default for ContractState {
    fn default() -> Self {
        Self {
            lifecycle: Lifecycle::Open,
            mux_started: false,
            package_ready_marked: false,
        }
    }
}

impl ContractState {
    fn start_mux(&mut self) -> Result<(), ContractError> {
        if self.lifecycle != Lifecycle::Open || self.mux_started {
            return Err(ContractError::InvalidState);
        }
        self.mux_started = true;
        Ok(())
    }

    fn mark_package_ready(&mut self) -> Result<(), ContractError> {
        if self.lifecycle != Lifecycle::Open || !self.mux_started || self.package_ready_marked {
            return Err(ContractError::InvalidState);
        }
        self.package_ready_marked = true;
        Ok(())
    }

    fn begin_close(&mut self) -> bool {
        if self.lifecycle != Lifecycle::Open {
            return false;
        }
        self.lifecycle = Lifecycle::Closing;
        true
    }

    fn finish_close(&mut self) {
        if self.lifecycle == Lifecycle::Closing {
            self.lifecycle = Lifecycle::Closed;
        }
    }
}

fn validate_frame_length(length: usize, limit: usize) -> Result<(), ContractError> {
    if length <= limit {
        Ok(())
    } else {
        Err(ContractError::FrameTooLarge)
    }
}

fn adoption_commit_digest(
    generation: u64,
    bootstrap_nonce: &[u8; 32],
    sync_hello: &[u8; 32],
    mux_hello: &[u8; 32],
    adoption: &[u8; 32],
    client_pid: u32,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"KLAUS_BROWSER_ADOPTION_COMMIT_V1");
    digest.update([0]);
    digest.update(generation.to_be_bytes());
    digest.update(bootstrap_nonce);
    digest.update(sync_hello);
    digest.update(mux_hello);
    digest.update(adoption);
    digest.update(client_pid.to_be_bytes());
    digest.finalize().into()
}

const BOOTSTRAP_RECORD_BYTES: usize = 48;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum BootstrapPhase {
    ServerHello = 1,
    ClientHello = 2,
    AdoptionChallenge = 3,
    AdoptionEcho = 4,
    Commit = 5,
    CommitEcho = 6,
    AdoptionAck = 7,
    PackageReady = 8,
}

fn encode_bootstrap_record(
    phase: BootstrapPhase,
    own_role: u8,
    peer_role: u8,
    generation: u64,
    token: &[u8; 32],
) -> [u8; BOOTSTRAP_RECORD_BYTES] {
    let mut output = [0_u8; BOOTSTRAP_RECORD_BYTES];
    output[..4].copy_from_slice(b"KBB1");
    output[4] = phase as u8;
    output[5] = own_role;
    output[6] = peer_role;
    output[8..16].copy_from_slice(&generation.to_be_bytes());
    output[16..].copy_from_slice(token);
    output
}

fn decode_bootstrap_record(
    bytes: &[u8; BOOTSTRAP_RECORD_BYTES],
    phase: BootstrapPhase,
    own_role: u8,
    peer_role: u8,
    generation: u64,
) -> Result<[u8; 32], ContractError> {
    if &bytes[..4] != b"KBB1"
        || bytes[4] != phase as u8
        || bytes[5] != own_role
        || bytes[6] != peer_role
        || bytes[7] != 0
        || bytes[8..16] != generation.to_be_bytes()
    {
        return Err(ContractError::InvalidBootstrap);
    }
    bytes[16..]
        .try_into()
        .map_err(|_| ContractError::InvalidBootstrap)
}

#[cfg(windows)]
mod windows {
    use std::collections::VecDeque;
    use std::ffi::{c_char, c_void};
    use std::ptr::{null, null_mut};
    use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::thread::{self, JoinHandle};

    use windows_sys::Win32::Foundation::{
        CloseHandle, GetLastError, ERROR_IO_PENDING, GENERIC_READ, GENERIC_WRITE, HANDLE,
        INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, ReadFile, WriteFile, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_OVERLAPPED,
        OPEN_EXISTING,
    };
    use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress};
    use windows_sys::Win32::System::Pipes::{
        GetNamedPipeHandleStateW, PIPE_NOWAIT, PIPE_READMODE_MESSAGE,
    };
    use windows_sys::Win32::System::Threading::{CreateEventW, GetCurrentProcessId};
    use windows_sys::Win32::System::IO::{
        CancelIoEx, CreateIoCompletionPort, GetOverlappedResult, GetQueuedCompletionStatus,
        PostQueuedCompletionStatus, OVERLAPPED,
    };

    use super::*;

    const NAPI_OK: i32 = 0;
    const NAPI_NUMBER: i32 = 3;
    const NAPI_STRING: i32 = 4;
    const NAPI_OBJECT: i32 = 6;
    const NAPI_FUNCTION: i32 = 7;
    const NAPI_UINT8_ARRAY: i32 = 1;
    const NAPI_DEFAULT_JSPROPERTY: u32 = 7;
    const NAPI_KEY_OWN_ONLY: i32 = 1;
    const NAPI_KEY_ALL_PROPERTIES: i32 = 0;
    const NAPI_KEY_NUMBERS_TO_STRINGS: i32 = 1;
    const UV_ASYNC: i32 = 1;
    const INFINITE: u32 = u32::MAX;
    const IOCP_READ: usize = 1;
    const IOCP_WRITE: usize = 2;
    const IOCP_WAKE: usize = 3;
    const IOCP_CLOSE: usize = 4;
    const ASYNC_RESOURCE_NAME: &[u8] = b"klaus.browser.pipe";

    type NapiEnv = *mut c_void;
    type NapiValue = *mut c_void;
    type NapiRef = *mut c_void;
    type NapiAsyncContext = *mut c_void;
    type NapiCallbackInfo = *mut c_void;
    type NapiHandleScope = *mut c_void;
    type NapiCallback = unsafe extern "C" fn(NapiEnv, NapiCallbackInfo) -> NapiValue;
    type NapiFinalize = unsafe extern "C" fn(NapiEnv, *mut c_void, *mut c_void);
    type NapiCleanupHook = unsafe extern "C" fn(*mut c_void);

    #[repr(C)]
    struct NapiPropertyDescriptor {
        utf8name: *const c_char,
        name: NapiValue,
        method: Option<NapiCallback>,
        getter: Option<NapiCallback>,
        setter: Option<NapiCallback>,
        value: NapiValue,
        attributes: u32,
        data: *mut c_void,
    }

    type DefineProperties =
        unsafe extern "C" fn(NapiEnv, NapiValue, usize, *const NapiPropertyDescriptor) -> i32;
    type GetCbInfo = unsafe extern "C" fn(
        NapiEnv,
        NapiCallbackInfo,
        *mut usize,
        *mut NapiValue,
        *mut NapiValue,
        *mut *mut c_void,
    ) -> i32;

    struct NapiApi {
        define_properties: DefineProperties,
        get_cb_info: GetCbInfo,
        create_object: unsafe extern "C" fn(NapiEnv, *mut NapiValue) -> i32,
        create_uint32: unsafe extern "C" fn(NapiEnv, u32, *mut NapiValue) -> i32,
        create_bigint_uint64: unsafe extern "C" fn(NapiEnv, u64, *mut NapiValue) -> i32,
        create_string_utf8:
            unsafe extern "C" fn(NapiEnv, *const c_char, usize, *mut NapiValue) -> i32,
        create_error: unsafe extern "C" fn(NapiEnv, NapiValue, NapiValue, *mut NapiValue) -> i32,
        get_undefined: unsafe extern "C" fn(NapiEnv, *mut NapiValue) -> i32,
        get_global: unsafe extern "C" fn(NapiEnv, *mut NapiValue) -> i32,
        get_value_string_utf8:
            unsafe extern "C" fn(NapiEnv, NapiValue, *mut c_char, usize, *mut usize) -> i32,
        get_value_double: unsafe extern "C" fn(NapiEnv, NapiValue, *mut f64) -> i32,
        typeof_value: unsafe extern "C" fn(NapiEnv, NapiValue, *mut i32) -> i32,
        throw_error: unsafe extern "C" fn(NapiEnv, *const c_char, *const c_char) -> i32,
        get_typedarray_info: unsafe extern "C" fn(
            NapiEnv,
            NapiValue,
            *mut i32,
            *mut usize,
            *mut *mut c_void,
            *mut NapiValue,
            *mut usize,
        ) -> i32,
        create_arraybuffer:
            unsafe extern "C" fn(NapiEnv, usize, *mut *mut c_void, *mut NapiValue) -> i32,
        create_typedarray:
            unsafe extern "C" fn(NapiEnv, i32, usize, NapiValue, usize, *mut NapiValue) -> i32,
        wrap: unsafe extern "C" fn(
            NapiEnv,
            NapiValue,
            *mut c_void,
            Option<NapiFinalize>,
            *mut c_void,
            *mut NapiRef,
        ) -> i32,
        unwrap: unsafe extern "C" fn(NapiEnv, NapiValue, *mut *mut c_void) -> i32,
        add_env_cleanup_hook:
            unsafe extern "C" fn(NapiEnv, Option<NapiCleanupHook>, *mut c_void) -> i32,
        get_all_property_names:
            unsafe extern "C" fn(NapiEnv, NapiValue, i32, i32, i32, *mut NapiValue) -> i32,
        get_array_length: unsafe extern "C" fn(NapiEnv, NapiValue, *mut u32) -> i32,
        get_element: unsafe extern "C" fn(NapiEnv, NapiValue, u32, *mut NapiValue) -> i32,
        get_named_property:
            unsafe extern "C" fn(NapiEnv, NapiValue, *const c_char, *mut NapiValue) -> i32,
        create_reference: unsafe extern "C" fn(NapiEnv, NapiValue, u32, *mut NapiRef) -> i32,
        delete_reference: unsafe extern "C" fn(NapiEnv, NapiRef) -> i32,
        get_reference_value: unsafe extern "C" fn(NapiEnv, NapiRef, *mut NapiValue) -> i32,
        async_init:
            unsafe extern "C" fn(NapiEnv, NapiValue, NapiValue, *mut NapiAsyncContext) -> i32,
        async_destroy: unsafe extern "C" fn(NapiEnv, NapiAsyncContext) -> i32,
        make_callback: unsafe extern "C" fn(
            NapiEnv,
            NapiAsyncContext,
            NapiValue,
            NapiValue,
            usize,
            *const NapiValue,
            *mut NapiValue,
        ) -> i32,
        object_freeze: unsafe extern "C" fn(NapiEnv, NapiValue) -> i32,
        open_handle_scope: unsafe extern "C" fn(NapiEnv, *mut NapiHandleScope) -> i32,
        close_handle_scope: unsafe extern "C" fn(NapiEnv, NapiHandleScope) -> i32,
        get_uv_event_loop: unsafe extern "C" fn(NapiEnv, *mut *mut c_void) -> i32,
    }

    impl NapiApi {
        unsafe fn load() -> Option<Self> {
            Some(Self {
                define_properties: unsafe { resolve_symbol(b"napi_define_properties\0")? },
                get_cb_info: unsafe { resolve_symbol(b"napi_get_cb_info\0")? },
                create_object: unsafe { resolve_symbol(b"napi_create_object\0")? },
                create_uint32: unsafe { resolve_symbol(b"napi_create_uint32\0")? },
                create_bigint_uint64: unsafe { resolve_symbol(b"napi_create_bigint_uint64\0")? },
                create_string_utf8: unsafe { resolve_symbol(b"napi_create_string_utf8\0")? },
                create_error: unsafe { resolve_symbol(b"napi_create_error\0")? },
                get_undefined: unsafe { resolve_symbol(b"napi_get_undefined\0")? },
                get_global: unsafe { resolve_symbol(b"napi_get_global\0")? },
                get_value_string_utf8: unsafe { resolve_symbol(b"napi_get_value_string_utf8\0")? },
                get_value_double: unsafe { resolve_symbol(b"napi_get_value_double\0")? },
                typeof_value: unsafe { resolve_symbol(b"napi_typeof\0")? },
                throw_error: unsafe { resolve_symbol(b"napi_throw_error\0")? },
                get_typedarray_info: unsafe { resolve_symbol(b"napi_get_typedarray_info\0")? },
                create_arraybuffer: unsafe { resolve_symbol(b"napi_create_arraybuffer\0")? },
                create_typedarray: unsafe { resolve_symbol(b"napi_create_typedarray\0")? },
                wrap: unsafe { resolve_symbol(b"napi_wrap\0")? },
                unwrap: unsafe { resolve_symbol(b"napi_unwrap\0")? },
                add_env_cleanup_hook: unsafe { resolve_symbol(b"napi_add_env_cleanup_hook\0")? },
                get_all_property_names: unsafe {
                    resolve_symbol(b"napi_get_all_property_names\0")?
                },
                get_array_length: unsafe { resolve_symbol(b"napi_get_array_length\0")? },
                get_element: unsafe { resolve_symbol(b"napi_get_element\0")? },
                get_named_property: unsafe { resolve_symbol(b"napi_get_named_property\0")? },
                create_reference: unsafe { resolve_symbol(b"napi_create_reference\0")? },
                delete_reference: unsafe { resolve_symbol(b"napi_delete_reference\0")? },
                get_reference_value: unsafe { resolve_symbol(b"napi_get_reference_value\0")? },
                async_init: unsafe { resolve_symbol(b"napi_async_init\0")? },
                async_destroy: unsafe { resolve_symbol(b"napi_async_destroy\0")? },
                make_callback: unsafe { resolve_symbol(b"napi_make_callback\0")? },
                object_freeze: unsafe { resolve_symbol(b"napi_object_freeze\0")? },
                open_handle_scope: unsafe { resolve_symbol(b"napi_open_handle_scope\0")? },
                close_handle_scope: unsafe { resolve_symbol(b"napi_close_handle_scope\0")? },
                get_uv_event_loop: unsafe { resolve_symbol(b"napi_get_uv_event_loop\0")? },
            })
        }
    }

    struct UvApi {
        handle_size: unsafe extern "C" fn(i32) -> usize,
        async_init: unsafe extern "C" fn(*mut c_void, *mut c_void, Option<UvAsyncCallback>) -> i32,
        async_send: unsafe extern "C" fn(*mut c_void) -> i32,
        handle_set_data: unsafe extern "C" fn(*mut c_void, *mut c_void),
        handle_get_data: unsafe extern "C" fn(*const c_void) -> *mut c_void,
        close: unsafe extern "C" fn(*mut c_void, Option<UvCloseCallback>),
    }

    type UvAsyncCallback = unsafe extern "C" fn(*mut c_void);
    type UvCloseCallback = unsafe extern "C" fn(*mut c_void);

    impl UvApi {
        unsafe fn load() -> Option<Self> {
            Some(Self {
                handle_size: unsafe { resolve_symbol(b"uv_handle_size\0")? },
                async_init: unsafe { resolve_symbol(b"uv_async_init\0")? },
                async_send: unsafe { resolve_symbol(b"uv_async_send\0")? },
                handle_set_data: unsafe { resolve_symbol(b"uv_handle_set_data\0")? },
                handle_get_data: unsafe { resolve_symbol(b"uv_handle_get_data\0")? },
                close: unsafe { resolve_symbol(b"uv_close\0")? },
            })
        }
    }

    static NAPI: OnceLock<NapiApi> = OnceLock::new();
    static UV: OnceLock<UvApi> = OnceLock::new();
    static OPEN_CALLED: AtomicBool = AtomicBool::new(false);

    unsafe fn resolve_symbol<T: Copy>(name: &[u8]) -> Option<T> {
        let module = unsafe { GetModuleHandleW(null()) };
        if module.is_null() {
            return None;
        }
        let symbol = unsafe { GetProcAddress(module, name.as_ptr()) }?;
        if std::mem::size_of::<T>() != std::mem::size_of_val(&symbol) {
            return None;
        }
        Some(unsafe { std::mem::transmute_copy(&symbol) })
    }

    fn napi() -> &'static NapiApi {
        NAPI.get()
            .expect("Node-API table initialized by module entry")
    }

    fn uv() -> Option<&'static UvApi> {
        if let Some(api) = UV.get() {
            return Some(api);
        }
        let loaded = unsafe { UvApi::load()? };
        let _ = UV.set(loaded);
        UV.get()
    }

    #[derive(Clone, Copy)]
    struct HandlerRefs {
        bytes: usize,
        close: usize,
    }

    fn delete_handler_references(env: NapiEnv, handlers: HandlerRefs) {
        let _ = unsafe { (napi().delete_reference)(env, handlers.bytes as NapiRef) };
        let _ = unsafe { (napi().delete_reference)(env, handlers.close as NapiRef) };
    }

    struct NapiReferenceGuard {
        env: NapiEnv,
        reference: Option<NapiRef>,
    }

    impl NapiReferenceGuard {
        fn new(env: NapiEnv, reference: NapiRef) -> Self {
            Self {
                env,
                reference: Some(reference),
            }
        }

        fn into_raw(mut self) -> NapiRef {
            self.reference.take().expect("reference guard is armed")
        }
    }

    impl Drop for NapiReferenceGuard {
        fn drop(&mut self) {
            if let Some(reference) = self.reference.take() {
                let _ = unsafe { (napi().delete_reference)(self.env, reference) };
            }
        }
    }

    struct WriteRequest {
        bytes: Vec<u8>,
        done: usize,
        zero_after_write: bool,
    }

    enum JsRecord {
        Bytes(Vec<u8>),
        Done { reference: usize, failed: bool },
        Close { failed: bool },
    }

    struct NativePipeSet {
        generation: u64,
        commit_digest: Mutex<Option<[u8; 32]>>,
        handles: [AtomicUsize; 4],
        sync_guard: Mutex<()>,
        state: Mutex<ContractState>,
        iocp: AtomicUsize,
        worker: Mutex<Option<JoinHandle<()>>>,
        writes: Mutex<VecDeque<WriteRequest>>,
        records: Mutex<VecDeque<JsRecord>>,
        handlers: Mutex<Option<HandlerRefs>>,
        uv_async: AtomicPtr<c_void>,
        env: AtomicPtr<c_void>,
        async_context: AtomicPtr<c_void>,
        async_resource: AtomicUsize,
        close_recorded: AtomicBool,
        close_failure_requested: AtomicBool,
        release_references_on_uv_close: AtomicBool,
    }

    impl NativePipeSet {
        fn new(
            generation: u64,
            commit_digest: [u8; 32],
            handles: [usize; 4],
            env: NapiEnv,
        ) -> Arc<Self> {
            Arc::new(Self {
                generation,
                commit_digest: Mutex::new(Some(commit_digest)),
                handles: handles.map(AtomicUsize::new),
                sync_guard: Mutex::new(()),
                state: Mutex::new(ContractState::default()),
                iocp: AtomicUsize::new(0),
                worker: Mutex::new(None),
                writes: Mutex::new(VecDeque::new()),
                records: Mutex::new(VecDeque::new()),
                handlers: Mutex::new(None),
                uv_async: AtomicPtr::new(null_mut()),
                env: AtomicPtr::new(env),
                async_context: AtomicPtr::new(null_mut()),
                async_resource: AtomicUsize::new(0),
                close_recorded: AtomicBool::new(false),
                close_failure_requested: AtomicBool::new(false),
                release_references_on_uv_close: AtomicBool::new(false),
            })
        }

        fn handle(&self, index: usize) -> Result<HANDLE, ContractError> {
            let value = self.handles[index].load(Ordering::Acquire);
            if value == 0 {
                Err(ContractError::InvalidState)
            } else {
                Ok(value as HANDLE)
            }
        }

        fn signal_js(&self) {
            let handle = self.uv_async.load(Ordering::Acquire);
            if !handle.is_null() {
                if let Some(api) = uv() {
                    let _ = unsafe { (api.async_send)(handle) };
                }
            }
        }

        fn push_record(&self, record: JsRecord) {
            self.records
                .lock()
                .expect("record lock poisoned")
                .push_back(record);
            self.signal_js();
        }

        fn record_close(&self, failed: bool) {
            if !self.close_recorded.swap(true, Ordering::AcqRel) {
                let mut records = self.records.lock().expect("record lock poisoned");
                let callbacks = records
                    .drain(..)
                    .filter_map(|record| match record {
                        JsRecord::Done { reference, .. } => Some(reference),
                        JsRecord::Bytes(_) | JsRecord::Close { .. } => None,
                    })
                    .collect::<Vec<_>>();
                for reference in callbacks {
                    records.push_back(JsRecord::Done {
                        reference,
                        failed: true,
                    });
                }
                records.push_back(JsRecord::Close { failed });
                drop(records);
                self.signal_js();
            }
        }

        fn close_handles(&self) {
            let _guard = self.sync_guard.lock().expect("sync lock poisoned");
            for handle in &self.handles {
                let raw = handle.swap(0, Ordering::AcqRel);
                if raw != 0 {
                    let _ = unsafe { CloseHandle(raw as HANDLE) };
                }
            }
            let port = self.iocp.swap(0, Ordering::AcqRel);
            if port != 0 {
                let _ = unsafe { CloseHandle(port as HANDLE) };
            }
        }

        fn drain_writes_as_failed(&self) {
            let mut writes = self.writes.lock().expect("write lock poisoned");
            for mut request in writes.drain(..) {
                if request.zero_after_write {
                    request.bytes.fill(0);
                }
                self.push_record(JsRecord::Done {
                    reference: request.done,
                    failed: true,
                });
            }
        }

        fn discard_js_references(&self) {
            let env = self.env.swap(null_mut(), Ordering::AcqRel);
            let references = self
                .records
                .lock()
                .expect("record lock poisoned")
                .drain(..)
                .filter_map(|record| match record {
                    JsRecord::Done { reference, .. } => Some(reference),
                    JsRecord::Bytes(_) | JsRecord::Close { .. } => None,
                })
                .collect::<Vec<_>>();
            let handlers = self.handlers.lock().expect("handler lock poisoned").take();
            if !env.is_null() {
                for reference in references {
                    let _ = unsafe { (napi().delete_reference)(env, reference as NapiRef) };
                }
                if let Some(handlers) = handlers {
                    delete_handler_references(env, handlers);
                }
                self.destroy_async_context(env);
            } else {
                self.async_context.store(null_mut(), Ordering::Release);
                self.async_resource.store(0, Ordering::Release);
            }
        }

        fn destroy_async_context(&self, env: NapiEnv) {
            let context = self.async_context.swap(null_mut(), Ordering::AcqRel);
            if !context.is_null() {
                let _ = unsafe { (napi().async_destroy)(env, context) };
            }
            let resource = self.async_resource.swap(0, Ordering::AcqRel);
            if resource != 0 {
                let _ = unsafe { (napi().delete_reference)(env, resource as NapiRef) };
            }
        }

        fn abandon_js_references(&self) {
            self.env.store(null_mut(), Ordering::Release);
            self.async_context.store(null_mut(), Ordering::Release);
            self.async_resource.store(0, Ordering::Release);
            self.records.lock().expect("record lock poisoned").clear();
            self.handlers.lock().expect("handler lock poisoned").take();
        }

        fn clear_commit_digest(&self) {
            if let Some(mut digest) = self
                .commit_digest
                .lock()
                .expect("digest lock poisoned")
                .take()
            {
                digest.fill(0);
            }
        }

        fn close_native(self: &Arc<Self>) {
            let first = self
                .state
                .lock()
                .expect("state lock poisoned")
                .begin_close();
            if first {
                for index in [2_usize, 3] {
                    if let Ok(handle) = self.handle(index) {
                        let _ = unsafe { CancelIoEx(handle, null()) };
                    }
                }
                let port = self.iocp.load(Ordering::Acquire);
                if port != 0 {
                    let _ = unsafe {
                        PostQueuedCompletionStatus(port as HANDLE, 0, IOCP_CLOSE, null())
                    };
                }
            }
            if let Some(worker) = self.worker.lock().expect("worker lock poisoned").take() {
                let _ = worker.join();
            }
            self.drain_writes_as_failed();
            self.close_handles();
            self.state
                .lock()
                .expect("state lock poisoned")
                .finish_close();
            self.clear_commit_digest();
        }

        fn request_close(self: &Arc<Self>, invoke_js: bool, failed: bool) {
            if failed {
                self.close_failure_requested.store(true, Ordering::Release);
            }
            self.close_native();
            if invoke_js {
                self.record_close(failed || self.close_failure_requested.load(Ordering::Acquire));
            } else {
                self.discard_js_references();
                close_uv(self);
            }
        }

        fn request_close_from_finalizer(self: &Arc<Self>) {
            self.close_native();
            self.release_references_on_uv_close
                .store(true, Ordering::Release);
            close_uv(self);
        }

        fn request_close_from_environment(self: &Arc<Self>) {
            self.env.store(null_mut(), Ordering::Release);
            self.close_native();
            self.abandon_js_references();
            close_uv(self);
        }

        fn enqueue_mux_write(&self, mut request: WriteRequest) -> Result<(), ContractError> {
            {
                let state = match self.state.lock() {
                    Ok(state) => state,
                    Err(_) => {
                        if request.zero_after_write {
                            request.bytes.fill(0);
                        }
                        return Err(ContractError::InvalidState);
                    }
                };
                if state.lifecycle != Lifecycle::Open
                    || !state.mux_started
                    || !state.package_ready_marked
                {
                    if request.zero_after_write {
                        request.bytes.fill(0);
                    }
                    return Err(ContractError::InvalidState);
                }
            }
            let mut writes = match self.writes.lock() {
                Ok(writes) => writes,
                Err(_) => {
                    if request.zero_after_write {
                        request.bytes.fill(0);
                    }
                    return Err(ContractError::InvalidState);
                }
            };
            writes.push_back(request);
            let port = self.iocp.load(Ordering::Acquire);
            if port == 0
                || unsafe { PostQueuedCompletionStatus(port as HANDLE, 0, IOCP_WAKE, null()) } == 0
            {
                if let Some(mut request) = writes.pop_back() {
                    if request.zero_after_write {
                        request.bytes.fill(0);
                    }
                }
                return Err(ContractError::InvalidState);
            }
            Ok(())
        }
    }

    impl Drop for NativePipeSet {
        fn drop(&mut self) {
            if let Ok(digest) = self.commit_digest.get_mut() {
                if let Some(mut digest) = digest.take() {
                    digest.fill(0);
                }
            }
            for handle in &self.handles {
                let raw = handle.swap(0, Ordering::AcqRel);
                if raw != 0 {
                    let _ = unsafe { CloseHandle(raw as HANDLE) };
                }
            }
            let port = self.iocp.swap(0, Ordering::AcqRel);
            if port != 0 {
                let _ = unsafe { CloseHandle(port as HANDLE) };
            }
        }
    }

    struct UvContext {
        inner: Arc<NativePipeSet>,
        _storage: Box<[usize]>,
    }

    #[repr(C)]
    struct IoOperation {
        overlapped: OVERLAPPED,
        kind: IoKind,
        bytes: Vec<u8>,
        offset: usize,
        done: usize,
        zero_after_write: bool,
    }

    impl Drop for IoOperation {
        fn drop(&mut self) {
            if self.zero_after_write {
                self.bytes.fill(0);
            }
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum IoKind {
        Read,
        Write,
    }

    #[no_mangle]
    pub extern "C" fn node_api_module_get_api_version_v1() -> i32 {
        9
    }

    #[no_mangle]
    pub unsafe extern "C" fn napi_register_module_v1(
        env: NapiEnv,
        exports: NapiValue,
    ) -> NapiValue {
        let Some(api) = (unsafe { NapiApi::load() }) else {
            return exports;
        };
        let _ = NAPI.set(api);
        let descriptor = method_descriptor(OPEN_EXPORT_C_KEY, open_pipes);
        if unsafe { (napi().define_properties)(env, exports, 1, &descriptor) } != NAPI_OK {
            return exports;
        }
        exports
    }

    fn method_descriptor(name: &'static [u8], method: NapiCallback) -> NapiPropertyDescriptor {
        NapiPropertyDescriptor {
            utf8name: name.as_ptr().cast(),
            name: null_mut(),
            method: Some(method),
            getter: None,
            setter: None,
            value: null_mut(),
            attributes: NAPI_DEFAULT_JSPROPERTY,
            data: null_mut(),
        }
    }

    fn value_descriptor(name: &'static [u8], value: NapiValue) -> NapiPropertyDescriptor {
        NapiPropertyDescriptor {
            utf8name: name.as_ptr().cast(),
            name: null_mut(),
            method: None,
            getter: None,
            setter: None,
            value,
            attributes: NAPI_DEFAULT_JSPROPERTY,
            data: null_mut(),
        }
    }

    unsafe extern "C" fn open_pipes(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
        if OPEN_CALLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return throw_broker_error(env);
        }
        match unsafe { open_pipes_impl(env, info) } {
            Ok(value) => value,
            Err(()) => throw_broker_error(env),
        }
    }

    unsafe fn open_pipes_impl(env: NapiEnv, info: NapiCallbackInfo) -> Result<NapiValue, ()> {
        let arguments = unsafe { callback_arguments(env, info, 1)? };
        let mut raw_value = unsafe { read_string(env, arguments[0])? };
        let parsed = parse_bootstrap(&raw_value).map_err(|_| ());
        raw_value.fill(0);
        let parsed = parsed?;
        let handles = open_pipe_handles(&parsed)?;
        let digest = match perform_handshake(&parsed, &handles) {
            Ok(digest) => digest,
            Err(()) => {
                close_raw_handles(&handles);
                return Err(());
            }
        };
        let inner = NativePipeSet::new(parsed.generation, digest, handles, env);
        unsafe { create_pipe_set_object(env, inner) }
    }

    unsafe fn create_pipe_set_object(
        env: NapiEnv,
        inner: Arc<NativePipeSet>,
    ) -> Result<NapiValue, ()> {
        let api = napi();
        let mut object = null_mut();
        check(unsafe { (api.create_object)(env, &mut object) })?;
        let mut version = null_mut();
        check(unsafe { (api.create_uint32)(env, 1, &mut version) })?;
        let mut generation = null_mut();
        check(unsafe { (api.create_bigint_uint64)(env, inner.generation, &mut generation) })?;
        let descriptors = [
            value_descriptor(API_C_KEYS[0], version),
            value_descriptor(API_C_KEYS[1], generation),
            method_descriptor(API_C_KEYS[2], write_sync),
            method_descriptor(API_C_KEYS[3], read_sync_exact),
            method_descriptor(API_C_KEYS[4], start_mux),
            method_descriptor(API_C_KEYS[5], write_mux),
            method_descriptor(API_C_KEYS[6], mark_package_ready),
            method_descriptor(API_C_KEYS[7], close),
        ];
        debug_assert_eq!(API_KEYS.len(), descriptors.len());
        check(unsafe {
            (api.define_properties)(env, object, descriptors.len(), descriptors.as_ptr())
        })?;
        let object_arc = Arc::into_raw(Arc::clone(&inner)) as *mut c_void;
        if check(unsafe {
            (api.wrap)(
                env,
                object,
                object_arc,
                Some(finalize_pipe_set),
                null_mut(),
                null_mut(),
            )
        })
        .is_err()
        {
            unsafe { drop(Arc::from_raw(object_arc.cast::<NativePipeSet>())) };
            inner.request_close(false, true);
            return Err(());
        }
        let cleanup_arc = Arc::into_raw(Arc::clone(&inner)) as *mut c_void;
        if check(unsafe { (api.add_env_cleanup_hook)(env, Some(environment_cleanup), cleanup_arc) })
            .is_err()
        {
            unsafe { drop(Arc::from_raw(cleanup_arc.cast::<NativePipeSet>())) };
            inner.request_close(false, true);
            return Err(());
        }
        if check(unsafe { (api.object_freeze)(env, object) }).is_err() {
            inner.request_close(false, true);
            return Err(());
        }
        Ok(object)
    }

    unsafe extern "C" fn finalize_pipe_set(_env: NapiEnv, data: *mut c_void, _hint: *mut c_void) {
        if !data.is_null() {
            let inner = unsafe { Arc::from_raw(data.cast::<NativePipeSet>()) };
            inner.request_close_from_finalizer();
        }
    }

    unsafe extern "C" fn environment_cleanup(data: *mut c_void) {
        if !data.is_null() {
            let inner = unsafe { Arc::from_raw(data.cast::<NativePipeSet>()) };
            inner.request_close_from_environment();
        }
    }

    unsafe extern "C" fn write_sync(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
        match unsafe { with_inner(env, info, 1) }.and_then(|(inner, arguments)| {
            let bytes = unsafe { copy_bounded_uint8_array(env, arguments[0], SYNC_FRAME_LIMIT) }?;
            let _guard = inner.sync_guard.lock().map_err(|_| ())?;
            let handle = inner.handle(0).map_err(|_| ())?;
            write_exact(handle, false, &bytes)?;
            Ok(())
        }) {
            Ok(()) => unsafe { undefined(env) },
            Err(()) => throw_broker_error(env),
        }
    }

    unsafe extern "C" fn read_sync_exact(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
        let result = unsafe { with_inner(env, info, 1) }.and_then(|(inner, arguments)| {
            let length = unsafe { exact_u32(env, arguments[0]) }? as usize;
            validate_frame_length(length, SYNC_FRAME_LIMIT).map_err(|_| ())?;
            let api = napi();
            let mut data = null_mut();
            let mut arraybuffer = null_mut();
            check(unsafe { (api.create_arraybuffer)(env, length, &mut data, &mut arraybuffer) })?;
            let bytes = if length == 0 {
                &mut [] as &mut [u8]
            } else {
                unsafe { std::slice::from_raw_parts_mut(data.cast::<u8>(), length) }
            };
            let _guard = inner.sync_guard.lock().map_err(|_| ())?;
            let handle = inner.handle(1).map_err(|_| ())?;
            read_exact(handle, false, bytes)?;
            let mut typedarray = null_mut();
            check(unsafe {
                (api.create_typedarray)(
                    env,
                    NAPI_UINT8_ARRAY,
                    length,
                    arraybuffer,
                    0,
                    &mut typedarray,
                )
            })?;
            Ok(typedarray)
        });
        match result {
            Ok(value) => value,
            Err(()) => throw_broker_error(env),
        }
    }

    unsafe extern "C" fn start_mux(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
        let result = unsafe { with_inner(env, info, 1) }.and_then(|(inner, arguments)| {
            let handlers = unsafe { validate_handlers(env, arguments[0]) }?;
            let start_result = inner
                .state
                .lock()
                .map_err(|_| ())
                .and_then(|mut state| state.start_mux().map_err(|_| ()));
            if start_result.is_err() {
                delete_handler_references(env, handlers);
                return Err(());
            }
            let Ok(mut handler_slot) = inner.handlers.lock() else {
                delete_handler_references(env, handlers);
                inner.request_close(false, true);
                return Err(());
            };
            *handler_slot = Some(handlers);
            drop(handler_slot);
            if initialize_uv(&inner, env).is_err() || start_iocp_worker(&inner).is_err() {
                if let Some(handlers) = inner.handlers.lock().map_err(|_| ())?.take() {
                    delete_handler_references(env, handlers);
                }
                inner.request_close(false, true);
                return Err(());
            }
            Ok(())
        });
        match result {
            Ok(()) => unsafe { undefined(env) },
            Err(()) => throw_broker_error(env),
        }
    }

    unsafe extern "C" fn write_mux(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
        let result = unsafe { with_inner(env, info, 2) }.and_then(|(inner, arguments)| {
            let bytes = unsafe { copy_bounded_uint8_array(env, arguments[0], MUX_FRAME_LIMIT) }?;
            let done = unsafe { create_callable_reference(env, arguments[1]) }?;
            if inner
                .enqueue_mux_write(WriteRequest {
                    bytes,
                    done: done as usize,
                    zero_after_write: false,
                })
                .is_err()
            {
                let _ = unsafe { (napi().delete_reference)(env, done) };
                return Err(());
            }
            Ok(())
        });
        match result {
            Ok(()) => unsafe { undefined(env) },
            Err(()) => throw_broker_error(env),
        }
    }

    unsafe extern "C" fn mark_package_ready(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
        let result = unsafe { with_inner(env, info, 1) }.and_then(|(inner, arguments)| {
            let done = NapiReferenceGuard::new(env, unsafe {
                create_callable_reference(env, arguments[0])
            }?);
            {
                let mut state = inner.state.lock().map_err(|_| ())?;
                if state.mark_package_ready().is_err() {
                    return Err(());
                }
            }
            let mut digest = inner
                .commit_digest
                .lock()
                .map_err(|_| ())?
                .take()
                .ok_or(())?;
            let mut sync_record = encode_bootstrap_record(
                BootstrapPhase::PackageReady,
                0,
                1,
                inner.generation,
                &digest,
            );
            let mut mux_record = encode_bootstrap_record(
                BootstrapPhase::PackageReady,
                2,
                3,
                inner.generation,
                &digest,
            );
            let sync_write_result = match inner.sync_guard.lock() {
                Ok(_guard) => inner
                    .handle(0)
                    .map_err(|_| ())
                    .and_then(|handle| write_exact(handle, false, &sync_record)),
                Err(_) => Err(()),
            };
            let done = done.into_raw();
            if sync_write_result.is_err() {
                sync_record.fill(0);
                digest.fill(0);
                inner.push_record(JsRecord::Done {
                    reference: done as usize,
                    failed: true,
                });
                inner.request_close(true, true);
                return Ok(());
            }
            sync_record.fill(0);
            digest.fill(0);
            let enqueue_result = inner.enqueue_mux_write(WriteRequest {
                bytes: mux_record.to_vec(),
                done: done as usize,
                zero_after_write: true,
            });
            mux_record.fill(0);
            if enqueue_result.is_err() {
                inner.push_record(JsRecord::Done {
                    reference: done as usize,
                    failed: true,
                });
                inner.request_close(true, true);
            }
            Ok(())
        });
        match result {
            Ok(()) => unsafe { undefined(env) },
            Err(()) => throw_broker_error(env),
        }
    }

    unsafe extern "C" fn close(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
        let result = unsafe { with_inner(env, info, 0) }.map(|(inner, _)| {
            inner.request_close(true, false);
        });
        match result {
            Ok(()) => unsafe { undefined(env) },
            Err(()) => throw_broker_error(env),
        }
    }

    unsafe fn with_inner(
        env: NapiEnv,
        info: NapiCallbackInfo,
        expected_arguments: usize,
    ) -> Result<(Arc<NativePipeSet>, Vec<NapiValue>), ()> {
        let arguments = unsafe { callback_arguments(env, info, expected_arguments)? };
        let mut this_arg = null_mut();
        let mut argc = 0_usize;
        check(unsafe {
            (napi().get_cb_info)(env, info, &mut argc, null_mut(), &mut this_arg, null_mut())
        })?;
        let mut raw = null_mut();
        check(unsafe { (napi().unwrap)(env, this_arg, &mut raw) })?;
        if raw.is_null() {
            return Err(());
        }
        unsafe { Arc::increment_strong_count(raw.cast::<NativePipeSet>()) };
        let inner = unsafe { Arc::from_raw(raw.cast::<NativePipeSet>()) };
        Ok((inner, arguments))
    }

    unsafe fn callback_arguments(
        env: NapiEnv,
        info: NapiCallbackInfo,
        expected: usize,
    ) -> Result<Vec<NapiValue>, ()> {
        let capacity = expected.saturating_add(1).max(1);
        let mut values = vec![null_mut(); capacity];
        let mut count = capacity;
        check(unsafe {
            (napi().get_cb_info)(
                env,
                info,
                &mut count,
                values.as_mut_ptr(),
                null_mut(),
                null_mut(),
            )
        })?;
        if count != expected {
            return Err(());
        }
        values.truncate(expected);
        Ok(values)
    }

    unsafe fn read_string(env: NapiEnv, value: NapiValue) -> Result<Vec<u8>, ()> {
        if unsafe { value_type(env, value)? } != NAPI_STRING {
            return Err(());
        }
        let mut length = 0_usize;
        check(unsafe { (napi().get_value_string_utf8)(env, value, null_mut(), 0, &mut length) })?;
        if length > VALUE_PREFIX.len() + MAX_BOOTSTRAP_BASE64URL_BYTES {
            return Err(());
        }
        let mut buffer = vec![0_u8; length + 1];
        let mut written = 0_usize;
        check(unsafe {
            (napi().get_value_string_utf8)(
                env,
                value,
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut written,
            )
        })?;
        if written != length {
            buffer.fill(0);
            return Err(());
        }
        buffer.truncate(length);
        Ok(buffer)
    }

    unsafe fn value_type(env: NapiEnv, value: NapiValue) -> Result<i32, ()> {
        let mut value_type = 0_i32;
        check(unsafe { (napi().typeof_value)(env, value, &mut value_type) })?;
        Ok(value_type)
    }

    unsafe fn exact_u32(env: NapiEnv, value: NapiValue) -> Result<u32, ()> {
        if unsafe { value_type(env, value)? } != NAPI_NUMBER {
            return Err(());
        }
        let mut number = 0_f64;
        check(unsafe { (napi().get_value_double)(env, value, &mut number) })?;
        if !number.is_finite()
            || number < 0.0
            || number > f64::from(u32::MAX)
            || number.fract() != 0.0
        {
            return Err(());
        }
        Ok(number as u32)
    }

    unsafe fn copy_bounded_uint8_array(
        env: NapiEnv,
        value: NapiValue,
        limit: usize,
    ) -> Result<Vec<u8>, ()> {
        let mut kind = 0_i32;
        let mut length = 0_usize;
        let mut data = null_mut();
        check(unsafe {
            (napi().get_typedarray_info)(
                env,
                value,
                &mut kind,
                &mut length,
                &mut data,
                null_mut(),
                null_mut(),
            )
        })?;
        if kind != NAPI_UINT8_ARRAY
            || validate_frame_length(length, limit).is_err()
            || (length != 0 && data.is_null())
        {
            return Err(());
        }
        if length == 0 {
            Ok(Vec::new())
        } else {
            Ok(unsafe { std::slice::from_raw_parts(data.cast::<u8>(), length) }.to_vec())
        }
    }

    unsafe fn validate_handlers(env: NapiEnv, value: NapiValue) -> Result<HandlerRefs, ()> {
        if unsafe { value_type(env, value)? } != NAPI_OBJECT {
            return Err(());
        }
        let api = napi();
        let mut keys = null_mut();
        check(unsafe {
            (api.get_all_property_names)(
                env,
                value,
                NAPI_KEY_OWN_ONLY,
                NAPI_KEY_ALL_PROPERTIES,
                NAPI_KEY_NUMBERS_TO_STRINGS,
                &mut keys,
            )
        })?;
        let mut length = 0_u32;
        check(unsafe { (api.get_array_length)(env, keys, &mut length) })?;
        if length != 2 {
            return Err(());
        }
        for (index, expected) in [b"bytes".as_slice(), b"close".as_slice()]
            .into_iter()
            .enumerate()
        {
            let mut key = null_mut();
            check(unsafe { (api.get_element)(env, keys, index as u32, &mut key) })?;
            if unsafe { read_string(env, key)? } != expected {
                return Err(());
            }
        }
        let bytes = unsafe { named_callable(env, value, b"bytes\0")? };
        let close = match unsafe { named_callable(env, value, b"close\0") } {
            Ok(close) => close,
            Err(()) => {
                let _ = unsafe { (api.delete_reference)(env, bytes) };
                return Err(());
            }
        };
        Ok(HandlerRefs {
            bytes: bytes as usize,
            close: close as usize,
        })
    }

    unsafe fn named_callable(
        env: NapiEnv,
        object: NapiValue,
        name: &'static [u8],
    ) -> Result<NapiRef, ()> {
        let mut value = null_mut();
        check(unsafe {
            (napi().get_named_property)(env, object, name.as_ptr().cast(), &mut value)
        })?;
        unsafe { create_callable_reference(env, value) }
    }

    unsafe fn create_callable_reference(env: NapiEnv, value: NapiValue) -> Result<NapiRef, ()> {
        if unsafe { value_type(env, value)? } != NAPI_FUNCTION {
            return Err(());
        }
        let mut reference = null_mut();
        check(unsafe { (napi().create_reference)(env, value, 1, &mut reference) })?;
        Ok(reference)
    }

    unsafe fn undefined(env: NapiEnv) -> NapiValue {
        let mut value = null_mut();
        let _ = unsafe { (napi().get_undefined)(env, &mut value) };
        value
    }

    /// `napi_make_callback` coerces its receiver with `ToObject`, so `undefined`
    /// makes it fail with `napi_object_expected` and leave a pending TypeError
    /// behind instead of invoking the callback. Every completion the broker hands
    /// back to JavaScript goes through that one call, so the receiver has to be a
    /// real object; the global is the receiver a plain sloppy-mode callback would
    /// have observed anyway.
    unsafe fn callback_receiver(env: NapiEnv) -> Result<NapiValue, ()> {
        let mut value = null_mut();
        check(unsafe { (napi().get_global)(env, &mut value) })?;
        if value.is_null() {
            return Err(());
        }
        Ok(value)
    }

    fn check(status: i32) -> Result<(), ()> {
        if status == NAPI_OK {
            Ok(())
        } else {
            Err(())
        }
    }

    fn throw_broker_error(env: NapiEnv) -> NapiValue {
        let _ = unsafe {
            (napi().throw_error)(
                env,
                null(),
                c"Browser broker native pipe binding failed.".as_ptr(),
            )
        };
        null_mut()
    }

    fn open_pipe_handles(bootstrap: &Bootstrap) -> Result<[usize; 4], ()> {
        let mut handles = [0_usize; 4];
        for (index, (name, contract)) in bootstrap.pipe_names.iter().zip(PIPE_CONTRACTS).enumerate()
        {
            if contract.lane.as_bytes() != PIPE_LANES[index] {
                close_raw_handles(&handles);
                return Err(());
            }
            let mut wide = name.iter().copied().map(u16::from).collect::<Vec<_>>();
            wide.push(0);
            let access = match contract.access {
                PipeAccess::Write => GENERIC_WRITE,
                PipeAccess::Read => GENERIC_READ,
            };
            let flags = FILE_ATTRIBUTE_NORMAL
                | if contract.overlapped {
                    FILE_FLAG_OVERLAPPED
                } else {
                    0
                };
            let disposition = if contract.open_existing {
                OPEN_EXISTING
            } else {
                close_raw_handles(&handles);
                return Err(());
            };
            let handle = unsafe {
                CreateFileW(
                    wide.as_ptr(),
                    access,
                    contract.share_mode,
                    null(),
                    disposition,
                    flags,
                    null_mut(),
                )
            };
            wide.fill(0);
            if handle == INVALID_HANDLE_VALUE || handle.is_null() {
                close_raw_handles(&handles);
                return Err(());
            }
            handles[index] = handle as usize;
            if contract.require_byte_read_mode {
                let mut mode = 0_u32;
                if unsafe {
                    GetNamedPipeHandleStateW(
                        handle,
                        &mut mode,
                        null_mut(),
                        null_mut(),
                        null_mut(),
                        null_mut(),
                        0,
                    )
                } == 0
                    || mode & (PIPE_READMODE_MESSAGE | PIPE_NOWAIT) != 0
                {
                    close_raw_handles(&handles);
                    return Err(());
                }
            }
        }
        Ok(handles)
    }

    fn close_raw_handles(handles: &[usize; 4]) {
        for handle in handles {
            if *handle != 0 {
                let _ = unsafe { CloseHandle(*handle as HANDLE) };
            }
        }
    }

    struct TranscriptSecrets {
        sync_hello: [u8; 32],
        mux_hello: [u8; 32],
        adoption: [u8; 32],
    }

    impl Drop for TranscriptSecrets {
        fn drop(&mut self) {
            self.sync_hello.fill(0);
            self.mux_hello.fill(0);
            self.adoption.fill(0);
        }
    }

    fn perform_handshake(bootstrap: &Bootstrap, handles: &[usize; 4]) -> Result<[u8; 32], ()> {
        let generation = bootstrap.generation;
        let mut secrets = TranscriptSecrets {
            sync_hello: [0; 32],
            mux_hello: [0; 32],
            adoption: [0; 32],
        };
        secrets.sync_hello = read_record(
            handles[1],
            false,
            BootstrapPhase::ServerHello,
            1,
            0,
            generation,
        )?;
        write_record(
            handles[0],
            false,
            BootstrapPhase::ClientHello,
            0,
            1,
            generation,
            &secrets.sync_hello,
        )?;
        secrets.mux_hello = read_record(
            handles[3],
            true,
            BootstrapPhase::ServerHello,
            3,
            2,
            generation,
        )?;
        write_record(
            handles[2],
            true,
            BootstrapPhase::ClientHello,
            2,
            3,
            generation,
            &secrets.mux_hello,
        )?;
        secrets.adoption = read_record(
            handles[1],
            false,
            BootstrapPhase::AdoptionChallenge,
            1,
            0,
            generation,
        )?;
        let mut mux_adoption = read_record(
            handles[3],
            true,
            BootstrapPhase::AdoptionChallenge,
            3,
            2,
            generation,
        )?;
        let adoption_matches = mux_adoption == secrets.adoption;
        mux_adoption.fill(0);
        if !adoption_matches {
            return Err(());
        }
        write_record(
            handles[0],
            false,
            BootstrapPhase::AdoptionEcho,
            0,
            1,
            generation,
            &secrets.adoption,
        )?;
        write_record(
            handles[2],
            true,
            BootstrapPhase::AdoptionEcho,
            2,
            3,
            generation,
            &secrets.adoption,
        )?;
        let expected_digest = adoption_commit_digest(
            generation,
            &bootstrap.nonce,
            &secrets.sync_hello,
            &secrets.mux_hello,
            &secrets.adoption,
            unsafe { GetCurrentProcessId() },
        );
        let sync_commit = read_record(handles[1], false, BootstrapPhase::Commit, 1, 0, generation)?;
        let mux_commit = read_record(handles[3], true, BootstrapPhase::Commit, 3, 2, generation)?;
        if sync_commit != expected_digest || mux_commit != expected_digest {
            return Err(());
        }
        write_record(
            handles[0],
            false,
            BootstrapPhase::CommitEcho,
            0,
            1,
            generation,
            &expected_digest,
        )?;
        write_record(
            handles[2],
            true,
            BootstrapPhase::CommitEcho,
            2,
            3,
            generation,
            &expected_digest,
        )?;
        let sync_ack = read_record(
            handles[1],
            false,
            BootstrapPhase::AdoptionAck,
            1,
            0,
            generation,
        )?;
        let mux_ack = read_record(
            handles[3],
            true,
            BootstrapPhase::AdoptionAck,
            3,
            2,
            generation,
        )?;
        if sync_ack != expected_digest || mux_ack != expected_digest {
            return Err(());
        }
        secrets.sync_hello.fill(0);
        secrets.mux_hello.fill(0);
        secrets.adoption.fill(0);
        Ok(expected_digest)
    }

    fn read_record(
        handle: usize,
        overlapped: bool,
        phase: BootstrapPhase,
        own_role: u8,
        peer_role: u8,
        generation: u64,
    ) -> Result<[u8; 32], ()> {
        let mut bytes = [0_u8; BOOTSTRAP_RECORD_BYTES];
        let result = read_exact(handle as HANDLE, overlapped, &mut bytes).and_then(|()| {
            decode_bootstrap_record(&bytes, phase, own_role, peer_role, generation).map_err(|_| ())
        });
        bytes.fill(0);
        result
    }

    fn write_record(
        handle: usize,
        overlapped: bool,
        phase: BootstrapPhase,
        own_role: u8,
        peer_role: u8,
        generation: u64,
        token: &[u8; 32],
    ) -> Result<(), ()> {
        let mut record = encode_bootstrap_record(phase, own_role, peer_role, generation, token);
        let result = write_exact(handle as HANDLE, overlapped, &record);
        record.fill(0);
        result
    }

    fn read_exact(handle: HANDLE, overlapped: bool, output: &mut [u8]) -> Result<(), ()> {
        let mut offset = 0_usize;
        while offset < output.len() {
            let transferred = blocking_read(handle, overlapped, &mut output[offset..])?;
            if transferred == 0 {
                return Err(());
            }
            offset += transferred;
        }
        Ok(())
    }

    fn write_exact(handle: HANDLE, overlapped: bool, input: &[u8]) -> Result<(), ()> {
        let mut offset = 0_usize;
        while offset < input.len() {
            let transferred = blocking_write(handle, overlapped, &input[offset..])?;
            if transferred == 0 {
                return Err(());
            }
            offset += transferred;
        }
        Ok(())
    }

    fn blocking_read(handle: HANDLE, overlapped: bool, output: &mut [u8]) -> Result<usize, ()> {
        let length = u32::try_from(output.len()).map_err(|_| ())?;
        if !overlapped {
            let mut transferred = 0_u32;
            if unsafe {
                ReadFile(
                    handle,
                    output.as_mut_ptr(),
                    length,
                    &mut transferred,
                    null_mut(),
                )
            } == 0
            {
                return Err(());
            }
            return Ok(transferred as usize);
        }
        blocking_overlapped_io(handle, output.as_mut_ptr(), length, false)
    }

    fn blocking_write(handle: HANDLE, overlapped: bool, input: &[u8]) -> Result<usize, ()> {
        let length = u32::try_from(input.len()).map_err(|_| ())?;
        if !overlapped {
            let mut transferred = 0_u32;
            if unsafe { WriteFile(handle, input.as_ptr(), length, &mut transferred, null_mut()) }
                == 0
            {
                return Err(());
            }
            return Ok(transferred as usize);
        }
        blocking_overlapped_io(handle, input.as_ptr() as *mut u8, length, true)
    }

    fn blocking_overlapped_io(
        handle: HANDLE,
        buffer: *mut u8,
        length: u32,
        write: bool,
    ) -> Result<usize, ()> {
        let event = unsafe { CreateEventW(null(), 1, 0, null()) };
        if event.is_null() {
            return Err(());
        }
        let mut overlapped = OVERLAPPED {
            hEvent: event,
            ..Default::default()
        };
        let mut transferred = 0_u32;
        let started = if write {
            unsafe { WriteFile(handle, buffer, length, &mut transferred, &mut overlapped) }
        } else {
            unsafe { ReadFile(handle, buffer, length, &mut transferred, &mut overlapped) }
        };
        let result = if started != 0 {
            Ok(transferred as usize)
        } else if unsafe { GetLastError() } != ERROR_IO_PENDING {
            Err(())
        } else if unsafe { GetOverlappedResult(handle, &overlapped, &mut transferred, 1) } != 0 {
            Ok(transferred as usize)
        } else {
            Err(())
        };
        let _ = unsafe { CloseHandle(event) };
        result
    }

    fn initialize_uv(inner: &Arc<NativePipeSet>, env: NapiEnv) -> Result<(), ()> {
        let uv_api = uv().ok_or(())?;
        let mut resource_name = null_mut();
        check(unsafe {
            (napi().create_string_utf8)(
                env,
                ASYNC_RESOURCE_NAME.as_ptr().cast(),
                ASYNC_RESOURCE_NAME.len(),
                &mut resource_name,
            )
        })?;
        let mut async_resource = null_mut();
        check(unsafe { (napi().create_object)(env, &mut async_resource) })?;
        let mut async_resource_ref = null_mut();
        check(unsafe {
            (napi().create_reference)(env, async_resource, 1, &mut async_resource_ref)
        })?;
        let mut async_context = null_mut();
        if check(unsafe {
            (napi().async_init)(env, async_resource, resource_name, &mut async_context)
        })
        .is_err()
            || async_context.is_null()
        {
            let _ = unsafe { (napi().delete_reference)(env, async_resource_ref) };
            return Err(());
        }
        let mut event_loop = null_mut();
        if check(unsafe { (napi().get_uv_event_loop)(env, &mut event_loop) }).is_err()
            || event_loop.is_null()
        {
            let _ = unsafe { (napi().async_destroy)(env, async_context) };
            let _ = unsafe { (napi().delete_reference)(env, async_resource_ref) };
            return Err(());
        }
        let byte_size = unsafe { (uv_api.handle_size)(UV_ASYNC) };
        if byte_size == 0 {
            let _ = unsafe { (napi().async_destroy)(env, async_context) };
            let _ = unsafe { (napi().delete_reference)(env, async_resource_ref) };
            return Err(());
        }
        let word_size = std::mem::size_of::<usize>();
        let words = byte_size.div_ceil(word_size);
        let mut context = Box::new(UvContext {
            inner: Arc::clone(inner),
            _storage: vec![0_usize; words].into_boxed_slice(),
        });
        let handle = context._storage.as_mut_ptr().cast::<c_void>();
        if unsafe { (uv_api.async_init)(event_loop, handle, Some(uv_async_callback)) } != 0 {
            let _ = unsafe { (napi().async_destroy)(env, async_context) };
            let _ = unsafe { (napi().delete_reference)(env, async_resource_ref) };
            return Err(());
        }
        unsafe { (uv_api.handle_set_data)(handle, Box::into_raw(context).cast()) };
        inner.async_context.store(async_context, Ordering::Release);
        inner
            .async_resource
            .store(async_resource_ref as usize, Ordering::Release);
        inner.uv_async.store(handle, Ordering::Release);
        Ok(())
    }

    fn close_uv(inner: &NativePipeSet) {
        let handle = inner.uv_async.swap(null_mut(), Ordering::AcqRel);
        if !handle.is_null() {
            if let Some(api) = uv() {
                unsafe { (api.close)(handle, Some(uv_close_callback)) };
            }
        }
    }

    unsafe extern "C" fn uv_close_callback(handle: *mut c_void) {
        if let Some(api) = uv() {
            let context = unsafe { (api.handle_get_data)(handle) };
            if !context.is_null() {
                let context = unsafe { Box::from_raw(context.cast::<UvContext>()) };
                if context
                    .inner
                    .release_references_on_uv_close
                    .swap(false, Ordering::AcqRel)
                {
                    context.inner.discard_js_references();
                }
                drop(context);
            }
        }
    }

    unsafe extern "C" fn uv_async_callback(handle: *mut c_void) {
        let Some(api) = uv() else {
            return;
        };
        let context = unsafe { (api.handle_get_data)(handle) };
        if context.is_null() {
            return;
        }
        let inner = &unsafe { &*context.cast::<UvContext>() }.inner;
        let env = inner.env.load(Ordering::Acquire);
        if env.is_null() {
            close_uv(inner);
            return;
        }
        let mut scope = null_mut();
        if unsafe { (napi().open_handle_scope)(env, &mut scope) } != NAPI_OK {
            inner.request_close(false, true);
            return;
        }
        let record = inner
            .records
            .lock()
            .expect("record lock poisoned")
            .pop_front();
        let mut close_after = false;
        if let Some(record) = record {
            close_after = unsafe { dispatch_js_record(env, inner, record) };
        }
        let _ = unsafe { (napi().close_handle_scope)(env, scope) };
        if close_after {
            close_uv(inner);
        } else if !inner
            .records
            .lock()
            .expect("record lock poisoned")
            .is_empty()
        {
            let _ = unsafe { (api.async_send)(handle) };
        }
    }

    unsafe fn dispatch_js_record(
        env: NapiEnv,
        inner: &Arc<NativePipeSet>,
        record: JsRecord,
    ) -> bool {
        match record {
            JsRecord::Bytes(bytes) => {
                let reference = inner
                    .handlers
                    .lock()
                    .expect("handler lock poisoned")
                    .map(|handlers| handlers.bytes);
                let Some(reference) = reference else {
                    inner.request_close(false, true);
                    return true;
                };
                let mut data = null_mut();
                let mut arraybuffer = null_mut();
                if check(unsafe {
                    (napi().create_arraybuffer)(env, bytes.len(), &mut data, &mut arraybuffer)
                })
                .is_err()
                {
                    inner.request_close(false, true);
                    return true;
                }
                if !bytes.is_empty() {
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            bytes.as_ptr(),
                            data.cast::<u8>(),
                            bytes.len(),
                        )
                    };
                }
                let mut typedarray = null_mut();
                if check(unsafe {
                    (napi().create_typedarray)(
                        env,
                        NAPI_UINT8_ARRAY,
                        bytes.len(),
                        arraybuffer,
                        0,
                        &mut typedarray,
                    )
                })
                .is_err()
                    || unsafe {
                        call_reference(env, inner, reference as NapiRef, Some(typedarray), false)
                    }
                    .is_err()
                {
                    inner.request_close(false, true);
                    return true;
                }
                false
            }
            JsRecord::Done { reference, failed } => {
                let _ = unsafe { call_reference(env, inner, reference as NapiRef, None, failed) };
                let _ = unsafe { (napi().delete_reference)(env, reference as NapiRef) };
                false
            }
            JsRecord::Close { failed } => {
                let handlers = inner.handlers.lock().expect("handler lock poisoned").take();
                if let Some(handlers) = handlers {
                    let _ = unsafe {
                        call_reference(env, inner, handlers.close as NapiRef, None, failed)
                    };
                    let _ = unsafe { (napi().delete_reference)(env, handlers.bytes as NapiRef) };
                    let _ = unsafe { (napi().delete_reference)(env, handlers.close as NapiRef) };
                }
                inner.destroy_async_context(env);
                true
            }
        }
    }

    unsafe fn call_reference(
        env: NapiEnv,
        inner: &NativePipeSet,
        reference: NapiRef,
        value: Option<NapiValue>,
        failed: bool,
    ) -> Result<(), ()> {
        let api = napi();
        let mut function = null_mut();
        check(unsafe { (api.get_reference_value)(env, reference, &mut function) })?;
        if function.is_null() {
            return Err(());
        }
        let async_context = inner.async_context.load(Ordering::Acquire);
        if async_context.is_null() {
            return Err(());
        }
        let receiver = unsafe { callback_receiver(env) }?;
        let mut arguments = [null_mut(); 1];
        let count = if let Some(value) = value {
            arguments[0] = value;
            1
        } else if failed {
            let mut message = null_mut();
            check(unsafe {
                (api.create_string_utf8)(
                    env,
                    BROKER_ERROR.as_ptr().cast(),
                    BROKER_ERROR.len(),
                    &mut message,
                )
            })?;
            let mut error = null_mut();
            check(unsafe { (api.create_error)(env, null_mut(), message, &mut error) })?;
            arguments[0] = error;
            1
        } else {
            0
        };
        check(unsafe {
            (api.make_callback)(
                env,
                async_context,
                receiver,
                function,
                count,
                if count == 0 {
                    null()
                } else {
                    arguments.as_ptr()
                },
                null_mut(),
            )
        })
    }

    fn start_iocp_worker(inner: &Arc<NativePipeSet>) -> Result<(), ()> {
        let read = inner.handle(3).map_err(|_| ())?;
        let write = inner.handle(2).map_err(|_| ())?;
        let port = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, null_mut(), 0, 1) };
        if port.is_null() {
            return Err(());
        }
        if unsafe { CreateIoCompletionPort(read, port, IOCP_READ, 0) } != port
            || unsafe { CreateIoCompletionPort(write, port, IOCP_WRITE, 0) } != port
        {
            let _ = unsafe { CloseHandle(port) };
            return Err(());
        }
        inner.iocp.store(port as usize, Ordering::Release);
        let worker_inner = Arc::clone(inner);
        let worker = thread::Builder::new()
            .name("klaus-browser-pipe-iocp".to_string())
            .spawn(move || iocp_worker(worker_inner))
            .map_err(|_| ())?;
        *inner.worker.lock().map_err(|_| ())? = Some(worker);
        Ok(())
    }

    fn worker_observes_close_request(inner: &NativePipeSet, locally_closing: bool) -> bool {
        locally_closing
            || inner.state.lock().expect("state lock poisoned").lifecycle != Lifecycle::Open
    }

    fn iocp_worker(inner: Arc<NativePipeSet>) {
        let mut closing = false;
        let mut failed = false;
        let mut read_active = issue_data_read(&inner).is_ok();
        let mut write_active = false;
        if !read_active {
            closing = true;
            failed = true;
            cancel_mux(&inner);
        }
        loop {
            if closing && !read_active && !write_active {
                break;
            }
            let port = inner.iocp.load(Ordering::Acquire);
            if port == 0 {
                failed = true;
                break;
            }
            let mut transferred = 0_u32;
            let mut key = 0_usize;
            let mut overlapped = null_mut();
            let ok = unsafe {
                GetQueuedCompletionStatus(
                    port as HANDLE,
                    &mut transferred,
                    &mut key,
                    &mut overlapped,
                    INFINITE,
                )
            };
            closing = worker_observes_close_request(&inner, closing);
            if overlapped.is_null() {
                match key {
                    IOCP_CLOSE => {
                        closing = true;
                        cancel_mux(&inner);
                    }
                    IOCP_WAKE if !closing && !write_active => match issue_next_write(&inner) {
                        Ok(active) => write_active = active,
                        Err(()) => {
                            closing = true;
                            failed = true;
                            cancel_mux(&inner);
                        }
                    },
                    IOCP_WAKE => {}
                    _ => {
                        closing = true;
                        failed = true;
                        cancel_mux(&inner);
                    }
                }
                continue;
            }

            let mut operation = unsafe { Box::from_raw(overlapped.cast::<IoOperation>()) };
            let io_error = ok == 0;
            let expected_key = match operation.kind {
                IoKind::Read => IOCP_READ,
                IoKind::Write => IOCP_WRITE,
            };
            if key != expected_key {
                match operation.kind {
                    IoKind::Read => read_active = false,
                    IoKind::Write => {
                        write_active = false;
                        inner.push_record(JsRecord::Done {
                            reference: operation.done,
                            failed: true,
                        });
                    }
                }
                closing = true;
                failed = true;
                cancel_mux(&inner);
                continue;
            }
            match operation.kind {
                IoKind::Read => {
                    read_active = false;
                    if closing {
                        continue;
                    }
                    if io_error || transferred == 0 || transferred as usize > operation.bytes.len()
                    {
                        closing = true;
                        failed = true;
                        cancel_mux(&inner);
                        continue;
                    }
                    operation.bytes.truncate(transferred as usize);
                    inner.push_record(JsRecord::Bytes(std::mem::take(&mut operation.bytes)));
                    match issue_data_read(&inner) {
                        Ok(()) => read_active = true,
                        Err(()) => {
                            closing = true;
                            failed = true;
                            cancel_mux(&inner);
                        }
                    }
                }
                IoKind::Write => {
                    write_active = false;
                    if closing {
                        inner.push_record(JsRecord::Done {
                            reference: operation.done,
                            failed: true,
                        });
                        continue;
                    }
                    if io_error || transferred == 0 {
                        inner.push_record(JsRecord::Done {
                            reference: operation.done,
                            failed: true,
                        });
                        closing = true;
                        failed = true;
                        cancel_mux(&inner);
                        continue;
                    }
                    operation.offset = match operation.offset.checked_add(transferred as usize) {
                        Some(offset) if offset <= operation.bytes.len() => offset,
                        _ => {
                            inner.push_record(JsRecord::Done {
                                reference: operation.done,
                                failed: true,
                            });
                            closing = true;
                            failed = true;
                            cancel_mux(&inner);
                            continue;
                        }
                    };
                    if operation.offset == operation.bytes.len() {
                        inner.push_record(JsRecord::Done {
                            reference: operation.done,
                            failed: false,
                        });
                        if !closing {
                            match issue_next_write(&inner) {
                                Ok(active) => write_active = active,
                                Err(()) => {
                                    closing = true;
                                    failed = true;
                                    cancel_mux(&inner);
                                }
                            }
                        }
                    } else {
                        match issue_write_operation(&inner, operation) {
                            Ok(()) => write_active = true,
                            Err(operation) => {
                                inner.push_record(JsRecord::Done {
                                    reference: operation.done,
                                    failed: true,
                                });
                                closing = true;
                                failed = true;
                                cancel_mux(&inner);
                            }
                        }
                    }
                }
            }
        }

        inner.drain_writes_as_failed();
        inner.close_handles();
        {
            let mut state = inner.state.lock().expect("state lock poisoned");
            if state.lifecycle == Lifecycle::Open {
                state.begin_close();
                failed = true;
            }
            state.finish_close();
        }
        inner.clear_commit_digest();
        failed |= inner.close_failure_requested.load(Ordering::Acquire);
        inner.record_close(failed);
    }

    fn cancel_mux(inner: &NativePipeSet) {
        for index in [2_usize, 3] {
            if let Ok(handle) = inner.handle(index) {
                let _ = unsafe { CancelIoEx(handle, null()) };
            }
        }
    }

    fn issue_data_read(inner: &NativePipeSet) -> Result<(), ()> {
        let operation = Box::new(IoOperation {
            overlapped: OVERLAPPED::default(),
            kind: IoKind::Read,
            bytes: vec![0_u8; MUX_READ_CHUNK_BYTES],
            offset: 0,
            done: 0,
            zero_after_write: false,
        });
        issue_read_operation(inner, operation)
    }

    fn issue_read_operation(inner: &NativePipeSet, operation: Box<IoOperation>) -> Result<(), ()> {
        let handle = inner.handle(3).map_err(|_| ())?;
        let length = u32::try_from(operation.bytes.len()).map_err(|_| ())?;
        if length == 0 {
            return Err(());
        }
        let raw = Box::into_raw(operation);
        let buffer = unsafe { (*raw).bytes.as_mut_ptr() };
        let started =
            unsafe { ReadFile(handle, buffer, length, null_mut(), &mut (*raw).overlapped) };
        if started == 0 && unsafe { GetLastError() } != ERROR_IO_PENDING {
            unsafe { drop(Box::from_raw(raw)) };
            return Err(());
        }
        Ok(())
    }

    fn issue_next_write(inner: &NativePipeSet) -> Result<bool, ()> {
        loop {
            let Some(request) = inner.writes.lock().map_err(|_| ())?.pop_front() else {
                return Ok(false);
            };
            if request.bytes.is_empty() {
                inner.push_record(JsRecord::Done {
                    reference: request.done,
                    failed: false,
                });
                continue;
            }
            let operation = Box::new(IoOperation {
                overlapped: OVERLAPPED::default(),
                kind: IoKind::Write,
                bytes: request.bytes,
                offset: 0,
                done: request.done,
                zero_after_write: request.zero_after_write,
            });
            return match issue_write_operation(inner, operation) {
                Ok(()) => Ok(true),
                Err(operation) => {
                    inner.push_record(JsRecord::Done {
                        reference: operation.done,
                        failed: true,
                    });
                    Err(())
                }
            };
        }
    }

    fn issue_write_operation(
        inner: &NativePipeSet,
        mut operation: Box<IoOperation>,
    ) -> Result<(), Box<IoOperation>> {
        let Ok(handle) = inner.handle(2) else {
            return Err(operation);
        };
        operation.overlapped = OVERLAPPED::default();
        let remaining = operation.bytes.len() - operation.offset;
        let Ok(length) = u32::try_from(remaining) else {
            return Err(operation);
        };
        let raw = Box::into_raw(operation);
        let started = unsafe {
            WriteFile(
                handle,
                (*raw).bytes.as_ptr().add((*raw).offset),
                length,
                null_mut(),
                &mut (*raw).overlapped,
            )
        };
        if started == 0 && unsafe { GetLastError() } != ERROR_IO_PENDING {
            return Err(unsafe { Box::from_raw(raw) });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical_bootstrap(generation: u64, nonce: [u8; 32]) -> Vec<u8> {
        let names = expected_pipe_names(generation, &nonce);
        let mut payload = Vec::new();
        payload.extend_from_slice(&generation.to_be_bytes());
        payload.extend_from_slice(&nonce);
        for name in names {
            payload.extend_from_slice(&u16::try_from(name.len()).unwrap().to_be_bytes());
            payload.extend_from_slice(&name);
        }
        let mut value = VALUE_PREFIX.to_vec();
        value.extend(encode_base64url(&payload));
        value
    }

    #[test]
    fn canonical_bootstrap_redundantly_freezes_generation_nonce_names_and_roles() {
        let nonce = [0xab; 32];
        let parsed = parse_bootstrap(&canonical_bootstrap(0x12, nonce)).unwrap();
        assert_eq!(parsed.generation, 0x12);
        assert_eq!(parsed.nonce, nonce);
        assert_eq!(parsed.pipe_names, expected_pipe_names(0x12, &nonce));
    }

    #[test]
    fn bootstrap_parser_rejects_noncanonical_malformed_and_authority_bearing_values() {
        let canonical = canonical_bootstrap(7, [9; 32]);
        for invalid in [
            Vec::new(),
            b"v2.AA".to_vec(),
            b"v1.A=".to_vec(),
            b"v1.*".to_vec(),
            b"v1.AR".to_vec(),
            [b"v1.".as_slice(), &[0xff]].concat(),
        ] {
            assert_eq!(
                parse_bootstrap(&invalid),
                Err(ContractError::InvalidBootstrap)
            );
        }

        let encoded = canonical.strip_prefix(VALUE_PREFIX).unwrap();
        let mut payload = decode_base64url(encoded).unwrap();
        payload.extend_from_slice(b"process=C:\\malicious.exe");
        let mut trailing = VALUE_PREFIX.to_vec();
        trailing.extend(encode_base64url(&payload));
        assert_eq!(
            parse_bootstrap(&trailing),
            Err(ContractError::InvalidBootstrap)
        );

        let mut wrong_lane_payload = decode_base64url(encoded).unwrap();
        let lane = wrong_lane_payload
            .windows(b"sync-c2h".len())
            .position(|window| window == b"sync-c2h")
            .unwrap();
        wrong_lane_payload[lane] = b'X';
        let mut wrong_lane = VALUE_PREFIX.to_vec();
        wrong_lane.extend(encode_base64url(&wrong_lane_payload));
        assert_eq!(
            parse_bootstrap(&wrong_lane),
            Err(ContractError::InvalidBootstrap)
        );

        assert_eq!(
            parse_bootstrap(&canonical_bootstrap(0, [1; 32])),
            Err(ContractError::InvalidBootstrap)
        );
        let oversized = [
            VALUE_PREFIX,
            vec![b'A'; MAX_BOOTSTRAP_BASE64URL_BYTES + 1].as_slice(),
        ]
        .concat();
        assert_eq!(
            parse_bootstrap(&oversized),
            Err(ContractError::InvalidBootstrap)
        );
    }

    #[test]
    fn public_contract_has_only_the_fixed_ordered_api_and_four_pipe_modes() {
        assert_eq!(
            API_KEYS,
            [
                "version",
                "generation",
                "writeSync",
                "readSyncExact",
                "startMux",
                "writeMux",
                "markPackageReady",
                "close",
            ]
        );
        assert_eq!(
            API_C_KEYS.map(|key| std::str::from_utf8(&key[..key.len() - 1]).unwrap()),
            API_KEYS
        );
        assert_eq!(
            &OPEN_EXPORT_C_KEY[..OPEN_EXPORT_C_KEY.len() - 1],
            b"openWindowsBrokerPipesV1"
        );
        assert_eq!(PIPE_CONTRACTS[0].access, PipeAccess::Write);
        assert_eq!(PIPE_CONTRACTS[1].access, PipeAccess::Read);
        assert!(!PIPE_CONTRACTS[0].overlapped);
        assert!(!PIPE_CONTRACTS[1].overlapped);
        assert!(PIPE_CONTRACTS[2].overlapped);
        assert!(PIPE_CONTRACTS[3].overlapped);
        assert!(PIPE_CONTRACTS
            .iter()
            .all(|contract| contract.share_mode == 0 && contract.open_existing));
        assert!(PIPE_CONTRACTS[1].require_byte_read_mode);
        assert!(PIPE_CONTRACTS[3].require_byte_read_mode);
        assert_eq!(BROKER_ERROR, "Browser broker native pipe binding failed.");
        let source = include_str!("lib.rs");
        let native = source.split("#[cfg(test)]").next().unwrap();
        assert_eq!(native.matches("GetNamedPipeHandleStateW(").count(), 1);
        assert!(!native.contains("SetNamedPipeHandleState"));
        assert!(
            !native.contains("FILE_WRITE_ATTRIBUTES"),
            "verifying byte mode must not widen the frozen GENERIC_READ access mask"
        );
    }

    #[test]
    fn native_surface_contains_no_generic_pipe_process_path_or_handle_entrypoint() {
        let source = include_str!("lib.rs");
        let native = source.split("#[cfg(test)]").next().unwrap();
        assert_eq!(native.matches("CreateFileW(").count(), 1);
        assert_eq!(native.matches("napi_register_module_v1").count(), 1);
        assert_eq!(native.matches(".compare_exchange(false, true").count(), 1);
        assert_eq!(native.matches("(api.object_freeze)(").count(), 1);
        for forbidden in [
            "WaitNamedPipeW",
            "CreateProcessW",
            "OpenProcess(",
            "DuplicateHandle",
            "std::process::Command",
            "openPipe",
            "openHandle",
            "spawnProcess",
        ] {
            assert!(
                !native.contains(forbidden),
                "forbidden generic native authority appeared: {forbidden}"
            );
        }
    }

    #[test]
    fn start_mux_and_package_ready_are_ordered_one_shots_and_close_is_idempotent() {
        let mut state = ContractState::default();
        assert_eq!(state.mark_package_ready(), Err(ContractError::InvalidState));
        state.start_mux().unwrap();
        assert_eq!(state.start_mux(), Err(ContractError::InvalidState));
        state.mark_package_ready().unwrap();
        assert_eq!(state.mark_package_ready(), Err(ContractError::InvalidState));
        assert!(state.begin_close());
        assert!(!state.begin_close());
        state.finish_close();
        assert_eq!(state.lifecycle, Lifecycle::Closed);
    }

    #[test]
    fn sync_and_mux_limits_are_inclusive_and_fail_closed() {
        assert_eq!(
            validate_frame_length(SYNC_FRAME_LIMIT, SYNC_FRAME_LIMIT),
            Ok(())
        );
        assert_eq!(
            validate_frame_length(SYNC_FRAME_LIMIT + 1, SYNC_FRAME_LIMIT),
            Err(ContractError::FrameTooLarge)
        );
        assert_eq!(
            validate_frame_length(MUX_FRAME_LIMIT, MUX_FRAME_LIMIT),
            Ok(())
        );
        assert_eq!(
            validate_frame_length(MUX_FRAME_LIMIT + 1, MUX_FRAME_LIMIT),
            Err(ContractError::FrameTooLarge)
        );
        let source = include_str!("lib.rs");
        let native = source.split("#[cfg(test)]").next().unwrap();
        assert!(
            native.contains("copy_bounded_uint8_array"),
            "oversized JS buffers must be rejected before allocating a native copy"
        );
    }

    #[test]
    fn adoption_digest_binds_every_transcript_component() {
        let baseline = adoption_commit_digest(7, &[1; 32], &[2; 32], &[3; 32], &[4; 32], 5);
        for changed in [
            adoption_commit_digest(8, &[1; 32], &[2; 32], &[3; 32], &[4; 32], 5),
            adoption_commit_digest(7, &[9; 32], &[2; 32], &[3; 32], &[4; 32], 5),
            adoption_commit_digest(7, &[1; 32], &[9; 32], &[3; 32], &[4; 32], 5),
            adoption_commit_digest(7, &[1; 32], &[2; 32], &[9; 32], &[4; 32], 5),
            adoption_commit_digest(7, &[1; 32], &[2; 32], &[3; 32], &[9; 32], 5),
            adoption_commit_digest(7, &[1; 32], &[2; 32], &[3; 32], &[4; 32], 6),
        ] {
            assert_ne!(baseline, changed);
        }
    }

    #[test]
    fn all_eight_bootstrap_records_freeze_magic_phase_roles_generation_and_token() {
        let generation = 0x0102_0304_0506_0708;
        let token = [0xa5; 32];
        let rows = [
            (BootstrapPhase::ServerHello, 1, 0),
            (BootstrapPhase::ClientHello, 0, 1),
            (BootstrapPhase::AdoptionChallenge, 3, 2),
            (BootstrapPhase::AdoptionEcho, 2, 3),
            (BootstrapPhase::Commit, 1, 0),
            (BootstrapPhase::CommitEcho, 0, 1),
            (BootstrapPhase::AdoptionAck, 3, 2),
            (BootstrapPhase::PackageReady, 2, 3),
        ];
        for (phase, own_role, peer_role) in rows {
            let encoded = encode_bootstrap_record(phase, own_role, peer_role, generation, &token);
            assert_eq!(
                decode_bootstrap_record(&encoded, phase, own_role, peer_role, generation),
                Ok(token)
            );
            for index in [0_usize, 4, 5, 6, 7, 8] {
                let mut mutated = encoded;
                mutated[index] ^= 1;
                assert_eq!(
                    decode_bootstrap_record(&mutated, phase, own_role, peer_role, generation),
                    Err(ContractError::InvalidBootstrap)
                );
            }
            let mut wrong_token = encoded;
            wrong_token[16] ^= 1;
            assert_ne!(
                decode_bootstrap_record(&wrong_token, phase, own_role, peer_role, generation),
                Ok(token)
            );
        }
    }

    #[test]
    fn windows_mux_read_never_uses_a_zero_byte_named_pipe_probe() {
        let source = include_str!("lib.rs");
        let native = source.split("#[cfg(test)]").next().unwrap();
        assert!(
            !native.contains("PeekNamedPipe"),
            "a zero-byte ReadFile plus PeekNamedPipe is not a pending named-pipe read"
        );
        assert!(
            !native.contains("ZeroRead"),
            "the IOCP worker must always own a non-empty read buffer"
        );
        assert!(
            native.contains("MUX_READ_CHUNK_BYTES"),
            "the fixed non-zero read chunk must remain explicit"
        );
    }

    #[test]
    fn windows_close_race_observes_global_closing_before_io_completion_dispatch() {
        let source = include_str!("lib.rs");
        let native = source.split("#[cfg(test)]").next().unwrap();
        assert!(
            native.contains("worker_observes_close_request"),
            "CancelIoEx completion can beat the explicit IOCP close packet"
        );
        assert!(
            native.contains("close_failure_requested"),
            "a requested failure must survive the worker winning close publication"
        );
        assert!(
            native
                .contains("if closing {\n                        inner.push_record(JsRecord::Done"),
            "a partial write completion must not reissue after Closing"
        );
    }

    #[test]
    fn each_uv_record_uses_a_real_node_async_callback_checkpoint() {
        let source = include_str!("lib.rs");
        let native = source.split("#[cfg(test)]").next().unwrap();
        assert!(
            native.contains("napi_make_callback")
                && native.matches("(api.make_callback)(").count() == 1,
            "raw uv callbacks need node::MakeCallback semantics for Promise checkpoints"
        );
        assert!(
            !native.contains("(api.call_function)("),
            "napi_call_function alone does not establish a custom async callback scope"
        );
        assert!(
            native.contains("fn callback_receiver(") && native.contains("napi_get_global\\0"),
            "napi_make_callback runs ToObject on its receiver, so undefined rejects the call"
        );
        assert!(
            !native.contains("let receiver = unsafe { undefined(env) }"),
            "an undefined receiver leaves a pending TypeError instead of invoking the callback"
        );
    }

    #[test]
    fn native_no_js_close_releases_all_callback_references() {
        let source = include_str!("lib.rs");
        let native = source.split("#[cfg(test)]").next().unwrap();
        assert!(
            native.contains("discard_js_references"),
            "finalizer and environment cleanup must not strand napi_ref roots"
        );
        assert!(
            native.contains("request_close_from_finalizer"),
            "the GC finalizer must defer napi_ref deletion out of the GC callback"
        );
        assert!(
            native.contains("request_close_from_environment"),
            "environment teardown must close natively without invoking JavaScript"
        );
    }
}
