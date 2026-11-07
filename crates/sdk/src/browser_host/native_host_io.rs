//! Process-facing implementation of the Rust Chrome native-messaging host.

use std::ffi::{OsStr, OsString};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use klaus_agent_core::runtime_values::{
    runtime_json, NodeBufferUtf8, RuntimeJson, RuntimeObject, RuntimeString,
};
use rand::RngCore;

use super::native_host::{
    next_native_host_reconnect_delay_ms, read_websocket_frame, write_masked_websocket_text,
    BridgeWriteQueue, ChromeNativeFramingError, ChromeNativeMessageReader, NativeHostStdoutWriter,
    NativeHostWriteAction, NativeHostWriteResult,
};

const NATIVE_HOST_VERSION: &str = "0.2.0";
const RECONNECT_BASE_MS: u64 = 250;
const NATIVE_RETRY_MS: u64 = 2_000;

static SHUTDOWN_SIGNAL: AtomicBool = AtomicBool::new(false);

enum HostEvent {
    ChromeBytes(Vec<u8>),
    ChromeClosed,
    BridgeMessage(Vec<u8>),
    StdoutFatal,
}

enum OutputCommand {
    Message(Vec<u8>),
    Shutdown,
}

enum BridgeCommand {
    Message { bytes: Vec<u8>, hello: bool },
    Shutdown,
}

#[derive(Default)]
struct BridgePublishedState {
    native_connected: AtomicBool,
    websocket_connected: AtomicBool,
}

struct NativeHostConfiguration {
    bridge_host: RuntimeString,
    bridge_port: u16,
    bridge_path: RuntimeString,
    bridge_token: RuntimeString,
    bridge_origin: RuntimeString,
    native_socket: PathBuf,
    native_socket_display: RuntimeString,
}

impl NativeHostConfiguration {
    fn from_process_environment() -> Self {
        let bridge_host = nonempty_env("KLAUS_BROWSER_EXTENSION_BRIDGE_HOST")
            .unwrap_or_else(|| RuntimeString::from("127.0.0.1"));
        let bridge_port = nonempty_env("KLAUS_BROWSER_EXTENSION_BRIDGE_PORT")
            .and_then(|value| scalar_boundary(&value).parse::<u16>().ok())
            .unwrap_or(18_765);
        let bridge_path = nonempty_env("KLAUS_BROWSER_EXTENSION_BRIDGE_PATH")
            .unwrap_or_else(|| RuntimeString::from("/klaus-browser"));
        let bridge_token = nonempty_env("KLAUS_BROWSER_EXTENSION_BRIDGE_TOKEN")
            .map(|value| RuntimeString::from(scalar_boundary(&value).trim().to_owned()))
            .unwrap_or_default();
        let bridge_origin =
            nonempty_env("KLAUS_BROWSER_EXTENSION_BRIDGE_ORIGIN").unwrap_or_else(|| {
                RuntimeString::from("chrome-extension://gjamccelebgpogpbiceajehcajghphnb")
            });
        let native_socket = nonempty_env_os("KLAUS_BROWSER_NATIVE_BRIDGE_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(default_native_socket);
        let native_socket_display = runtime_string_from_os(native_socket.as_os_str());
        Self {
            bridge_host,
            bridge_port,
            bridge_path,
            bridge_token,
            bridge_origin,
            native_socket,
            native_socket_display,
        }
    }
}

/// Run the optional legacy Chrome compatibility host. It never creates a
/// child process and all bridge traffic remains JSON/native-messaging data.
pub fn run_browser_native_host_main() -> u8 {
    SHUTDOWN_SIGNAL.store(false, Ordering::Release);
    install_signal_handlers();

    let configuration = NativeHostConfiguration::from_process_environment();
    let state = Arc::new(BridgePublishedState::default());
    let (event_tx, event_rx) = mpsc::channel();
    let output_tx = spawn_stdout_writer(event_tx.clone());
    let bridge_tx = spawn_bridge_worker(configuration, state.clone(), event_tx.clone());
    spawn_stdin_reader(event_tx);

    let mut chrome_reader = ChromeNativeMessageReader::default();
    loop {
        if SHUTDOWN_SIGNAL.load(Ordering::Acquire) {
            shutdown_workers(&output_tx, &bridge_tx);
            return 0;
        }
        match event_rx.recv_timeout(Duration::from_millis(20)) {
            Ok(HostEvent::ChromeBytes(bytes)) => {
                let batch = chrome_reader.feed(&bytes);
                for frame in batch.frames {
                    handle_chrome_payload(&frame, &output_tx, &bridge_tx, &state);
                }
                if let Some(error) = batch.terminal_error {
                    if error == ChromeNativeFramingError::MessageTooLarge {
                        send_error(&output_tx, "Chrome native message exceeds 1MB.");
                    }
                    shutdown_workers(&output_tx, &bridge_tx);
                    return 1;
                }
            }
            Ok(HostEvent::BridgeMessage(bytes)) => {
                let _ = output_tx.send(OutputCommand::Message(bytes));
            }
            Ok(HostEvent::ChromeClosed) => {
                shutdown_workers(&output_tx, &bridge_tx);
                return 0;
            }
            Ok(HostEvent::StdoutFatal) => {
                let _ = bridge_tx.send(BridgeCommand::Shutdown);
                return 1;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return 1,
        }
    }
}

fn handle_chrome_payload(
    payload: &[u8],
    output: &mpsc::Sender<OutputCommand>,
    bridge: &mpsc::Sender<BridgeCommand>,
    state: &BridgePublishedState,
) {
    let text = NodeBufferUtf8::decode(payload);
    let Ok(message) = runtime_json::parse(&text) else {
        send_error(output, "Invalid JSON from Chrome extension.");
        return;
    };
    let message_type = message.get("type").and_then(RuntimeJson::as_string);
    if message_type.is_some_and(|value| runtime_eq_ascii(value, "ping")) {
        let mut response = RuntimeObject::new();
        response.insert("type", RuntimeJson::from("pong"));
        response.insert("timestamp", RuntimeJson::from(unix_time_millis()));
        response.insert("nativeHostVersion", RuntimeJson::from(NATIVE_HOST_VERSION));
        send_chrome_json(output, RuntimeJson::Object(response));
        return;
    }
    if message_type.is_some_and(|value| runtime_eq_ascii(value, "get_status")) {
        let mut response = RuntimeObject::new();
        response.insert("type", RuntimeJson::from("status_response"));
        response.insert("nativeHostVersion", RuntimeJson::from(NATIVE_HOST_VERSION));
        response.insert(
            "nativeSocketPath",
            RuntimeJson::from(current_native_socket_display()),
        );
        response.insert(
            "nativeBridgeConnected",
            RuntimeJson::Bool(state.native_connected.load(Ordering::Acquire)),
        );
        response.insert(
            "websocketBridgeConnected",
            RuntimeJson::Bool(state.websocket_connected.load(Ordering::Acquire)),
        );
        send_chrome_json(output, RuntimeJson::Object(response));
        return;
    }
    let hello = message_type.is_some_and(|value| runtime_eq_ascii(value, "hello"));
    let response = message_type.is_some_and(|value| runtime_eq_ascii(value, "response"));
    if hello || response {
        let bytes = runtime_json::stringify(&message).to_node_utf8();
        let _ = bridge.send(BridgeCommand::Message { bytes, hello });
        return;
    }
    let unsupported = message_type
        .cloned()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| RuntimeString::from("unknown"));
    let error = RuntimeString::from("Unsupported native host message: ")
        .concat(&unsupported)
        .to_node_utf8();
    send_error_bytes(output, error);
}

fn send_chrome_json(output: &mpsc::Sender<OutputCommand>, value: RuntimeJson) {
    let _ = output.send(OutputCommand::Message(
        runtime_json::stringify(&value).to_node_utf8(),
    ));
}

fn send_error(output: &mpsc::Sender<OutputCommand>, error: &str) {
    send_error_bytes(output, error.as_bytes().to_vec());
}

fn send_error_bytes(output: &mpsc::Sender<OutputCommand>, error: Vec<u8>) {
    let mut message = RuntimeObject::new();
    message.insert("type", RuntimeJson::from("error"));
    message.insert("error", RuntimeJson::from(NodeBufferUtf8::decode(&error)));
    send_chrome_json(output, RuntimeJson::Object(message));
}

fn spawn_stdin_reader(events: mpsc::Sender<HostEvent>) {
    thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            match stdin.read(&mut buffer) {
                Ok(0) | Err(_) => {
                    let _ = events.send(HostEvent::ChromeClosed);
                    return;
                }
                Ok(read) => {
                    if events
                        .send(HostEvent::ChromeBytes(buffer[..read].to_vec()))
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    });
}

fn spawn_stdout_writer(events: mpsc::Sender<HostEvent>) -> mpsc::Sender<OutputCommand> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        set_stdout_nonblocking();
        let mut stdout = std::io::stdout().lock();
        let mut writer = NativeHostStdoutWriter::default();
        loop {
            while let Ok(command) = rx.try_recv() {
                match command {
                    OutputCommand::Message(payload) => {
                        if writer.enqueue_message(&payload).is_err() {
                            return;
                        }
                    }
                    OutputCommand::Shutdown => {
                        writer.shutdown();
                        return;
                    }
                }
            }
            let Some(bytes) = writer.current_bytes() else {
                match rx.recv() {
                    Ok(OutputCommand::Message(payload)) => {
                        if writer.enqueue_message(&payload).is_err() {
                            return;
                        }
                    }
                    Ok(OutputCommand::Shutdown) | Err(_) => {
                        writer.shutdown();
                        return;
                    }
                }
                continue;
            };
            let result = match stdout.write(bytes) {
                Ok(written) => NativeHostWriteResult::Written(written),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                    NativeHostWriteResult::Interrupted
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    NativeHostWriteResult::WouldBlock
                }
                Err(_) => NativeHostWriteResult::Fatal,
            };
            match writer.apply_write_result(result) {
                NativeHostWriteAction::Continue | NativeHostWriteAction::Retry => {}
                NativeHostWriteAction::WaitWritable => thread::sleep(Duration::from_millis(2)),
                NativeHostWriteAction::Exit => {
                    let _ = events.send(HostEvent::StdoutFatal);
                    return;
                }
            }
        }
    });
    tx
}

fn spawn_bridge_worker(
    configuration: NativeHostConfiguration,
    state: Arc<BridgePublishedState>,
    events: mpsc::Sender<HostEvent>,
) -> mpsc::Sender<BridgeCommand> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || bridge_worker(configuration, state, events, rx));
    tx
}

fn bridge_worker(
    configuration: NativeHostConfiguration,
    state: Arc<BridgePublishedState>,
    events: mpsc::Sender<HostEvent>,
    commands: mpsc::Receiver<BridgeCommand>,
) {
    set_current_native_socket_display(configuration.native_socket_display.clone());
    let mut last_hello = None;
    let mut native_unavailable_until = Instant::now();
    let mut reconnect_delay = RECONNECT_BASE_MS;
    loop {
        if drain_disconnected_commands(&commands, &mut last_hello) {
            return;
        }
        let outcome = if Instant::now() >= native_unavailable_until {
            match connect_native_bridge(&configuration.native_socket) {
                Ok(stream) => {
                    reconnect_delay = RECONNECT_BASE_MS;
                    run_native_connection(stream, &state, &events, &commands, &mut last_hello)
                }
                Err(_) => {
                    native_unavailable_until =
                        Instant::now() + Duration::from_millis(NATIVE_RETRY_MS);
                    run_websocket_connection(
                        &configuration,
                        &state,
                        &events,
                        &commands,
                        &mut last_hello,
                    )
                }
            }
        } else {
            run_websocket_connection(&configuration, &state, &events, &commands, &mut last_hello)
        };
        state.native_connected.store(false, Ordering::Release);
        state.websocket_connected.store(false, Ordering::Release);
        if outcome == ConnectionOutcome::Shutdown {
            return;
        }
        let deadline = Instant::now() + Duration::from_millis(reconnect_delay);
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match commands.recv_timeout(remaining.min(Duration::from_millis(20))) {
                Ok(BridgeCommand::Message { bytes, hello }) if hello => last_hello = Some(bytes),
                Ok(BridgeCommand::Message { .. }) => {}
                Ok(BridgeCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
        }
        reconnect_delay = next_native_host_reconnect_delay_ms(reconnect_delay);
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ConnectionOutcome {
    Closed,
    Shutdown,
}

#[cfg(unix)]
type NativeBridgeStream = std::os::unix::net::UnixStream;

#[cfg(unix)]
fn connect_native_bridge(path: &PathBuf) -> std::io::Result<NativeBridgeStream> {
    let stream = NativeBridgeStream::connect(path)?;
    stream.set_read_timeout(Some(Duration::from_millis(20)))?;
    stream.set_write_timeout(Some(Duration::from_millis(20)))?;
    Ok(stream)
}

#[cfg(windows)]
struct NativeBridgeStream(std::fs::File);

#[cfg(windows)]
fn connect_native_bridge(path: &PathBuf) -> std::io::Result<NativeBridgeStream> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map(NativeBridgeStream)
}

#[cfg(windows)]
impl Read for NativeBridgeStream {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(bytes)
    }
}

#[cfg(windows)]
impl Write for NativeBridgeStream {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

fn run_native_connection(
    mut stream: NativeBridgeStream,
    state: &BridgePublishedState,
    events: &mpsc::Sender<HostEvent>,
    commands: &mpsc::Receiver<BridgeCommand>,
    last_hello: &mut Option<Vec<u8>>,
) -> ConnectionOutcome {
    state.native_connected.store(true, Ordering::Release);
    let mut writes = BridgeWriteQueue::default();
    if let Some(hello) = last_hello.as_ref() {
        let mut line = hello.clone();
        line.push(b'\n');
        writes.enqueue(&line);
    }
    let mut line_buffer = Vec::new();
    let mut read_buffer = [0_u8; 16 * 1024];
    loop {
        if let Some(outcome) = drain_connected_commands(commands, last_hello, &mut writes, false) {
            return outcome;
        }
        if drive_bridge_writes(&mut stream, &mut writes).is_err() {
            return ConnectionOutcome::Closed;
        }
        match stream.read(&mut read_buffer) {
            Ok(0) => return ConnectionOutcome::Closed,
            Ok(read) => {
                line_buffer.extend_from_slice(&read_buffer[..read]);
                while let Some(index) = line_buffer.iter().position(|byte| *byte == b'\n') {
                    let line = line_buffer.drain(..=index).collect::<Vec<_>>();
                    let raw = &line[..line.len().saturating_sub(1)];
                    if raw.iter().all(u8::is_ascii_whitespace) {
                        continue;
                    }
                    if let Ok(value) = runtime_json::parse(&NodeBufferUtf8::decode(raw)) {
                        let _ = events.send(HostEvent::BridgeMessage(
                            runtime_json::stringify(&value).to_node_utf8(),
                        ));
                    }
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                ) => {}
            Err(_) => return ConnectionOutcome::Closed,
        }
    }
}

fn run_websocket_connection(
    configuration: &NativeHostConfiguration,
    state: &BridgePublishedState,
    events: &mpsc::Sender<HostEvent>,
    commands: &mpsc::Receiver<BridgeCommand>,
    last_hello: &mut Option<Vec<u8>>,
) -> ConnectionOutcome {
    if configuration.bridge_token.is_empty() {
        return ConnectionOutcome::Closed;
    }
    let host = scalar_boundary(&configuration.bridge_host);
    let address = (host.as_str(), configuration.bridge_port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut addresses| addresses.next());
    let Some(address) = address else {
        return ConnectionOutcome::Closed;
    };
    let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_secs(2)) else {
        return ConnectionOutcome::Closed;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(20)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(20)));
    let request = websocket_upgrade_request(configuration);
    if stream.write_all(&request).is_err() {
        return ConnectionOutcome::Closed;
    }
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 16 * 1024];
    loop {
        if drain_disconnected_commands(commands, last_hello) {
            return ConnectionOutcome::Shutdown;
        }
        match stream.read(&mut chunk) {
            Ok(0) => return ConnectionOutcome::Closed,
            Ok(read) => {
                buffer.extend_from_slice(&chunk[..read]);
                if let Some(end) = find_bytes(&buffer, b"\r\n\r\n") {
                    let status_end = find_bytes(&buffer[..end], b"\r\n").unwrap_or(end);
                    if !NodeBufferUtf8::decode(&buffer[..status_end]).contains("101") {
                        return ConnectionOutcome::Closed;
                    }
                    buffer.drain(..end + 4);
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                ) => {}
            Err(_) => return ConnectionOutcome::Closed,
        }
    }
    state.websocket_connected.store(true, Ordering::Release);
    let mut writes = BridgeWriteQueue::default();
    if let Some(hello) = last_hello.as_ref() {
        enqueue_websocket(&mut writes, hello);
    }
    loop {
        if let Some(outcome) = drain_connected_commands(commands, last_hello, &mut writes, true) {
            return outcome;
        }
        if drive_bridge_writes(&mut stream, &mut writes).is_err() {
            return ConnectionOutcome::Closed;
        }
        match stream.read(&mut chunk) {
            Ok(0) => return ConnectionOutcome::Closed,
            Ok(read) => {
                buffer.extend_from_slice(&chunk[..read]);
                loop {
                    let frame = match read_websocket_frame(&buffer) {
                        Ok(Some(frame)) => frame,
                        Ok(None) => break,
                        Err(_) => return ConnectionOutcome::Closed,
                    };
                    buffer.drain(..frame.consumed);
                    if frame.opcode == 8 {
                        return ConnectionOutcome::Closed;
                    }
                    if frame.opcode != 1 {
                        continue;
                    }
                    if let Ok(value) = runtime_json::parse(&NodeBufferUtf8::decode(&frame.payload))
                    {
                        let _ = events.send(HostEvent::BridgeMessage(
                            runtime_json::stringify(&value).to_node_utf8(),
                        ));
                    }
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                ) => {}
            Err(_) => return ConnectionOutcome::Closed,
        }
    }
}

fn drain_connected_commands(
    commands: &mpsc::Receiver<BridgeCommand>,
    last_hello: &mut Option<Vec<u8>>,
    writes: &mut BridgeWriteQueue,
    websocket: bool,
) -> Option<ConnectionOutcome> {
    while let Ok(command) = commands.try_recv() {
        match command {
            BridgeCommand::Message { bytes, hello } => {
                if hello {
                    *last_hello = Some(bytes.clone());
                }
                if websocket {
                    enqueue_websocket(writes, &bytes);
                } else {
                    let mut line = bytes;
                    line.push(b'\n');
                    writes.enqueue(&line);
                }
            }
            BridgeCommand::Shutdown => return Some(ConnectionOutcome::Shutdown),
        }
    }
    None
}

fn drain_disconnected_commands(
    commands: &mpsc::Receiver<BridgeCommand>,
    last_hello: &mut Option<Vec<u8>>,
) -> bool {
    while let Ok(command) = commands.try_recv() {
        match command {
            BridgeCommand::Message { bytes, hello } if hello => *last_hello = Some(bytes),
            BridgeCommand::Message { .. } => {}
            BridgeCommand::Shutdown => return true,
        }
    }
    false
}

fn drive_bridge_writes<W: Write>(stream: &mut W, writes: &mut BridgeWriteQueue) -> Result<(), ()> {
    while let Some(bytes) = writes.current_bytes() {
        let result = match stream.write(bytes) {
            Ok(written) => NativeHostWriteResult::Written(written),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                NativeHostWriteResult::Interrupted
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                NativeHostWriteResult::WouldBlock
            }
            Err(_) => NativeHostWriteResult::Fatal,
        };
        match writes.apply_write_result(result) {
            NativeHostWriteAction::Continue | NativeHostWriteAction::Retry => {}
            NativeHostWriteAction::WaitWritable => return Ok(()),
            NativeHostWriteAction::Exit => return Err(()),
        }
    }
    Ok(())
}

fn enqueue_websocket(writes: &mut BridgeWriteQueue, bytes: &[u8]) {
    let mut mask = [0_u8; 4];
    if rand::rngs::OsRng.try_fill_bytes(&mut mask).is_ok() {
        writes.enqueue(&write_masked_websocket_text(bytes, mask));
    } else {
        writes.fail();
    }
}

fn websocket_upgrade_request(configuration: &NativeHostConfiguration) -> Vec<u8> {
    let mut key = [0_u8; 16];
    let _ = rand::rngs::OsRng.try_fill_bytes(&mut key);
    let host = scalar_boundary(&configuration.bridge_host);
    let path = scalar_boundary(&configuration.bridge_path);
    let token = form_urlencode(&scalar_boundary(&configuration.bridge_token));
    let origin = scalar_boundary(&configuration.bridge_origin);
    format!(
        "GET {path}?token={token} HTTP/1.1\r\nHost: {host}:{}\r\nOrigin: {origin}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\n\r\n",
        configuration.bridge_port,
        base64(&key),
    )
    .into_bytes()
}

fn shutdown_workers(output: &mpsc::Sender<OutputCommand>, bridge: &mpsc::Sender<BridgeCommand>) {
    let _ = bridge.send(BridgeCommand::Shutdown);
    let _ = output.send(OutputCommand::Shutdown);
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::new();
    for chunk in bytes.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(TABLE[((value >> 18) & 63) as usize] as char);
        output.push(TABLE[((value >> 12) & 63) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[((value >> 6) & 63) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(value & 63) as usize] as char
        } else {
            '='
        });
    }
    output
}

fn form_urlencode(value: &str) -> String {
    let mut output = String::new();
    for byte in value.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                output.push(*byte as char)
            }
            b' ' => output.push('+'),
            byte => output.push_str(&format!("%{byte:02X}")),
        }
    }
    output
}

fn scalar_boundary(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 projection is scalar")
}

fn runtime_eq_ascii(value: &RuntimeString, expected: &str) -> bool {
    value
        .as_utf16()
        .iter()
        .copied()
        .eq(expected.bytes().map(u16::from))
}

fn nonempty_env(name: &str) -> Option<RuntimeString> {
    nonempty_env_os(name).map(|value| runtime_string_from_os(&value))
}

fn nonempty_env_os(name: &str) -> Option<OsString> {
    std::env::var_os(name).filter(|value| !value.is_empty())
}

fn runtime_string_from_os(value: &OsStr) -> RuntimeString {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        NodeBufferUtf8::decode(value.as_bytes())
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        RuntimeString::from_utf16(value.encode_wide().collect::<Vec<_>>())
    }
}

fn default_native_socket() -> PathBuf {
    #[cfg(windows)]
    {
        PathBuf::from(r"\\.\pipe\klaus-browser-native-host")
    }
    #[cfg(unix)]
    {
        nonempty_env_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default()
            .join(".klaus/browser/native-host.sock")
    }
}

fn current_native_socket_display() -> RuntimeString {
    NATIVE_SOCKET_DISPLAY.get().cloned().unwrap_or_default()
}

fn set_current_native_socket_display(value: RuntimeString) {
    let _ = NATIVE_SOCKET_DISPLAY.set(value);
}

static NATIVE_SOCKET_DISPLAY: std::sync::OnceLock<RuntimeString> = std::sync::OnceLock::new();

#[cfg(unix)]
fn set_stdout_nonblocking() {
    unsafe {
        let flags = libc::fcntl(libc::STDOUT_FILENO, libc::F_GETFL);
        if flags >= 0 {
            libc::fcntl(libc::STDOUT_FILENO, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
}

#[cfg(windows)]
fn set_stdout_nonblocking() {}

#[cfg(unix)]
extern "C" fn shutdown_signal_handler(_signal: libc::c_int) {
    SHUTDOWN_SIGNAL.store(true, Ordering::Release);
}

#[cfg(unix)]
fn install_signal_handlers() {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
        let handler = shutdown_signal_handler as extern "C" fn(libc::c_int);
        libc::signal(libc::SIGTERM, handler as libc::sighandler_t);
        libc::signal(libc::SIGINT, handler as libc::sighandler_t);
    }
}

#[cfg(windows)]
fn install_signal_handlers() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_helpers_match_the_locked_native_host_shapes() {
        assert_eq!(base64(&[0; 16]), "AAAAAAAAAAAAAAAAAAAAAA==");
        assert_eq!(form_urlencode("a b+c"), "a+b%2Bc");
        assert_eq!(find_bytes(b"a\r\n\r\nb", b"\r\n\r\n"), Some(1));
    }
}
