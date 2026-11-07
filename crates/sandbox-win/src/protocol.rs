//! The engine-to-wrapper protocol: what a sandbox request contains, how it
//! reaches the wrapper, and how the host steers a running sandbox.
//!
//! # Why the request does not travel on the command line
//!
//! A sandbox request carries the target's environment, and an agent's environment
//! routinely holds provider tokens. A process command line is not private on
//! Windows: any process that can open yours with `PROCESS_QUERY_LIMITED_INFORMATION`
//! can read it, and Task Manager, Process Explorer and a WMI query all show it by
//! default. Passing the environment as a command-line JSON blob therefore
//! publishes every secret in it to anything running on the machine, for as long as
//! the sandbox lives.
//!
//! So the command line carries only handle numbers, and the request itself is
//! written into an inherited pipe. Handle values are meaningless to anyone outside
//! the parent/child pair, and the pipe contents never appear in any process
//! listing.
//!
//! # Framing
//!
//! A pipe is a byte stream, so each message is length-prefixed. The length is
//! checked against [`MAX_FRAME_BYTES`] *before* any buffer is reserved: a frame
//! header is the one input here an untrusted writer could control, and
//! `Vec::with_capacity(attacker_chosen_length)` is an out-of-memory abort waiting
//! to happen.

use std::collections::BTreeMap;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::boundary::{AbsPath, FilesystemBoundary, NetworkBoundary};

/// Largest message this protocol will read or write.
///
/// A request holds an environment and several path lists, so a few hundred
/// kilobytes is generous; anything past this is a bug or an attack, and either way
/// refusing beats allocating.
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

/// Terminal dimensions in character cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalSize {
    pub rows: u16,
    pub cols: u16,
}

/// Everything the wrapper needs to build the sandbox and start the target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxRequest {
    /// argv of the target. Never empty.
    pub command: Vec<String>,
    /// Working directory for the target. Always inside a writable root.
    pub cwd: AbsPath,
    /// Environment for the target. Ordered so a request serializes identically
    /// twice, which is what makes the round-trip tests meaningful.
    pub env: BTreeMap<String, String>,
    pub filesystem: FilesystemBoundary,
    pub network: NetworkBoundary,
    /// Session-owned broker capability supplied by the trusted route owner.
    /// Distinct from the execution's AppContainer SID and never used as a file grant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_route_capability_sid: Option<String>,
    /// Fresh canonical execution identity, generated once by the host and retained
    /// through inspection and launch. The wrapper exclusively creates this profile
    /// and removes it after target drainage and permission cleanup.
    pub container_name: String,
    /// `Some` puts the target on a ConPTY of this size; `None` wires plain pipes.
    pub terminal: Option<TerminalSize>,
}

/// Native Windows execution selected by the captured operating-system config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NativeSandboxLevel {
    Unelevated,
    Elevated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSandboxContext {
    pub level: NativeSandboxLevel,
    pub config_home: String,
    #[serde(default)]
    pub unconfined_writes: bool,
}

impl NativeSandboxContext {
    pub fn validate(&self) -> Result<(), String> {
        if self.unconfined_writes {
            return Err("permission profile requests full-disk filesystem writes, which cannot be enforced by the Windows sandbox".into());
        }
        Ok(())
    }
}

/// The private transport keeps policy, native selection, and configuration home
/// together. A bare request is retained only for the explicitly scoped container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSandboxRequest {
    #[serde(flatten)]
    pub request: SandboxRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<NativeSandboxContext>,
}

/// Host-to-wrapper messages for a sandbox that is already running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ControlMessage {
    /// Deliver the equivalent of Ctrl+C to the target's process group.
    Interrupt,
    /// The host's terminal changed size.
    Resize(TerminalSize),
    /// Kill the target and everything it started, then exit.
    Terminate,
}

/// Why a frame could not be read or written.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("frame is {0} bytes, over the {MAX_FRAME_BYTES} byte limit")]
    FrameTooLarge(usize),
    #[error("stream ended mid-frame: wanted {wanted} bytes, got {got}")]
    Truncated { wanted: usize, got: usize },
    #[error("frame is not valid JSON: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Write one length-prefixed JSON message.
pub fn write_frame<T: Serialize>(writer: &mut impl Write, value: &T) -> Result<(), ProtocolError> {
    let body = serde_json::to_vec(value)?;
    if body.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge(body.len()));
    }
    // Fixed-width little-endian length: the reader has to know how much to expect
    // before it trusts anything, and a self-describing format would mean parsing
    // untrusted bytes to decide how many untrusted bytes to read.
    writer.write_all(&(body.len() as u32).to_le_bytes())?;
    writer.write_all(&body)?;
    writer.flush()?;
    Ok(())
}

/// Read one length-prefixed JSON message.
///
/// `Ok(None)` means the stream ended cleanly at a frame boundary, which is how the
/// control channel reports "the host is done sending" rather than an error.
pub fn read_frame<T: for<'de> Deserialize<'de>>(
    reader: &mut impl Read,
) -> Result<Option<T>, ProtocolError> {
    let mut header = [0u8; 4];
    match read_exact_or_eof(reader, &mut header)? {
        0 => return Ok(None),
        4 => {}
        got => return Err(ProtocolError::Truncated { wanted: 4, got }),
    }
    let length = u32::from_le_bytes(header) as usize;
    // Checked before allocating, on purpose: see the module note on framing.
    if length > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge(length));
    }
    let mut body = vec![0u8; length];
    let got = read_exact_or_eof(reader, &mut body)?;
    if got != length {
        return Err(ProtocolError::Truncated {
            wanted: length,
            got,
        });
    }
    Ok(Some(serde_json::from_slice(&body)?))
}

/// Fill `buf`, tolerating short reads, and report how much arrived before EOF.
fn read_exact_or_eof(reader: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(filled)
}

/// The handle a wrapper invocation is told to read from.
///
/// One pipe carries everything, in order: the first frame is the
/// [`SandboxRequest`], and every frame after it is a [`ControlMessage`]. A second
/// pipe would be a second thing that can fail to be created, inherited, or closed,
/// for no gain — the two streams are never concurrent, because the request is fully
/// read before the target exists to be controlled.
///
/// The raw handle value is safe to put on a command line precisely because it means
/// nothing outside the parent/child pair, unlike the request it points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrapperHandles {
    /// Read end of the pipe carrying the request and then the control stream.
    pub channel: usize,
}

/// Build the argv the engine launches its own binary with.
///
/// Deliberately two items and no payload: adding a field here means adding it to
/// the request instead, so nothing about a sandbox ends up publicly readable by
/// accident.
pub fn wrapper_argv(handles: WrapperHandles) -> Vec<String> {
    vec![crate::WRAPPER_ARG.to_string(), handles.channel.to_string()]
}

/// Recover the handle from a wrapper invocation's arguments.
///
/// `args` is everything after the executable name. Returns `None` when this is not
/// a wrapper invocation, so an ordinary startup can carry on.
pub fn parse_wrapper_argv<S: AsRef<str>>(args: &[S]) -> Option<Result<WrapperHandles, String>> {
    let first = args.first()?.as_ref();
    if first != crate::WRAPPER_ARG {
        return None;
    }
    // Past this point it IS a wrapper invocation, so a malformed tail is an error
    // rather than "not for us": falling through would start an agent session with
    // a sandbox the caller believes exists.
    let Some(channel) = args.get(1).map(AsRef::as_ref) else {
        return Some(Err(
            "wrapper invocation is missing the channel handle".into()
        ));
    };
    if args.len() > 2 {
        return Some(Err(format!(
            "wrapper invocation carries {} unexpected extra arguments",
            args.len() - 2
        )));
    }
    let parsed = channel
        .parse::<usize>()
        .map_err(|_| format!("channel handle {channel:?} is not a handle value"))
        .and_then(|value| {
            if value == 0 {
                Err("channel handle is null".to_string())
            } else {
                Ok(WrapperHandles { channel: value })
            }
        });
    Some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boundary::AbsPath;

    fn abs(path: &str) -> AbsPath {
        AbsPath::new(path).expect("test path parses")
    }

    fn sample_request() -> SandboxRequest {
        SandboxRequest {
            command: vec!["cmd.exe".into(), "/c".into(), "echo hi".into()],
            cwd: abs("C:\\ws"),
            env: BTreeMap::from([
                ("PATH".to_string(), "C:\\Windows\\System32".to_string()),
                (
                    "ANTHROPIC_API_KEY".to_string(),
                    "sk-secret-value".to_string(),
                ),
            ]),
            filesystem: FilesystemBoundary {
                writable: vec![abs("C:\\ws")],
                protected_metadata: vec![".git".into()],
                ..Default::default()
            },
            network: NetworkBoundary::LoopbackPorts {
                ports: vec![8080],
                allow_local_binding: false,
            },
            network_route_capability_sid: None,
            container_name: "klaus-sandbox-ws".into(),
            terminal: Some(TerminalSize {
                rows: 40,
                cols: 120,
            }),
        }
    }

    #[test]
    fn a_request_survives_the_pipe_round_trip() {
        let request = sample_request();
        let mut pipe = Vec::new();
        write_frame(&mut pipe, &request).expect("write request");
        let restored: SandboxRequest = read_frame(&mut pipe.as_slice())
            .expect("read request")
            .expect("frame present");
        assert_eq!(restored, request);
    }

    #[test]
    fn no_part_of_the_request_reaches_the_command_line() {
        // The reason this protocol exists: a command line is world-readable on
        // Windows, so the secret in the environment must not be anywhere near it.
        let request = sample_request();
        let argv = wrapper_argv(WrapperHandles { channel: 123 });
        let joined = argv.join(" ");
        assert!(
            !joined.contains("sk-secret-value"),
            "environment leaked into argv: {joined}"
        );
        assert!(!joined.contains("ANTHROPIC_API_KEY"), "argv: {joined}");
        assert!(
            !joined.contains("C:\\ws"),
            "paths leaked into argv: {joined}"
        );
        assert!(
            !joined.contains("cmd.exe"),
            "command leaked into argv: {joined}"
        );
        // Only the marker and one opaque number.
        assert_eq!(argv, vec![crate::WRAPPER_ARG, "123"]);
        // And the secret does live in the framed request, so the test above is
        // proving absence from argv rather than absence everywhere.
        let mut pipe = Vec::new();
        write_frame(&mut pipe, &request).expect("write request");
        let body = String::from_utf8_lossy(&pipe);
        assert!(body.contains("sk-secret-value"));
    }

    #[test]
    fn the_request_and_the_control_stream_share_one_pipe_in_order() {
        // The wrapper reads exactly one request frame and then treats everything
        // after it as control. A reader that expected two pipes would block forever
        // on the second one.
        let request = sample_request();
        let mut pipe = Vec::new();
        write_frame(&mut pipe, &request).expect("write request");
        write_frame(
            &mut pipe,
            &ControlMessage::Resize(TerminalSize {
                rows: 30,
                cols: 100,
            }),
        )
        .expect("write resize");
        write_frame(&mut pipe, &ControlMessage::Terminate).expect("write terminate");

        let mut reader = pipe.as_slice();
        let restored: SandboxRequest = read_frame(&mut reader)
            .expect("read request")
            .expect("request present");
        assert_eq!(restored, request);
        let first: ControlMessage = read_frame(&mut reader)
            .expect("read control")
            .expect("control present");
        assert_eq!(
            first,
            ControlMessage::Resize(TerminalSize {
                rows: 30,
                cols: 100
            })
        );
        let second: ControlMessage = read_frame(&mut reader)
            .expect("read control")
            .expect("control present");
        assert_eq!(second, ControlMessage::Terminate);
    }

    #[test]
    fn several_frames_read_back_in_order() {
        let messages = [
            ControlMessage::Resize(TerminalSize { rows: 24, cols: 80 }),
            ControlMessage::Interrupt,
            ControlMessage::Resize(TerminalSize {
                rows: 50,
                cols: 200,
            }),
            ControlMessage::Terminate,
        ];
        let mut pipe = Vec::new();
        for message in &messages {
            write_frame(&mut pipe, message).expect("write control");
        }
        let mut reader = pipe.as_slice();
        for expected in &messages {
            let got: ControlMessage = read_frame(&mut reader)
                .expect("read control")
                .expect("frame present");
            assert_eq!(&got, expected);
        }
        // Clean end of stream, not an error: the host simply stopped sending.
        let end: Option<ControlMessage> = read_frame(&mut reader).expect("clean eof");
        assert!(end.is_none());
    }

    #[test]
    fn a_frame_split_across_reads_is_reassembled() {
        // Pipes deliver partial reads; a reader that assumed one read per frame
        // would mangle every message that crossed a buffer boundary.
        struct OneByteAtATime<'a> {
            data: &'a [u8],
            position: usize,
        }
        impl Read for OneByteAtATime<'_> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.position >= self.data.len() || buf.is_empty() {
                    return Ok(0);
                }
                buf[0] = self.data[self.position];
                self.position += 1;
                Ok(1)
            }
        }

        let request = sample_request();
        let mut pipe = Vec::new();
        write_frame(&mut pipe, &request).expect("write request");
        let mut dribble = OneByteAtATime {
            data: &pipe,
            position: 0,
        };
        let restored: SandboxRequest = read_frame(&mut dribble)
            .expect("read request")
            .expect("frame present");
        assert_eq!(restored, request);
    }

    #[test]
    fn an_oversized_length_prefix_is_refused_before_allocating() {
        // The attack this blocks: a 4 GiB length header makes the reader reserve
        // 4 GiB and the process dies. The check must come before the allocation.
        let mut pipe = Vec::new();
        pipe.extend_from_slice(&u32::MAX.to_le_bytes());
        pipe.extend_from_slice(b"{}");
        let result: Result<Option<ControlMessage>, _> = read_frame(&mut pipe.as_slice());
        assert!(matches!(result, Err(ProtocolError::FrameTooLarge(_))));
    }

    #[test]
    fn a_truncated_frame_is_an_error_not_a_partial_message() {
        let message = ControlMessage::Resize(TerminalSize { rows: 24, cols: 80 });
        let mut pipe = Vec::new();
        write_frame(&mut pipe, &message).expect("write control");
        pipe.truncate(pipe.len() - 3);
        let result: Result<Option<ControlMessage>, _> = read_frame(&mut pipe.as_slice());
        assert!(matches!(result, Err(ProtocolError::Truncated { .. })));

        // A header cut in half is equally an error, not a clean EOF.
        let result: Result<Option<ControlMessage>, _> = read_frame(&mut [0u8, 0u8].as_slice());
        assert!(matches!(
            result,
            Err(ProtocolError::Truncated { wanted: 4, got: 2 })
        ));
    }

    #[test]
    fn a_non_json_frame_is_reported_as_malformed() {
        let body = b"not json at all";
        let mut pipe = Vec::new();
        pipe.extend_from_slice(&(body.len() as u32).to_le_bytes());
        pipe.extend_from_slice(body);
        let result: Result<Option<ControlMessage>, _> = read_frame(&mut pipe.as_slice());
        assert!(matches!(result, Err(ProtocolError::Malformed(_))));
    }

    #[test]
    fn argv_parsing_is_symmetric_with_argv_building() {
        let handles = WrapperHandles { channel: 7 };
        let argv = wrapper_argv(handles);
        let parsed = parse_wrapper_argv(&argv)
            .expect("recognized as wrapper")
            .expect("valid handle");
        assert_eq!(parsed, handles);
    }

    #[test]
    fn a_normal_startup_is_not_mistaken_for_a_wrapper() {
        assert!(parse_wrapper_argv::<String>(&[]).is_none());
        assert!(parse_wrapper_argv(&["--app-server"]).is_none());
        assert!(parse_wrapper_argv(&["exec", "--model", "x"]).is_none());
    }

    #[test]
    fn a_malformed_wrapper_invocation_fails_instead_of_starting_an_agent() {
        // The dangerous failure mode: treat a broken wrapper argv as a normal
        // startup and run the command with no sandbox at all while the caller
        // believes one is in place. Every case below must be an Err, not None.
        for args in [
            vec![crate::WRAPPER_ARG],
            vec![crate::WRAPPER_ARG, "seven"],
            vec![crate::WRAPPER_ARG, "0"],
            vec![crate::WRAPPER_ARG, "7", "extra"],
        ] {
            let outcome = parse_wrapper_argv(&args);
            assert!(
                matches!(outcome, Some(Err(_))),
                "{args:?} must be a hard error"
            );
        }
    }

    #[test]
    fn a_request_at_the_size_limit_is_written_and_one_past_it_is_not() {
        let mut request = sample_request();
        // A frame just under the cap round-trips.
        request.env.insert("BIG".into(), "x".repeat(1024));
        let mut pipe = Vec::new();
        write_frame(&mut pipe, &request).expect("under the cap");
        assert!(pipe.len() < MAX_FRAME_BYTES);

        // Past the cap the writer refuses rather than emitting a frame no reader
        // would accept.
        request
            .env
            .insert("HUGE".into(), "x".repeat(MAX_FRAME_BYTES + 1));
        let mut pipe = Vec::new();
        let result = write_frame(&mut pipe, &request);
        assert!(matches!(result, Err(ProtocolError::FrameTooLarge(_))));
        assert!(pipe.is_empty(), "nothing partial should be written");
    }
}
