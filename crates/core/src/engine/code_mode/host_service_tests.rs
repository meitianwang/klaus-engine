//! Host-service tests: drive the real host state machine from a scripted
//! client over an in-memory duplex.

use tokio::io::{ReadHalf, WriteHalf};

use super::serve;
use crate::engine::code_mode::host::{
    CapabilitySet, ClientHello, ClientToHost, DelegateRequest, DelegateResponse, FramedReader,
    FramedWriter, HandshakeRejectReason, HostRequest, HostResponse, HostToClient, ProtocolVersion,
    RequestId, SessionId, SupportedProtocolVersions, WireCellId, WireExecuteRequest, WireResult,
    WireWaitRequest,
};
use crate::runtime_values::RuntimeString;

fn rs(value: &str) -> RuntimeString {
    RuntimeString::from(value)
}

fn session_id(value: &str) -> SessionId {
    SessionId::new(rs(value)).expect("session id")
}

fn execute(source: &str) -> WireExecuteRequest {
    WireExecuteRequest {
        tool_call_id: rs("exec"),
        enabled_tools: Vec::new(),
        source: rs(source),
        yield_time_ms: Some(5_000),
        max_output_tokens: None,
    }
}

/// A scripted client wired to a real host task.
struct ScriptedClient {
    reader: FramedReader<ReadHalf<tokio::io::DuplexStream>>,
    writer: FramedWriter<WriteHalf<tokio::io::DuplexStream>>,
    host: tokio::task::JoinHandle<Result<(), RuntimeString>>,
    next_request_id: i64,
}

impl ScriptedClient {
    /// Start a host and complete the handshake.
    async fn connect() -> Self {
        let mut client = Self::connect_without_handshake().await;
        client
            .send(ClientToHost::ClientHello(
                ClientHello::new(
                    SupportedProtocolVersions::try_new([ProtocolVersion::V1]).expect("versions"),
                    CapabilitySet::empty(),
                    CapabilitySet::empty(),
                )
                .expect("hello"),
            ))
            .await;
        let ready = client.next().await;
        assert!(
            matches!(ready, HostToClient::HostHello(ref hello) if hello.selected_version() == ProtocolVersion::V1),
            "the host accepts V1: {ready:?}"
        );
        client
    }

    async fn connect_without_handshake() -> Self {
        let (client_side, host_side) = tokio::io::duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_side);
        let (host_read, host_write) = tokio::io::split(host_side);
        let host = tokio::spawn(async move { serve(host_read, host_write).await });
        Self {
            reader: FramedReader::new(client_read),
            writer: FramedWriter::new(client_write),
            host,
            next_request_id: 1,
        }
    }

    async fn send(&mut self, message: ClientToHost) {
        self.writer.write(&message).await.expect("writes a frame");
    }

    async fn next(&mut self) -> HostToClient {
        self.reader
            .read()
            .await
            .expect("reads a host frame")
            .expect("the host is still connected")
    }

    /// Send one operation and return its id.
    async fn request(&mut self, request: HostRequest) -> RequestId {
        let id = RequestId::new(self.next_request_id);
        self.next_request_id += 1;
        self.send(ClientToHost::Request { id, request }).await;
        id
    }

    /// Await the answer to one operation, skipping unrelated host traffic.
    async fn response_for(&mut self, id: RequestId) -> Result<HostResponse, RuntimeString> {
        loop {
            match self.next().await {
                HostToClient::Response {
                    id: response_id,
                    result,
                } if response_id == id => return result.into_result(),
                _ => continue,
            }
        }
    }

    async fn open_session(&mut self, name: &str) -> SessionId {
        let id = self
            .request(HostRequest::OpenSession {
                session_id: session_id(name),
            })
            .await;
        let response = self.response_for(id).await.expect("the session opens");
        assert!(
            matches!(response, HostResponse::SessionReady { .. }),
            "{response:?}"
        );
        session_id(name)
    }

    /// Run one cell to completion, returning its output text.
    async fn run_cell(&mut self, session: &SessionId, source: &str) -> Vec<RuntimeString> {
        let request_id = self
            .request(HostRequest::Execute {
                session_id: session.clone(),
                request: execute(source),
            })
            .await;
        let started = self.response_for(request_id).await.expect("cell starts");
        assert!(
            matches!(started, HostResponse::ExecutionStarted { .. }),
            "{started:?}"
        );
        loop {
            match self.next().await {
                HostToClient::InitialResponse { id, result } if id == request_id => {
                    let response = result.expect_ok();
                    return response_texts(&response);
                }
                _ => continue,
            }
        }
    }

    /// Close the client end and await the host's exit status.
    async fn shutdown(self) -> Result<(), RuntimeString> {
        drop(self.reader);
        drop(self.writer);
        self.host.await.expect("the host task joins")
    }
}

trait ExpectOk<T> {
    fn expect_ok(self) -> T;
}

impl<T> ExpectOk<T> for WireResult<T> {
    fn expect_ok(self) -> T {
        self.into_result().expect("the host answered successfully")
    }
}

fn response_texts(
    response: &crate::engine::code_mode::host::WireRuntimeResponse,
) -> Vec<RuntimeString> {
    use crate::engine::code_mode::host::{WireContentItem, WireRuntimeResponse};
    let items = match response {
        WireRuntimeResponse::Yielded { content_items, .. }
        | WireRuntimeResponse::Terminated { content_items, .. }
        | WireRuntimeResponse::Result { content_items, .. } => content_items,
    };
    items
        .iter()
        .filter_map(|item| match item {
            WireContentItem::InputText { text } => Some(text.clone()),
            WireContentItem::InputImage { .. } => None,
        })
        .collect()
}

#[tokio::test]
async fn the_host_runs_a_cell_and_reports_its_output() {
    let mut client = ScriptedClient::connect().await;
    let session = client.open_session("s1").await;

    let texts = client.run_cell(&session, r#"text("hello host")"#).await;
    assert_eq!(texts, vec![rs("hello host")]);

    client.shutdown().await.expect("the host exits cleanly");
}

#[tokio::test]
async fn stored_values_are_shared_within_a_session_and_isolated_between_sessions() {
    let mut client = ScriptedClient::connect().await;
    let first = client.open_session("s1").await;
    let second = client.open_session("s2").await;

    let _ = client.run_cell(&first, r#"store("k", 7)"#).await;
    assert_eq!(
        client.run_cell(&first, r#"text(String(load("k")))"#).await,
        vec![rs("7")],
        "a later cell of the same session sees the stored value"
    );
    assert_eq!(
        client
            .run_cell(&second, r#"text(String(load("k") === undefined))"#)
            .await,
        vec![rs("true")],
        "a different session must not see it"
    );

    client.shutdown().await.expect("the host exits cleanly");
}

#[tokio::test]
async fn a_nested_tool_call_is_delegated_to_the_client_and_its_result_reaches_the_cell() {
    let mut client = ScriptedClient::connect().await;
    let session = client.open_session("s1").await;

    let request_id = client
        .request(HostRequest::Execute {
            session_id: session.clone(),
            request: WireExecuteRequest {
                enabled_tools: vec![crate::engine::code_mode::host::WireToolDefinition {
                    name: rs("probe"),
                    tool_name: rs("probe"),
                    description: rs("a probe"),
                    kind: crate::engine::code_mode::host::WireToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                ..execute(r#"text(JSON.stringify(await tools.probe({"n":1})))"#)
            },
        })
        .await;
    let started = client.response_for(request_id).await.expect("cell starts");
    let HostResponse::ExecutionStarted { cell_id } = started else {
        panic!("expected a started cell");
    };

    // The host asks this client to run the nested tool.
    let (delegate_id, invocation) = loop {
        match client.next().await {
            HostToClient::DelegateRequest {
                id,
                request: DelegateRequest::InvokeTool { invocation },
                ..
            } => break (id, invocation),
            _ => continue,
        }
    };
    assert_eq!(invocation.cell_id, cell_id);
    assert_eq!(invocation.tool_name, rs("probe"));

    client
        .send(ClientToHost::DelegateResponse {
            id: delegate_id,
            result: WireResult::from_result(Ok(DelegateResponse::ToolResult {
                result: crate::runtime_values::RuntimeJson::String(rs("ok")),
            })),
        })
        .await;

    let texts = loop {
        match client.next().await {
            HostToClient::InitialResponse { id, result } if id == request_id => {
                break response_texts(&result.expect_ok());
            }
            _ => continue,
        }
    };
    assert_eq!(
        texts,
        vec![rs(r#""ok""#)],
        "the delegate's result is what the cell observed"
    );

    client.shutdown().await.expect("the host exits cleanly");
}

#[tokio::test]
async fn a_delegate_failure_surfaces_inside_the_cell_as_a_tool_error() {
    let mut client = ScriptedClient::connect().await;
    let session = client.open_session("s1").await;

    let request_id = client
        .request(HostRequest::Execute {
            session_id: session.clone(),
            request: WireExecuteRequest {
                enabled_tools: vec![crate::engine::code_mode::host::WireToolDefinition {
                    name: rs("probe"),
                    tool_name: rs("probe"),
                    description: rs("a probe"),
                    kind: crate::engine::code_mode::host::WireToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                ..execute(
                    r#"try { await tools.probe({}); } catch (error) { text(String(error.message)); }"#,
                )
            },
        })
        .await;
    let _ = client.response_for(request_id).await.expect("cell starts");

    let delegate_id = loop {
        match client.next().await {
            HostToClient::DelegateRequest { id, .. } => break id,
            _ => continue,
        }
    };
    client
        .send(ClientToHost::DelegateResponse {
            id: delegate_id,
            result: WireResult::<DelegateResponse>::from_result(Err(rs("permission denied"))),
        })
        .await;

    let texts = loop {
        match client.next().await {
            HostToClient::InitialResponse { id, result } if id == request_id => {
                break response_texts(&result.expect_ok());
            }
            _ => continue,
        }
    };
    assert_eq!(texts, vec![rs("permission denied")]);

    client.shutdown().await.expect("the host exits cleanly");
}

#[tokio::test]
async fn a_closed_cell_is_reported_once_the_cell_finishes() {
    let mut client = ScriptedClient::connect().await;
    let session = client.open_session("s1").await;

    let request_id = client
        .request(HostRequest::Execute {
            session_id: session.clone(),
            request: execute(r#"text("done")"#),
        })
        .await;
    let started = client.response_for(request_id).await.expect("cell starts");
    let HostResponse::ExecutionStarted { cell_id } = started else {
        panic!("expected a started cell");
    };

    let mut saw_initial = false;
    let closed = loop {
        match client.next().await {
            HostToClient::InitialResponse { id, .. } if id == request_id => saw_initial = true,
            HostToClient::CellClosed {
                cell_id: closed_id, ..
            } => break closed_id,
            _ => continue,
        }
    };
    assert!(saw_initial, "the frontier precedes the close");
    assert_eq!(closed, cell_id);

    client.shutdown().await.expect("the host exits cleanly");
}

#[tokio::test]
async fn a_duplicate_session_id_is_refused_without_ending_the_connection() {
    let mut client = ScriptedClient::connect().await;
    let _session = client.open_session("s1").await;

    let id = client
        .request(HostRequest::OpenSession {
            session_id: session_id("s1"),
        })
        .await;
    let error = client
        .response_for(id)
        .await
        .expect_err("a live id is refused");
    assert!(error.contains("already open"), "{error:?}");

    // The host still serves this client.
    let texts = client.run_cell(&session_id("s1"), r#"text("alive")"#).await;
    assert_eq!(texts, vec![rs("alive")]);

    client.shutdown().await.expect("the host exits cleanly");
}

#[tokio::test]
async fn a_reused_session_id_is_refused_after_that_session_closed() {
    let mut client = ScriptedClient::connect().await;
    let session = client.open_session("s1").await;

    let id = client
        .request(HostRequest::ShutdownSession {
            session_id: session.clone(),
        })
        .await;
    let closed = client.response_for(id).await.expect("the session closes");
    assert!(
        matches!(closed, HostResponse::SessionClosed { .. }),
        "{closed:?}"
    );

    let id = client
        .request(HostRequest::OpenSession {
            session_id: session_id("s1"),
        })
        .await;
    let error = client
        .response_for(id)
        .await
        .expect_err("a retired id cannot be reused");
    assert!(error.contains("was reused"), "{error:?}");

    client.shutdown().await.expect("the host exits cleanly");
}

#[tokio::test]
async fn a_duplicate_request_id_ends_the_connection() {
    let mut client = ScriptedClient::connect().await;
    let session = client.open_session("s1").await;

    // Reuse request id 1, which the open above already consumed.
    client
        .send(ClientToHost::Request {
            id: RequestId::new(1),
            request: HostRequest::Terminate {
                session_id: session,
                cell_id: WireCellId::new(rs("1")),
            },
        })
        .await;

    let error = client
        .shutdown()
        .await
        .expect_err("a replayed request id ends the connection");
    assert!(
        error.contains("duplicate code-mode request id"),
        "{error:?}"
    );
}

#[tokio::test]
async fn work_for_an_unknown_session_is_refused() {
    let mut client = ScriptedClient::connect().await;

    let id = client
        .request(HostRequest::Execute {
            session_id: session_id("never-opened"),
            request: execute("1"),
        })
        .await;
    let error = client
        .response_for(id)
        .await
        .expect_err("an unknown session cannot run cells");
    assert!(error.contains("unknown code-mode session"), "{error:?}");

    let id = client
        .request(HostRequest::Wait {
            session_id: session_id("never-opened"),
            request: WireWaitRequest {
                cell_id: WireCellId::new(rs("1")),
                yield_time_ms: Some(10),
                max_output_tokens: None,
                terminate: None,
            },
        })
        .await;
    assert!(client.response_for(id).await.is_err());

    client.shutdown().await.expect("the host exits cleanly");
}

#[tokio::test]
async fn a_second_hello_ends_the_connection() {
    let mut client = ScriptedClient::connect().await;
    client
        .send(ClientToHost::ClientHello(
            ClientHello::new(
                SupportedProtocolVersions::try_new([ProtocolVersion::V1]).expect("versions"),
                CapabilitySet::empty(),
                CapabilitySet::empty(),
            )
            .expect("hello"),
        ))
        .await;

    let error = client
        .shutdown()
        .await
        .expect_err("a second hello is a protocol violation");
    assert!(error.contains("second code-mode client hello"), "{error:?}");
}

#[tokio::test]
async fn a_client_that_speaks_no_known_version_is_rejected_by_handshake() {
    let mut client = ScriptedClient::connect_without_handshake().await;
    let unknown = ProtocolVersion::new(9_999).expect("a nonzero version");
    client
        .send(ClientToHost::ClientHello(
            ClientHello::new(
                SupportedProtocolVersions::try_new([unknown]).expect("versions"),
                CapabilitySet::empty(),
                CapabilitySet::empty(),
            )
            .expect("hello"),
        ))
        .await;

    let rejection = client.next().await;
    assert!(
        matches!(
            rejection,
            HostToClient::HandshakeRejected {
                reason: HandshakeRejectReason::NoCompatibleVersion { .. }
            }
        ),
        "{rejection:?}"
    );
    client.shutdown().await.expect("the host closes cleanly");
}

#[tokio::test]
async fn a_required_capability_the_host_lacks_is_rejected_by_handshake() {
    let mut client = ScriptedClient::connect_without_handshake().await;
    let capability =
        crate::engine::code_mode::host::Capability::new(rs("time-travel")).expect("capability");
    client
        .send(ClientToHost::ClientHello(
            ClientHello::new(
                SupportedProtocolVersions::try_new([ProtocolVersion::V1]).expect("versions"),
                CapabilitySet::try_new([capability]).expect("required"),
                CapabilitySet::empty(),
            )
            .expect("hello"),
        ))
        .await;

    let rejection = client.next().await;
    assert!(
        matches!(
            rejection,
            HostToClient::HandshakeRejected {
                reason: HandshakeRejectReason::MissingRequiredCapability { .. }
            }
        ),
        "{rejection:?}"
    );
    client.shutdown().await.expect("the host closes cleanly");
}

#[tokio::test]
async fn an_operation_before_the_handshake_is_rejected_as_an_invalid_hello() {
    let mut client = ScriptedClient::connect_without_handshake().await;
    client
        .send(ClientToHost::Request {
            id: RequestId::new(1),
            request: HostRequest::OpenSession {
                session_id: session_id("s1"),
            },
        })
        .await;

    let rejection = client.next().await;
    assert!(
        matches!(
            rejection,
            HostToClient::HandshakeRejected {
                reason: HandshakeRejectReason::InvalidHello { .. }
            }
        ),
        "{rejection:?}"
    );
    client.shutdown().await.expect("the host closes cleanly");
}

#[tokio::test]
async fn terminating_a_live_cell_reports_the_frontier_it_stopped_at() {
    let mut client = ScriptedClient::connect().await;
    let session = client.open_session("s1").await;

    // A cell that yields stays alive until it is waited on or terminated.
    let request_id = client
        .request(HostRequest::Execute {
            session_id: session.clone(),
            request: WireExecuteRequest {
                yield_time_ms: Some(10),
                ..execute(
                    r#"text("before");
await new Promise((resolve) => setTimeout(resolve, 10_000));"#,
                )
            },
        })
        .await;
    let started = client.response_for(request_id).await.expect("cell starts");
    let HostResponse::ExecutionStarted { cell_id } = started else {
        panic!("expected a started cell");
    };
    let _ = loop {
        match client.next().await {
            HostToClient::InitialResponse { id, result } if id == request_id => {
                break result.expect_ok()
            }
            _ => continue,
        }
    };

    let id = client
        .request(HostRequest::Terminate {
            session_id: session.clone(),
            cell_id: cell_id.clone(),
        })
        .await;
    let response = client.response_for(id).await.expect("the cell terminates");
    let HostResponse::WaitCompleted { outcome } = response else {
        panic!("expected a wait outcome, got {response:?}");
    };
    assert!(
        matches!(
            outcome,
            crate::engine::code_mode::host::WireWaitOutcome::LiveCell(_)
        ),
        "a terminated live cell reports its frontier"
    );

    client.shutdown().await.expect("the host exits cleanly");
}

#[tokio::test]
async fn closing_the_client_shuts_every_session_down() {
    let mut client = ScriptedClient::connect().await;
    let session = client.open_session("s1").await;
    // Start a long-running cell so shutdown has real work to cancel.
    let request_id = client
        .request(HostRequest::Execute {
            session_id: session,
            request: WireExecuteRequest {
                yield_time_ms: Some(10),
                ..execute(r#"await new Promise((resolve) => setTimeout(resolve, 10_000));"#)
            },
        })
        .await;
    let _ = client.response_for(request_id).await.expect("cell starts");

    // Dropping the client's transport must not hang or fail the host.
    client.shutdown().await.expect("the host exits cleanly");
}

#[tokio::test]
async fn waiting_on_a_yielded_cell_resumes_it() {
    let mut client = ScriptedClient::connect().await;
    let session = client.open_session("s1").await;

    let request_id = client
        .request(HostRequest::Execute {
            session_id: session.clone(),
            request: WireExecuteRequest {
                yield_time_ms: Some(10),
                ..execute(
                    r#"text("first");
await new Promise((resolve) => setTimeout(resolve, 40));
text("second");"#,
                )
            },
        })
        .await;
    let started = client.response_for(request_id).await.expect("cell starts");
    let HostResponse::ExecutionStarted { cell_id } = started else {
        panic!("expected a started cell");
    };
    let first = loop {
        match client.next().await {
            HostToClient::InitialResponse { id, result } if id == request_id => {
                break response_texts(&result.expect_ok())
            }
            _ => continue,
        }
    };
    assert_eq!(
        first,
        vec![rs("first")],
        "the yield carries what ran so far"
    );

    let id = client
        .request(HostRequest::Wait {
            session_id: session,
            request: WireWaitRequest {
                cell_id,
                yield_time_ms: Some(5_000),
                max_output_tokens: None,
                terminate: None,
            },
        })
        .await;
    let response = client.response_for(id).await.expect("the wait completes");
    let HostResponse::WaitCompleted { outcome } = response else {
        panic!("expected a wait outcome");
    };
    let crate::engine::code_mode::host::WireWaitOutcome::LiveCell(response) = outcome else {
        panic!("expected a live cell");
    };
    assert_eq!(
        response_texts(&response),
        vec![rs("second")],
        "resuming continues from where the cell yielded"
    );

    client.shutdown().await.expect("the host exits cleanly");
}
