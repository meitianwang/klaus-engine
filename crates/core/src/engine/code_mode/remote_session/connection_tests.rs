//! Remote-carrier behaviour tests.
//!
//! These drive the real connection driver against a scripted host that speaks
//! the wire protocol over an in-memory duplex, so the correlation, cancellation
//! and teardown rules are exercised without a host binary.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use tokio::io::{ReadHalf, WriteHalf};
use tokio_util::sync::CancellationToken;

use super::{Connection, RemoteSession};
use crate::engine::code_mode::host::{
    CapabilitySet, ClientToHost, DelegateRequest, DelegateRequestId, FramedReader, FramedWriter,
    HostHello, HostRequest, HostResponse, HostToClient, ProtocolVersion, RequestId, SessionId,
    WireCellId, WireContentItem, WireResult, WireRuntimeResponse, WireWaitOutcome,
};
use crate::engine::code_mode::protocol::{
    CellId, CodeModeNestedToolCall, ExecuteRequest, RuntimeResponse, WaitOutcome, WaitRequest,
};
use crate::engine::code_mode::session::{
    CodeModeSessionDelegate, NotificationFuture, ToolInvocationFuture,
};
use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::types::errors::CodeModeToolRejection;

fn rs(value: &str) -> RuntimeString {
    RuntimeString::from(value)
}

fn session(id: &str, generation: u64) -> RemoteSession {
    RemoteSession {
        id: SessionId::new(rs(id)).expect("session id"),
        generation,
    }
}

fn execute_request(source: &str) -> ExecuteRequest {
    ExecuteRequest {
        tool_call_id: rs("exec"),
        enabled_tools: Vec::new(),
        source: rs(source),
        yield_time_ms: Some(1_000),
        max_output_tokens: None,
    }
}

fn result_response(cell_id: &str) -> WireRuntimeResponse {
    WireRuntimeResponse::Result {
        cell_id: WireCellId::new(rs(cell_id)),
        content_items: vec![WireContentItem::InputText { text: rs("done") }],
        error_text: None,
    }
}

/// One end of a scripted host: reads client frames, writes host frames.
struct ScriptedHost {
    reader: FramedReader<ReadHalf<tokio::io::DuplexStream>>,
    writer: FramedWriter<WriteHalf<tokio::io::DuplexStream>>,
}

impl ScriptedHost {
    /// Build a connection whose peer is this scripted host, completing the
    /// handshake the way a real host would.
    async fn connect() -> (Connection, Self) {
        let (client_side, host_side) = tokio::io::duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_side);
        let (host_read, host_write) = tokio::io::split(host_side);
        let mut host = Self {
            reader: FramedReader::new(host_read),
            writer: FramedWriter::new(host_write),
        };
        let connect = Connection::establish_for_tests(client_read, client_write);
        let handshake = async {
            let hello = host.next().await;
            assert!(
                matches!(hello, ClientToHost::ClientHello(_)),
                "the client opens with a hello: {hello:?}"
            );
            host.send(HostToClient::HostHello(HostHello::new(
                ProtocolVersion::V1,
                CapabilitySet::empty(),
            )))
            .await;
        };
        let (connection, ()) = tokio::join!(connect, handshake);
        (connection.expect("handshake succeeds"), host)
    }

    async fn next(&mut self) -> ClientToHost {
        self.reader
            .read()
            .await
            .expect("reads a client frame")
            .expect("the client is still connected")
    }

    async fn send(&mut self, message: HostToClient) {
        self.writer.write(&message).await.expect("writes a frame");
    }

    /// Answer the next request, asserting it is the expected operation.
    async fn expect_request(&mut self) -> (RequestId, HostRequest) {
        match self.next().await {
            ClientToHost::Request { id, request } => (id, request),
            other => panic!("expected an operation request, got {other:?}"),
        }
    }

    async fn open_session(&mut self, session_id: &str) {
        let (id, request) = self.expect_request().await;
        assert!(
            matches!(&request, HostRequest::OpenSession { session_id: opened } if opened.as_runtime_string() == &rs(session_id)),
            "expected an open for {session_id}, got {request:?}"
        );
        self.send(HostToClient::Response {
            id,
            result: WireResult::from_result(Ok(HostResponse::SessionReady {
                session_id: SessionId::new(rs(session_id)).expect("session id"),
            })),
        })
        .await;
    }
}

/// A delegate that records what the engine was asked to do.
#[derive(Default)]
struct RecordingDelegate {
    invocations: StdMutex<Vec<CodeModeNestedToolCall>>,
    statuses: StdMutex<Vec<(CellId, RuntimeString)>>,
    closed: StdMutex<Vec<CellId>>,
}

impl CodeModeSessionDelegate for RecordingDelegate {
    fn invoke_tool<'a>(
        &'a self,
        invocation: CodeModeNestedToolCall,
        _cancellation_token: CancellationToken,
    ) -> ToolInvocationFuture<'a> {
        self.invocations.lock().unwrap().push(invocation.clone());
        Box::pin(async move {
            Ok(RuntimeJson::String(
                rs("echo:").concat(&invocation.tool_name),
            ))
        })
    }

    fn notify<'a>(
        &'a self,
        _call_id: RuntimeString,
        cell_id: CellId,
        text: RuntimeString,
        _cancellation_token: CancellationToken,
    ) -> NotificationFuture<'a> {
        self.statuses.lock().unwrap().push((cell_id, text));
        Box::pin(async { Ok(()) })
    }

    fn cell_closed(&self, cell_id: &CellId) {
        self.closed.lock().unwrap().push(cell_id.clone());
    }
}

#[tokio::test]
async fn a_started_cell_reports_its_id_before_its_first_frontier() {
    let (connection, mut host) = ScriptedHost::connect().await;
    let remote = session("s1", 1);
    let delegate = Arc::new(RecordingDelegate::default());

    let open = connection.open_session(remote.clone(), delegate.clone());
    let (_cleanup, ()) = tokio::join!(
        async { open.await.expect("session opens") },
        host.open_session("s1")
    );

    let execute = connection.execute(remote.clone(), execute_request(r#"text("hi")"#));
    let host_side = async {
        let (id, request) = host.expect_request().await;
        assert!(
            matches!(request, HostRequest::Execute { .. }),
            "{request:?}"
        );
        host.send(HostToClient::Response {
            id,
            result: WireResult::from_result(Ok(HostResponse::ExecutionStarted {
                cell_id: WireCellId::new(rs("7")),
            })),
        })
        .await;
        id
    };
    let (started, id) = tokio::join!(execute, host_side);
    let started = started.expect("execute is admitted");
    // The id is available before the frontier: this is what lets a caller
    // attach status routing to the cell it just started.
    assert_eq!(started.cell_id, CellId::new(rs("7")));

    host.send(HostToClient::InitialResponse {
        id,
        result: WireResult::from_result(Ok(result_response("7"))),
    })
    .await;
    let response = started.initial_response().await.expect("frontier arrives");
    assert!(matches!(response, RuntimeResponse::Result { .. }));
}

#[tokio::test]
async fn nested_tool_calls_and_statuses_reach_the_engine_delegate() {
    let (connection, mut host) = ScriptedHost::connect().await;
    let remote = session("s1", 1);
    let delegate = Arc::new(RecordingDelegate::default());

    let open = connection.open_session(remote.clone(), delegate.clone());
    let (_cleanup, ()) = tokio::join!(
        async { open.await.expect("session opens") },
        host.open_session("s1")
    );

    let execute = connection.execute(remote.clone(), execute_request("1"));
    let host_side = async {
        let (id, _) = host.expect_request().await;
        host.send(HostToClient::Response {
            id,
            result: WireResult::from_result(Ok(HostResponse::ExecutionStarted {
                cell_id: WireCellId::new(rs("1")),
            })),
        })
        .await;
        id
    };
    let (started, execute_id) = tokio::join!(execute, host_side);
    let started = started.expect("execute is admitted");

    // The host asks the engine to invoke a tool for the running cell.
    host.send(HostToClient::DelegateRequest {
        id: DelegateRequestId::new(1),
        session_id: remote.id.clone(),
        request: DelegateRequest::InvokeTool {
            invocation: crate::engine::code_mode::host::WireNestedToolCall {
                cell_id: WireCellId::new(rs("1")),
                runtime_tool_call_id: rs("call-1"),
                tool_name: rs("read"),
                tool_kind: crate::engine::code_mode::host::WireToolKind::Function,
                input: None,
            },
        },
    })
    .await;
    let delegate_answer = host.next().await;
    let ClientToHost::DelegateResponse { id, result } = delegate_answer else {
        panic!("expected a delegate response, got {delegate_answer:?}");
    };
    assert_eq!(id, DelegateRequestId::new(1));
    assert!(matches!(
        result.into_result().expect("the tool succeeded"),
        crate::engine::code_mode::host::DelegateResponse::ToolResult { .. }
    ));
    assert_eq!(delegate.invocations.lock().unwrap().len(), 1);

    // A status message reaches the delegate with the engine-visible cell id.
    host.send(HostToClient::DelegateRequest {
        id: DelegateRequestId::new(2),
        session_id: remote.id.clone(),
        request: DelegateRequest::Notify {
            call_id: rs("call-2"),
            cell_id: WireCellId::new(rs("1")),
            text: rs("halfway"),
        },
    })
    .await;
    let _ = host.next().await;
    assert_eq!(
        delegate.statuses.lock().unwrap().as_slice(),
        &[(CellId::new(rs("1")), rs("halfway"))]
    );

    host.send(HostToClient::InitialResponse {
        id: execute_id,
        result: WireResult::from_result(Ok(result_response("1"))),
    })
    .await;
    let _ = started.initial_response().await.expect("frontier arrives");

    // Closing the cell tells the delegate to release its per-cell state.
    host.send(HostToClient::CellClosed {
        session_id: remote.id.clone(),
        cell_id: WireCellId::new(rs("1")),
    })
    .await;
    // Give the driver a turn to apply the close.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(
        delegate.closed.lock().unwrap().as_slice(),
        &[CellId::new(rs("1"))]
    );
}

#[tokio::test]
async fn a_response_for_the_wrong_cell_kills_the_connection() {
    let (connection, mut host) = ScriptedHost::connect().await;
    let remote = session("s1", 1);
    let delegate = Arc::new(RecordingDelegate::default());

    let open = connection.open_session(remote.clone(), delegate.clone());
    let (_cleanup, ()) = tokio::join!(
        async { open.await.expect("session opens") },
        host.open_session("s1")
    );

    let wait = connection.wait(
        remote.clone(),
        WaitRequest {
            cell_id: CellId::new(rs("1")),
            yield_time_ms: Some(50),
            max_output_tokens: None,
            terminate: None,
        },
    );
    let host_side = async {
        let (id, _) = host.expect_request().await;
        // Answer with a DIFFERENT cell than the request targeted.
        host.send(HostToClient::Response {
            id,
            result: WireResult::from_result(Ok(HostResponse::WaitCompleted {
                outcome: WireWaitOutcome::LiveCell(result_response("99")),
            })),
        })
        .await;
    };
    let (outcome, ()) = tokio::join!(wait, host_side);
    let error = outcome.expect_err("a mismatched cell is a contract violation");
    assert!(error.contains("for request targeting"), "{error:?}");
    assert!(
        !connection.is_alive(),
        "a contract violation must take the connection down"
    );
}

#[tokio::test]
async fn an_unknown_request_id_kills_the_connection_and_fails_live_callers() {
    let (connection, mut host) = ScriptedHost::connect().await;
    let remote = session("s1", 1);
    let delegate = Arc::new(RecordingDelegate::default());

    let open = connection.open_session(remote.clone(), delegate.clone());
    let (_cleanup, ()) = tokio::join!(
        async { open.await.expect("session opens") },
        host.open_session("s1")
    );

    let wait = connection.wait(
        remote.clone(),
        WaitRequest {
            cell_id: CellId::new(rs("1")),
            yield_time_ms: Some(50),
            max_output_tokens: None,
            terminate: None,
        },
    );
    let host_side = async {
        let _ = host.expect_request().await;
        host.send(HostToClient::Response {
            id: RequestId::new(9_999),
            result: WireResult::from_result(Ok(HostResponse::SessionClosed {
                session_id: remote.id.clone(),
            })),
        })
        .await;
    };
    let (outcome, ()) = tokio::join!(wait, host_side);
    let error = outcome.expect_err("the live caller fails with the recorded reason");
    assert!(error.contains("unknown request id"), "{error:?}");
    assert!(!connection.is_alive());
}

#[tokio::test]
async fn a_dead_host_reports_every_live_cell_closed() {
    let (connection, mut host) = ScriptedHost::connect().await;
    let remote = session("s1", 1);
    let delegate = Arc::new(RecordingDelegate::default());

    let open = connection.open_session(remote.clone(), delegate.clone());
    let (cleanup, ()) = tokio::join!(
        async { open.await.expect("session opens") },
        host.open_session("s1")
    );

    let execute = connection.execute(remote.clone(), execute_request("1"));
    let host_side = async {
        let (id, _) = host.expect_request().await;
        host.send(HostToClient::Response {
            id,
            result: WireResult::from_result(Ok(HostResponse::ExecutionStarted {
                cell_id: WireCellId::new(rs("1")),
            })),
        })
        .await;
    };
    let (started, ()) = tokio::join!(execute, host_side);
    let started = started.expect("execute is admitted");

    // The host vanishes: dropping its transport ends the reader.
    drop(host);

    let error = started
        .initial_response()
        .await
        .expect_err("the pending frontier fails");
    assert!(!error.is_empty(), "the failure carries a reason");
    cleanup.wait().await;
    assert_eq!(
        delegate.closed.lock().unwrap().as_slice(),
        &[CellId::new(rs("1"))],
        "every live cell is reported closed exactly once"
    );
    assert!(!connection.is_alive());
}

#[tokio::test]
async fn a_later_generation_prefixes_cell_ids_so_stale_ids_cannot_address_live_cells() {
    let (connection, mut host) = ScriptedHost::connect().await;
    let remote = session("s2", 2);
    let delegate = Arc::new(RecordingDelegate::default());

    let open = connection.open_session(remote.clone(), delegate.clone());
    let (_cleanup, ()) = tokio::join!(
        async { open.await.expect("session opens") },
        host.open_session("s2")
    );

    let execute = connection.execute(remote.clone(), execute_request("1"));
    let host_side = async {
        let (id, _) = host.expect_request().await;
        host.send(HostToClient::Response {
            id,
            result: WireResult::from_result(Ok(HostResponse::ExecutionStarted {
                cell_id: WireCellId::new(rs("4")),
            })),
        })
        .await;
    };
    let (started, ()) = tokio::join!(execute, host_side);
    let started = started.expect("execute is admitted");
    assert_eq!(
        started.cell_id,
        CellId::new(rs("g2:4")),
        "a second-generation cell is prefixed"
    );

    // An unprefixed (stale) id no longer resolves in this generation.
    let stale = connection
        .terminate(remote.clone(), CellId::new(rs("4")))
        .await
        .expect_err("a stale id is rejected");
    assert!(
        stale.contains("stale code-mode host generation"),
        "{stale:?}"
    );
    assert!(
        connection.is_alive(),
        "rejecting a stale id is an ordinary answer, not a connection fault"
    );
}

#[tokio::test]
async fn an_abandoned_execute_terminates_the_cell_it_created() {
    let (connection, mut host) = ScriptedHost::connect().await;
    let remote = session("s1", 1);
    let delegate = Arc::new(RecordingDelegate::default());

    let open = connection.open_session(remote.clone(), delegate.clone());
    let (_cleanup, ()) = tokio::join!(
        async { open.await.expect("session opens") },
        host.open_session("s1")
    );

    // Drop the execute future before the host answers.
    {
        let execute = connection.execute(remote.clone(), execute_request("1"));
        tokio::pin!(execute);
        let _ = futures::poll!(execute.as_mut());
    }
    let (id, _) = host.expect_request().await;
    host.send(HostToClient::Response {
        id,
        result: WireResult::from_result(Ok(HostResponse::ExecutionStarted {
            cell_id: WireCellId::new(rs("1")),
        })),
    })
    .await;

    // The driver cancels the request and terminates the orphaned cell instead
    // of leaving it running on the host.
    let mut saw_cancel = false;
    let mut saw_terminate = false;
    for _ in 0..4 {
        match host.next().await {
            ClientToHost::CancelRequest { .. } => saw_cancel = true,
            ClientToHost::Request {
                request: HostRequest::Terminate { cell_id, .. },
                ..
            } => {
                assert_eq!(cell_id, WireCellId::new(rs("1")));
                saw_terminate = true;
            }
            other => panic!("unexpected client frame {other:?}"),
        }
        if saw_cancel && saw_terminate {
            break;
        }
    }
    assert!(saw_cancel, "the abandoned request is cancelled");
    assert!(saw_terminate, "the orphaned cell is terminated");
}

#[tokio::test]
async fn shutting_down_a_session_closes_it_locally_and_remotely() {
    let (connection, mut host) = ScriptedHost::connect().await;
    let remote = session("s1", 1);
    let delegate = Arc::new(RecordingDelegate::default());

    let open = connection.open_session(remote.clone(), delegate.clone());
    let (_cleanup, ()) = tokio::join!(
        async { open.await.expect("session opens") },
        host.open_session("s1")
    );

    let shutdown = connection.shutdown_session(remote.clone());
    let host_side = async {
        let (id, request) = host.expect_request().await;
        assert!(
            matches!(request, HostRequest::ShutdownSession { .. }),
            "{request:?}"
        );
        host.send(HostToClient::Response {
            id,
            result: WireResult::from_result(Ok(HostResponse::SessionClosed {
                session_id: remote.id.clone(),
            })),
        })
        .await;
    };
    let (result, ()) = tokio::join!(shutdown, host_side);
    result.expect("the session closes");

    // A closed session no longer accepts work.
    let outcome = connection.execute(remote, execute_request("1")).await;
    let Err(error) = outcome else {
        panic!("a closed session must reject execute");
    };
    assert!(error.contains("unknown code-mode session"), "{error:?}");
}

#[tokio::test]
async fn a_failed_operation_is_an_answer_not_a_connection_fault() {
    let (connection, mut host) = ScriptedHost::connect().await;
    let remote = session("s1", 1);
    let delegate = Arc::new(RecordingDelegate::default());

    let open = connection.open_session(remote.clone(), delegate.clone());
    let (_cleanup, ()) = tokio::join!(
        async { open.await.expect("session opens") },
        host.open_session("s1")
    );

    let wait = connection.wait(
        remote.clone(),
        WaitRequest {
            cell_id: CellId::new(rs("1")),
            yield_time_ms: Some(50),
            max_output_tokens: None,
            terminate: None,
        },
    );
    let host_side = async {
        let (id, _) = host.expect_request().await;
        host.send(HostToClient::Response {
            id,
            result: WireResult::<HostResponse>::from_result(Err(rs("cell 1 is gone"))),
        })
        .await;
    };
    let (outcome, ()) = tokio::join!(wait, host_side);
    assert_eq!(
        outcome.expect_err("the failure is delivered"),
        "cell 1 is gone"
    );
    assert!(
        connection.is_alive(),
        "an operation failure leaves the connection usable"
    );

    // The connection still serves the next request.
    let terminate = connection.terminate(remote.clone(), CellId::new(rs("2")));
    let host_side = async {
        let (id, _) = host.expect_request().await;
        host.send(HostToClient::Response {
            id,
            result: WireResult::from_result(Ok(HostResponse::WaitCompleted {
                outcome: WireWaitOutcome::MissingCell(result_response("2")),
            })),
        })
        .await;
    };
    let (outcome, ()) = tokio::join!(terminate, host_side);
    assert!(matches!(
        outcome.expect("the terminate is answered"),
        WaitOutcome::MissingCell(_)
    ));
}

#[tokio::test]
async fn a_duplicate_delegate_id_kills_the_connection() {
    let (connection, mut host) = ScriptedHost::connect().await;
    let remote = session("s1", 1);
    let delegate = Arc::new(RecordingDelegate::default());

    let open = connection.open_session(remote.clone(), delegate.clone());
    let (_cleanup, ()) = tokio::join!(
        async { open.await.expect("session opens") },
        host.open_session("s1")
    );

    let execute = connection.execute(remote.clone(), execute_request("1"));
    let host_side = async {
        let (id, _) = host.expect_request().await;
        host.send(HostToClient::Response {
            id,
            result: WireResult::from_result(Ok(HostResponse::ExecutionStarted {
                cell_id: WireCellId::new(rs("1")),
            })),
        })
        .await;
    };
    let (started, ()) = tokio::join!(execute, host_side);
    let _started = started.expect("execute is admitted");

    let notify = |id: i64| HostToClient::DelegateRequest {
        id: DelegateRequestId::new(id),
        session_id: remote.id.clone(),
        request: DelegateRequest::Notify {
            call_id: rs("call"),
            cell_id: WireCellId::new(rs("1")),
            text: rs("tick"),
        },
    };
    host.send(notify(1)).await;
    let _ = host.next().await;
    // Replaying a completed id would start a second call for one request.
    host.send(notify(1)).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        !connection.is_alive(),
        "a replayed delegate id is a contract violation"
    );
}

#[tokio::test]
async fn a_delegate_rejection_is_reported_to_the_host_as_a_failed_result() {
    struct RejectingDelegate;

    impl CodeModeSessionDelegate for RejectingDelegate {
        fn invoke_tool<'a>(
            &'a self,
            _invocation: CodeModeNestedToolCall,
            _cancellation_token: CancellationToken,
        ) -> ToolInvocationFuture<'a> {
            Box::pin(async { Err(CodeModeToolRejection::plain("permission denied")) })
        }

        fn notify<'a>(
            &'a self,
            _call_id: RuntimeString,
            _cell_id: CellId,
            _text: RuntimeString,
            _cancellation_token: CancellationToken,
        ) -> NotificationFuture<'a> {
            Box::pin(async { Ok(()) })
        }

        fn cell_closed(&self, _cell_id: &CellId) {}
    }

    let (connection, mut host) = ScriptedHost::connect().await;
    let remote = session("s1", 1);

    let open = connection.open_session(remote.clone(), Arc::new(RejectingDelegate));
    let (_cleanup, ()) = tokio::join!(
        async { open.await.expect("session opens") },
        host.open_session("s1")
    );

    let execute = connection.execute(remote.clone(), execute_request("1"));
    let host_side = async {
        let (id, _) = host.expect_request().await;
        host.send(HostToClient::Response {
            id,
            result: WireResult::from_result(Ok(HostResponse::ExecutionStarted {
                cell_id: WireCellId::new(rs("1")),
            })),
        })
        .await;
    };
    let (started, ()) = tokio::join!(execute, host_side);
    let _started = started.expect("execute is admitted");

    host.send(HostToClient::DelegateRequest {
        id: DelegateRequestId::new(1),
        session_id: remote.id.clone(),
        request: DelegateRequest::InvokeTool {
            invocation: crate::engine::code_mode::host::WireNestedToolCall {
                cell_id: WireCellId::new(rs("1")),
                runtime_tool_call_id: rs("call-1"),
                tool_name: rs("write"),
                tool_kind: crate::engine::code_mode::host::WireToolKind::Function,
                input: None,
            },
        },
    })
    .await;
    let ClientToHost::DelegateResponse { result, .. } = host.next().await else {
        panic!("expected a delegate response");
    };
    assert_eq!(
        result.into_result().expect_err("the tool was refused"),
        "permission denied",
        "the delegate's own message reaches the host unchanged"
    );
    assert!(
        connection.is_alive(),
        "a refused tool is an answer, not a connection fault"
    );
}

/// The registry keys cells per session, so two sessions may use the same host
/// cell id without colliding.
#[tokio::test]
async fn two_sessions_may_hold_the_same_host_cell_id() {
    let (connection, mut host) = ScriptedHost::connect().await;
    let first = session("s1", 1);
    let second = session("s2", 1);
    let delegate = Arc::new(RecordingDelegate::default());

    for (remote, name) in [(&first, "s1"), (&second, "s2")] {
        let open = connection.open_session(remote.clone(), delegate.clone());
        let (_cleanup, ()) = tokio::join!(
            async { open.await.expect("session opens") },
            host.open_session(name)
        );
    }

    let mut cells = HashMap::new();
    for remote in [&first, &second] {
        let execute = connection.execute(remote.clone(), execute_request("1"));
        let host_side = async {
            let (id, _) = host.expect_request().await;
            host.send(HostToClient::Response {
                id,
                result: WireResult::from_result(Ok(HostResponse::ExecutionStarted {
                    cell_id: WireCellId::new(rs("1")),
                })),
            })
            .await;
        };
        let (started, ()) = tokio::join!(execute, host_side);
        let started = started.expect("execute is admitted");
        cells.insert(remote.id.clone(), started.cell_id.clone());
    }

    assert_eq!(cells.len(), 2);
    assert!(
        connection.is_alive(),
        "the same host cell id in two sessions is not a duplicate"
    );
}
