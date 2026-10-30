//! End-to-end code-mode carrier test: a real host subprocess serving a real
//! remote session, driven through the public session seam.
//!
//! This is the only test that exercises the whole carrier at once — provider →
//! connection → host process → V8 cell → delegate callback → back to the
//! engine — so it pins the contract the individual unit tests can only cover in
//! halves.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use klaus_agent_core::engine::code_mode::protocol::{
    CellId, CodeModeNestedToolCall, CodeModeToolKind, ExecuteRequest, RuntimeResponse,
    ToolDefinition, WaitOutcome, WaitRequest,
};
use klaus_agent_core::engine::code_mode::remote_session::ProcessOwnedCodeModeSessionProvider;
use klaus_agent_core::engine::code_mode::session::{
    CodeModeSessionDelegate, CodeModeSessionProvider, NotificationFuture, ToolInvocationFuture,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeString};
use klaus_agent_core::types::errors::CodeModeToolRejection;
use tokio_util::sync::CancellationToken;

fn rs(value: &str) -> RuntimeString {
    RuntimeString::from(value)
}

/// A live process context, as a host-launching engine would own.
///
/// The supervisor behind it is the test binary's (see `all.rs`): it is bound to
/// one runtime for the whole process, so this file borrows that one instead of
/// binding a second and racing the other integration file for the global slot.
struct ProcessHost {
    context: RuntimeProcessContext,
}

impl ProcessHost {
    fn start() -> Self {
        Self {
            context: crate::shared_process::context(),
        }
    }

    fn context(&self) -> RuntimeProcessContext {
        self.context.clone()
    }

    /// Drops this test's handle on the shared context. The context and the
    /// supervisor themselves stay up — they are the binary's, and shutting them
    /// down here would strand whatever test is still running.
    fn stop(self) {
        drop(self.context);
    }
}

/// The host binary built alongside this test.
fn host_program() -> PathBuf {
    let mut path = std::env::current_exe().expect("the test executable has a path");
    // target/<profile>/deps/<test>-<hash> → target/<profile>/
    path.pop();
    path.pop();
    path.join(if cfg!(windows) {
        "klaus-code-mode-host.exe"
    } else {
        "klaus-code-mode-host"
    })
}

/// An engine-side delegate that answers nested tool calls and records statuses.
#[derive(Default)]
struct EngineDelegate {
    invocations: Mutex<Vec<RuntimeString>>,
    statuses: Mutex<Vec<RuntimeString>>,
    closed: Mutex<Vec<CellId>>,
}

impl CodeModeSessionDelegate for EngineDelegate {
    fn invoke_tool<'a>(
        &'a self,
        invocation: CodeModeNestedToolCall,
        _cancellation_token: CancellationToken,
    ) -> ToolInvocationFuture<'a> {
        self.invocations
            .lock()
            .unwrap()
            .push(invocation.tool_name.clone());
        Box::pin(async move {
            if invocation.tool_name == "deny" {
                return Err(CodeModeToolRejection::plain("permission denied"));
            }
            Ok(RuntimeJson::String(
                rs("engine:").concat(&invocation.tool_name),
            ))
        })
    }

    fn notify<'a>(
        &'a self,
        _call_id: RuntimeString,
        _cell_id: CellId,
        text: RuntimeString,
        _cancellation_token: CancellationToken,
    ) -> NotificationFuture<'a> {
        self.statuses.lock().unwrap().push(text);
        Box::pin(async { Ok(()) })
    }

    fn cell_closed(&self, cell_id: &CellId) {
        self.closed.lock().unwrap().push(cell_id.clone());
    }
}

fn tool(name: &str) -> ToolDefinition {
    ToolDefinition {
        name: rs(name),
        tool_name: rs(name),
        description: rs("an engine tool"),
        kind: CodeModeToolKind::Function,
        input_schema: None,
        output_schema: None,
    }
}

fn request(source: &str, tools: Vec<ToolDefinition>) -> ExecuteRequest {
    ExecuteRequest {
        tool_call_id: rs("exec"),
        enabled_tools: tools,
        source: rs(source),
        yield_time_ms: Some(15_000),
        max_output_tokens: None,
    }
}

fn texts(response: &RuntimeResponse) -> Vec<RuntimeString> {
    use klaus_agent_core::engine::code_mode::protocol::OutputContentItem;
    let items = match response {
        RuntimeResponse::Yielded { content_items, .. }
        | RuntimeResponse::Terminated { content_items, .. }
        | RuntimeResponse::Result { content_items, .. } => content_items,
    };
    items
        .iter()
        .filter_map(|item| match item {
            OutputContentItem::InputText { text } => Some(text.clone()),
            OutputContentItem::InputImage { .. } => None,
        })
        .collect()
}

async fn a_host_process_runs_cells_and_calls_back_into_the_engine(host: &ProcessHost) {
    let host_program = host_program();
    assert!(
        host_program.exists(),
        "the host binary must be built alongside this test: {host_program:?}"
    );
    let provider =
        ProcessOwnedCodeModeSessionProvider::with_host_program(host.context(), host_program)
            .without_in_process_fallback();
    let delegate = Arc::new(EngineDelegate::default());
    let session = provider
        .create_session(delegate.clone())
        .await
        .expect("the host serves a session");

    // 1. A cell runs in the host process and its output returns to the engine.
    let started = session
        .execute(request(r#"text("from the host")"#, Vec::new()))
        .await
        .expect("the cell starts");
    let response = started
        .initial_response()
        .await
        .expect("the frontier arrives");
    assert_eq!(texts(&response), vec![rs("from the host")]);

    // 2. A nested tool call crosses back to THIS engine's delegate, and its
    //    result is what the cell observes.
    let started = session
        .execute(request(
            r#"text(JSON.stringify(await tools.probe({})))"#,
            vec![tool("probe")],
        ))
        .await
        .expect("the cell starts");
    let response = started
        .initial_response()
        .await
        .expect("the frontier arrives");
    assert_eq!(texts(&response), vec![rs(r#""engine:probe""#)]);
    assert!(
        delegate.invocations.lock().unwrap().contains(&rs("probe")),
        "the engine's delegate ran the nested tool"
    );

    // 3. A refused tool surfaces inside the cell as a catchable error.
    let started = session
        .execute(request(
            r#"try { await tools.deny({}); } catch (error) { text(String(error.message)); }"#,
            vec![tool("deny")],
        ))
        .await
        .expect("the cell starts");
    let response = started
        .initial_response()
        .await
        .expect("the frontier arrives");
    assert_eq!(texts(&response), vec![rs("permission denied")]);

    // 4. Status text reaches the engine's delegate.
    let started = session
        .execute(request(r#"notify("working"); text("done")"#, Vec::new()))
        .await
        .expect("the cell starts");
    let _ = started
        .initial_response()
        .await
        .expect("the frontier arrives");
    // The status is delivered on the host's own schedule; wait briefly for it.
    for _ in 0..50 {
        if !delegate.statuses.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        delegate.statuses.lock().unwrap().as_slice(),
        &[rs("working")]
    );

    // 5. Stored values are shared across cells of one session.
    let started = session
        .execute(request(r#"store("memo", 42)"#, Vec::new()))
        .await
        .expect("the cell starts");
    let _ = started
        .initial_response()
        .await
        .expect("the cell completes");
    let started = session
        .execute(request(r#"text(String(load("memo")))"#, Vec::new()))
        .await
        .expect("the cell starts");
    let response = started
        .initial_response()
        .await
        .expect("the cell completes");
    assert_eq!(texts(&response), vec![rs("42")]);

    // 6. A yielded cell can be resumed by waiting on it.
    let started = session
        .execute(ExecuteRequest {
            yield_time_ms: Some(10),
            ..request(
                r#"text("first");
await new Promise((resolve) => setTimeout(resolve, 60));
text("second");"#,
                Vec::new(),
            )
        })
        .await
        .expect("the cell starts");
    let cell_id = started.cell_id.clone();
    let first = started.initial_response().await.expect("the cell yields");
    assert_eq!(texts(&first), vec![rs("first")]);
    let outcome = session
        .wait(WaitRequest {
            cell_id: cell_id.clone(),
            yield_time_ms: Some(15_000),
            max_output_tokens: None,
            terminate: None,
        })
        .await
        .expect("the wait completes");
    let WaitOutcome::LiveCell(response) = outcome else {
        panic!("expected a live cell");
    };
    assert_eq!(texts(&response), vec![rs("second")]);

    // 7. Shutting the session down closes it on the host too.
    session.shutdown().await.expect("the session shuts down");
    let error = session
        .wait(WaitRequest {
            cell_id,
            yield_time_ms: Some(100),
            max_output_tokens: None,
            terminate: None,
        })
        .await
        .expect_err("a shut-down session serves nothing");
    assert!(error.contains("shutting down"), "{error:?}");
}

async fn a_missing_host_program_falls_back_to_the_in_process_carrier(host: &ProcessHost) {
    let provider = ProcessOwnedCodeModeSessionProvider::with_host_program(
        host.context(),
        PathBuf::from("/nonexistent/klaus-code-mode-host"),
    );
    let session = provider
        .create_session(Arc::new(EngineDelegate::default()))
        .await
        .expect("a missing host falls back rather than failing the turn");

    let started = session
        .execute(request(r#"text("in process")"#, Vec::new()))
        .await
        .expect("the cell starts");
    let response = started
        .initial_response()
        .await
        .expect("the cell completes");
    assert_eq!(texts(&response), vec![rs("in process")]);
}

async fn a_missing_host_program_fails_when_isolation_is_required(host: &ProcessHost) {
    let provider = ProcessOwnedCodeModeSessionProvider::with_host_program(
        host.context(),
        PathBuf::from("/nonexistent/klaus-code-mode-host"),
    )
    .without_in_process_fallback();
    let outcome = provider
        .create_session(Arc::new(EngineDelegate::default()))
        .await;
    let Err(error) = outcome else {
        panic!("a deployment that requires isolation must fail loudly");
    };
    assert!(
        error.contains("failed to spawn code-mode host"),
        "{error:?}"
    );
}

async fn every_session_of_one_provider_shares_the_host_but_not_its_values(host: &ProcessHost) {
    let host_program = host_program();
    assert!(host_program.exists(), "{host_program:?}");
    let provider =
        ProcessOwnedCodeModeSessionProvider::with_host_program(host.context(), host_program)
            .without_in_process_fallback();

    let first = provider
        .create_session(Arc::new(EngineDelegate::default()))
        .await
        .expect("the first session opens");
    let second = provider
        .create_session(Arc::new(EngineDelegate::default()))
        .await
        .expect("the second session opens on the same host");

    let started = first
        .execute(request(r#"store("k", 1)"#, Vec::new()))
        .await
        .expect("the cell starts");
    let _ = started
        .initial_response()
        .await
        .expect("the cell completes");

    let started = second
        .execute(request(
            r#"text(String(load("k") === undefined))"#,
            Vec::new(),
        ))
        .await
        .expect("the cell starts");
    let response = started
        .initial_response()
        .await
        .expect("the cell completes");
    assert_eq!(
        texts(&response),
        vec![rs("true")],
        "sessions sharing one host must not share stored values"
    );
}

/// One runtime and one process supervisor serve every case: the supervisor is a
/// process-level singleton bound to the runtime that initialized it, so the
/// carrier's cases share a single harness instead of each building their own.
#[test]
fn the_code_mode_host_carrier_serves_an_engine_end_to_end() {
    // On the runtime the process supervisor is bound to, so spawning a host and
    // awaiting it happen on one runtime, as they do in production.
    crate::shared_process::runtime().block_on(async {
        let host = ProcessHost::start();
        a_host_process_runs_cells_and_calls_back_into_the_engine(&host).await;
        every_session_of_one_provider_shares_the_host_but_not_its_values(&host).await;
        a_missing_host_program_falls_back_to_the_in_process_carrier(&host).await;
        a_missing_host_program_fails_when_isolation_is_required(&host).await;
        host.stop();
    });
}
