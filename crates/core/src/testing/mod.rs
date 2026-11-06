//! Test-support helpers exported for embedders: token-usage and stream-event
//! builders, a scripted model-call seam, and a fake provider. These let
//! downstream crates script provider streams and drive tools directly in their
//! own tests without standing up a full engine.

use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use crate::schemas::message::Usage;
use crate::types::abort::AbortSignal;
use crate::types::errors::ProviderError;
use crate::types::provider::{
    CallModel, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub mod config_sandbox_trace;
pub mod exec_wait_trace;
pub mod immediate_rollout;
pub mod mcp_trace;
pub mod permission_trace;
pub mod provider_behavior;
pub mod query_trace;
pub mod session_memory_trace;
pub mod session_persistence_trace;
pub mod tool_result_trace;
pub mod tool_schema_trace;
pub mod workflow_trace;

/// The process-wide runtime-process supervisor every supervisor-owning test
/// SHARES. The supervisor admits one live Tokio runtime binding per process,
/// while each `#[tokio::test]` body runs on its own runtime: tests that each
/// called `RuntimeProcessSupervisor::initialize_current` collided on the
/// global slot whenever two ran in parallel. Bind once to this shared runtime
/// and give each test its own namespace off the shared supervisor instead —
/// the supervisor itself is never shut down, so its runtime handle stays
/// alive for the whole test process.
pub fn shared_test_process_supervisor() -> Arc<crate::runtime_process::RuntimeProcessSupervisor> {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    static SUPERVISOR: std::sync::OnceLock<Arc<crate::runtime_process::RuntimeProcessSupervisor>> =
        std::sync::OnceLock::new();
    SUPERVISOR
        .get_or_init(|| {
            let runtime = RUNTIME.get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("shared test runtime")
            });
            crate::runtime_process::RuntimeProcessSupervisor::initialize(runtime.handle().clone())
                .expect("shared test process supervisor")
        })
        .clone()
}

/// Whether this host has the Node interpreter the raw-JavaScript and Node-interop
/// test oracles need, resolved exactly the way the runtime resolves it: the
/// `KLAUS_NODE` override, else the first `node` on PATH.
///
/// Tests that stand a real Node program up (workflow scripts written as raw
/// JavaScript, the `proper-lockfile` interop checks, the provider-behavior trace
/// recompute) must ask this BEFORE they spawn. Without the probe a Node-less host
/// fails them on a bare ENOENT or a mismatched recompute, which reads as a defect
/// in the code under test instead of as a missing interpreter. This is the single
/// source of truth for that decision, shared by the lib unit tests and the
/// `tests/` integration binaries.
pub fn node_interpreter_present() -> bool {
    if std::env::var_os("KLAUS_NODE").is_some_and(|value| !value.is_empty()) {
        return true;
    }
    let Some(search) = std::env::var_os("PATH") else {
        return false;
    };
    let name = if cfg!(windows) { "node.exe" } else { "node" };
    std::env::split_paths(&search).any(|dir| dir.join(name).is_file())
}

/// Build a [`Usage`] with the given input/output token counts. `None` falls back
/// to the defaults (10 input, 5 output).
pub fn make_usage(input: Option<u64>, output: Option<u64>) -> Usage {
    Usage {
        input_tokens: input.unwrap_or(10),
        output_tokens: output.unwrap_or(5),
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

/// Builders for scripting provider stream events in tests.
pub mod ev {
    use super::*;

    /// A `message_start` event. `None` defaults the model name to `fake-model`.
    pub fn message_start(model: Option<&str>) -> ModelStreamEvent {
        ModelStreamEvent::MessageStart {
            model: model.unwrap_or("fake-model").into(),
        }
    }

    /// A `text_delta` event.
    pub fn text(text: &str) -> ModelStreamEvent {
        ModelStreamEvent::TextDelta { text: text.into() }
    }

    /// A `thinking_delta` event.
    pub fn thinking(text: &str) -> ModelStreamEvent {
        ModelStreamEvent::ThinkingDelta { text: text.into() }
    }

    /// A completed `thinking_block` event, optionally carrying a replay
    /// signature (omitted when `None`).
    pub fn thinking_block(text: &str, signature: Option<&str>) -> ModelStreamEvent {
        ModelStreamEvent::ThinkingBlock {
            text: text.into(),
            signature: signature.map(Into::into),
            encrypted_content: None,
            is_summary: false,
        }
    }

    /// The three events of a complete tool call: start, the JSON-encoded input
    /// delta, and stop.
    pub fn tool_use(id: &str, name: &str, input: &RuntimeJson) -> Vec<ModelStreamEvent> {
        vec![
            ModelStreamEvent::ToolUseStart {
                id: id.into(),
                name: name.into(),
                thought_signature: None,
                tool_kind: None,
                input_format: None,
            },
            ModelStreamEvent::ToolUseInputDelta {
                id: id.into(),
                partial_json: runtime_json::stringify(input),
            },
            ModelStreamEvent::ToolUseStop { id: id.into() },
        ]
    }

    /// A `message_stop` event. `None` defaults the stop reason to `end_turn` and
    /// the usage to [`make_usage`]'s defaults.
    pub fn stop(stop_reason: Option<ModelStopReason>, usage: Option<Usage>) -> ModelStreamEvent {
        ModelStreamEvent::MessageStop {
            stop_reason: stop_reason.unwrap_or(ModelStopReason::EndTurn),
            usage: usage.unwrap_or_else(|| make_usage(None, None)),
        }
    }

    /// An `error` event.
    pub fn error(error: ProviderError) -> ModelStreamEvent {
        ModelStreamEvent::Error { error }
    }
}

/// A scripted script: one sequence of stream events per model invocation.
pub type ScriptedTurns = Vec<Vec<ModelStreamEvent>>;

/// A [`CallModel`] that replays one scripted event-array per invocation. Once the
/// script is exhausted it yields a minimal closing turn: `message_start`, one
/// closing text delta, `message_stop` (`end_turn`). The closing text matters —
/// an EMPTY normally-stopped response after visible work is a defect shape the
/// turn loop now retries and then fails, so a fallback that emitted it would
/// fail every script that ends on a tool round. A real model that is done
/// speaks; tests that want the empty-closing shape script it explicitly. Each
/// replayed turn checks the abort signal before every event and stops early if
/// cancellation has been requested.
pub fn scripted_call_model(turns: ScriptedTurns) -> CallModel {
    let turns = Arc::new(turns);
    let counter = Arc::new(AtomicUsize::new(0));
    Box::new(
        move |_req: ModelRequest, signal: AbortSignal| -> ModelStream {
            let turns = Arc::clone(&turns);
            let counter = Arc::clone(&counter);
            // The turn index is read (and advanced) on first poll, so successive
            // invocations walk the script in order.
            let stream = futures::stream::once(async move {
                let i = counter.fetch_add(1, Ordering::SeqCst);
                let turn = turns.get(i).cloned().unwrap_or_else(|| {
                    vec![
                        ev::message_start(None),
                        ev::text("done"),
                        ev::stop(None, None),
                    ]
                });
                futures::stream::iter(turn)
                    .take_while(move |_| futures::future::ready(!signal.is_cancelled()))
            })
            .flatten();
            Box::pin(stream)
        },
    )
}

/// A fake [`ModelProvider`] whose [`stream`](ModelProvider::stream) must never be
/// called — the model-call seam is injected via [`CallModel`] instead.
pub struct FakeProvider {
    info: ProviderInfo,
}

/// A provider whose `stream` must never be called (the call seam is injected).
pub fn fake_provider() -> FakeProvider {
    FakeProvider {
        info: ProviderInfo {
            name: "fake".into(),
            base_url: None,
            env_key: None,
            default_model: "fake-model".into(),
        },
    }
}

impl ModelProvider for FakeProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            // Default stand-in for the (capable) main provider, so enabling the
            // 1-hour cache TTL in unrelated tests doesn't trip the no-op warning.
            prompt_cache_control: true,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, _req: ModelRequest, _signal: AbortSignal) -> ModelStream {
        // A generator only to satisfy the type: it panics instead of yielding, so
        // a stray call is caught rather than silently producing empty output.
        Box::pin(futures::stream::once(async {
            panic!("fakeProvider.stream should not be called; inject deps.callModel")
        }))
    }

    fn estimate_tokens(&self, _req: &ModelRequest) -> Option<u64> {
        Some(100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! exact_json {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    fn dummy_req() -> ModelRequest {
        ModelRequest {
            supports_image_detail_original: false,
            model_verbosity: None,
            reasoning_summary: Default::default(),
            image_limits: None,
            max_media_blocks: None,
            model: "fake-model".into(),
            system: vec![],
            messages: vec![],
            tools: vec![],
            deferred_tools: Vec::new(),
            tool_choice: None,
            max_output_tokens: 100,
            task_budget: None,
            output_schema: None,
            web_search: None,
            thinking: None,
            effort: None,
            skip_cache_write: None,
            cache_ttl: None,
            cache_key: None,
            context_management: None,
            context_hints: None,
            thinking_context: None,
        }
    }

    #[test]
    fn make_usage_uses_defaults_and_overrides() {
        let d = make_usage(None, None);
        assert_eq!(d.input_tokens, 10);
        assert_eq!(d.output_tokens, 5);
        assert_eq!(d.cache_read_input_tokens, 0);
        assert_eq!(d.cache_creation_input_tokens, 0);
        assert_eq!(d.total_tokens, None);

        let c = make_usage(Some(20), Some(3));
        assert_eq!(c.input_tokens, 20);
        assert_eq!(c.output_tokens, 3);
    }

    #[test]
    fn ev_message_start_defaults_model() {
        assert_eq!(
            ev::message_start(None),
            ModelStreamEvent::MessageStart {
                model: "fake-model".into()
            }
        );
        assert_eq!(
            ev::message_start(Some("m")),
            ModelStreamEvent::MessageStart { model: "m".into() }
        );
    }

    #[test]
    fn ev_thinking_block_omits_absent_signature() {
        assert_eq!(
            ev::thinking_block("t", None),
            ModelStreamEvent::ThinkingBlock {
                text: "t".into(),
                signature: None,
                encrypted_content: None,
                is_summary: false,
            }
        );
        assert_eq!(
            ev::thinking_block("t", Some("sig")),
            ModelStreamEvent::ThinkingBlock {
                text: "t".into(),
                signature: Some("sig".into()),
                encrypted_content: None,
                is_summary: false,
            }
        );
    }

    #[test]
    fn ev_tool_use_emits_start_input_stop() {
        let evs = ev::tool_use("id1", "read", &exact_json!({ "path": "/x" }));
        assert_eq!(evs.len(), 3);
        assert_eq!(
            evs[0],
            ModelStreamEvent::ToolUseStart {
                id: "id1".into(),
                name: "read".into(),
                thought_signature: None,
                tool_kind: None,
                input_format: None,
            }
        );
        assert_eq!(
            evs[1],
            ModelStreamEvent::ToolUseInputDelta {
                id: "id1".into(),
                partial_json: "{\"path\":\"/x\"}".into(),
            }
        );
        assert_eq!(evs[2], ModelStreamEvent::ToolUseStop { id: "id1".into() });
    }

    #[test]
    fn ev_stop_defaults() {
        assert_eq!(
            ev::stop(None, None),
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: make_usage(None, None),
            }
        );
        assert_eq!(
            ev::stop(
                Some(ModelStopReason::ToolUse),
                Some(make_usage(Some(1), Some(2)))
            ),
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::ToolUse,
                usage: make_usage(Some(1), Some(2)),
            }
        );
    }

    #[test]
    fn ev_tool_use_preserves_non_scalar_runtime_json() {
        let input = runtime_json::parse(&RuntimeString::from(
            r#"{"value":"\ud800","replacement":"\ufffd"}"#,
        ))
        .unwrap();
        let evs = ev::tool_use("id-surrogate", "exact", &input);
        let ModelStreamEvent::ToolUseInputDelta { partial_json, .. } = &evs[1] else {
            panic!("second event must carry the exact JSON delta");
        };

        assert_eq!(runtime_json::parse(partial_json).unwrap(), input);
        assert_eq!(
            partial_json,
            &RuntimeString::from(r#"{"value":"\ud800","replacement":"�"}"#)
        );
        assert_ne!(
            partial_json,
            &RuntimeString::from(r#"{"value":"�","replacement":"�"}"#)
        );
    }

    #[tokio::test]
    async fn scripted_replays_turns_then_falls_back() {
        let turns: ScriptedTurns = vec![vec![
            ev::message_start(None),
            ev::text("hi"),
            ev::stop(None, None),
        ]];
        let cm = scripted_call_model(turns);

        let first: Vec<ModelStreamEvent> = cm(dummy_req(), AbortSignal::new()).collect().await;
        assert_eq!(first.len(), 3);
        assert_eq!(first[1], ev::text("hi"));

        // Script exhausted -> closing turn: message_start, closing text, end_turn
        // stop. The text is what keeps exhausted scripts from ending on the
        // empty-closing defect shape.
        let second: Vec<ModelStreamEvent> = cm(dummy_req(), AbortSignal::new()).collect().await;
        assert_eq!(
            second,
            vec![
                ev::message_start(None),
                ev::text("done"),
                ev::stop(None, None)
            ]
        );
    }

    #[tokio::test]
    async fn scripted_stops_when_signal_already_aborted() {
        let turns: ScriptedTurns = vec![vec![ev::text("a"), ev::text("b")]];
        let cm = scripted_call_model(turns);
        let signal = AbortSignal::new();
        signal.cancel();
        let out: Vec<ModelStreamEvent> = cm(dummy_req(), signal).collect().await;
        assert!(out.is_empty());
    }

    #[test]
    fn fake_provider_info_and_capabilities() {
        let p = fake_provider();
        assert_eq!(p.info().name, "fake");
        assert_eq!(p.info().default_model, "fake-model");
        let caps = p.capabilities(&RuntimeString::from("anything"));
        assert!(caps.streaming);
        assert!(caps.tools);
        assert!(!caps.thinking);
        assert!(!caps.structured_output);
        assert!(!caps.web_search);
        assert!(caps.prompt_cache_control);
        assert_eq!(caps.context_window, 200_000);
        assert_eq!(caps.max_output_tokens, 8_192);
        assert!(caps.native_pdf);
        assert_eq!(p.estimate_tokens(&dummy_req()), Some(100));
    }

    #[tokio::test]
    #[should_panic(expected = "should not be called")]
    async fn fake_provider_stream_panics() {
        let p = fake_provider();
        let mut s = p.stream(dummy_req(), AbortSignal::new());
        let _ = s.next().await;
    }
}
