//! Live per-turn memory recall — a relevant-memory prefetch. Once per turn the
//! engine recalls the stored memories most relevant to the user's message and injects
//! them so the agent works from durable context it would otherwise have to re-read by
//! hand. Each recalled memory carries its freshness, and a memory is surfaced at most
//! once per session (tracked as already-surfaced — the recall budget stays on fresh
//! candidates).
//!
//! DESIGN NOTES: (1) rather than a NON-BLOCKING prefetch running concurrently
//! with the model stream, the engine runs recall
//! synchronously at turn entry (one extra side-query of latency — faithful content,
//! simpler concurrency). (2) recalled memories are surfaced not as a USER-CONTEXT
//! attachment; the engine injects a `<system-reminder>` system part (the reminder
//! loop's wrapper), which is role-alternation-safe — same channel the engine uses for
//! every other per-turn reminder.

use crate::memory::memory_age::{memory_section, MemorySectionInput};
use crate::memory::types::MemoryRecord;
use crate::runtime_values::RuntimeString;
use crate::schemas::content::ContentBlock;
use crate::schemas::message::Message;
use crate::types::abort::AbortSignal;
use crate::types::reminder::{ReminderContext, ReminderProvider};
use crate::types::tool::BoxFuture;
use async_trait::async_trait;
use std::sync::Mutex;

/// Preamble framing the recalled block.
const PREAMBLE: &str = "Relevant memories recalled for this turn. These are point-in-time notes, not live state — verify any code references against the current code before relying on them:";

/// What the recall dependency sees per invocation.
#[derive(Debug, Clone)]
pub struct RecallContext {
    /// The most recently called tool names (so the selector can skip docs for tools
    /// already in use).
    pub recent_tools: Vec<RuntimeString>,
    /// Memory names to exclude (already surfaced this session).
    pub exclude: Vec<RuntimeString>,
    /// The turn's abort signal, when the caller supplied one — cancels the side-query
    /// when the turn aborts.
    pub signal: Option<AbortSignal>,
}

/// Any failure of the recall dependency. The provider swallows every error (degrades
/// to injecting nothing), so the payload only serves the injector's own diagnostics.
pub type RecallError = Box<dyn std::error::Error + Send + Sync>;

/// The future a [`RecallFn`] returns.
pub type RecallFuture = BoxFuture<'static, Result<Vec<MemoryRecord>, RecallError>>;

/// Recall the memories most relevant to the query, given the recently-used tools and
/// the names to exclude. Returns the picks (possibly empty); never fails fatally —
/// the provider degrades to injecting nothing on any error.
pub type RecallFn = Box<dyn Fn(RuntimeString, RecallContext) -> RecallFuture + Send + Sync>;

pub struct MemoryRecallDeps {
    /// Recall the memories most relevant to `query`, given the recently-used tools
    /// (so the selector can skip docs for tools already in use) and the names to
    /// exclude (already surfaced this session). Returns the picks (possibly empty);
    /// never fails fatally — the provider degrades to injecting nothing on any error.
    pub recall: RecallFn,
    /// Reference time for freshness annotation (the engine's `now()` seam).
    pub now: Box<dyn Fn() -> i64 + Send + Sync>,
    /// How many recent tool calls to feed the selector as `recent_tools`. Default 8.
    pub recent_tool_window: Option<usize>,
}

/// The last user message's text, or `None` if there is no user message.
fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

fn is_ecmascript_whitespace(unit: u16) -> bool {
    matches!(
        unit,
        0x0009..=0x000d
            | 0x0020
            | 0x00a0
            | 0x1680
            | 0x2000..=0x200a
            | 0x2028
            | 0x2029
            | 0x202f
            | 0x205f
            | 0x3000
            | 0xfeff
    )
}

fn trim_runtime(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_ecmascript_whitespace(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_ecmascript_whitespace(*unit))
        .map_or(start, |index| index + 1);
    value.slice_utf16(start..end)
}

fn runtime_word_count(value: &RuntimeString) -> usize {
    let mut count = 0;
    let mut in_word = false;
    for unit in value.as_utf16() {
        if is_ecmascript_whitespace(*unit) {
            in_word = false;
        } else if !in_word {
            count += 1;
            in_word = true;
        }
    }
    count
}

fn last_user_text(messages: &[Message]) -> Option<RuntimeString> {
    for m in messages.iter().rev() {
        if let Message::User(u) = m {
            let parts = u
                .content
                .iter()
                .map(|b| match b {
                    ContentBlock::Text(t) => t.text.clone(),
                    _ => RuntimeString::default(),
                })
                .collect::<Vec<_>>();
            return Some(trim_runtime(&join_runtime_strings(&parts, " ")));
        }
    }
    None
}

/// Names of the most recently called tools (newest-first, de-duplicated).
fn recent_tool_names(messages: &[Message], window: usize) -> Vec<RuntimeString> {
    let mut names: Vec<RuntimeString> = Vec::new();
    for m in messages.iter().rev() {
        if names.len() >= window {
            break;
        }
        let Message::Assistant(a) = m else { continue };
        for b in &a.content {
            if let ContentBlock::ToolUse(t) = b {
                if !names.iter().any(|n| n == &t.name) {
                    names.push(t.name.clone());
                    if names.len() >= window {
                        break;
                    }
                }
            }
        }
    }
    names
}

/// The per-turn memory-recall [`ReminderProvider`]. The surfaced-once dedup is per
/// INSTANCE, so the engine holds one instance per stable agent id (reused across a
/// persistent teammate's wakes) for the dedup to span turns. Injects nothing when
/// there is no usable query, a single-word prompt, no relevant memory, or any recall
/// failure.
pub struct MemoryRecallReminderProvider {
    recall: RecallFn,
    now: Box<dyn Fn() -> i64 + Send + Sync>,
    window: usize,
    /// Names already surfaced this session — excluded from future recalls so each
    /// memory appears once and the selector budget is spent on fresh candidates.
    /// Insertion-ordered; membership scans are linear but the set stays tiny.
    surfaced: Mutex<Vec<RuntimeString>>,
}

impl MemoryRecallReminderProvider {
    /// The async core (the trait method delegates to this).
    pub async fn tick(&self, ctx: &ReminderContext) -> Option<RuntimeString> {
        let query = last_user_text(&ctx.messages)?;
        if query.is_empty() {
            return None;
        }
        // Skip trivial single-word prompts — a lone word carries too little
        // signal to recall on.
        if runtime_word_count(&query) < 2 {
            return None;
        }

        let recent_tools = recent_tool_names(&ctx.messages, self.window);
        let exclude = self.surfaced.lock().unwrap().clone();
        let picked = match (self.recall)(
            query,
            RecallContext {
                recent_tools,
                exclude,
                signal: ctx.signal.clone(),
            },
        )
        .await
        {
            Ok(picked) => picked,
            Err(_) => return None,
        };

        // Defensive re-filter: the recall caller already excludes surfaced names, but
        // guard against a stale/echoed pick so a memory never re-surfaces.
        let mut surfaced = self.surfaced.lock().unwrap();
        let fresh: Vec<&MemoryRecord> = picked
            .iter()
            .filter(|r| !surfaced.iter().any(|name| name == &r.name))
            .collect();
        if fresh.is_empty() {
            return None;
        }
        for r in &fresh {
            if !surfaced.iter().any(|name| name == &r.name) {
                surfaced.push(r.name.clone());
            }
        }
        drop(surfaced);

        let now_ms = (self.now)();
        let sections: Vec<RuntimeString> = fresh
            .iter()
            .map(|r| {
                memory_section(
                    &MemorySectionInput {
                        name: r.name.clone(),
                        description: Some(r.description.clone()),
                        body: r.body.clone(),
                        mtime_ms: r.mtime_ms,
                    },
                    now_ms,
                )
            })
            .collect();
        Some(
            RuntimeString::from(PREAMBLE)
                .concat(&RuntimeString::from("\n\n"))
                .concat(&join_runtime_strings(&sections, "\n\n")),
        )
    }
}

#[async_trait]
impl ReminderProvider for MemoryRecallReminderProvider {
    async fn reminder(&self, ctx: &ReminderContext) -> Option<RuntimeString> {
        self.tick(ctx).await
    }
}

/// Build a per-turn memory-recall reminder provider. Hold ONE instance per stable
/// agent id so the surfaced-once dedup spans turns.
pub fn create_memory_recall_reminder_provider(
    deps: MemoryRecallDeps,
) -> MemoryRecallReminderProvider {
    MemoryRecallReminderProvider {
        recall: deps.recall,
        now: deps.now,
        window: deps.recent_tool_window.unwrap_or(8),
        surfaced: Mutex::new(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::types::{MemoryRecord, MemoryType};
    use crate::runtime_values::runtime_json;
    use crate::schemas::content::{ContentBlock, TextBlock, ToolUseBlock};
    use crate::schemas::message::{AssistantMessage, Message, UserMessage};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    const DAY: i64 = 86_400_000;
    const NOW: i64 = 1000 * DAY;

    fn rec(name: &str) -> MemoryRecord {
        MemoryRecord {
            name: name.into(),
            description: RuntimeString::default(),
            memory_type: MemoryType::Reference,
            body: "body".into(),
            path: format!("/m/{name}.md").into(),
            mtime_ms: NOW,
        }
    }

    fn user_msg(text: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: "u".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })
    }

    fn assistant_tool_use(name: &str) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: "a".into(),
            model: "m".into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: format!("t-{name}").into(),
                name: name.into(),
                input: runtime_json::parse(&RuntimeString::from("{}"))
                    .expect("valid exact test JSON"),
                tool_kind: None,
                thought_signature: None,
            })],
            usage: None,
        })
    }

    fn ctx(messages: Vec<Message>) -> ReminderContext {
        ReminderContext {
            messages,
            cwd: "/w".into(),
            task_list_id: None,
            task_plan: None,
            signal: None,

            plan_file_path: None,
            plan_file_exists: None,
            has_task_tool: false,
        }
    }

    #[tokio::test]
    async fn recalls_for_the_last_user_message_and_injects_each_pick_fresh_shows_saved_age() {
        #[allow(clippy::type_complexity)]
        let calls: Arc<
            Mutex<Vec<(RuntimeString, Vec<RuntimeString>, Vec<RuntimeString>)>>,
        > = Arc::new(Mutex::new(Vec::new()));
        let calls2 = Arc::clone(&calls);
        let provider = create_memory_recall_reminder_provider(MemoryRecallDeps {
            recall: Box::new(
                move |query: RuntimeString, c: RecallContext| -> RecallFuture {
                    calls2
                        .lock()
                        .unwrap()
                        .push((query, c.recent_tools, c.exclude));
                    let picks = vec![MemoryRecord {
                        description: "auth flow".into(),
                        body: "login via foo".into(),
                        ..rec("auth")
                    }];
                    Box::pin(async move { Ok(picks) })
                },
            ),
            now: Box::new(|| NOW),
            recent_tool_window: None,
        });
        let out = provider
            .tick(&ctx(vec![user_msg("how does auth work")]))
            .await
            .unwrap();
        assert_eq!(calls.lock().unwrap()[0].0, "how does auth work");
        assert!(out.contains("## auth — auth flow"));
        assert!(out.contains("(saved today)"));
        assert!(out.contains("login via foo"));
    }

    #[tokio::test]
    async fn renders_a_stale_pick_with_the_verify_caveat_instead_of_a_saved_age() {
        let provider = create_memory_recall_reminder_provider(MemoryRecallDeps {
            recall: Box::new(|_q: RuntimeString, _c: RecallContext| -> RecallFuture {
                Box::pin(async {
                    Ok(vec![MemoryRecord {
                        mtime_ms: NOW - 5 * DAY,
                        ..rec("old")
                    }])
                })
            }),
            now: Box::new(|| NOW),
            recent_tool_window: None,
        });
        let out = provider
            .tick(&ctx(vec![user_msg("two words")]))
            .await
            .unwrap_or_default();
        assert!(out.contains("5 days ago"));
        assert!(out.contains("Verify"));
    }

    #[tokio::test]
    async fn skips_a_single_word_prompt_too_little_signal() {
        let called = Arc::new(AtomicBool::new(false));
        let called2 = Arc::clone(&called);
        let provider = create_memory_recall_reminder_provider(MemoryRecallDeps {
            recall: Box::new(
                move |_q: RuntimeString, _c: RecallContext| -> RecallFuture {
                    called2.store(true, Ordering::SeqCst);
                    Box::pin(async { Ok(vec![rec("x")]) })
                },
            ),
            now: Box::new(|| NOW),
            recent_tool_window: None,
        });
        assert_eq!(provider.tick(&ctx(vec![user_msg("hello")])).await, None);
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn returns_none_when_there_is_no_user_message() {
        let provider = create_memory_recall_reminder_provider(MemoryRecallDeps {
            recall: Box::new(|_q: RuntimeString, _c: RecallContext| -> RecallFuture {
                Box::pin(async { Ok(vec![rec("x")]) })
            }),
            now: Box::new(|| NOW),
            recent_tool_window: None,
        });
        assert_eq!(
            provider.tick(&ctx(vec![assistant_tool_use("read")])).await,
            None
        );
    }

    #[tokio::test]
    async fn threads_recently_used_tool_names_newest_first_de_duped_to_recall() {
        let seen_tools: Arc<Mutex<Vec<RuntimeString>>> = Arc::new(Mutex::new(Vec::new()));
        let seen2 = Arc::clone(&seen_tools);
        let provider = create_memory_recall_reminder_provider(MemoryRecallDeps {
            recall: Box::new(move |_q: RuntimeString, c: RecallContext| -> RecallFuture {
                *seen2.lock().unwrap() = c.recent_tools;
                Box::pin(async { Ok(vec![]) })
            }),
            now: Box::new(|| NOW),
            recent_tool_window: None,
        });
        let _ = provider
            .tick(&ctx(vec![
                assistant_tool_use("read"),
                assistant_tool_use("grep"),
                assistant_tool_use("read"), // duplicate — only newest kept
                user_msg("please investigate"),
            ]))
            .await;
        assert_eq!(
            *seen_tools.lock().unwrap(),
            vec![RuntimeString::from("read"), RuntimeString::from("grep")]
        );
    }

    #[tokio::test]
    async fn recall_fn_receives_exact_query_and_recent_tool_utf16() {
        let query = RuntimeString::from_utf16(vec![b'a' as u16, b' ' as u16, 0xd800]);
        let tool_name = RuntimeString::from_utf16(vec![b't' as u16, 0xdc00]);
        let seen = Arc::new(Mutex::new(None));
        let seen2 = Arc::clone(&seen);
        let provider = create_memory_recall_reminder_provider(MemoryRecallDeps {
            recall: Box::new(
                move |query: RuntimeString, ctx: RecallContext| -> RecallFuture {
                    *seen2.lock().unwrap() = Some((query, ctx.recent_tools));
                    Box::pin(async { Ok(Vec::new()) })
                },
            ),
            now: Box::new(|| NOW),
            recent_tool_window: None,
        });
        let assistant = Message::Assistant(AssistantMessage {
            uuid: "a-exact".into(),
            model: "m".into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "t-exact".into(),
                name: tool_name.clone(),
                input: runtime_json::parse(&RuntimeString::from("{}"))
                    .expect("valid exact test JSON"),
                tool_kind: None,
                thought_signature: None,
            })],
            usage: None,
        });
        let user = Message::User(UserMessage {
            question_answer: None,
            uuid: "u-exact".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: query.clone(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        });
        assert_eq!(provider.tick(&ctx(vec![assistant, user])).await, None);
        let (seen_query, seen_tools) = seen.lock().unwrap().clone().expect("recall invoked");
        assert_eq!(seen_query, query);
        assert_eq!(seen_tools, vec![tool_name]);
        assert!(!seen_query.as_utf16().contains(&0xfffd));
    }

    #[tokio::test]
    async fn surfaces_each_memory_at_most_once_and_excludes_prior_picks_from_later_recalls() {
        let provider = create_memory_recall_reminder_provider(MemoryRecallDeps {
            recall: Box::new(|_q: RuntimeString, c: RecallContext| -> RecallFuture {
                // Always offers the same memory; the provider must not re-surface it.
                let picks = if c.exclude.iter().any(|n| n == "dup") {
                    vec![]
                } else {
                    vec![MemoryRecord {
                        body: "once".into(),
                        ..rec("dup")
                    }]
                };
                Box::pin(async move { Ok(picks) })
            }),
            now: Box::new(|| NOW),
            recent_tool_window: None,
        });
        let first = provider.tick(&ctx(vec![user_msg("first turn")])).await;
        assert!(first.unwrap().contains("dup"));
        let second = provider.tick(&ctx(vec![user_msg("second turn")])).await;
        assert_eq!(second, None); // already surfaced → excluded → nothing fresh
    }

    #[tokio::test]
    async fn degrades_to_none_when_recall_errors() {
        let provider = create_memory_recall_reminder_provider(MemoryRecallDeps {
            recall: Box::new(|_q: RuntimeString, _c: RecallContext| -> RecallFuture {
                Box::pin(async { Err("boom".into()) })
            }),
            now: Box::new(|| NOW),
            recent_tool_window: None,
        });
        assert_eq!(
            provider.tick(&ctx(vec![user_msg("some query")])).await,
            None
        );
    }

    #[tokio::test]
    async fn forwards_the_turn_abort_signal_to_recall() {
        let seen_signal: Arc<Mutex<Option<AbortSignal>>> = Arc::new(Mutex::new(None));
        let seen2 = Arc::clone(&seen_signal);
        let provider = create_memory_recall_reminder_provider(MemoryRecallDeps {
            recall: Box::new(move |_q: RuntimeString, c: RecallContext| -> RecallFuture {
                *seen2.lock().unwrap() = c.signal;
                Box::pin(async { Ok(vec![]) })
            }),
            now: Box::new(|| NOW),
            recent_tool_window: None,
        });
        let ac = AbortSignal::new();
        let mut context = ctx(vec![user_msg("please investigate")]);
        context.signal = Some(ac.clone());
        let _ = provider.tick(&context).await;
        // The forwarded signal shares the turn token's cancellation state —
        // aborting the turn cancels the side-query.
        let captured = seen_signal
            .lock()
            .unwrap()
            .clone()
            .expect("signal forwarded to recall");
        assert!(!captured.is_cancelled());
        ac.cancel();
        assert!(captured.is_cancelled());
    }
}
