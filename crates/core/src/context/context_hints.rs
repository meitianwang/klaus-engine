//! Optional server context hints and the bounded request-only fallback when a
//! server cannot apply them. The transcript remains intact; only an outgoing
//! request replaces old reconstructible tool results.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimePath, RuntimeString};
use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock};
use crate::schemas::message::Message;
use crate::schemas::rollout::RolloutItem;
use crate::tools::result_storage::{maybe_spill_result, SpillOptions};
use crate::types::errors::ProviderError;
use crate::types::provider::ModelRequest;

pub const CONTEXT_HINT_BETA: &str = "context-hint-2026-04-09";
pub const CONTEXT_HINT_TARGET_TOKENS: u64 = 75_000;
pub const CONTEXT_HINT_MIN_SAVINGS: u64 = 20_000;
pub const CONTEXT_HINT_KEEP_RECENT: usize = 5;
pub const CONTEXT_HINT_STREAM_FALLBACK_CODE: &str = "context-hint-stream-fallback";
pub const CONTEXT_HINT_CLEARED_MARKER: &str = "[tool output cleared to save context]";
pub const CONTEXT_HINT_ROLLOUT_EVENT: &str = "context_hint_records";

/// Captured native request intent. Decoded or auxiliary requests do not inherit
/// it. The transport separately verifies its own first-party identity.
#[derive(Debug, Clone)]
pub struct ContextHintRequest {
    pub enabled: bool,
    pub main_thread: bool,
    /// Full text can be saved here before replacing an old result with a path.
    /// Without a directory, the original transcript still retains that text.
    pub artifact_dir: Option<RuntimePath>,
    pub state: Arc<ContextHintState>,
}

impl PartialEq for ContextHintRequest {
    fn eq(&self, other: &Self) -> bool {
        self.enabled == other.enabled
            && self.main_thread == other.main_thread
            && self.artifact_dir == other.artifact_dir
            && Arc::ptr_eq(&self.state, &other.state)
    }
}

impl Eq for ContextHintRequest {}

#[derive(Default)]
struct FrozenHintResults {
    model: Option<RuntimeString>,
    replacements: HashMap<RuntimeString, RuntimeString>,
    persisted: Option<(RuntimeString, Vec<(RuntimeString, RuntimeString)>)>,
}

/// Session-owned frozen request replacements. Sharing this state across one
/// conversation keeps later tool continuations from resending cleared output.
#[derive(Default)]
pub struct ContextHintState {
    inner: Mutex<FrozenHintResults>,
}

impl std::fmt::Debug for ContextHintState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        formatter
            .debug_struct("ContextHintState")
            .field("model", &inner.model)
            .field("replacement_count", &inner.replacements.len())
            .finish()
    }
}

impl ContextHintState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true when a previously bound model changes. The query owner can
    /// use this before estimating context to invalidate prior usage anchors.
    pub fn bind_model(&self, model: &RuntimeString) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let changed = inner
            .model
            .as_ref()
            .is_some_and(|previous| previous != model);
        if inner.model.as_ref() != Some(model) {
            inner.model = Some(model.clone());
            inner.replacements.clear();
        }
        changed
    }

    /// Reapply only this model's still-retained tool ids. Model changes discard
    /// the old request projection; compaction naturally prunes retired ids.
    pub fn apply_frozen(&self, model: &RuntimeString, messages: &mut [Message]) {
        let ids = {
            let mut ids = HashSet::new();
            visit_results(messages, |id, _| {
                ids.insert(id.clone());
            });
            ids
        };
        let replacements = {
            let mut inner = self.inner.lock().unwrap();
            if inner.model.as_ref() != Some(model) {
                inner.model = Some(model.clone());
                inner.replacements.clear();
            }
            inner.replacements.retain(|id, _| ids.contains(id));
            inner.replacements.clone()
        };
        ContextHintProjection {
            replacements,
            estimated_tokens_saved: 0,
        }
        .apply(messages);
    }

    pub fn record_projection(&self, model: &RuntimeString, projection: &ContextHintProjection) {
        let mut inner = self.inner.lock().unwrap();
        // A request captured before a model change cannot repopulate the newer
        // model's prefix with its stale result.
        if inner.model.as_ref() == Some(model) {
            inner.replacements.extend(projection.replacements.clone());
        }
    }

    pub fn records(&self) -> Vec<(RuntimeString, RuntimeString)> {
        let mut records: Vec<_> = self
            .inner
            .lock()
            .unwrap()
            .replacements
            .iter()
            .map(|(id, body)| (id.clone(), body.clone()))
            .collect();
        records.sort_by(|a, b| a.0.cmp(&b.0));
        records
    }

    /// Restore native persisted decisions; the next request prunes ids that are
    /// no longer present before applying any replacement.
    pub fn restore_records(
        &self,
        model: &RuntimeString,
        records: Vec<(RuntimeString, RuntimeString)>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner.model = Some(model.clone());
        inner.replacements = records.into_iter().collect();
    }

    /// Check for an actual write without allocating a second snapshot.
    pub(crate) fn has_pending_record(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        let Some(model) = &inner.model else {
            return false;
        };
        match &inner.persisted {
            Some((previous_model, records)) => {
                if inner.replacements.is_empty() && records.is_empty() {
                    return false;
                }
                model != previous_model
                    || records.len() != inner.replacements.len()
                    || records
                        .iter()
                        .any(|(id, body)| inner.replacements.get(id) != Some(body))
            }
            None => !inner.replacements.is_empty(),
        }
    }

    /// Return a complete changed snapshot for the ignored native rollout event.
    /// Repeated messages in one tool round do not duplicate unchanged records.
    pub fn take_pending_record(&self) -> Option<RuntimeJson> {
        let mut inner = self.inner.lock().unwrap();
        let model = inner.model.clone()?;
        let mut records: Vec<_> = inner
            .replacements
            .iter()
            .map(|(id, body)| (id.clone(), body.clone()))
            .collect();
        records.sort_by(|a, b| a.0.cmp(&b.0));
        let current = (model.clone(), records.clone());
        if inner.persisted.as_ref() == Some(&current) {
            return None;
        }
        let first_empty = records.is_empty()
            && inner
                .persisted
                .as_ref()
                .is_none_or(|(_, previous)| previous.is_empty());
        inner.persisted = Some(current);
        if first_empty {
            return None;
        }
        let mut payload = RuntimeObject::new();
        payload.insert("model", RuntimeJson::String(model));
        payload.insert(
            "records",
            RuntimeJson::Array(
                records
                    .into_iter()
                    .map(|(id, body)| {
                        RuntimeJson::Array(vec![RuntimeJson::String(id), RuntimeJson::String(body)])
                    })
                    .collect(),
            ),
        );
        Some(RuntimeJson::Object(payload))
    }

    /// Native event payloads remain separate from the model-visible transcript.
    /// Malformed records cannot replace a previous valid snapshot.
    pub fn restore_rollout(&self, items: &[RolloutItem]) {
        for item in items.iter().rev() {
            let RolloutItem::Event(event) = item else {
                continue;
            };
            if event.event != CONTEXT_HINT_ROLLOUT_EVENT {
                continue;
            }
            let Some(payload) = event.payload.as_ref() else {
                continue;
            };
            let Some(model) = payload.get("model").and_then(RuntimeJson::as_str) else {
                continue;
            };
            let Some(entries) = payload.get("records").and_then(RuntimeJson::as_array) else {
                continue;
            };
            let records: Option<Vec<_>> = entries
                .iter()
                .map(|entry| {
                    let pair = entry.as_array()?;
                    if pair.len() != 2 {
                        return None;
                    }
                    Some((pair[0].as_str()?.clone(), pair[1].as_str()?.clone()))
                })
                .collect();
            let Some(mut records) = records else { continue };
            records.sort_by(|a, b| a.0.cmp(&b.0));
            self.restore_records(model, records.clone());
            self.inner.lock().unwrap().persisted = Some((model.clone(), records));
            return;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HintErrorAction {
    None,
    /// Disable the hint and resend once after locally replacing old results.
    ClearAndRetry,
    /// Disable an unsupported beta and resend the otherwise unchanged request.
    StripAndRetry,
    /// Keep normal error handling, but remove the hint from subsequent retries.
    Strip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextHintParams {
    /// The beta is declared even when there is not yet enough old output to
    /// justify the optional body field.
    pub enabled: bool,
    pub target_tokens_saved: u64,
}

/// One controller per provider call. Disabling it is sticky across physical
/// retries, so a rejected hint never creates an unbounded remediation loop.
#[derive(Debug, Clone, Default)]
pub struct ContextHintController {
    request: Option<ContextHintRequest>,
    declared: bool,
    disabled: bool,
}

impl ContextHintController {
    pub fn new(request: Option<&ContextHintRequest>, first_party: bool) -> Self {
        Self {
            request: request
                .filter(|request| first_party && request.enabled && request.main_thread)
                .cloned(),
            ..Self::default()
        }
    }

    pub fn build_request(&mut self, messages: &[Message]) -> Option<ContextHintParams> {
        self.declared = false;
        if self.request.is_none() || self.disabled {
            return None;
        }
        self.declared = true;
        Some(ContextHintParams {
            enabled: hint_candidates(messages).tokens_saved >= CONTEXT_HINT_MIN_SAVINGS,
            target_tokens_saved: CONTEXT_HINT_TARGET_TOKENS,
        })
    }

    pub fn beta_active(&self) -> bool {
        self.declared && !self.disabled
    }

    pub fn artifact_dir(&self) -> Option<&RuntimePath> {
        self.request.as_ref()?.artifact_dir.as_ref()
    }

    pub fn request_intent(&self) -> Option<&ContextHintRequest> {
        self.request.as_ref()
    }

    pub fn on_request_error(&mut self, error: &ProviderError) -> HintErrorAction {
        if !self.beta_active() {
            return HintErrorAction::None;
        }
        let action = match error.status {
            Some(422 | 424) => HintErrorAction::ClearAndRetry,
            Some(400)
                if error.message.contains("Unexpected value")
                    && error.message.contains("anthropic-beta") =>
            {
                HintErrorAction::StripAndRetry
            }
            Some(409 | 529) => HintErrorAction::Strip,
            _ => HintErrorAction::None,
        };
        if action != HintErrorAction::None {
            self.disabled = true;
        }
        action
    }

    /// A statusless invalid-request event requires query-owned partial-output
    /// retirement before resampling. Other stream failures remain unchanged.
    pub fn classify_stream_error(&mut self, error: &ProviderError) -> bool {
        if !self.beta_active()
            || error.status.is_some()
            || error
                .error_type
                .as_ref()
                .is_none_or(|kind| kind != "invalid_request_error")
        {
            return false;
        }
        self.disabled = true;
        true
    }
}

#[derive(Debug, Clone, Default)]
struct HintCandidates {
    results: HashMap<RuntimeString, Vec<OutputBlock>>,
    tokens_saved: u64,
}

fn eligible_tool(name: &RuntimeString) -> bool {
    matches!(
        name.try_to_utf8().ok().as_deref(),
        Some(
            "Read"
                | "read"
                | "Bash"
                | "bash"
                | "PowerShell"
                | "powershell"
                | "exec_command"
                | "write_stdin"
                | "Grep"
                | "grep"
                | "Glob"
                | "glob"
                | "WebSearch"
                | "web_search"
                | "WebFetch"
                | "web_fetch"
                | "Edit"
                | "edit"
                | "apply_patch"
                | "Write"
                | "write"
        )
    )
}

fn already_replaced(content: &[OutputBlock]) -> bool {
    matches!(content, [OutputBlock::Text(text)] if
        text.text == CONTEXT_HINT_CLEARED_MARKER
            || text.text == "[Old tool result content cleared]"
            || text.text.starts_with("<persisted-output>")
            || text.text.starts_with(crate::context::tool_result_budget::BUDGET_REPLACEMENT_TAG))
}

fn result_tokens(content: &[OutputBlock]) -> u64 {
    content
        .iter()
        .map(|block| match block {
            OutputBlock::Text(text) => (text.text.len_utf16() as u64).saturating_add(2) / 4,
            OutputBlock::Image(_) => 2_000,
        })
        .sum()
}

fn visit_results(messages: &[Message], mut visit: impl FnMut(&RuntimeString, &[OutputBlock])) {
    for message in messages {
        match message {
            Message::ToolResult(result) => {
                visit(&result.tool_use_id, &result.content);
                for block in result.extra_user_blocks.as_deref().unwrap_or_default() {
                    if let ContentBlock::ToolResult(result) = block {
                        visit(&result.tool_use_id, &result.content);
                    }
                }
            }
            Message::User(user) => {
                for block in &user.content {
                    if let ContentBlock::ToolResult(result) = block {
                        visit(&result.tool_use_id, &result.content);
                    }
                }
            }
            _ => {}
        }
    }
}

fn hint_candidates(messages: &[Message]) -> HintCandidates {
    let eligible: Vec<_> = messages
        .iter()
        .filter_map(|message| match message {
            Message::Assistant(assistant) => Some(&assistant.content),
            _ => None,
        })
        .flatten()
        .filter_map(|block| match block {
            ContentBlock::ToolUse(tool) if eligible_tool(&tool.name) => Some(tool.id.clone()),
            _ => None,
        })
        .collect();
    let keep: HashSet<RuntimeString> = eligible
        .iter()
        .rev()
        .take(CONTEXT_HINT_KEEP_RECENT)
        .cloned()
        .collect();
    let clear: HashSet<RuntimeString> = eligible
        .iter()
        .filter(|id| !keep.contains(*id))
        .cloned()
        .collect();
    let mut result = HintCandidates::default();
    visit_results(messages, |id, content| {
        if clear.contains(id) && !already_replaced(content) {
            result.tokens_saved = result.tokens_saved.saturating_add(result_tokens(content));
            result.results.insert(id.clone(), content.to_vec());
        }
    });
    result
}

/// The complete replacement decisions for a single request projection. Keeping
/// these exact bodies across transport retries preserves the request prefix.
#[derive(Debug, Clone, Default)]
pub struct ContextHintProjection {
    pub replacements: HashMap<RuntimeString, RuntimeString>,
    pub estimated_tokens_saved: u64,
}

impl ContextHintProjection {
    pub fn apply(&self, messages: &mut [Message]) {
        for message in messages {
            match message {
                Message::ToolResult(result) => {
                    replace_result(&self.replacements, &result.tool_use_id, &mut result.content);
                    for block in result.extra_user_blocks.as_deref_mut().unwrap_or_default() {
                        if let ContentBlock::ToolResult(result) = block {
                            replace_result(
                                &self.replacements,
                                &result.tool_use_id,
                                &mut result.content,
                            );
                        }
                    }
                }
                Message::User(user) => {
                    for block in &mut user.content {
                        if let ContentBlock::ToolResult(result) = block {
                            replace_result(
                                &self.replacements,
                                &result.tool_use_id,
                                &mut result.content,
                            );
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

fn replace_result(
    replacements: &HashMap<RuntimeString, RuntimeString>,
    id: &RuntimeString,
    content: &mut Vec<OutputBlock>,
) {
    if let Some(replacement) = replacements.get(id) {
        *content = vec![OutputBlock::Text(TextBlock {
            text: replacement.clone(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })];
    }
}

/// Plan and persist one local fallback. A missing/unwritable artifact directory
/// uses a marker in the request while the durable transcript retains the body.
pub async fn project_hint_edits(
    messages: &[Message],
    artifact_dir: Option<&RuntimePath>,
) -> ContextHintProjection {
    let candidates = hint_candidates(messages);
    if candidates.tokens_saved < CONTEXT_HINT_MIN_SAVINGS {
        return ContextHintProjection::default();
    }
    let mut projection = ContextHintProjection {
        estimated_tokens_saved: candidates.tokens_saved,
        ..Default::default()
    };
    for (id, content) in candidates.results {
        let mut replacement = RuntimeString::from(CONTEXT_HINT_CLEARED_MARKER);
        if let Some(dir) = artifact_dir {
            let stored = maybe_spill_result(
                content,
                SpillOptions {
                    max_chars: 0.0,
                    dir: dir.clone(),
                    id: format!("hint-{}", uuid::Uuid::new_v4()).into(),
                },
                |_, _, path| {
                    RuntimeString::from(crate::context::tool_result_budget::BUDGET_REPLACEMENT_TAG)
                        .concat(&"\nTool result saved to: ".into())
                        .concat(path.as_runtime_string())
                        .concat(&"\nUse read to view the complete output.".into())
                },
            )
            .await;
            if stored.spilled_path.is_some() {
                if let [OutputBlock::Text(text)] = stored.content.as_slice() {
                    replacement = text.text.clone();
                }
            }
        }
        projection.replacements.insert(id, replacement);
    }
    projection
}

/// The query layer owns retiring any partial visible response. It can then use
/// this request clone for one retry; clearing the native intent prevents another
/// hint fallback for the same episode.
pub async fn stream_fallback_request(
    request: &ModelRequest,
    error: &ProviderError,
) -> Option<ModelRequest> {
    let intent = request.context_hints.as_ref()?;
    if !intent.enabled
        || !intent.main_thread
        || error
            .code
            .as_ref()
            .is_none_or(|code| code != CONTEXT_HINT_STREAM_FALLBACK_CODE)
    {
        return None;
    }
    let mut next = request.clone();
    intent
        .state
        .apply_frozen(&request.model, &mut next.messages);
    let projection = project_hint_edits(&next.messages, intent.artifact_dir.as_ref()).await;
    intent.state.record_projection(&request.model, &projection);
    next.context_hints = None;
    projection.apply(&mut next.messages);
    Some(next)
}
