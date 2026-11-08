use super::bare_tool_context;
use super::runtime_error_message_to_public_host;
use super::runtime_trim;
use super::validate_native_pdf_provider;
use super::AnyTool;
use super::CacheTtl;
use super::Session;
use super::SystemPromptResolvable;
use super::SystemPromptThunk;
use super::ThinkingConfig;
use crate::context::env_info::environment_block;
use crate::context::env_info::EnvBlockOptions;
use crate::engine::query::QueryParams;
use crate::engine::query::ToolContextFactory;
use crate::runtime_values::runtime_codec;
use crate::runtime_values::RuntimeCodecError;
use crate::runtime_values::RuntimeDecode;
use crate::runtime_values::RuntimeEncode;
use crate::runtime_values::RuntimeEncoder;
use crate::runtime_values::RuntimeJson;
use crate::runtime_values::RuntimeObjectEncoder;
use crate::runtime_values::RuntimeString;
use crate::schemas::content::ContentBlock;
use crate::schemas::content::TextBlock;
use crate::schemas::message::Message;
use crate::schemas::message::Usage;
use crate::schemas::message::UserMessage;
use crate::sync_ext::MutexExt;
use crate::tools::pool::to_tool_specs;
use crate::types::abort::AbortSignal;
use crate::types::errors::TerminalReason;
use crate::types::events::EngineEvent;
use crate::types::events::ThreadItem;
use crate::types::file_state::FileStateCache;
use crate::types::provider::SystemPromptPart;
use futures::stream::StreamExt;
use std::sync::{Arc, Mutex};

// The review task mode: a host-initiated, isolated review turn.
//
// A review runs as its OWN delegate query — a dedicated review system prompt
// REPLACES the session's agent prompt, the transcript starts fresh from the
// resolved review-target prompt, and nothing the reviewer does is written into
// the main conversation history. The host receives the structured result
// (findings + overall verdict) and decides what, if anything, to surface back
// into the conversation.

/// What the reviewer is asked to look at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewTarget {
    /// Review the working tree: staged, unstaged, and untracked files.
    UncommittedChanges,
    /// Review changes between the current branch and the given base branch.
    BaseBranch { branch: RuntimeString },
    /// Review the changes introduced by a specific commit.
    Commit {
        sha: RuntimeString,
        /// Optional human-readable label (e.g., commit subject) for UIs.
        title: Option<RuntimeString>,
    },
    /// Review the code in an explicit set of files or directories.
    Files { paths: Vec<RuntimeString> },
    /// Arbitrary review instructions provided by the user.
    Custom { instructions: RuntimeString },
}

impl RuntimeEncode for ReviewTarget {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::UncommittedChanges => "uncommitted_changes",
            Self::BaseBranch { branch } => {
                fields.field("branch", branch)?;
                "base_branch"
            }
            Self::Commit { sha, title } => {
                fields.field("sha", sha)?;
                if let Some(title) = title {
                    fields.field("title", title)?;
                }
                "commit"
            }
            Self::Files { paths } => {
                fields.field("paths", paths)?;
                "files"
            }
            Self::Custom { instructions } => {
                fields.field("instructions", instructions)?;
                "custom"
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ReviewTarget {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        use crate::runtime_values::runtime_codec::RuntimeEnumVariant;
        const VARIANTS: &[RuntimeEnumVariant<'_>] = &[
            RuntimeEnumVariant::new("uncommitted_changes", &[]),
            RuntimeEnumVariant::new("base_branch", &[]),
            RuntimeEnumVariant::new("commit", &[]),
            RuntimeEnumVariant::new("files", &[]),
            RuntimeEnumVariant::new("custom", &[]),
        ];
        runtime_codec::decode_internal(value, true, "type", &[], VARIANTS, |variant, object| {
            match variant {
                0 => Ok(Self::UncommittedChanges),
                1 => Ok(Self::BaseBranch {
                    branch: object.required("branch", &[])?,
                }),
                2 => Ok(Self::Commit {
                    sha: object.required("sha", &[])?,
                    title: object.optional("title", &[])?,
                }),
                3 => Ok(Self::Files {
                    paths: object.required("paths", &[])?,
                }),
                4 => Ok(Self::Custom {
                    instructions: object.required("instructions", &[])?,
                }),
                _ => unreachable!("variant index bounded by VARIANTS"),
            }
        })
    }
}

/// A host review request: the target plus an optional label override for UIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewRequest {
    pub target: ReviewTarget,
    pub user_facing_hint: Option<RuntimeString>,
}

/// Inclusive line range in a file associated with a finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewLineRange {
    pub start: u64,
    pub end: u64,
}

impl RuntimeEncode for ReviewLineRange {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("start", &self.start)?;
        object.field("end", &self.end)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ReviewLineRange {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                start: object.required("start", &[])?,
                end: object.required("end", &[])?,
            })
        })
    }
}

/// Location of the code related to a review finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewCodeLocation {
    pub absolute_file_path: RuntimeString,
    pub line_range: ReviewLineRange,
}

impl RuntimeEncode for ReviewCodeLocation {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("absolute_file_path", &self.absolute_file_path)?;
        object.field("line_range", &self.line_range)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ReviewCodeLocation {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                absolute_file_path: object.required("absolute_file_path", &[])?,
                line_range: object.required("line_range", &[])?,
            })
        })
    }
}

/// A single review finding describing an observed issue or recommendation.
///
/// Field spelling follows the reviewer-facing output schema (snake_case), so
/// the same codec decodes the model's JSON and encodes the host result.
#[derive(Debug, Clone, PartialEq)]
pub struct ReviewFinding {
    pub title: RuntimeString,
    pub body: RuntimeString,
    pub confidence_score: f64,
    /// 0 (P0, drop everything) .. 3 (P3, nice to have). The rubric allows the
    /// reviewer to omit it or send null when a priority cannot be determined,
    /// so parsing is tolerant here.
    pub priority: Option<i64>,
    pub code_location: ReviewCodeLocation,
}

impl RuntimeEncode for ReviewFinding {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("title", &self.title)?;
        object.field("body", &self.body)?;
        object.field("confidence_score", &self.confidence_score)?;
        if let Some(priority) = &self.priority {
            object.field("priority", priority)?;
        }
        object.field("code_location", &self.code_location)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ReviewFinding {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            // A JSON null priority reads as "no priority", per the rubric.
            let priority = match object.optional::<RuntimeJson>("priority", &[])? {
                None | Some(RuntimeJson::Null) => None,
                Some(other) => Some(i64::decode_runtime(&other)?),
            };
            Ok(Self {
                title: object.required("title", &[])?,
                body: object.required("body", &[])?,
                confidence_score: object.required("confidence_score", &[])?,
                priority,
                code_location: object.required("code_location", &[])?,
            })
        })
    }
}

/// The reviewer's structured verdict.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReviewOutput {
    pub findings: Vec<ReviewFinding>,
    pub overall_correctness: RuntimeString,
    pub overall_explanation: RuntimeString,
    pub overall_confidence_score: f64,
}

impl RuntimeEncode for ReviewOutput {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("findings", &self.findings)?;
        object.field("overall_correctness", &self.overall_correctness)?;
        object.field("overall_explanation", &self.overall_explanation)?;
        object.field("overall_confidence_score", &self.overall_confidence_score)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ReviewOutput {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                findings: object.required("findings", &[])?,
                overall_correctness: object.required("overall_correctness", &[])?,
                overall_explanation: object.required("overall_explanation", &[])?,
                overall_confidence_score: object.required("overall_confidence_score", &[])?,
            })
        })
    }
}

/// The completed review handed back to the host.
#[derive(Debug, Clone, PartialEq)]
pub struct ReviewSessionResult {
    pub output: ReviewOutput,
    pub user_facing_hint: RuntimeString,
    pub usage: Option<Usage>,
    pub stop_reason: TerminalReason,
}

impl RuntimeEncode for ReviewSessionResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("output", &self.output)?;
        object.field("userFacingHint", &self.user_facing_hint)?;
        if let Some(usage) = &self.usage {
            object.field("usage", usage)?;
        }
        object.field("stopReason", &self.stop_reason)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ReviewSessionResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                output: object.required("output", &[])?,
                user_facing_hint: object.required("userFacingHint", &[])?,
                usage: object.optional("usage", &[])?,
                stop_reason: object.required("stopReason", &[])?,
            })
        })
    }
}

/// The review-turn system prompt. It REPLACES the session's agent prompt (the
/// reviewer is a reviewer, not the coding agent) and pins the structured JSON
/// output contract the result parser expects.
pub(crate) const REVIEW_SYSTEM_PROMPT: &str = r#"# Review guidelines

You are acting as a reviewer for a proposed code change made by another engineer.

Below are the default guidelines for determining whether the original author would appreciate an issue being flagged. More specific guidelines may appear elsewhere (in a developer message, a user message, or a file) and override these general instructions.

An issue qualifies as a bug worth flagging when:

1. It meaningfully impacts the accuracy, performance, security, or maintainability of the code.
2. It is discrete and actionable (not a general issue with the codebase or a combination of multiple issues).
3. Fixing it does not demand a level of rigor absent from the rest of the codebase.
4. It was introduced by the change under review (pre-existing bugs should not be flagged).
5. The author would likely fix the issue if made aware of it.
6. It does not rely on unstated assumptions about the codebase or the author's intent.
7. Any claimed impact on other parts of the codebase identifies the code that is provably affected, not speculation.
8. It is clearly not just an intentional change by the original author.

For every flagged issue, write an accompanying comment:

1. Be clear about why the issue is a bug.
2. Communicate severity accurately; do not overstate it.
3. Keep the body to at most one paragraph.
4. Do not include code chunks longer than 3 lines; wrap any code in inline code tags or a code block.
5. State the scenarios, environments, or inputs required for the bug to arise.
6. Keep the tone matter-of-fact, not accusatory and not flattering.
7. Write so the original author can immediately grasp the idea without close reading.

HOW MANY FINDINGS TO RETURN:

Output every finding the original author would fix if they knew about it. If no finding meets that bar, prefer outputting no findings. Do not stop at the first qualifying finding.

GUIDELINES:

- Ignore trivial style unless it obscures meaning or violates documented standards.
- Use one comment per distinct issue.
- Keep each finding's line range as short as possible (avoid ranges over 5-10 lines; pick the most suitable subrange).

Tag each finding title with a priority level, e.g. "[P1] Un-padding slices along wrong tensor dimensions". [P0] - drop everything to fix; blocking release or major usage. [P1] - urgent, next cycle. [P2] - normal, fix eventually. [P3] - low, nice to have. Also set the numeric "priority" field to 0-3 accordingly; omit it or use null when a priority cannot be determined.

At the end, output an "overall correctness" verdict of whether the patch should be considered "correct": correct implies existing code and tests will not break and the patch is free of bugs and other blocking issues. Ignore non-blocking issues such as style, formatting, typos, and documentation.

OUTPUT FORMAT:

## Output schema - MUST MATCH *exactly*

```json
{
  "findings": [
    {
      "title": "<= 80 chars, imperative>",
      "body": "<valid Markdown explaining *why* this is a problem; cite files/lines/functions>",
      "confidence_score": <float 0.0-1.0>,
      "priority": <int 0-3, optional>,
      "code_location": {
        "absolute_file_path": "<file path>",
        "line_range": {"start": <int>, "end": <int>}
      }
    }
  ],
  "overall_correctness": "patch is correct" | "patch is incorrect",
  "overall_explanation": "<1-3 sentence explanation justifying the overall_correctness verdict>",
  "overall_confidence_score": <float 0.0-1.0>
}
```

* **Do not** wrap the JSON in markdown fences or extra prose.
* The code_location field is required and must include absolute_file_path and line_range, and should overlap with the diff.
* Do not generate a PR fix."#;

const UNCOMMITTED_PROMPT: &str = "Review the current code changes (staged, unstaged, and untracked files) and provide prioritized findings.";

/// The reviewer computes the merge base itself (the engine does not shell out
/// to git while resolving the target).
fn base_branch_prompt(branch: &RuntimeString) -> RuntimeString {
    let branch = review_scalar(branch);
    RuntimeString::from(format!(
        "Review the code changes against the base branch '{branch}'. Start by finding the merge \
         diff between the current branch and {branch}'s upstream e.g. (`git merge-base HEAD \
         \"$(git rev-parse --abbrev-ref \"{branch}@{{upstream}}\")\"`), then run `git diff` \
         against that SHA to see what changes we would merge into the {branch} branch. Provide \
         prioritized, actionable findings."
    ))
}

fn commit_prompt(sha: &RuntimeString, title: Option<&RuntimeString>) -> RuntimeString {
    let sha = review_scalar(sha);
    RuntimeString::from(match title {
        Some(title) => format!(
            "Review the code changes introduced by commit {sha} (\"{}\"). Provide prioritized, \
             actionable findings.",
            review_scalar(title)
        ),
        None => {
            format!("Review the code changes introduced by commit {sha}. Provide prioritized, actionable findings.")
        }
    })
}

fn files_prompt(paths: &[RuntimeString]) -> RuntimeString {
    let list = paths
        .iter()
        .map(|path| format!("- {}", review_scalar(path)))
        .collect::<Vec<_>>()
        .join("\n");
    RuntimeString::from(format!(
        "Review the code in the following files or directories and provide prioritized, \
         actionable findings:\n{list}"
    ))
}

/// Resolve the review-target user prompt.
pub(crate) fn review_prompt_for_target(target: &ReviewTarget) -> Result<RuntimeString, String> {
    match target {
        ReviewTarget::UncommittedChanges => Ok(RuntimeString::from(UNCOMMITTED_PROMPT)),
        ReviewTarget::BaseBranch { branch } => {
            if runtime_trim(branch).is_empty() {
                return Err("review base branch cannot be empty".to_string());
            }
            Ok(base_branch_prompt(branch))
        }
        ReviewTarget::Commit { sha, title } => {
            if runtime_trim(sha).is_empty() {
                return Err("review commit sha cannot be empty".to_string());
            }
            Ok(commit_prompt(sha, title.as_ref()))
        }
        ReviewTarget::Files { paths } => {
            if paths.iter().all(|path| runtime_trim(path).is_empty()) {
                return Err("review file scope cannot be empty".to_string());
            }
            Ok(files_prompt(paths))
        }
        ReviewTarget::Custom { instructions } => {
            let trimmed = runtime_trim(instructions);
            if trimmed.is_empty() {
                return Err("review prompt cannot be empty".to_string());
            }
            Ok(trimmed)
        }
    }
}

/// Short human-readable label for what is being reviewed.
pub(crate) fn review_user_facing_hint(target: &ReviewTarget) -> RuntimeString {
    match target {
        ReviewTarget::UncommittedChanges => RuntimeString::from("current changes"),
        ReviewTarget::BaseBranch { branch } => {
            RuntimeString::from(format!("changes against '{}'", review_scalar(branch)))
        }
        ReviewTarget::Commit { sha, title } => {
            let short_sha: String = review_scalar(sha).chars().take(7).collect();
            RuntimeString::from(match title {
                Some(title) => format!("commit {short_sha}: {}", review_scalar(title)),
                None => format!("commit {short_sha}"),
            })
        }
        ReviewTarget::Files { paths } => match paths.len() {
            1 => paths[0].clone(),
            n => RuntimeString::from(format!("{n} files")),
        },
        ReviewTarget::Custom { instructions } => runtime_trim(instructions),
    }
}

/// Host-lossy projection for prompt interpolation; review targets are
/// host-supplied scalar strings.
fn review_scalar(value: &RuntimeString) -> String {
    String::from_utf8_lossy(&value.to_node_utf8()).into_owned()
}

/// Parse the reviewer's final message into a [`ReviewOutput`].
///
/// Parses the text as follows: try the whole text as JSON, then the first
/// `{` .. last `}` substring, and finally fall back to a structured shell that
/// carries the plain text in `overall_explanation`.
pub(crate) fn parse_review_output(text: &RuntimeString) -> ReviewOutput {
    if let Ok(value) = crate::runtime_values::runtime_json::parse(text) {
        if let Ok(output) = ReviewOutput::decode_runtime(&value) {
            return output;
        }
    }
    let units = text.as_utf16();
    let start = units.iter().position(|unit| *unit == u16::from(b'{'));
    let end = units.iter().rposition(|unit| *unit == u16::from(b'}'));
    if let (Some(start), Some(end)) = (start, end) {
        if start < end {
            let slice = text.slice_utf16(start..=end);
            if let Ok(value) = crate::runtime_values::runtime_json::parse(&slice) {
                if let Ok(output) = ReviewOutput::decode_runtime(&value) {
                    return output;
                }
            }
        }
    }
    ReviewOutput {
        overall_explanation: text.clone(),
        ..Default::default()
    }
}

/// The reviewer's tool surface: inspect-only builtins plus shell access for
/// `git diff`/`git show` style commands. Mutating tools (write/edit/patch),
/// web access, and sub-agent spawning stay out, per the
/// review-mode restrictions; this is a task-mode filter over the session's
/// EXISTING tools, never a new default tool.
pub(crate) const REVIEW_TOOL_NAMES: &[&str] =
    &["bash", "read", "ls", "grep", "glob", "file_search", "think"];

impl Session {
    /// Run a review task to completion. See [`Session::run_review_with_signal`].
    pub async fn run_review(
        self: &Arc<Self>,
        request: ReviewRequest,
    ) -> Result<ReviewSessionResult, String> {
        self.run_review_with_signal(request, AbortSignal::new())
            .await
    }

    /// Host-triggered review task: run an isolated review turn against the
    /// requested target and return the parsed structured verdict.
    ///
    /// Isolation contract: the reviewer starts from a FRESH transcript (the
    /// resolved target prompt only), runs under the dedicated review system
    /// prompt, and never writes to the main conversation history — the
    /// structured result is the only artifact handed back.
    pub async fn run_review_with_signal(
        self: &Arc<Self>,
        request: ReviewRequest,
        signal: AbortSignal,
    ) -> Result<ReviewSessionResult, String> {
        // The review query drives the full tool loop, so its future is large;
        // box it to keep the caller's task frame small.
        let this = self.clone();
        Box::pin(this.run_review_impl(request, signal)).await
    }

    async fn run_review_impl(
        self: Arc<Self>,
        request: ReviewRequest,
        signal: AbortSignal,
    ) -> Result<ReviewSessionResult, String> {
        let capture = self.capture_inference(&signal).await?;
        let bundle = capture.bundle.clone();
        if self.is_disposed() {
            return Err("cannot run a review on a disposed session".to_string());
        }
        if signal.is_cancelled() {
            return Err("review cancelled".to_string());
        }
        let prompt = review_prompt_for_target(&request.target)?;
        let user_facing_hint = request
            .user_facing_hint
            .clone()
            .filter(|hint| !runtime_trim(hint).is_empty())
            .unwrap_or_else(|| review_user_facing_hint(&request.target));

        let model = bundle.config.model.clone();
        let abort = signal;
        // Fresh delegate transcript: the review prompt is the whole history.
        let messages = vec![Message::User(UserMessage {
            question_answer: None,
            uuid: (self.deps.uuid)().into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: prompt,
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })];
        let config = bundle.config_for_model(&model);

        // The review rubric REPLACES the agent prompt; only the environment
        // block is carried over so the reviewer knows where it is running.
        let system_prompt: SystemPromptThunk = {
            let cwd = self.cwd_now();
            let session_config = bundle.config.clone();
            let model_for_env = model.clone();
            let is_worktree = self.workspace.is_in_worktree();
            Arc::new(move || {
                let env = environment_block(
                    &cwd,
                    &session_config,
                    EnvBlockOptions {
                        plat: None,
                        shell: None,
                        model: Some(model_for_env.clone()),
                        is_worktree,
                    },
                );
                Box::pin(async move {
                    vec![
                        SystemPromptPart {
                            name: "review".into(),
                            text: RuntimeString::from(REVIEW_SYSTEM_PROMPT),
                            cacheable: Some(true),
                            cache_scope: None,
                        },
                        SystemPromptPart {
                            name: "env".into(),
                            text: env,
                            cacheable: Some(true),
                            cache_scope: None,
                        },
                    ]
                })
            })
        };

        // Inspect-only tool surface; never prompts (dontAsk denies instead of
        // asking): an approval-free review delegate.
        let tools: Vec<AnyTool> = self
            .tools
            .iter()
            .filter(|tool| REVIEW_TOOL_NAMES.iter().any(|name| tool.name() == *name))
            .cloned()
            .collect();
        let mut permission = self.permission_context();
        permission.should_avoid_prompts = true;

        let tool_context_base: ToolContextFactory = {
            let this = self.clone();
            let abort = abort.clone();
            let permission = permission.clone();
            Arc::new(move || {
                let mut ctx = bare_tool_context(
                    this.process_context.clone(),
                    this.background_processes.clone(),
                    this.cwd_now(),
                    abort.clone(),
                    this.file_state.clone() as Arc<dyn FileStateCache>,
                    permission.clone(),
                    this.runtime_policy_snapshot().sandbox_backend.clone(),
                );
                let policy = this.runtime_policy_snapshot();
                policy.apply_to_context(&mut ctx);
                ctx
            })
        };
        let cost_so_far: crate::engine::query::CostSoFar = {
            let cost = self.cost.clone();
            Box::new(move || cost.lock_recover().total_cost_usd())
        };
        let review_suffix =
            RuntimeString::from(":review:").concat(&RuntimeString::from((self.deps.uuid)()));
        let params = QueryParams {
            multi_agent_instructions: false,
            thread_id: self.id.concat(&review_suffix),
            turn_id: RuntimeString::from((self.deps.uuid)()),
            model: model.clone(),
            provider_info: Some(bundle.provider.info().clone()),
            messages,
            tool_specs: to_tool_specs(&tools),
            tools,
            refresh_tools: None,
            refresh_tools_async: None,
            agent_tool_policy: self.root_agent_definition.clone(),
            compaction_protected_message_ids: Default::default(),
            context_window: None,
            precompute: None,
            compaction_history: None,
            recover_compaction_history: None,
            compaction_output: None,
            system_prompt: SystemPromptResolvable::Fn(system_prompt),
            config,
            max_turns: None,
            cost_so_far: Some(cost_so_far),
            tool_context_base,
            permission_engine: self.permission_engine.clone(),
            hook_bus: None,
            signal: abort.clone(),
            approval_handler: None,
            approval_reviewer: None,
            output_schema: None,
            token_budget: None,
            goal: None,
            goal_request_identity: None,
            goal_pending_input: None,
            goal_execution_enabled: None,
            goal_usage: None,
            web_search: None,
            on_message: None,
            prepare_request_usage: Some(self.request_usage_recorder({
                let bundle = bundle.clone();
                Arc::new(move || bundle.clone())
            })),
            retained_tool_tasks: Some(self.persistence.tool_tasks.clone()),
            message_rollout: None,
            on_permission_update: None,
            on_compaction: None,
            surfaced_tools: None,
            on_surface_tools: None,
            on_tool_lifecycle: None,
            provider_call_admission: None,
            on_provider_request_admitted: None,
            emit_thread_started: false,
            reminder_providers: Vec::new(),
            edited_files_reminder: None,
            effort: bundle.config.effort.clone(),
            ultra_mode: bundle.config.ultra_mode,
            thinking: if bundle.config.thinking {
                Some(ThinkingConfig::Adaptive)
            } else {
                None
            },
            skip_cache_write: Some(true),
            cache_ttl: if self.config.cache1h_ttl {
                Some(CacheTtl::OneHour)
            } else {
                None
            },
            cache_domain: None,
            cache_key: Some(self.id.clone()),
            plan_file_path: None,
            message_queue: None,
            input_pdf_bytes: 0,
            input_pdf_documents: 0,
            validate_native_pdf: {
                let provider = bundle.provider.clone();
                Some(Arc::new(move |model| {
                    validate_native_pdf_provider(provider.as_ref(), model)
                }))
            },
            agent_id: None,
            agent_type: Some("review".into()),
            // A forked review is a one-shot side query with its own thread id; it
            // owns no long-lived cached prefix, so the idle pass never applies.
            context_hint_state: None,
            file_state: None,
            tool_result_budget_state: None,
            on_tool_result_budget_records: None,
            thinking_context_cold: self.turn.thinking_context_cold(),
            padded_countdown_spend: self.turn.padded_countdown_spend(),
            task_countdown_anchor: None,
            live_model_override: None,
        };
        let mut events = crate::engine::query::query_with_inference(
            params,
            bundle.deps.to_engine_deps(),
            crate::engine::query::InferenceCapture {
                slot: Arc::new(Mutex::new(Some(capture))),
                next: None,
                model_override: None,
                effort_override: None,
                ultra_override: None,
            },
        );
        let mut final_text = RuntimeString::default();
        let mut usage = None;
        let mut stop_reason = TerminalReason::Completed;
        loop {
            let ev = tokio::select! {
                biased;
                _ = abort.cancelled() => {
                    return Err("review cancelled".to_string());
                }
                ev = events.next() => ev,
            };
            let Some(ev) = ev else {
                break;
            };
            match ev {
                EngineEvent::ItemCompleted {
                    inference: _,
                    item: ThreadItem::AgentMessage { text, .. },
                } => final_text = text,
                EngineEvent::TurnCompleted {
                    usage: u,
                    stop_reason: reason,
                    ..
                } => {
                    usage = u;
                    stop_reason = reason;
                }
                EngineEvent::TurnFailed { error, .. } => {
                    return Err(runtime_error_message_to_public_host(&error.message));
                }
                _ => {}
            }
        }
        Ok(ReviewSessionResult {
            output: parse_review_output(&final_text),
            user_facing_hint,
            usage,
            stop_reason,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests: target resolution + findings parsing (pure halves of review mode).
// The isolated review turn itself is covered in `session_turn_tests.rs`.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn finding_json(priority: &str) -> String {
        format!(
            r#"{{
  "findings": [
    {{
      "title": "[P1] Off-by-one in range check",
      "body": "The loop bound skips the final element.",
      "confidence_score": 0.9,
      "priority": {priority},
      "code_location": {{
        "absolute_file_path": "/repo/src/lib.rs",
        "line_range": {{"start": 10, "end": 12}}
      }}
    }}
  ],
  "overall_correctness": "patch is incorrect",
  "overall_explanation": "One qualifying bug was found.",
  "overall_confidence_score": 0.8
}}"#
        )
    }

    #[test]
    fn parses_a_strict_review_output_document() {
        let out = parse_review_output(&RuntimeString::from(finding_json("1")));
        assert_eq!(out.findings.len(), 1);
        let finding = &out.findings[0];
        assert_eq!(finding.title, "[P1] Off-by-one in range check");
        assert_eq!(finding.priority, Some(1));
        assert_eq!(finding.confidence_score, 0.9);
        assert_eq!(finding.code_location.absolute_file_path, "/repo/src/lib.rs");
        assert_eq!(finding.code_location.line_range.start, 10);
        assert_eq!(finding.code_location.line_range.end, 12);
        assert_eq!(out.overall_correctness, "patch is incorrect");
        assert_eq!(out.overall_confidence_score, 0.8);
    }

    // The rubric allows priority to be omitted or null.
    #[test]
    fn tolerates_a_null_priority() {
        let out = parse_review_output(&RuntimeString::from(finding_json("null")));
        assert_eq!(out.findings[0].priority, None);
    }

    // When the reviewer wraps the JSON in prose, the first-`{`-to-last-`}`
    // substring is retried.
    #[test]
    fn extracts_an_embedded_json_object_from_prose() {
        let wrapped = format!("Here are my findings:\n{}\nDone.", finding_json("2"));
        let out = parse_review_output(&RuntimeString::from(wrapped));
        assert_eq!(out.findings.len(), 1);
        assert_eq!(out.findings[0].priority, Some(2));
    }

    // Unparseable output degrades to a structured shell carrying the raw text.
    #[test]
    fn falls_back_to_plain_text_in_overall_explanation() {
        let text = RuntimeString::from("The patch looks fine to me.");
        let out = parse_review_output(&text);
        assert!(out.findings.is_empty());
        assert_eq!(out.overall_explanation, "The patch looks fine to me.");
        assert_eq!(out.overall_correctness, RuntimeString::default());
        assert_eq!(out.overall_confidence_score, 0.0);
    }

    // A finding missing its required code_location fails strict decoding and
    // lands in the fallback, per the strict-parse contract.
    #[test]
    fn missing_code_location_falls_back_to_raw_text() {
        let text = RuntimeString::from(
            r#"{"findings":[{"title":"t","body":"b","confidence_score":0.5}],"overall_correctness":"patch is correct","overall_explanation":"x","overall_confidence_score":1.0}"#,
        );
        let out = parse_review_output(&text);
        assert!(out.findings.is_empty());
        assert_eq!(out.overall_explanation, text);
    }

    #[test]
    fn resolves_target_prompts_with_reference_wording() {
        let uncommitted = review_prompt_for_target(&ReviewTarget::UncommittedChanges).unwrap();
        assert_eq!(
            uncommitted,
            "Review the current code changes (staged, unstaged, and untracked files) and provide prioritized findings."
        );

        let commit = review_prompt_for_target(&ReviewTarget::Commit {
            sha: "abc1234def".into(),
            title: Some("Fix parser".into()),
        })
        .unwrap();
        assert_eq!(
            commit,
            "Review the code changes introduced by commit abc1234def (\"Fix parser\"). Provide prioritized, actionable findings."
        );

        let branch = review_prompt_for_target(&ReviewTarget::BaseBranch {
            branch: "main".into(),
        })
        .unwrap();
        assert!(branch.contains("against the base branch 'main'"));
        assert!(branch.contains("git merge-base HEAD"));

        let files = review_prompt_for_target(&ReviewTarget::Files {
            paths: vec!["src/a.rs".into(), "src/b.rs".into()],
        })
        .unwrap();
        assert!(files.contains("- src/a.rs"));
        assert!(files.contains("- src/b.rs"));

        let custom = review_prompt_for_target(&ReviewTarget::Custom {
            instructions: "  check error handling  ".into(),
        })
        .unwrap();
        assert_eq!(custom, "check error handling");
    }

    #[test]
    fn rejects_empty_review_targets() {
        assert!(review_prompt_for_target(&ReviewTarget::Custom {
            instructions: "   ".into(),
        })
        .is_err());
        assert!(review_prompt_for_target(&ReviewTarget::BaseBranch { branch: "".into() }).is_err());
        assert!(review_prompt_for_target(&ReviewTarget::Commit {
            sha: " ".into(),
            title: None,
        })
        .is_err());
        assert!(review_prompt_for_target(&ReviewTarget::Files { paths: vec![] }).is_err());
    }

    #[test]
    fn renders_user_facing_hints() {
        assert_eq!(
            review_user_facing_hint(&ReviewTarget::UncommittedChanges),
            "current changes"
        );
        assert_eq!(
            review_user_facing_hint(&ReviewTarget::BaseBranch {
                branch: "main".into()
            }),
            "changes against 'main'"
        );
        assert_eq!(
            review_user_facing_hint(&ReviewTarget::Commit {
                sha: "abc1234def".into(),
                title: Some("Fix parser".into()),
            }),
            "commit abc1234: Fix parser"
        );
        assert_eq!(
            review_user_facing_hint(&ReviewTarget::Files {
                paths: vec!["src/a.rs".into(), "src/b.rs".into()],
            }),
            "2 files"
        );
    }

    #[test]
    fn review_target_wire_codec_round_trips() {
        use crate::runtime_values::runtime_codec;
        for target in [
            ReviewTarget::UncommittedChanges,
            ReviewTarget::BaseBranch {
                branch: "main".into(),
            },
            ReviewTarget::Commit {
                sha: "abc".into(),
                title: Some("t".into()),
            },
            ReviewTarget::Files {
                paths: vec!["src/a.rs".into()],
            },
            ReviewTarget::Custom {
                instructions: "look closely".into(),
            },
        ] {
            let encoded = runtime_codec::encode_value(&target).unwrap();
            let decoded = ReviewTarget::decode_runtime(&encoded).unwrap();
            assert_eq!(decoded, target);
        }
    }
}
