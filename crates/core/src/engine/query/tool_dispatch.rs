//! Tool-call dispatch: concurrency grouping, permission + hook + approval
//! flow, execution, and tool lifecycle recording.

use super::base_hook_input;
use super::build_tool_catalog;
use super::concat_runtime_strings;
use super::find_runtime_tool;
use super::invoked_tool_name_to_registry_host;
use super::join_runtime_strings;
use super::permission_denial_message;
use super::query_path_dialect;
use super::render_failed_tool_result_content;
use super::render_interrupted_tool_result_content;
use super::render_successful_tool_result_content;
use super::result_trim::render_spilled_tool_result;
use super::result_trim::tool_result_spill_limit;
use super::safe_json_text;
use super::successful_result_provider_metadata;
use super::support::notify;
use super::text_out_exact;
use super::to_json_safe;
use super::tool_reported_error_detail;
use super::unknown_tool_message;
use super::ResolvedToolCall;
use super::Shared;
use super::ToolCallResult;
use crate::hooks::types::HookEvent;
use crate::permission::engine::PermissionDecision;
use crate::permission::rules::parse_rule_strings;
use crate::runtime_process::path_join;
use crate::runtime_values::runtime_codec;
use crate::runtime_values::RuntimeJson;
use crate::runtime_values::RuntimeObject;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::schemas::content::HarnessEnvelope;
use crate::schemas::content::OutputBlock;
use crate::schemas::content::TextBlock;
use crate::schemas::content::ToolUseBlock;
use crate::schemas::message::Message;
use crate::schemas::message::ToolResultMessage;
/// Execute tool calls preserving the model's order. Consecutive
/// concurrency-safe tools run in parallel (bounded); others run alone as
/// barriers. Results are emitted in input order.
use crate::sync_ext::MutexExt;
use crate::tools::result_storage::maybe_spill_result;
use crate::tools::result_storage::output_blocks_text;
use crate::tools::result_storage::persist_per_message_budget;
use crate::tools::result_storage::SpillOptions;
use crate::types::errors::ToolError;
use crate::types::errors::ToolErrorKind;
use crate::types::events::EngineEvent;
use crate::types::events::ItemError;
use crate::types::events::ItemStatus;
use crate::types::events::NotificationLevel;
use crate::types::events::ThreadItem;
use crate::types::events::ToolDomainEvent;
use crate::types::events::ToolLifecyclePhase;
use crate::types::events::ToolLifecycleRecord;
use crate::types::events::ToolLifecycleSource;
use crate::types::events::ToolLifecycleSourceKind;
use crate::types::json::Json;
use crate::types::permission::ApprovalKind;
use crate::types::permission::ApprovalRequest;
use crate::types::permission::ApprovalResponse;
use crate::types::permission::ApprovalReviewResponse;
use crate::types::permission::ApprovalReviewer;
use crate::types::permission::PermissionReason;
use crate::types::permission::PermissionUpdate;
use crate::types::tool::InvokeToolResult;
use crate::types::tool::NetworkAccessRequest;
use crate::types::tool::SkillContextMod;
use crate::types::tool::Tool;
use crate::types::tool::ToolExposure;
use crate::types::tool::ToolProgress;
use crate::types::tool::ValidationResult;
use futures::future::FutureExt;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

#[derive(Clone, PartialEq, Eq)]
struct RuntimePermissionStamp {
    approval: Option<crate::types::permission::ApprovalPolicy>,
    sandbox_mode: Option<crate::types::permission::SandboxMode>,
    reviewer: Option<crate::types::permission::ApprovalsReviewer>,
    network: Option<bool>,
    retry: Option<bool>,
    sandbox: crate::types::sandbox::SandboxKind,
    planning_read_only: bool,
    roots: Vec<RuntimePath>,
    allow: Vec<crate::types::permission::PermissionRule>,
    deny: Vec<crate::types::permission::PermissionRule>,
    ask: Vec<crate::types::permission::PermissionRule>,
    durable: crate::permission::permission_update::SessionPermissionState,
}

impl RuntimePermissionStamp {
    fn capture(
        context: &crate::types::tool::ToolContext,
        engine: &dyn crate::permission::engine::PermissionEngine,
    ) -> Self {
        Self {
            approval: context.approval_policy,
            sandbox_mode: context.sandbox_mode,
            reviewer: context.approvals_reviewer,
            network: context.network_allowed,
            retry: context.sandbox_retry_on_denial,
            sandbox: context.sandbox.kind(),
            planning_read_only: context.planning_read_only,
            roots: context.permission.additional_writable_roots.clone(),
            allow: context.permission.allow_rules.clone(),
            deny: context.permission.deny_rules.clone(),
            ask: context.permission.ask_rules.clone(),
            durable: engine.rule_snapshot(),
        }
    }

    fn apply_owned_updates(&mut self, updates: &[PermissionUpdate]) -> Result<(), RuntimeString> {
        for update in updates {
            match update {
                PermissionUpdate::SetMode { .. } => return Err("The approval changed the runtime policy; retry this action under the new policy.".into()),
                PermissionUpdate::AddRules { destination: crate::types::permission::PermissionDestination::Session, .. } => {
                    let mut state = crate::permission::permission_update::SessionPermissionState {
                        allow_rules: self.allow.clone(), deny_rules: self.deny.clone(), ask_rules: self.ask.clone(),
                    };
                    crate::permission::permission_update::apply_add_rules(&mut state, update);
                    self.allow = state.allow_rules;
                    self.deny = state.deny_rules;
                    self.ask = state.ask_rules;
                }
                PermissionUpdate::AddRules { .. } => crate::permission::permission_update::apply_add_rules(&mut self.durable, update),
            }
        }
        Ok(())
    }
}

struct ApprovedToolInput {
    input: Json,
    policy: RuntimePermissionStamp,
}
/// Gate the owned request view after its selected model's media declaration
/// has been bound. Never apply this to history or queue projections: the next
/// capture may select different axes or a different encoded-size ceiling.
pub(crate) fn gate_request_images(request: &mut crate::types::provider::ModelRequest) {
    use crate::context::images::GatedImage;
    use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock};
    use crate::types::provider::ImageLimits;

    fn marker(text: RuntimeString) -> TextBlock {
        TextBlock {
            text,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        }
    }
    fn gate_output(
        blocks: Vec<OutputBlock>,
        supports: bool,
        declaration: Option<&ImageLimits>,
    ) -> Vec<OutputBlock> {
        blocks
            .into_iter()
            .map(|block| match block {
                OutputBlock::Image(image) => {
                    match crate::context::inspection_images::gate_selected_image(
                        &image,
                        supports,
                        declaration,
                    ) {
                        GatedImage::Keep(image) | GatedImage::Downscaled(image) => {
                            OutputBlock::Image(image)
                        }
                        GatedImage::Rejected(reason) => OutputBlock::Text(marker(reason)),
                    }
                }
                block => block,
            })
            .collect()
    }
    fn gate_content(
        blocks: Vec<ContentBlock>,
        supports: bool,
        declaration: Option<&ImageLimits>,
    ) -> Vec<ContentBlock> {
        blocks
            .into_iter()
            .map(|block| match block {
                ContentBlock::Image(image) => {
                    match crate::context::inspection_images::gate_selected_image(
                        &image,
                        supports,
                        declaration,
                    ) {
                        GatedImage::Keep(image) | GatedImage::Downscaled(image) => {
                            ContentBlock::Image(image)
                        }
                        GatedImage::Rejected(reason) => ContentBlock::Text(marker(reason)),
                    }
                }
                ContentBlock::ToolResult(mut result) => {
                    result.content = gate_output(result.content, supports, declaration);
                    ContentBlock::ToolResult(result)
                }
                block => block,
            })
            .collect()
    }
    let supports = request.supports_image_detail_original;
    let declaration = request.image_limits.as_ref();
    for message in &mut request.messages {
        match message {
            Message::User(user) => {
                user.content =
                    gate_content(std::mem::take(&mut user.content), supports, declaration)
            }
            Message::Assistant(assistant) => {
                assistant.content = gate_content(
                    std::mem::take(&mut assistant.content),
                    supports,
                    declaration,
                )
            }
            Message::ToolResult(result) => {
                result.content =
                    gate_output(std::mem::take(&mut result.content), supports, declaration);
                if let Some(blocks) = &mut result.extra_user_blocks {
                    *blocks = gate_content(std::mem::take(blocks), supports, declaration);
                }
            }
            _ => {}
        }
    }
}

/// Project aggregate native media only on an outgoing request copy. The image
/// preparation pass must run first so validation sees each final encoded image.
pub(crate) fn project_request_media(
    request: &mut crate::types::provider::ModelRequest,
) -> Result<(), Box<crate::types::errors::InputValidationError>> {
    use crate::context::media_budget::{
        MediaBudget, MEDIA_BASE64_HEADROOM, MEDIA_COUNT_HEADROOM, OTHER_MEDIA_BASE64_BYTES,
    };
    let max_blocks = request
        .max_media_blocks
        .unwrap_or(crate::types::provider::DEFAULT_MAX_MEDIA_BLOCKS)
        .get() as usize;
    let max_image_base64_bytes = request
        .image_limits
        .as_ref()
        .and_then(|limits| limits.max_base64_size)
        .map_or(crate::context::images::IMAGE_MAX_BASE64_CHARS, |limit| {
            limit.get() as usize
        });
    let budget = MediaBudget {
        max_blocks,
        max_base64_bytes: OTHER_MEDIA_BASE64_BYTES,
        count_headroom: MEDIA_COUNT_HEADROOM,
        base64_headroom: MEDIA_BASE64_HEADROOM,
    };
    let projected = crate::context::request_media_budget::project_native_media(
        &request.messages,
        budget,
        max_image_base64_bytes,
    )
    .map_err(|mut error| {
        error.detail.model = Some(request.model.clone());
        error
    })?;
    if let std::borrow::Cow::Owned(projected) = projected {
        request.messages = projected;
    }
    Ok(())
}

pub(super) async fn dispatch_tool_calls(
    shared: &Arc<Shared>,
    calls: &[ResolvedToolCall],
    mut prestarted: HashMap<RuntimeString, JoinHandle<ToolCallResult>>,
) -> Result<Vec<ToolResultMessage>, ToolError> {
    let limit = Arc::new(Semaphore::new(
        shared.config().max_tool_concurrency.max(1) as usize
    ));
    let tools = shared.tools.lock_recover().clone();
    let groups = group_by_concurrency(calls, &tools);
    let mut results: Vec<ToolResultMessage> = Vec::new();
    let mut handled: HashSet<RuntimeString> = HashSet::new();

    for group in &groups {
        if let Some(request) = shared.advisor_request.lock_recover().as_mut() {
            // Completed earlier groups in this same response are effective context.
            for result in &results {
                if !request.messages.iter().any(|message| matches!(
                    message, Message::ToolResult(existing) if existing.tool_use_id == result.tool_use_id
                )) {
                    request.messages.push(Message::ToolResult(result.clone()));
                }
            }
        }
        if shared.params.signal.is_cancelled() {
            break;
        }
        if group.len() == 1 {
            let call = group[0].clone();
            let r = match prestarted.remove(&call.visible.id) {
                Some(handle) => await_prestarted_tool_call(handle).await,
                None => {
                    let permit = Arc::clone(&limit).acquire_owned().await.unwrap();
                    let r = execute_tool_call(Arc::clone(shared), call.clone()).await;
                    drop(permit);
                    r
                }
            }?;
            handled.insert(call.visible.id.clone());
            results.push(r);
        } else {
            // Promise.all starts every sibling before observing the first
            // rejection, and rejecting one promise does not cancel the rest.
            // Spawn every non-prestarted call up front; dropping a JoinHandle
            // after a fatal sibling detaches rather than aborts its task.
            //
            // The limiter admits queued functions in the order they were
            // submitted, and an admitted function publishes its start record
            // synchronously before its awaited work begins. Mirror that in two
            // phases: every sibling that fits under the limit RIGHT NOW gets
            // its start published synchronously here, in model order, before
            // any of their executions is spawned — so no fast sibling can
            // complete before a later immediately-admitted sibling starts.
            // Only the overflow beyond the limit keeps the chained admission,
            // where each start is published before the next sibling is
            // released.
            enum PlannedSpawn {
                Prestarted(JoinHandle<ToolCallResult>),
                Immediate(
                    ResolvedToolCall,
                    OwnedSemaphorePermit,
                    super::tool_tasks::RetainedToolTask,
                ),
                Queued(ResolvedToolCall),
            }
            let handoff = shared.persistence_in_flight.enter()?;
            let mut planned = Vec::with_capacity(group.len());
            for call in group {
                if let Some(handle) = prestarted.remove(&call.visible.id) {
                    planned.push(PlannedSpawn::Prestarted(handle));
                } else if let Ok(permit) = Arc::clone(&limit).try_acquire_owned() {
                    let registration = retained_tool_tasks(shared).reserve()?;
                    push_tool_call_started(shared, call).await?;
                    planned.push(PlannedSpawn::Immediate(call.clone(), permit, registration));
                } else {
                    planned.push(PlannedSpawn::Queued(call.clone()));
                }
            }
            let mut handles = Vec::with_capacity(group.len());
            let mut prior_non_prestarted_admission = None;
            for plan in planned {
                match plan {
                    PlannedSpawn::Prestarted(handle) => handles.push(handle),
                    PlannedSpawn::Immediate(call, permit, registration) => {
                        let s = Arc::clone(shared);
                        handles.push(registration.spawn(async move {
                            let _permit = permit;
                            #[cfg(test)]
                            test_parallel_admission_delay(&call).await;
                            execute_tool_call_after_started(s, call).await
                        }));
                    }
                    PlannedSpawn::Queued(call) => {
                        // Chain admission notifications so each queued sibling
                        // reaches the semaphore in model order, while sending
                        // the next notification immediately after publishing
                        // the start keeps the configured parallelism fully
                        // utilized.
                        let prior_admission = prior_non_prestarted_admission.take();
                        let (admitted, next_admission) = tokio::sync::oneshot::channel();
                        prior_non_prestarted_admission = Some(next_admission);
                        let s = Arc::clone(shared);
                        let limit = Arc::clone(&limit);
                        handles.push(spawn_tool_task(shared, async move {
                            if let Some(prior_admission) = prior_admission {
                                let _ = prior_admission.await;
                            }
                            #[cfg(test)]
                            test_parallel_admission_delay(&call).await;
                            let _permit = limit.acquire_owned().await.unwrap();
                            // Publish the start BEFORE releasing the next
                            // sibling: the admission chain must also order the
                            // started/lifecycle records, or a scheduler-skewed
                            // sibling can publish its start first and break
                            // the model-order start contract.
                            push_tool_call_started(&s, &call).await?;
                            let _ = admitted.send(());
                            execute_tool_call_after_started(s, call).await
                        })?);
                    }
                }
            }
            drop(handoff);
            let mut pending = FuturesUnordered::new();
            for (index, handle) in handles.into_iter().enumerate() {
                pending.push(async move { (index, handle.await) });
            }
            let mut group_results = std::iter::repeat_with(|| None)
                .take(group.len())
                .collect::<Vec<Option<ToolResultMessage>>>();
            while let Some((index, joined)) = pending.next().await {
                match resolve_tool_task(joined) {
                    Ok(result) => group_results[index] = Some(result),
                    Err(error) => return Err(error),
                }
            }
            for (call, r) in group.iter().zip(group_results) {
                handled.insert(call.visible.id.clone());
                results.push(r.expect("every parallel tool task must produce one result"));
            }
        }
    }

    // Synthesize tool_result for any tool_use left unhandled (abort).
    for call in calls {
        if handled.contains(&call.visible.id) {
            continue;
        }
        results.push(interrupted_tool_result(shared, call).await);
    }

    let profile = &calls
        .first()
        .expect("dispatch_tool_calls requires at least one resolved call")
        .profile;
    let spill_dir = path_join(
        query_path_dialect(),
        &[
            (shared.params.tool_context_base)().cwd,
            RuntimePath::from(".klaus"),
            RuntimePath::from("tool-results"),
        ],
    );
    let read_ids: HashSet<_> = calls
        .iter()
        .filter(|call| call.internal_name == "read")
        .map(|call| call.visible.id.clone())
        .collect();
    if !shared.config().aggregate_tool_results {
        return Ok(results);
    }
    Ok(persist_per_message_budget(
        results,
        shared.config().max_tool_results_per_message_chars as usize,
        spill_dir.clone(),
        |result| {
            // The per-item replacement already points at the full artifact.
            // Keep that stable reference instead of spilling its preview again.
            let path = path_join(
                query_path_dialect(),
                &[
                    spill_dir.clone(),
                    RuntimePath::from_runtime_string(result.tool_use_id.concat(&".txt".into())),
                ],
            );
            let rendered_path = crate::fs::node_path::posix_rendered_runtime(&path);
            let path_units = rendered_path.as_runtime_string().as_utf16();
            read_ids.contains(&result.tool_use_id)
                || output_blocks_text(&result.content)
                    .as_utf16()
                    .windows(path_units.len())
                    .any(|window| window == path_units)
        },
        |preview, full, path| render_spilled_tool_result(preview, full, path, profile),
    )
    .await)
}

async fn interrupted_tool_result(
    shared: &Arc<Shared>,
    call: &ResolvedToolCall,
) -> ToolResultMessage {
    let persistence = record_tool_lifecycle(
        shared,
        call,
        ToolLifecyclePhase::Cancelled,
        Some(call.initial_internal_input.clone()),
        Some(true),
        Some("Tool execution was interrupted.".into()),
    )
    .await;
    if let Err(error) = persistence {
        report_tool_persistence_failure(shared, &error);
    }
    ToolResultMessage {
        uuid: (shared.deps.uuid)().into(),
        tool_use_id: call.visible.id.clone(),
        content: render_interrupted_tool_result_content(&call.visible.name, &call.profile),
        is_error: true,
        tool_kind: call.visible.tool_kind,
        native_tool_output: None,
        output: None,
        extra_user_blocks: None,
    }
}

/// Deterministically model a spawned sibling reaching the limiter late. The
/// fixed implementation must preserve model-order admission even under this
/// scheduler skew; production builds contain no test delay.
#[cfg(test)]
async fn test_parallel_admission_delay(call: &ResolvedToolCall) {
    if let Some(delay_ms) = call
        .initial_internal_input
        .get("test_admission_delay_ms")
        .and_then(Json::as_u64)
    {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

async fn await_prestarted_tool_call(handle: JoinHandle<ToolCallResult>) -> ToolCallResult {
    resolve_tool_task(handle.await)
}

fn resolve_tool_task(joined: Result<ToolCallResult, tokio::task::JoinError>) -> ToolCallResult {
    match joined {
        Ok(result) => result,
        Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
        // A non-panic JoinError means the task was cancelled (runtime
        // shutdown). Surface it as a fatal tool error so the turn halts
        // cleanly instead of aborting the whole process.
        Err(err) => Err(ToolError::fatal(format!("tool task was cancelled: {err}"))),
    }
}

pub(super) fn safe_is_read_only(tool: &dyn Tool, input: &Json) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tool.is_read_only(input)))
        .unwrap_or(false)
}

pub(super) fn safe_is_concurrency_safe(tool: &dyn Tool, input: &Json) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tool.is_concurrency_safe(input)
    }))
    .unwrap_or(false)
}

/// Group into maximal runs of concurrency-safe tools; unsafe tools stand alone.
fn group_by_concurrency(
    calls: &[ResolvedToolCall],
    tools: &[Arc<dyn Tool>],
) -> Vec<Vec<ResolvedToolCall>> {
    let mut groups: Vec<Vec<ResolvedToolCall>> = Vec::new();
    let mut current: Vec<ResolvedToolCall> = Vec::new();
    for call in calls {
        let safe = find_runtime_tool(tools, &call.internal_name)
            .map(|tool| safe_is_concurrency_safe(tool.as_ref(), &call.initial_internal_input))
            .unwrap_or(false);
        if safe {
            current.push(call.clone());
        } else {
            if !current.is_empty() {
                groups.push(std::mem::take(&mut current));
            }
            groups.push(vec![call.clone()]);
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

/// Returns a boxed `Send` future so the (indirect) recursion through
/// `ToolContext::invoke_tool` type-checks — a bare `async fn` cannot prove its
/// own future `Send` while it recurses into itself.
fn execute_tool_call(
    shared: Arc<Shared>,
    call: ResolvedToolCall,
) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send>> {
    Box::pin(async move {
        let handle = spawn_tool_task(&shared, execute_tool_call_inner(shared.clone(), call, true))?;
        await_prestarted_tool_call(handle).await
    })
}

pub(super) fn execute_tool_call_after_started(
    shared: Arc<Shared>,
    call: ResolvedToolCall,
) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send>> {
    execute_tool_call_inner(shared, call, false)
}

fn retained_tool_tasks(shared: &Arc<Shared>) -> &Arc<super::RetainedToolTasks> {
    shared
        .params
        .retained_tool_tasks
        .as_ref()
        .expect("query owns retained tool tasks")
}

pub(super) fn spawn_tool_task<F>(
    shared: &Arc<Shared>,
    future: F,
) -> Result<JoinHandle<ToolCallResult>, ToolError>
where
    F: Future<Output = ToolCallResult> + Send + 'static,
{
    retained_tool_tasks(shared).spawn(future)
}

struct FinishApprovalAction {
    reviewer: Option<Arc<dyn ApprovalReviewer>>,
    id: RuntimeString,
}
impl Drop for FinishApprovalAction {
    fn drop(&mut self) {
        if let Some(reviewer) = &self.reviewer {
            reviewer.finish_action(&self.id);
        }
    }
}

fn execute_tool_call_inner(
    shared: Arc<Shared>,
    call: ResolvedToolCall,
    emit_started: bool,
) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send>> {
    Box::pin(async move {
        let signal = shared.params.signal.clone();
        if signal.is_cancelled() {
            return Ok(interrupted_tool_result(&shared, &call).await);
        }
        let persistence = Arc::new(super::tool_tasks::ToolPersistence::default());
        let mut work = Box::pin(persistence.scope(execute_tool_call_body(
            Arc::clone(&shared),
            call.clone(),
            emit_started,
            persistence.clone(),
        )));
        let result = tokio::select! {
            biased;
            result = &mut work => result,
            _ = signal.cancelled() => {
                // Give cooperative tools time to release their owned resources.
                // A stalled hook or host callback cannot keep the foreground turn
                // occupied indefinitely after cancellation.
                match tokio::time::timeout(Duration::from_millis(100), &mut work).await {
                    Ok(result) => result,
                    Err(_) => tokio::select! {
                        biased;
                        result = &mut work => result,
                        () = persistence.drain() => {
                            drop(work);
                            Ok(interrupted_tool_result(&shared, &call).await)
                        }
                    }
                }
            }
        };
        // An asynchronous producer may finish its callback after the tool
        // returns. Its accepted write remains part of this execution's fence.
        persistence.drain().await;
        result
    })
}

fn execute_tool_call_body(
    shared: Arc<Shared>,
    mut call: ResolvedToolCall,
    emit_started: bool,
    persistence: Arc<super::tool_tasks::ToolPersistence>,
) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send>> {
    Box::pin(async move {
        let _finish_approval = FinishApprovalAction {
            reviewer: shared.params.approval_reviewer.clone(),
            id: call.visible.id.clone(),
        };
        let arguments_json = call.initial_internal_input.clone();
        let tools = shared.tools.lock_recover().clone();
        let tool = find_runtime_tool(&tools, &call.internal_name);
        if !tool_call_authorized(
            &shared,
            &call,
            tool.as_deref(),
            &call.initial_internal_input,
        ) {
            return Ok(error_result(
                &call,
                &shared,
                "The selected agent definition does not permit this tool".into(),
                &[],
            )
            .await);
        }
        if emit_started {
            push_tool_call_started(&shared, &call).await?;
        }
        let Some(tool) = tool else {
            return Ok(error_result(
                &call,
                &shared,
                unknown_tool_message(&call.internal_name),
                &[],
            )
            .await);
        };

        // The dispatcher parses the provider-normalized input before
        // validation, hooks, or permissions. Keep the initial input immutable
        // for lifecycle/model-history arguments, while every policy/execution
        // consumer receives the parsed effective value.
        let input = match tool.parse_input(&call.initial_internal_input) {
            Ok(input) => input,
            Err(error) if error.kind == ToolErrorKind::Fatal => return Err(error),
            Err(error) => return Ok(error_result(&call, &shared, error.message, &[]).await),
        };
        call.replace_effective_input(input.clone());
        let context_effective_input = Arc::new(Mutex::new(call.effective_internal_input.clone()));

        // Build the per-call tool context from a fresh base, wiring the per-call
        // seams and merging any turn-scoped skill grants.
        let persistence_failure: Arc<Mutex<Option<ToolError>>> = Arc::new(Mutex::new(None));
        let mut ctx = (shared.params.tool_context_base)();
        let mut permission_stamp =
            RuntimePermissionStamp::capture(&ctx, shared.params.permission_engine.as_ref());
        ctx.web_search_access = Some(shared.config().web_search.access);
        if call.internal_name == "advisor" {
            if let Some(advisor) = ctx.advisor.as_mut() {
                advisor.request = shared.advisor_request.lock_recover().clone();
                if let Some(request) = &mut advisor.request {
                    // The question is appended separately. Keep the prefix before this
                    // invocation reusable when its result appears in later history.
                    for message in &mut request.messages {
                        if let Message::Assistant(assistant) = message {
                            assistant.content.retain(|block| {
                                !matches!(block,
                                crate::schemas::content::ContentBlock::ToolUse(tool)
                                    if tool.id == call.visible.id)
                            });
                        }
                    }
                }
            }
        }
        ctx.signal = shared.params.signal.clone();
        let (effective_effort, effective_ultra_mode) = {
            let context = shared.turn_context.lock_recover();
            (context.effort.clone(), context.ultra_mode)
        };
        super::orchestration::inherit_parent_effort(
            &mut ctx,
            effective_effort,
            effective_ultra_mode,
        );
        super::orchestration::inherit_parent_context(
            &mut ctx,
            shared.step_subagent_context.lock_recover().clone(),
        );
        if let Some(question) = &mut ctx.async_question {
            question.tool_call_id = call.visible.id.clone();
            let q = shared.q.clone();
            question.emit = Some(Arc::new(move |event| q.push(event)));
        }
        if let Some(bundle) = shared.bundle() {
            let model = shared.step_model.lock_recover().clone();
            let capabilities = bundle.provider.capabilities(&model);
            ctx.vision = Some(capabilities.vision);
            ctx.native_pdf = Some(capabilities.native_pdf);
        }
        {
            let granted: Vec<RuntimeString> = {
                let tc = shared.turn_context.lock_recover();
                tc.granted_tools.iter().cloned().collect()
            };
            if !granted.is_empty() {
                let mut extra = parse_rule_strings(&granted);
                ctx.permission.allow_rules.append(&mut extra);
            }
        }
        {
            let s = Arc::clone(&shared);
            let q = Arc::clone(&shared.q);
            let id = call.visible.id.clone();
            let tool_name = call.internal_name.clone();
            let turn_state = shared.turn_state.clone();
            let failure = persistence_failure.clone();
            let persistence = persistence.clone();
            ctx.emit = std::sync::Arc::new(move |event| {
                let admission = persistence.enter();
                let s = s.clone();
                let q = q.clone();
                let id = id.clone();
                let tool_name = tool_name.clone();
                let turn_state = turn_state.clone();
                let failure = failure.clone();
                Box::pin(async move {
                    let Ok(_admission) = admission else {
                        return;
                    };
                    match record_tool_lifecycle_progress(&s, &id, &tool_name, &event).await {
                        Ok(()) => q.push(turn_state.tool_event(id, event)),
                        Err(error) => note_tool_persistence_failure(&s, &failure, error),
                    }
                })
            });
        }
        {
            let s = Arc::clone(&shared);
            let call_owned = call.clone();
            let current_input = Arc::clone(&context_effective_input);
            let approval_stamp = permission_stamp.clone();
            ctx.request_approval = Some(Arc::new(move |message: RuntimeString| {
                let s = Arc::clone(&s);
                let call_owned = call_owned.clone();
                let input = current_input.lock_recover().clone();
                let approval_stamp = approval_stamp.clone();
                Box::pin(async move {
                    if approval_stamp
                        != RuntimePermissionStamp::capture(
                            &(s.params.tool_context_base)(),
                            s.params.permission_engine.as_ref(),
                        )
                    {
                        notify(&s.q, NotificationLevel::Info, "Permissions changed while this action was running; retry it under the current policy.");
                        return false;
                    }
                    match request_approval(&s, message, input.clone(), &call_owned, None, true)
                        .await
                    {
                        Ok(approved) if approved.input == input => true,
                        Ok(_) => {
                            notify(&s.q, NotificationLevel::Info, "The approved command was edited; retry the revised command so its permissions can be checked before execution.");
                            false
                        }
                        Err(_) => false,
                    }
                })
            }));
        }
        if let Some(record) = ctx.record_rollout_event.take() {
            let s = shared.clone();
            let persistence = persistence.clone();
            ctx.record_rollout_event = Some(Arc::new(move |event, payload| {
                let admission = persistence.enter();
                let record = record.clone();
                let s = s.clone();
                Box::pin(async move {
                    let _admission = admission?;
                    let result = record(event, payload).await;
                    if let Err(error) = &result {
                        report_tool_persistence_failure(&s, error);
                    }
                    result
                })
            }));
        }
        {
            let s = Arc::clone(&shared);
            let call_owned = call.clone();
            ctx.request_network_access = Some(Arc::new(move |req: NetworkAccessRequest| {
                let s = Arc::clone(&s);
                let call_owned = call_owned.clone();
                Box::pin(async move { request_network_access(&s, req, &call_owned).await.is_ok() })
            }));
        }
        {
            let s = Arc::clone(&shared);
            let persistence = persistence.clone();
            ctx.surface_tools = Some(Box::new(move |names: Vec<RuntimeString>| {
                let admission = persistence.enter();
                let s = s.clone();
                Box::pin(async move {
                    let _admission = admission?;
                    if let Some(cb) = &s.params.on_surface_tools {
                        if let Err(error) = cb(names.clone()).await {
                            report_tool_persistence_failure(&s, &error);
                            return Err(error);
                        }
                    }
                    let mut sf = s.surfaced.lock_recover();
                    for n in names {
                        sf.insert(n);
                    }
                    Ok(())
                })
            }));
        }
        {
            let s = Arc::clone(&shared);
            let profile = call.profile.clone();
            ctx.tool_catalog = Some(Box::new(move || {
                let tools = s.tools.lock_recover().clone();
                build_tool_catalog(&tools, &profile)
            }));
        }
        {
            let s = Arc::clone(&shared);
            let on_applied_skill = ctx.apply_skill_context.take();
            ctx.apply_skill_context = Some(Box::new(move |mut m: SkillContextMod| {
                crate::schemas::effort::normalize_legacy_ultra(&mut m.effort, &mut m.ultra_mode);
                if let Some(on_applied_skill) = &on_applied_skill {
                    on_applied_skill(m.clone());
                }
                let config = s.config();
                let mut tc = s.turn_context.lock_recover();
                if let Some(model) = m.model {
                    tc.model = model.into();
                    tc.model_override = Some(tc.model.clone());
                }
                if let Some(effort) = m.effort {
                    tc.effort = Some(effort.clone());
                    tc.effort_override = Some(effort);
                    if m.ultra_mode.is_none() {
                        tc.ultra_override = None;
                    }
                }
                if let Some(enabled) = m.ultra_mode {
                    tc.ultra_mode = enabled;
                    tc.ultra_override = Some(enabled);
                }
                tc.effort = super::selected_effort(
                    config.effort.clone(),
                    config.ultra_mode,
                    tc.effort_override.clone(),
                    tc.ultra_override,
                );
                tc.ultra_mode = tc
                    .effort
                    .as_ref()
                    .is_some_and(crate::schemas::effort::EffortValue::is_ultra);
                if let Some(tools) = m.allow_tools {
                    for n in tools {
                        tc.granted_tools.insert(n.into());
                    }
                }
            }));
        }
        {
            let s = Arc::clone(&shared);
            let owner = call.internal_name.clone();
            let owner_id = call.visible.id.clone();
            let profile = call.profile.clone();
            ctx.invoke_tool = Some(Arc::new(move |name: RuntimeString, input: Json| {
                let s = Arc::clone(&s);
                let owner = owner.clone();
                let owner_id = owner_id.clone();
                let profile = profile.clone();
                Box::pin(async move {
                    if s.params.signal.is_cancelled() {
                        return Err(ToolError::new("Tool invocation cancelled before admission"));
                    }
                    if name == owner {
                        let name_host = invoked_tool_name_to_registry_host(&name);
                        return Err(ToolError::new(format!(
                            "Tool {name_host} cannot invoke itself"
                        )));
                    }
                    let tools = s.tools.lock_recover().clone();
                    match find_runtime_tool(&tools, &name) {
                        None => {
                            let name_host = invoked_tool_name_to_registry_host(&name);
                            Err(ToolError::new(format!("Unknown tool: {name_host}")))
                        }
                        Some(inner) => {
                            let inner_block = ToolUseBlock {
                                id: (s.deps.uuid)().into(),
                                name: inner.name(),
                                input,
                                tool_kind: None,
                                thought_signature: None,
                            };
                            let is_code = owner == "exec";
                            let mut inner_call = ResolvedToolCall::from_internal(
                                inner_block,
                                profile.clone(),
                                owner,
                            );
                            if is_code {
                                inner_call.approval_call_source =
                                    crate::types::permission::ApprovalActionCallSource::CodeMode {
                                        originating_tool_call_id: owner_id,
                                        runtime_tool_call_id: inner_call.visible.id.clone(),
                                    };
                            }
                            record_tool_lifecycle_parts(
                                &s,
                                ToolLifecycleRecordParts {
                                    tool_use_id: &inner_call.visible.id,
                                    tool_name: &inner_call.internal_name,
                                    phase: ToolLifecyclePhase::Requested,
                                    source: Some(tool_lifecycle_source_for_tool(inner.as_ref())),
                                    arguments: Some(inner_call.initial_internal_input.clone()),
                                    is_error: None,
                                    error: None,
                                },
                            )
                            .await?;
                            match execute_tool_call(Arc::clone(&s), inner_call).await {
                                Ok(res) => Ok(InvokeToolResult {
                                    is_error: res.is_error,
                                    output: res.output,
                                    content: res.content,
                                }),
                                Err(error) => Err(error),
                            }
                        }
                    }
                })
            }));
        }

        match tool.validate_input(&input, &ctx).await {
            Some(ValidationResult::Err { message, .. }) => {
                return Ok(error_result(&call, &shared, message.into(), &[]).await);
            }
            Some(ValidationResult::Fatal { error }) => return Err(error),
            Some(ValidationResult::Ok) | None => {}
        }

        // PreToolUse hook runs BEFORE the permission engine.
        let mut pre_approved = false;
        let mut hook_ask = false;
        let mut hook_ask_reason: Option<RuntimeString> = None;
        let mut hook_context: Vec<RuntimeString> = Vec::new();
        if let Some(bus) = &shared.params.hook_bus {
            if bus.has(HookEvent::PreToolUse) {
                let mut hinput =
                    base_hook_input(HookEvent::PreToolUse, &ctx.cwd, &shared.params.thread_id);
                hinput.tool_name = Some(call.internal_name.clone());
                hinput.tool_input = Some(call.effective_internal_input.clone());
                hinput.agent_id = shared.params.agent_id.clone();
                let pre = bus.dispatch(HookEvent::PreToolUse, &hinput).await;
                for m in &pre.system_messages {
                    notify(&shared.q, NotificationLevel::Info, m.clone());
                }
                if pre.aborted {
                    shared.turn_context.lock_recover().hook_aborted = true;
                    let msg = pre
                        .system_messages
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "Turn halted by PreToolUse hook".into());
                    return Ok(error_result(&call, &shared, msg, &[]).await);
                }
                if pre.blocked {
                    let msg = pre
                        .block_reason
                        .clone()
                        .unwrap_or_else(|| "Blocked by PreToolUse hook".into());
                    return Ok(error_result(&call, &shared, msg, &[]).await);
                }
                if let Some(updated) = pre.updated_input {
                    call.replace_effective_input(updated);
                }
                if !pre.additional_context.is_empty() {
                    hook_context.extend(pre.additional_context.iter().cloned());
                }
                pre_approved = pre.approved;
                hook_ask = pre.asked;
                hook_ask_reason = pre.ask_reason.clone();
            }
        }

        if tool.name() == "read" {
            if let (Some(goal), Some(identity)) = (&ctx.goal, &ctx.goal_generation) {
                let identity = identity
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                ctx.goal_owned_resources =
                    goal.owned_read_resources(&identity)
                        .await
                        .map_err(|error| {
                            ToolError::new(format!(
                                "goal file capability could not be validated: {error}"
                            ))
                        })?;
            }
        }
        if let Some(reviewer) = &shared.params.approval_reviewer {
            if let Ok(mut metadata) = crate::engine::approval_action::metadata_for_tool(
                tool.as_ref(),
                &call.effective_internal_input,
                &ctx,
            ) {
                metadata.originating_item_id = Some(call.visible.id.clone());
                metadata.call_source = call.approval_call_source.clone();
                reviewer.observe_action_with_metadata(
                    ApprovalRequest {
                        id: call.visible.id.clone(),
                        kind: ApprovalKind::Tool,
                        tool_name: call.internal_name.clone(),
                        input: call.effective_internal_input.clone(),
                        message: RuntimeString::default(),
                        suggestions: None,
                    },
                    metadata,
                );
            }
        }
        // Permission engine.
        let ignore_prefix_rules = shared
            .params
            .approval_reviewer
            .as_ref()
            .is_some_and(|reviewer| reviewer.ignore_allow_prefix_rules());
        let decision = if ignore_prefix_rules {
            shared
                .params
                .permission_engine
                .evaluate_without_allow_prefix_rules(
                    tool.as_ref(),
                    &call.effective_internal_input,
                    &ctx,
                )
                .await
        } else {
            shared
                .params
                .permission_engine
                .evaluate(tool.as_ref(), &call.effective_internal_input, &ctx)
                .await
        };
        let decision = crate::permission::planning::intersect(tool.as_ref(), &ctx, decision).await;
        let mut approved_file_effect = false;
        match decision {
            PermissionDecision::Fatal { error } => return Err(error),
            PermissionDecision::Deny { message, .. } => {
                return Ok(
                    error_result(&call, &shared, permission_denial_message(&message), &[]).await,
                );
            }
            PermissionDecision::Ask {
                message,
                input: decided_input,
                reason,
                suggestions,
            } => {
                // Explicit rules, security checks and planning restrictions
                // require a decision from the configured approval flow. A hook
                // approval cannot satisfy those independent gates.
                let ask_wins_over_approval = matches!(
                    reason,
                    PermissionReason::Rule { .. }
                        | PermissionReason::SafetyCheck
                        | PermissionReason::Mode { .. }
                );
                if pre_approved && !ask_wins_over_approval && !hook_ask {
                    call.replace_effective_input(decided_input);
                } else {
                    match request_approval(
                        &shared,
                        message.into(),
                        decided_input,
                        &call,
                        suggestions,
                        !ask_wins_over_approval && !hook_ask,
                    )
                    .await
                    {
                        Ok(approved_input) => {
                            approved_file_effect = true;
                            permission_stamp = approved_input.policy;
                            call.replace_effective_input(approved_input.input);
                        }
                        Err(msg) => return Ok(error_result(&call, &shared, msg, &[]).await),
                    }
                }
            }
            PermissionDecision::Allow {
                input: allowed_input,
                ..
            } => {
                if hook_ask {
                    let msg = hook_ask_reason.clone().unwrap_or_else(|| {
                        "A PreToolUse hook requested confirmation for this tool.".into()
                    });
                    match request_approval(&shared, msg, allowed_input, &call, None, false).await {
                        Ok(approved_input) => {
                            permission_stamp = approved_input.policy;
                            call.replace_effective_input(approved_input.input);
                        }
                        Err(m) => return Ok(error_result(&call, &shared, m, &[]).await),
                    }
                } else {
                    call.replace_effective_input(allowed_input);
                }
            }
        }

        // Neither a hook nor a human approval can widen this query's captured
        // effect ceiling by replacing a read or plan-document argument.
        match crate::permission::planning::enforce(
            tool.as_ref(),
            &call.effective_internal_input,
            &ctx,
        )
        .await
        {
            Ok(input) => call.replace_effective_input(input),
            Err(PermissionDecision::Deny { message, .. }) => {
                return Ok(
                    error_result(&call, &shared, permission_denial_message(&message), &[]).await,
                )
            }
            Err(PermissionDecision::Fatal { error }) => return Err(error),
            Err(_) => unreachable!("planning effects never request approval"),
        }
        *context_effective_input.lock_recover() = call.effective_internal_input.clone();

        // Approval can change arguments, but never the task's hard resource
        // boundary. Check the exact input that is about to reach the tool.
        if let Err(message) = shared
            .params
            .permission_engine
            .check_task_resource_scope(tool.as_ref(), &call.effective_internal_input, &ctx)
            .await
        {
            return Ok(
                error_result(&call, &shared, permission_denial_message(&message), &[]).await,
            );
        }

        if approved_file_effect
            && ctx.sandbox_mode.is_some()
            && tool.permission_path_kind() == Some(crate::types::tool::PermissionPathKind::Edit)
        {
            // An approval grants only this call's declared targets. The stored
            // workspace roots and future calls retain their original ceiling.
            let targets = match tool.get_path(&call.effective_internal_input) {
                Some(crate::types::tool::PathTargets::One(path)) => vec![path],
                Some(crate::types::tool::PathTargets::Many(paths)) => paths,
                None => Vec::new(),
            };
            for path in targets {
                let path = crate::fs::node_path::resolve_runtime(&ctx.cwd, &[path])
                    .map_err(|error| ToolError::fatal(format!("{error:?}")))?;
                if !ctx.permission.additional_writable_roots.contains(&path) {
                    ctx.permission.additional_writable_roots.push(path);
                }
            }
        }
        let goal_read = crate::permission::engine::goal_owned_read_allowed(
            tool.as_ref(),
            &call.effective_internal_input,
            &ctx,
        )
        .await;
        if let Some(scope) = shared
            .params
            .permission_engine
            .bound_task_resource_scope()
            .filter(|_| !goal_read)
        {
            match scope.prepare_file_tool(tool.as_ref(), &call.effective_internal_input, &mut ctx) {
                Ok(Some(updated)) => call.replace_effective_input(updated),
                Ok(None) => {}
                Err(message) => {
                    return Ok(error_result(
                        &call,
                        &shared,
                        permission_denial_message(&message),
                        &[],
                    )
                    .await)
                }
            }
        }

        let _goal_read_guard = if goal_read {
            if let (Some(goal), Some(identity), Some(resources)) =
                (&ctx.goal, &ctx.goal_generation, &ctx.goal_owned_resources)
            {
                let identity = identity
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                Some(
                    goal.guard_owned_read(&identity, resources)
                        .await
                        .map_err(|error| ToolError::new(error.to_string()))?,
                )
            } else {
                None
            }
        } else {
            None
        };
        let progress_q = Arc::clone(&shared.q);
        let progress_id = call.visible.id.clone();
        let progress_shared = Arc::clone(&shared);
        let progress_tool_name = call.internal_name.clone();
        let progress_turn_state = shared.turn_state.clone();
        let progress_failure = persistence_failure.clone();
        let progress_persistence = persistence.clone();
        let on_progress: ToolProgress = Box::new(move |event| {
            let admission = progress_persistence.enter();
            let shared = progress_shared.clone();
            let id = progress_id.clone();
            let tool_name = progress_tool_name.clone();
            let q = progress_q.clone();
            let turn_state = progress_turn_state.clone();
            let failure = progress_failure.clone();
            Box::pin(async move {
                let Ok(_admission) = admission else {
                    return;
                };
                match record_tool_lifecycle_progress(&shared, &id, &tool_name, &event).await {
                    Ok(()) => q.push(turn_state.tool_event(id, event)),
                    Err(error) => note_tool_persistence_failure(&shared, &failure, error),
                }
            })
        });

        // Hooks and permission approvals can replace the input. Rebind the final
        // request to the same trusted caller immediately before execution.
        if !tool_call_authorized(
            &shared,
            &call,
            Some(tool.as_ref()),
            &call.effective_internal_input,
        ) {
            return Ok(error_result(
                &call,
                &shared,
                "The selected agent definition does not permit this tool".into(),
                &hook_context,
            )
            .await);
        }
        if permission_stamp
            != RuntimePermissionStamp::capture(
                &(shared.params.tool_context_base)(),
                shared.params.permission_engine.as_ref(),
            )
        {
            return Ok(error_result(
                &call,
                &shared,
                "Permissions changed before execution; retry this action under the current policy."
                    .into(),
                &hook_context,
            )
            .await);
        }
        let tool_outcome = tool
            .call_with_id(
                &call.visible.id,
                call.effective_internal_input.clone(),
                &ctx,
                Some(on_progress),
            )
            .await;
        if !shared.params.signal.is_cancelled() {
            if let Some(goal) = &shared.params.goal {
                let succeeded = tool_outcome
                    .as_ref()
                    .is_ok_and(|result| !result.serializable.is_error.unwrap_or(false));
                let execution_tool = call.internal_name == "exec";
                goal.note_tool_outcome(execution_tool && tool_outcome.is_err(), succeeded);
            }
        }
        match tool_outcome {
            Ok(result) => {
                let mut is_error = result.serializable.is_error.unwrap_or(false);
                let mut reported_error =
                    is_error.then(|| tool_reported_error_detail(&result.serializable));
                let mut persistence_error = persistence_failure.lock_recover().take();
                if let Some(error) = &persistence_error {
                    is_error = true;
                    reported_error = Some(partial_tool_persistence_error(error));
                }
                // Preserve native search output before hooks, spill handling, or
                // any later provider-aware text rendering can alter model content.
                let (result_tool_kind, native_tool_output) =
                    successful_result_provider_metadata(&call, &result.serializable.output);
                let terminal_persistence = record_tool_lifecycle(
                    &shared,
                    &call,
                    if is_error {
                        // A tool the user's stop killed did not fail on its own
                        // merits. Recorded as `failed` it is indistinguishable
                        // from a real failure, and a reopened thread then shows
                        // the stop as a red error the user never caused.
                        tool_failure_phase(&shared)
                    } else {
                        ToolLifecyclePhase::Completed
                    },
                    is_error.then(|| call.initial_internal_input.clone()),
                    Some(is_error),
                    reported_error.clone(),
                )
                .await;
                if let Err(error) = terminal_persistence {
                    report_tool_persistence_failure(&shared, &error);
                    reported_error = Some(partial_tool_persistence_error(&error));
                    persistence_error = Some(error);
                    is_error = true;
                }
                shared.q.push(if is_error {
                    EngineEvent::ItemUpdated {
                        inference: None,
                        item: ThreadItem::ToolCall {
                            id: call.visible.id.clone(),
                            tool_name: call.internal_name.clone(),
                            arguments: arguments_json.clone(),
                            result: Some(to_json_safe(&result.serializable.output)),
                            error: Some(ItemError {
                                message: reported_error
                                    .clone()
                                    .expect("is_error has an extracted detail"),
                            }),
                            status: ItemStatus::Failed,
                        },
                    }
                } else {
                    EngineEvent::ItemCompleted {
                        inference: None,
                        item: ThreadItem::ToolCall {
                            id: call.visible.id.clone(),
                            tool_name: call.internal_name.clone(),
                            arguments: arguments_json.clone(),
                            result: Some(to_json_safe(&result.serializable.output)),
                            error: None,
                            status: ItemStatus::Completed,
                        },
                    }
                });

                // PostToolUse hook — may REPLACE the model-visible output, append
                // context, or halt the turn.
                let mut model_content: Vec<OutputBlock> = result.serializable.model_content.clone();
                if let Some(bus) = &shared.params.hook_bus {
                    if bus.has(HookEvent::PostToolUse) {
                        let mut hinput = base_hook_input(
                            HookEvent::PostToolUse,
                            &ctx.cwd,
                            &shared.params.thread_id,
                        );
                        hinput.tool_name = Some(call.internal_name.clone());
                        hinput.tool_input = Some(call.effective_internal_input.clone());
                        hinput.tool_result = Some(to_json_safe(&result.serializable.output));
                        hinput.agent_id = shared.params.agent_id.clone();
                        let post = bus.dispatch(HookEvent::PostToolUse, &hinput).await;
                        for m in &post.system_messages {
                            notify(&shared.q, NotificationLevel::Info, m.clone());
                        }
                        if let Some(updated) = &post.updated_tool_output {
                            model_content = match updated {
                                Json::String(s) => vec![text_out_exact(s.clone())],
                                other => vec![text_out_exact(safe_json_text(other))],
                            };
                        }
                        if !post.additional_context.is_empty() {
                            hook_context.extend(post.additional_context.iter().cloned());
                        }
                        if post.aborted {
                            shared.turn_context.lock_recover().hook_aborted = true;
                        }
                    }
                }
                if !hook_context.is_empty() {
                    model_content.push(OutputBlock::Text(TextBlock {
                        text: join_runtime_strings(&hook_context, "\n"),
                        citations: None,
                        harness_envelope: Some(HarnessEnvelope::SystemReminder),
                        message_id: None,
                        phase: None,
                    }));
                }

                // Level 1 — spill an oversized single result to disk, hand back a
                // preview.
                let max_chars = tool_result_spill_limit(tool.as_ref(), &call.internal_name);
                let spill_dir = path_join(
                    query_path_dialect(),
                    &[
                        ctx.cwd.clone(),
                        RuntimePath::from(".klaus"),
                        RuntimePath::from("tool-results"),
                    ],
                );
                let spill = maybe_spill_result(
                    model_content,
                    SpillOptions {
                        max_chars,
                        dir: spill_dir,
                        id: call.visible.id.clone(),
                    },
                    |preview, full, path| {
                        render_spilled_tool_result(preview, full, path, &call.profile)
                    },
                )
                .await;
                // The next captured model owns image policy. Persist the original
                // output so switching models cannot inherit an earlier resize.
                let mut model_content = spill.content;
                if let Some(error) = &persistence_error {
                    model_content.push(text_out_exact(partial_tool_persistence_error(error)));
                }
                let content = if is_error {
                    render_failed_tool_result_content(
                        model_content,
                        &call.visible.name,
                        &call.profile,
                    )
                } else {
                    render_successful_tool_result_content(
                        model_content,
                        &call.visible.name,
                        &call.profile,
                    )
                };

                let extra_user_blocks = result
                    .serializable
                    .extra_user_blocks
                    .clone()
                    .filter(|b| !b.is_empty());
                Ok(ToolResultMessage {
                    uuid: (shared.deps.uuid)().into(),
                    tool_use_id: call.visible.id.clone(),
                    content,
                    is_error,
                    tool_kind: result_tool_kind,
                    native_tool_output,
                    output: Some(result.serializable.output.clone()),
                    extra_user_blocks,
                })
            }
            Err(err) => {
                let message = err.message.clone();
                // The tool RAN and THREW — fire PostToolUseFailure.
                let mut failure_context = hook_context.clone();
                if let Some(bus) = &shared.params.hook_bus {
                    if bus.has(HookEvent::PostToolUseFailure) {
                        let mut hinput = base_hook_input(
                            HookEvent::PostToolUseFailure,
                            &ctx.cwd,
                            &shared.params.thread_id,
                        );
                        hinput.tool_name = Some(call.internal_name.clone());
                        hinput.tool_input = Some(call.effective_internal_input.clone());
                        hinput.error = Some(message.clone());
                        // A user interrupt surfaces as the turn's abort signal being
                        // cancelled when the tool threw.
                        hinput.is_interrupt = Some(shared.params.signal.is_cancelled());
                        hinput.agent_id = shared.params.agent_id.clone();
                        let post = bus.dispatch(HookEvent::PostToolUseFailure, &hinput).await;
                        for m in &post.system_messages {
                            notify(&shared.q, NotificationLevel::Info, m.clone());
                        }
                        if !post.additional_context.is_empty() {
                            failure_context.extend(post.additional_context.iter().cloned());
                        }
                    }
                }
                if err.kind == ToolErrorKind::Fatal {
                    return Err(err);
                }
                Ok(error_result(&call, &shared, message, &failure_context).await)
            }
        }
    })
}

fn tool_call_authorized(
    shared: &Arc<Shared>,
    call: &ResolvedToolCall,
    tool: Option<&dyn Tool>,
    input: &Json,
) -> bool {
    let dependency = tool.and_then(|tool| {
        tool.browser_dependency_authorization(call.trusted_caller.as_ref(), input)
    });
    if dependency == Some(false) {
        return false;
    }
    shared
        .params
        .agent_tool_policy
        .as_ref()
        .is_none_or(|policy| {
            policy.allows_tool(&call.internal_name)
                || (dependency == Some(true)
                    && call
                        .trusted_caller
                        .as_ref()
                        .is_some_and(|caller| policy.allows_tool(caller)))
        })
}

pub(super) async fn push_tool_call_started(
    shared: &Arc<Shared>,
    call: &ResolvedToolCall,
) -> Result<(), ToolError> {
    let tools = shared.tools.lock_recover().clone();
    let tool = find_runtime_tool(&tools, &call.internal_name);
    if !tool_call_authorized(shared, call, tool.as_deref(), &call.initial_internal_input) {
        return Ok(());
    }
    record_tool_lifecycle(
        shared,
        call,
        ToolLifecyclePhase::Started,
        Some(call.initial_internal_input.clone()),
        None,
        None,
    )
    .await?;
    shared.q.push(EngineEvent::ItemStarted {
        inference: None,
        item: ThreadItem::ToolCall {
            id: call.visible.id.clone(),
            tool_name: call.internal_name.clone(),
            arguments: call.initial_internal_input.clone(),
            result: None,
            error: None,
            status: ItemStatus::InProgress,
        },
    });
    Ok(())
}

fn tool_lifecycle_source_from_pool(
    shared: &Arc<Shared>,
    tool_name: &RuntimeString,
) -> Option<ToolLifecycleSource> {
    let tools = shared.tools.lock_recover().clone();
    find_runtime_tool(&tools, tool_name).map(|tool| tool_lifecycle_source_for_tool(tool.as_ref()))
}

pub(super) fn tool_lifecycle_source_for_tool(tool: &dyn Tool) -> ToolLifecycleSource {
    let exposure = Some(tool_exposure_name(tool.exposure()).into());
    if let Some(server) = tool.mcp_server() {
        return ToolLifecycleSource {
            kind: ToolLifecycleSourceKind::Mcp,
            name: Some(server),
            exposure,
        };
    }

    ToolLifecycleSource {
        kind: tool_lifecycle_source_kind_for_name(&tool.name()),
        name: None,
        exposure,
    }
}

fn tool_exposure_name(exposure: ToolExposure) -> &'static str {
    match exposure {
        ToolExposure::Direct => "direct",
        ToolExposure::Deferred => "deferred",
        ToolExposure::DirectModelOnly => "direct_model_only",
        ToolExposure::Hidden => "hidden",
    }
}

fn tool_lifecycle_source_kind_for_name(name: &RuntimeString) -> ToolLifecycleSourceKind {
    if name == "workflow" {
        ToolLifecycleSourceKind::Workflow
    } else if matches!(
        name,
        value if value == "task"
            || value == "task_output"
            || value == "task_stop"
            || value == "TeamCreate"
            || value == "TeamDelete"
            || value == "ListPeers"
            || value == "SendMessage"
            || value == "WaitForTeammates"
    ) {
        ToolLifecycleSourceKind::Subagent
    } else if name == "exec" || name == "wait" {
        ToolLifecycleSourceKind::CodeMode
    } else if name == "bash"
        || name == "bash_output"
        || name == "kill_shell"
        || name == "exec_command"
        || name == "write_stdin"
    {
        ToolLifecycleSourceKind::Shell
    } else if name == "browser_navigate"
        || name == "browser_snapshot"
        || name == "browser_screenshot"
        || name == "browser_click"
        || name == "browser_type"
        || name == "browser_eval"
        || name == "browser_console"
        || name == "browser_network"
        || name == "browser_close"
    {
        ToolLifecycleSourceKind::Browser
    } else if [
        "apply_patch",
        "ask_user",
        "ask_user_async",
        "edit",
        "EnterWorktree",
        "ExitWorktree",
        "file_search",
        "get_mcp_prompt",
        "glob",
        "grep",
        "list_mcp_prompts",
        "list_mcp_resources",
        "ls",
        "lsp_definition",
        "lsp_diagnostics",
        "lsp_document_symbols",
        "lsp_hover",
        "lsp_implementation",
        "lsp_incoming_calls",
        "lsp_outgoing_calls",
        "lsp_prepare_call_hierarchy",
        "lsp_references",
        "lsp_workspace_symbol",
        "mcp_search",
        "multi_edit",
        "read",
        "read_mcp_resource",
        "request_mcp_install",
        "request_skill_install",
        "save_memory",
        "skill",
        "skill_search",
        "sleep",
        "TaskCreate",
        "TaskGet",
        "TaskList",
        "TaskUpdate",
        "TaskWrite",
        "think",
        "tool_search",
        "web_fetch",
        "web_search",
        "write",
    ]
    .iter()
    .any(|candidate| name == *candidate)
    {
        ToolLifecycleSourceKind::Builtin
    } else {
        ToolLifecycleSourceKind::Host
    }
}

async fn record_tool_lifecycle(
    shared: &Arc<Shared>,
    call: &ResolvedToolCall,
    phase: ToolLifecyclePhase,
    arguments: Option<Json>,
    is_error: Option<bool>,
    error: Option<RuntimeString>,
) -> Result<(), ToolError> {
    record_tool_lifecycle_parts(
        shared,
        ToolLifecycleRecordParts {
            tool_use_id: &call.visible.id,
            tool_name: &call.internal_name,
            phase,
            source: tool_lifecycle_source_from_pool(shared, &call.internal_name),
            arguments,
            is_error,
            error,
        },
    )
    .await
}

pub(super) struct ToolLifecycleRecordParts<'a> {
    pub(super) tool_use_id: &'a RuntimeString,
    pub(super) tool_name: &'a RuntimeString,
    pub(super) phase: ToolLifecyclePhase,
    pub(super) source: Option<ToolLifecycleSource>,
    pub(super) arguments: Option<Json>,
    pub(super) is_error: Option<bool>,
    pub(super) error: Option<RuntimeString>,
}

pub(super) async fn record_tool_lifecycle_parts(
    shared: &Arc<Shared>,
    parts: ToolLifecycleRecordParts<'_>,
) -> Result<(), ToolError> {
    if let Some(cb) = &shared.params.on_tool_lifecycle {
        let terminal = matches!(
            &parts.phase,
            ToolLifecyclePhase::Completed
                | ToolLifecyclePhase::Failed
                | ToolLifecyclePhase::Cancelled
        );
        let _persistence = super::tool_tasks::ToolPersistence::enter_current(terminal)?;
        cb(ToolLifecycleRecord {
            turn_id: shared.turn_state.turn_id.clone(),
            tool_use_id: parts.tool_use_id.clone(),
            tool_name: parts.tool_name.clone(),
            phase: parts.phase,
            source: parts.source,
            arguments: parts.arguments,
            approval_kind: None,
            event: None,
            is_error: parts.is_error,
            error: parts.error,
        })
        .await
        .inspect_err(|error| {
            report_tool_persistence_failure(shared, error);
        })?;
    }
    Ok(())
}

async fn record_tool_lifecycle_approval(
    shared: &Arc<Shared>,
    call: &ResolvedToolCall,
    approval_kind: ApprovalKind,
    arguments: Json,
) -> Result<(), ToolError> {
    if let Some(cb) = &shared.params.on_tool_lifecycle {
        let _persistence = super::tool_tasks::ToolPersistence::enter_current(false)?;
        cb(ToolLifecycleRecord {
            turn_id: shared.turn_state.turn_id.clone(),
            tool_use_id: call.visible.id.clone(),
            tool_name: call.internal_name.clone(),
            phase: ToolLifecyclePhase::Approved,
            source: tool_lifecycle_source_from_pool(shared, &call.internal_name),
            arguments: Some(arguments),
            approval_kind: Some(approval_kind),
            event: None,
            is_error: Some(false),
            error: None,
        })
        .await
        .inspect_err(|error| {
            report_tool_persistence_failure(shared, error);
        })?;
    }
    Ok(())
}

async fn record_tool_lifecycle_progress(
    shared: &Arc<Shared>,
    tool_use_id: &RuntimeString,
    tool_name: &RuntimeString,
    event: &ToolDomainEvent,
) -> Result<(), ToolError> {
    if let Some(cb) = &shared.params.on_tool_lifecycle {
        // The producer callback acquired its guard before queuing this work.
        cb(ToolLifecycleRecord {
            turn_id: shared.turn_state.turn_id.clone(),
            tool_use_id: tool_use_id.clone(),
            tool_name: tool_name.clone(),
            phase: ToolLifecyclePhase::Progress,
            source: tool_lifecycle_source_from_pool(shared, tool_name),
            arguments: None,
            approval_kind: None,
            event: runtime_codec::encode_value(event).ok(),
            is_error: None,
            error: None,
        })
        .await
        .inspect_err(|error| {
            report_tool_persistence_failure(shared, error);
        })?;
    }
    Ok(())
}

async fn request_approval(
    shared: &Arc<Shared>,
    message: RuntimeString,
    input: Json,
    call: &ResolvedToolCall,
    suggestions: Option<Vec<PermissionUpdate>>,
    automatic_eligible: bool,
) -> Result<ApprovedToolInput, RuntimeString> {
    let mut policy = RuntimePermissionStamp::capture(
        &(shared.params.tool_context_base)(),
        shared.params.permission_engine.as_ref(),
    );
    let request = ApprovalRequest {
        id: call.visible.id.clone(),
        kind: ApprovalKind::Tool,
        tool_name: call.internal_name.clone(),
        input: input.clone(),
        message,
        suggestions: suggestions.map(|mut updates| {
            if shared.params.approval_reviewer.as_ref().is_some_and(|reviewer| reviewer.ignore_allow_prefix_rules()) {
                for update in &mut updates {
                    if let PermissionUpdate::AddRules { behavior: crate::types::permission::PermissionBehavior::Allow, rules, .. } = update {
                        rules.retain(|rule| rule.argument_prefix.is_none());
                    }
                }
                updates.retain(|update| !matches!(update, PermissionUpdate::AddRules { rules, .. } if rules.is_empty()));
            }
            updates
        }).filter(|s| !s.is_empty()),
    };

    let resp = resolve_approval_request(shared, request, automatic_eligible).await?;
    match resp {
        ApprovalResponse::Deny { message } => {
            Err(message.unwrap_or_else(|| "Denied by user.".into()))
        }
        ApprovalResponse::Allow {
            updated_input,
            updated_permissions,
        } => {
            if let Some(mut updates) = updated_permissions {
                if shared
                    .params
                    .approval_reviewer
                    .as_ref()
                    .is_some_and(|reviewer| reviewer.ignore_allow_prefix_rules())
                {
                    for update in &mut updates {
                        if let PermissionUpdate::AddRules {
                            behavior: crate::types::permission::PermissionBehavior::Allow,
                            rules,
                            ..
                        } = update
                        {
                            rules.retain(|rule| rule.argument_prefix.is_none());
                        }
                    }
                    updates.retain(|update| !matches!(update, PermissionUpdate::AddRules { rules, .. } if rules.is_empty()));
                }
                if !updates.is_empty() {
                    if let Some(cb) = &shared.params.on_permission_update {
                        cb(updates.clone());
                        policy.apply_owned_updates(&updates)?;
                    }
                    for update in &updates {
                        let destination = match update {
                            PermissionUpdate::AddRules { destination, .. } => *destination,
                            PermissionUpdate::SetMode { destination, .. } => *destination,
                        };
                        if destination != crate::types::permission::PermissionDestination::Session {
                            shared.q.push(EngineEvent::PermissionUpdate {
                                destination,
                                update: update.clone(),
                            });
                        }
                    }
                }
            }
            let approved_input = updated_input
                .filter(|updated| !updated.is_null())
                .unwrap_or(input);
            record_tool_lifecycle_approval(
                shared,
                call,
                ApprovalKind::Tool,
                approved_input.clone(),
            )
            .await
            .map_err(|error| {
                report_tool_persistence_failure(shared, &error);
                error.message
            })?;
            if policy
                != RuntimePermissionStamp::capture(
                    &(shared.params.tool_context_base)(),
                    shared.params.permission_engine.as_ref(),
                )
            {
                return Err("Permissions changed before the approval could be applied; retry this action under the current policy.".into());
            }
            Ok(ApprovedToolInput {
                input: approved_input,
                policy,
            })
        }
    }
}

async fn request_network_access(
    shared: &Arc<Shared>,
    req: NetworkAccessRequest,
    call: &ResolvedToolCall,
) -> Result<(), RuntimeString> {
    // Plan research may fetch pages after the tool and URL safety checks.
    let context = (shared.params.tool_context_base)();
    if context.network_allowed == Some(true)
        || (context.planning_read_only && call.internal_name == "web_fetch")
    {
        return Ok(());
    }
    let message = concat_runtime_strings(&[
        "Allow network access to ".into(),
        req.protocol.clone(),
        "://".into(),
        req.host.clone(),
        ":".into(),
        req.port.to_string().into(),
        " for ".into(),
        call.internal_name.clone(),
        "?".into(),
    ]);
    let mut fields = RuntimeObject::new();
    fields.insert("id", RuntimeJson::String(req.id));
    fields.insert("url", RuntimeJson::String(req.url));
    fields.insert("host", RuntimeJson::String(req.host));
    fields.insert("protocol", RuntimeJson::String(req.protocol));
    fields.insert("port", RuntimeJson::from(req.port));
    fields.insert("toolName", RuntimeJson::String(req.tool_name));
    let input = RuntimeJson::Object(fields);
    let request_input = input.clone();
    let request = ApprovalRequest {
        id: call.visible.id.clone(),
        kind: ApprovalKind::Network,
        tool_name: call.internal_name.clone(),
        input,
        message,
        suggestions: None,
    };
    if let Some(reviewer) = &shared.params.approval_reviewer {
        let tools = shared.tools.lock_recover();
        if let Some(tool) = tools.iter().find(|tool| tool.name() == call.internal_name) {
            if let Ok(mut metadata) = crate::engine::approval_action::metadata_for_tool(
                tool.as_ref(),
                &call.effective_internal_input,
                &(shared.params.tool_context_base)(),
            ) {
                metadata.originating_item_id = Some(call.visible.id.clone());
                let mut trigger = RuntimeObject::new();
                trigger.insert("tool", RuntimeJson::String(call.internal_name.clone()));
                trigger.insert("arguments", call.effective_internal_input.clone());
                metadata.network_trigger = Some(RuntimeJson::Object(trigger));
                reviewer.set_action_metadata(&request.id, metadata);
            }
        }
    }
    match resolve_approval_request(shared, request, true).await? {
        ApprovalResponse::Allow { .. } => {
            record_tool_lifecycle_approval(shared, call, ApprovalKind::Network, request_input)
                .await
                .map_err(|error| {
                    report_tool_persistence_failure(shared, &error);
                    error.message
                })?;
            Ok(())
        }
        ApprovalResponse::Deny { message } => {
            Err(message.unwrap_or_else(|| "Denied by user.".into()))
        }
    }
}

async fn resolve_approval_request(
    shared: &Arc<Shared>,
    request: ApprovalRequest,
    automatic_eligible: bool,
) -> Result<ApprovalResponse, RuntimeString> {
    if shared.params.signal.is_cancelled() {
        return Err("Approval was cancelled.".into());
    }
    let context = (shared.params.tool_context_base)();
    if let Some(reviewer) = &shared.params.approval_reviewer {
        if request.kind != ApprovalKind::Network {
            let tools = shared.tools.lock_recover();
            if let Some(tool) = tools.iter().find(|tool| tool.name() == request.tool_name) {
                if let Ok(mut metadata) = crate::engine::approval_action::metadata_for_tool(
                    tool.as_ref(),
                    &request.input,
                    &context,
                ) {
                    metadata.originating_item_id = Some(request.id.clone());
                    reviewer.set_action_metadata(&request.id, metadata);
                }
            }
        }
    }
    let permission_stamp =
        RuntimePermissionStamp::capture(&context, shared.params.permission_engine.as_ref());
    if context.approval_policy == Some(crate::types::permission::ApprovalPolicy::Never) {
        return Err(
            "This action requires approval, but the current approval policy is never.".into(),
        );
    }
    let strict_review = shared
        .params
        .approval_reviewer
        .as_ref()
        .is_some_and(|reviewer| reviewer.requires_review());
    let automatic = automatic_eligible
        && (strict_review
            || (context
                .approval_policy
                .unwrap_or_default()
                .routes_to_automatic_review()
                && context.approvals_reviewer
                    == Some(crate::types::permission::ApprovalsReviewer::Automatic)));
    let (reviewed, review_notice) = review_approval_request(
        shared.params.approval_reviewer.as_deref(),
        request.clone(),
        shared.params.signal.clone(),
        shared.config().approval_timeout_ms,
        automatic,
    )
    .await?;
    if shared.params.signal.is_cancelled() {
        return Err("Approval was cancelled.".into());
    }
    if permission_stamp
        != RuntimePermissionStamp::capture(
            &(shared.params.tool_context_base)(),
            shared.params.permission_engine.as_ref(),
        )
    {
        return Err("Permissions changed during approval review; retry this action under the current policy.".into());
    }
    if let Some(message) = review_notice {
        notify(&shared.q, NotificationLevel::Info, message);
    }
    if let Some(response) = reviewed {
        return Ok(response);
    }

    shared.q.push(EngineEvent::ApprovalRequested {
        request: request.clone(),
    });

    let handler = match &shared.params.approval_handler {
        Some(h) => h,
        None => return Err("No approval handler available; denied by default.".into()),
    };
    let resp = match shared.config().approval_timeout_ms {
        None => Some(handler.approve(request, shared.params.signal.clone()).await),
        Some(ms) => tokio::time::timeout(
            Duration::from_millis(ms),
            handler.approve(request, shared.params.signal.clone()),
        )
        .await
        .ok(),
    };
    if permission_stamp
        != RuntimePermissionStamp::capture(
            &(shared.params.tool_context_base)(),
            shared.params.permission_engine.as_ref(),
        )
    {
        return Err("Permissions changed while awaiting approval; retry this action under the current policy.".into());
    }
    resp.ok_or_else(|| "Approval timed out; denied.".into())
}

#[cfg(test)]
pub(super) async fn review_approval_before_prompt(
    reviewer: Option<&dyn ApprovalReviewer>,
    request: ApprovalRequest,
    signal: crate::types::abort::AbortSignal,
    timeout_ms: Option<u64>,
) -> Result<(), RuntimeString> {
    review_approval_request(reviewer, request, signal, timeout_ms, true)
        .await
        .map(|_| ())
}

async fn invoke_approval_reviewer(
    reviewer: &dyn ApprovalReviewer,
    request: ApprovalRequest,
    signal: crate::types::abort::AbortSignal,
    timeout_ms: Option<u64>,
    automatic: bool,
) -> Result<ApprovalReviewResponse, RuntimeString> {
    let review = std::panic::AssertUnwindSafe(async {
        if automatic {
            reviewer.review(request, signal.clone()).await
        } else {
            reviewer.review_for_user(request, signal.clone()).await
        }
    })
    .catch_unwind();
    let bounded = async {
        match if automatic { None } else { timeout_ms } {
            None => review
                .await
                .map_err(|_| "Approval reviewer failed; denied."),
            Some(ms) => match tokio::time::timeout(Duration::from_millis(ms), review).await {
                Ok(result) => result.map_err(|_| "Approval reviewer failed; denied."),
                Err(_) => Err("Approval reviewer timed out; denied."),
            },
        }
    };
    let result = if automatic {
        // The private reviewer owns cancellation and terminal event delivery.
        // Dropping it here would abandon its cleanup and leave a started review open.
        bounded.await
    } else {
        tokio::select! {
            biased;
            _ = signal.cancelled() => return Err("Approval was cancelled.".into()),
            result = bounded => result,
        }
    };
    match result {
        Ok(response) => Ok(response),
        Err(message) => Err(message.into()),
    }
}

async fn review_approval_request(
    reviewer: Option<&dyn ApprovalReviewer>,
    request: ApprovalRequest,
    signal: crate::types::abort::AbortSignal,
    timeout_ms: Option<u64>,
    automatic: bool,
) -> Result<(Option<ApprovalResponse>, Option<RuntimeString>), RuntimeString> {
    let Some(reviewer) = reviewer else {
        return Ok((None, None));
    };
    // Safety callbacks only veto or require user confirmation. A positive
    // safety review never authorizes execution on the user's behalf.
    match invoke_approval_reviewer(reviewer, request.clone(), signal.clone(), timeout_ms, false)
        .await?
    {
        ApprovalReviewResponse::Allow | ApprovalReviewResponse::Approved { .. } => {}
        ApprovalReviewResponse::AskUser { message } => return Ok((None, message)),
        ApprovalReviewResponse::Deny { message } => {
            return Err(message.unwrap_or_else(|| "Denied by approval reviewer.".into()))
        }
        ApprovalReviewResponse::Unavailable { message } => {
            return Err(message.unwrap_or_else(|| "Approval reviewer unavailable; denied.".into()))
        }
        ApprovalReviewResponse::Error { message } => {
            return Err(message.unwrap_or_else(|| "Approval reviewer failed; denied.".into()))
        }
    }
    if !automatic || !reviewer.is_automatic() {
        return Ok((None, None));
    }
    match invoke_approval_reviewer(reviewer, request, signal, timeout_ms, true).await? {
        ApprovalReviewResponse::Approved { message } => Ok((
            Some(ApprovalResponse::Allow {
                updated_input: None,
                updated_permissions: None,
            }),
            message,
        )),
        ApprovalReviewResponse::Allow => Ok((None, None)),
        ApprovalReviewResponse::AskUser { message } => Ok((
            None,
            Some(message.unwrap_or_else(|| "Automatic approval needs user review.".into())),
        )),
        ApprovalReviewResponse::Deny { message } => {
            Err(message.unwrap_or_else(|| "Denied by approval reviewer.".into()))
        }
        ApprovalReviewResponse::Unavailable { message }
        | ApprovalReviewResponse::Error { message } => Err(message.unwrap_or_else(|| {
            "Automatic approval review failed; the action was not executed.".into()
        })),
    }
}

/// Which lifecycle phase an errored tool call belongs in.
///
/// A turn whose signal is already cancelled did not get a tool FAILURE: the stop
/// killed the call. Both outcomes carry `is_error` for the model, but only the
/// phase tells a reopened thread which one it was, so a stopped tool must not
/// come back as a red error.
fn report_tool_persistence_failure(shared: &Arc<Shared>, error: &ToolError) {
    retained_tool_tasks(shared).record_failure(error.message.clone());
    notify(&shared.q, NotificationLevel::Warn, error.message.clone());
    // Keep the returned result, but stop the next model step from reissuing an
    // action whose effects may already exist without a durable acknowledgement.
    shared.params.signal.cancel();
}

fn note_tool_persistence_failure(
    shared: &Arc<Shared>,
    failure: &Arc<Mutex<Option<ToolError>>>,
    error: ToolError,
) {
    report_tool_persistence_failure(shared, &error);
    let mut failure = failure.lock_recover();
    if failure.is_none() {
        *failure = Some(error);
    }
}

fn partial_tool_persistence_error(error: &ToolError) -> RuntimeString {
    RuntimeString::from("Tool execution returned a result, but its persistence failed. Its effects may already be complete; do not repeat the action automatically. Persistence error: ").concat(&error.message)
}

fn tool_failure_phase(shared: &Arc<Shared>) -> ToolLifecyclePhase {
    if shared.params.signal.is_cancelled() {
        ToolLifecyclePhase::Cancelled
    } else {
        ToolLifecyclePhase::Failed
    }
}

/// A failed tool result the model can act on: the call is reported as an error,
/// with the lifecycle record and the renderer item marked failed to match.
pub(super) async fn error_result(
    call: &ResolvedToolCall,
    shared: &Arc<Shared>,
    mut message: RuntimeString,
    extra_context: &[RuntimeString],
) -> ToolResultMessage {
    let persistence = record_tool_lifecycle(
        shared,
        call,
        tool_failure_phase(shared),
        Some(call.initial_internal_input.clone()),
        Some(true),
        Some(message.clone()),
    )
    .await;
    if let Err(error) = persistence {
        report_tool_persistence_failure(shared, &error);
        message = message
            .concat(&RuntimeString::from(
                "
Tool lifecycle persistence failed: ",
            ))
            .concat(&error.message);
    }
    shared.q.push(EngineEvent::ItemUpdated {
        inference: None,
        item: ThreadItem::ToolCall {
            id: call.visible.id.clone(),
            tool_name: call.internal_name.clone(),
            arguments: call.initial_internal_input.clone(),
            result: None,
            error: Some(ItemError {
                message: message.clone(),
            }),
            status: ItemStatus::Failed,
        },
    });
    let mut content: Vec<OutputBlock> = vec![text_out_exact(message)];
    if !extra_context.is_empty() {
        content.push(OutputBlock::Text(TextBlock {
            text: join_runtime_strings(extra_context, "\n"),
            citations: None,
            harness_envelope: Some(HarnessEnvelope::SystemReminder),
            message_id: None,
            phase: None,
        }));
    }
    let content = render_failed_tool_result_content(content, &call.visible.name, &call.profile);
    ToolResultMessage {
        uuid: (shared.deps.uuid)().into(),
        tool_use_id: call.visible.id.clone(),
        content,
        is_error: true,
        tool_kind: call.visible.tool_kind,
        native_tool_output: None,
        output: None,
        extra_user_blocks: None,
    }
}
