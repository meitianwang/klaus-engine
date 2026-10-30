//! Defect: Last turn could only recover the last per-edit diff for a file.
//! Root cause: file-change events and persisted turn rows omitted baseline-to-final content.
//! Fixed by: 2026-10-05 cumulative turn diff work item, based on 9d4221e.

use klaus_agent_core::context::edited_files_diff::{full_file_diff, full_turn_file_diff};
use klaus_agent_core::engine::session::SessionFileChanges;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimePath, RuntimeString};
use klaus_agent_core::sandbox::process::ProcessSandbox;
use klaus_agent_core::tools::apply_patch::tool::create_apply_patch_tool;
use klaus_agent_core::tools::builtins::edit::create_edit_tool;
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::tools::builtins::write::create_write_tool;
use klaus_agent_core::tools::file_state::InMemoryFileStateCache;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::{FileChange, ToolDomainEvent};
use klaus_agent_core::types::permission::PermissionContext;
use klaus_agent_core::types::tool::{Tool, ToolContext};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

struct Directory(std::path::PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn call(tool: &Arc<dyn Tool>, value: Value, ctx: &ToolContext) {
    let input = runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap();
    let parsed = tool.parse_input(&input).unwrap();
    let result = tool.call(parsed, ctx, None).await.unwrap();
    assert_ne!(
        result.serializable.is_error,
        Some(true),
        "{:?}",
        result.serializable.model_content
    );
}

fn last(events: &Arc<Mutex<Vec<ToolDomainEvent>>>) -> FileChange {
    events
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find_map(|event| match event {
            ToolDomainEvent::FileChange { change } => Some(change.clone()),
            _ => None,
        })
        .expect("file change emitted")
}

fn make_ctx(
    cwd: &str,
    process: RuntimeProcessContext,
    events: Arc<std::sync::Mutex<Vec<ToolDomainEvent>>>,
) -> ToolContext {
    ToolContext {
        async_question: None,
        background_processes: Arc::new(
            klaus_agent_core::command_runtime::background::BackgroundCommandRegistry::new(),
        ),
        process_context: process,
        cwd: RuntimePath::from(cwd),
        signal: AbortSignal::new(),
        file_state: Arc::new(InMemoryFileStateCache::new()),
        permission: PermissionContext {
            allow_rules: Vec::new(),
            deny_rules: Vec::new(),
            ask_rules: Vec::new(),
            additional_writable_roots: Vec::new(),
            internal_writable_roots: Vec::new(),
            pinned_memory_root: None,
            should_avoid_prompts: false,
        },
        sandbox: Arc::new(ProcessSandbox),
        planning_read_only: false,
        approval_policy: None,
        sandbox_mode: None,
        approvals_reviewer: None,
        web_search_access: None,
        network_allowed: None,
        emit: std::sync::Arc::new(move |event| {
            events.lock().unwrap().push(event);
            Box::pin(async {})
        }),
        native_pdf: None,
        vision: None,
        shell_env: None,
        depth: None,
        forward_subagent_text: None,
        spawn_sub_agent: None,
        spawn_background_agent: None,
        spawn_workflow_agent: None,
        surface_tools: None,
        activate_skill_paths: None,
        tool_catalog: None,
        elicit: None,
        plan_file: None,
        apply_skill_context: None,
        run_bash: None,
        mcp_clients: None,
        background_tasks: None,
        task_list_id: None,
        task_plan: None,
        worktree: None,
        workflow_journal: None,
        swarm: None,
        cron: None,
        goal: None,
        goal_generation: None,
        goal_owned_resources: None,
        lsp: None,
        browser: None,
        browser_tool_mapping: None,
        run_hook: None,
        invoke_tool: None,
        advisor: None,
        sandbox_retry_on_denial: None,
        request_approval: None,
        request_network_access: None,
        enqueue_background_notice: None,
        record_rollout_event: None,
    }
}

#[tokio::test]
async fn real_writers_report_the_whole_turn_and_shared_child_baseline() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let directory =
        Directory(std::env::temp_dir().join(format!("turn-diff-{}", uuid::Uuid::new_v4())));
    std::fs::create_dir_all(&directory.0).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let parent = make_ctx(
        directory.0.to_str().unwrap(),
        process.clone(),
        events.clone(),
    );
    parent.file_state.begin_turn();
    let write = create_write_tool();
    let edit = create_edit_tool();
    let patch = create_apply_patch_tool();
    call(
        &write,
        json!({"path":"file.txt","content":"one\ntwo\n"}),
        &parent,
    )
    .await;
    call(
        &edit,
        json!({"path":"file.txt","oldString":"two","newString":"TWO"}),
        &parent,
    )
    .await;
    let parent_change = last(&events);
    assert_eq!(
        parent_change.turn_diff,
        Some(full_turn_file_diff(
            &RuntimeString::default(),
            &"one\nTWO\n".into(),
            false,
            true
        ))
    );
    assert_eq!(
        parent_change.diff,
        Some(full_file_diff(&"one\ntwo\n".into(), &"one\nTWO\n".into())),
        "per-edit preview remains independent"
    );

    let mut child = make_ctx(directory.0.to_str().unwrap(), process, events.clone());
    child.file_state = Arc::from(parent.file_state.clone_box());
    call(&patch, json!({"patch":"*** Begin Patch\n*** Update File: file.txt\n@@\n-one\n+ONE\n TWO\n*** End Patch"}), &child).await;
    let child_change = last(&events);
    assert_eq!(
        child_change.turn_diff,
        Some(full_turn_file_diff(
            &RuntimeString::default(),
            &"ONE\nTWO\n".into(),
            false,
            true
        )),
        "child shares the parent's first-touch baseline"
    );
    assert_eq!(
        (child_change.added, child_change.removed),
        (Some(2), Some(0))
    );
    let mut state = SessionFileChanges::default();
    let turn = RuntimeString::from("parent-turn");
    state.record_change(Some(&turn), &parent_change);
    state.record_change(Some(&turn), &child_change);
    assert_eq!(state.turns[0].files[0].turn_diff, child_change.turn_diff);

    call(
        &patch,
        json!({"patch":"*** Begin Patch\n*** Delete File: file.txt\n*** End Patch"}),
        &child,
    )
    .await;
    let deleted = last(&events);
    assert_eq!(
        deleted.turn_diff,
        Some(RuntimeString::default()),
        "create then edit then delete leaves no net text change"
    );
    assert_eq!((deleted.added, deleted.removed), (Some(0), Some(0)));
    assert_eq!(
        deleted.diff,
        Some("ONE\nTWO\n".into()),
        "per-delete content is retained"
    );

    std::fs::write(directory.0.join("existing.txt"), "original\n").unwrap();
    parent.file_state.begin_turn();
    call(&patch, json!({"patch":"*** Begin Patch\n*** Update File: existing.txt\n@@\n-original\n+middle\n*** End Patch"}), &parent).await;
    call(&patch, json!({"patch":"*** Begin Patch\n*** Update File: existing.txt\n@@\n-middle\n+final\n*** End Patch"}), &parent).await;
    assert_eq!(
        last(&events).turn_diff,
        Some(full_file_diff(&"original\n".into(), &"final\n".into()))
    );
    call(
        &patch,
        json!({"patch":"*** Begin Patch\n*** Delete File: existing.txt\n*** End Patch"}),
        &parent,
    )
    .await;
    assert_eq!(
        last(&events).turn_diff,
        Some(full_turn_file_diff(
            &"original\n".into(),
            &RuntimeString::default(),
            true,
            false
        ))
    );

    std::fs::write(directory.0.join("overwritten.txt"), "previous\n").unwrap();
    parent.file_state.begin_turn();
    call(&patch, json!({"patch":"*** Begin Patch\n*** Add File: overwritten.txt\n+replacement\n*** End Patch"}), &parent).await;
    assert_eq!(
        last(&events).turn_diff,
        Some(full_file_diff(
            &"previous\n".into(),
            &"replacement\n".into()
        )),
        "add over existing file preserves its real baseline"
    );
}

#[test]
fn historical_missing_turn_diff_is_not_inferred_from_the_last_edit() {
    let historical = runtime_json::parse(&RuntimeString::from(
        r#"{"path":"a.txt","kind":"modify","diff":"last edit"}"#,
    ))
    .unwrap();
    let mut change: FileChange = runtime_codec::decode_value(&historical).unwrap();
    assert_eq!(change.turn_diff, None);
    let mut state = SessionFileChanges::default();
    state.record_change(None, &change);
    assert_eq!(state.turns[0].files[0].turn_diff, None);
    change.turn_diff = Some(RuntimeString::from_utf16(vec![0xd800, 0x000a]));
    let encoded = runtime_codec::encode_text(&change).unwrap();
    let decoded: FileChange = runtime_codec::decode_text(&encoded).unwrap();
    assert_eq!(
        decoded.turn_diff, change.turn_diff,
        "wire retains exact UTF-16"
    );
}

#[tokio::test]
async fn empty_file_existence_survives_and_true_cancellations_are_empty() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let directory =
        Directory(std::env::temp_dir().join(format!("turn-existence-{}", uuid::Uuid::new_v4())));
    std::fs::create_dir_all(&directory.0).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let ctx = make_ctx(directory.0.to_str().unwrap(), process, events.clone());
    let write = create_write_tool();
    let patch = create_apply_patch_tool();
    ctx.file_state.begin_turn();
    call(&write, json!({"path":"empty.txt","content":""}), &ctx).await;
    assert_eq!(
        last(&events).turn_diff,
        Some("new file mode 100644\n--- before\n+++ after\n".into())
    );
    assert_eq!(
        (last(&events).added, last(&events).removed),
        (Some(0), Some(0))
    );
    call(
        &patch,
        json!({"patch":"*** Begin Patch\n*** Delete File: empty.txt\n*** End Patch"}),
        &ctx,
    )
    .await;
    assert_eq!(
        last(&events).turn_diff,
        Some(RuntimeString::default()),
        "create then delete cancels existence too"
    );

    std::fs::write(directory.0.join("existing-empty.txt"), "").unwrap();
    ctx.file_state.begin_turn();
    call(
        &patch,
        json!({"patch":"*** Begin Patch\n*** Delete File: existing-empty.txt\n*** End Patch"}),
        &ctx,
    )
    .await;
    assert_eq!(
        last(&events).turn_diff,
        Some("deleted file mode 100644\n--- before\n+++ after\n".into())
    );
    call(
        &write,
        json!({"path":"existing-empty.txt","content":""}),
        &ctx,
    )
    .await;
    assert_eq!(
        last(&events).turn_diff,
        Some(RuntimeString::default()),
        "delete then recreate identical empty file cancels"
    );
    ctx.file_state.begin_turn();
    call(
        &patch,
        json!({"patch":"*** Begin Patch\n*** Add File: existing-empty.txt\n*** End Patch"}),
        &ctx,
    )
    .await;
    assert_eq!(
        last(&events).turn_diff,
        Some(RuntimeString::default()),
        "overwrite of an existing empty file does not create it"
    );
}

#[tokio::test]
async fn typed_child_keeps_parent_start_diff_with_fresh_read_guards() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let directory =
        Directory(std::env::temp_dir().join(format!("typed-turn-diff-{}", uuid::Uuid::new_v4())));
    std::fs::create_dir_all(&directory.0).unwrap();
    std::fs::write(directory.0.join("existing.txt"), "start\n").unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let cache = Arc::new(InMemoryFileStateCache::new());
    let mut parent = make_ctx(
        directory.0.to_str().unwrap(),
        process.clone(),
        events.clone(),
    );
    parent.file_state = cache.clone();
    parent.file_state.begin_turn();
    call(&create_read_tool(), json!({"path":"existing.txt"}), &parent).await;
    call(
        &create_edit_tool(),
        json!({"path":"existing.txt","oldString":"start","newString":"middle"}),
        &parent,
    )
    .await;
    let path = RuntimePath::from(directory.0.join("existing.txt").to_str().unwrap());
    assert!(parent.file_state.has(&path));
    let mut child = make_ctx(directory.0.to_str().unwrap(), process, events.clone());
    child.file_state = Arc::new(cache.child_with_fresh_read_state());
    assert!(
        !child.file_state.has(&path),
        "typed child inherits no parent read guard"
    );
    call(
        &create_edit_tool(),
        json!({"path":"existing.txt","oldString":"middle","newString":"final"}),
        &child,
    )
    .await;
    assert_eq!(
        last(&events).turn_diff,
        Some(full_file_diff(&"start\n".into(), &"final\n".into())),
        "typed child reports parent-start to child-final content"
    );
}
