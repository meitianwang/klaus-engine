//! Defect: friend task approvals could inherit global grants and escape selected resources.
//! Root cause: ordinary permission rules and shell defaults were not hard resource boundaries.
//! Fixed by: 2026-10-06 friend task resource scope work item.

use super::r20261006_stored_permission_rule_isolation::{fixture, Directory};
use klaus_agent_core::permission::durable::DurablePermissionLayer;
use klaus_agent_core::permission::engine::{
    PermissionDecision, PermissionEngine, StaticPermissionEngine,
};
use klaus_agent_core::permission::rule_store::DurableRules;
use klaus_agent_core::permission::task_scope::CanonicalTaskResourceScope;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimePath, RuntimeString};
use klaus_agent_core::sandbox::task_scope::TaskScopedSandbox;
use klaus_agent_core::schemas::config::{default_config, TaskResourceMode, TaskResourceScope};
use klaus_agent_core::tools::builtins::{
    bash::create_bash_tool, read::create_read_tool, write::create_write_tool,
};
#[cfg(target_os = "macos")]
use klaus_agent_core::types::permission::SandboxMode;
use klaus_agent_core::types::permission::{ApprovalPolicy, ApprovalsReviewer};
use klaus_agent_core::types::sandbox::{
    ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
};
use serde_json::json;
use std::sync::{Arc, Mutex};

fn input(value: serde_json::Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}

fn definition(directory: &Directory, mode: TaskResourceMode) -> TaskResourceScope {
    let read = directory.0.join("read");
    let write = directory.0.join("write");
    std::fs::create_dir_all(&read).unwrap();
    std::fs::create_dir_all(&write).unwrap();
    std::fs::write(read.join("material.txt"), "authorized material").unwrap();
    std::fs::write(directory.0.join("outside/private.txt"), "outside-secret").unwrap();
    TaskResourceScope {
        task_id: "friend-task-1".into(),
        mode,
        network_allowed: false,
        read_roots: vec![RuntimeString::from(
            std::fs::canonicalize(read).unwrap().to_str().unwrap(),
        )],
        write_roots: vec![RuntimeString::from(
            std::fs::canonicalize(write).unwrap().to_str().unwrap(),
        )],
    }
}

#[derive(Default)]
struct CaptureSandbox(Mutex<Option<SandboxPolicy>>);
impl SandboxBackend for CaptureSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Seatbelt
    }
    fn is_available(&self) -> bool {
        true
    }
    fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        *self.0.lock().unwrap() = Some(policy.clone());
        SpawnSpec {
            file: request.argv[0].clone(),
            args: request.argv[1..].to_vec(),
            cwd: request.cwd.clone(),
            env: request.env.clone().unwrap_or_default(),
        }
    }
}

#[test]
fn task_resource_scope_wire_validates_and_keeps_normal_sessions_unscoped() {
    assert!(default_config().task_resource_scope.is_none());
    let config: klaus_agent_core::schemas::config::EngineConfig =
        serde_json::from_value(json!({"taskResourceScope": {
            "taskId":"one", "mode":"scoped-auto", "readRoots":[], "writeRoots":[]
        }}))
        .unwrap();
    assert_eq!(
        config.task_resource_scope.unwrap().mode,
        TaskResourceMode::ScopedAuto
    );
    assert!(
        serde_json::from_value::<klaus_agent_core::schemas::config::EngineConfig>(
            json!({"taskResourceScope": {
                "taskId":"one", "mode":"bypassPermissions", "readRoots":[], "writeRoots":[]
            }})
        )
        .is_err()
    );
}

#[tokio::test]
async fn task_resource_scope_hard_denies_unscoped_native_side_effect_tools() {
    use klaus_agent_core::tools::builtins::index::{default_builtins, BuiltinsOptions};
    let (directory, mut ctx) = fixture();
    ctx.sandbox_mode = Some(klaus_agent_core::types::permission::SandboxMode::DangerFullAccess);
    ctx.approval_policy = Some(klaus_agent_core::types::permission::ApprovalPolicy::Never);
    let mut config = default_config();
    config.task_resource_scope = Some(definition(&directory, TaskResourceMode::ScopedAuto));
    config.cron = true;
    let tools = default_builtins(
        &config,
        BuiltinsOptions {
            browser_runtime_available: true,
            plugin_management_home: Some(directory.0.to_str().unwrap().to_owned()),
            ..Default::default()
        },
    );
    let engine = StaticPermissionEngine::new(&config);
    for name in [
        "REPL",
        "workflow",
        "save_memory",
        "task",
        "exec",
        "wait",
        "exec_command",
        "web_search",
        "web_fetch",
        "browser_eval",
        "plugin_manage",
        "CronCreate",
        "notebook_edit",
        "apply_patch",
    ] {
        let tool = tools
            .iter()
            .find(|tool| tool.name() == name)
            .unwrap_or_else(|| panic!("missing production tool {name}"));
        assert!(
            matches!(
                engine
                    .evaluate(tool.as_ref(), &input(json!({})), &ctx)
                    .await,
                PermissionDecision::Deny { .. }
            ),
            "scope did not deny {name}"
        );
        assert!(
            engine
                .check_task_resource_scope(tool.as_ref(), &input(json!({})), &ctx)
                .await
                .is_err(),
            "dispatch guard did not deny {name}"
        );
    }
    let repl = tools.iter().find(|tool| tool.name() == "REPL").unwrap();
    assert!(matches!(
        StaticPermissionEngine::new(&default_config())
            .evaluate(repl.as_ref(), &input(json!({})), &ctx)
            .await,
        PermissionDecision::Allow { .. }
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn task_resource_scope_native_file_handles_reject_concurrent_namespace_swaps() {
    use std::os::unix::fs::symlink;
    let (directory, mut ctx) = fixture();
    let scope = CanonicalTaskResourceScope::bind(
        &definition(&directory, TaskResourceMode::ScopedAuto),
        &ctx.cwd,
    )
    .unwrap();
    let read = create_read_tool();
    let write = create_write_tool();
    let edit = klaus_agent_core::tools::builtins::edit::create_edit_tool();
    let multi = klaus_agent_core::tools::builtins::multi_edit::create_multi_edit_tool();
    let positive = directory.0.join("write/positive.txt");
    let prepared = scope
        .prepare_file_tool(
            write.as_ref(),
            &input(json!({"path":positive,"content":"authorized material"})),
            &mut ctx,
        )
        .unwrap()
        .unwrap();
    let written = write.call(prepared, &ctx, None).await.unwrap();
    assert_eq!(
        written.serializable.is_error,
        Some(false),
        "{:#?}",
        written.serializable
    );
    let prepared = scope
        .prepare_file_tool(read.as_ref(), &input(json!({"path":positive})), &mut ctx)
        .unwrap()
        .unwrap();
    let read_back = read.call(prepared, &ctx, None).await.unwrap();
    assert!(
        format!("{:?}", read_back.serializable).contains("authorized material"),
        "{:#?}",
        read_back.serializable
    );
    let prepared = scope.prepare_file_tool(edit.as_ref(), &input(json!({"path":positive,"oldString":"authorized material","newString":"edited material"})), &mut ctx).unwrap().unwrap();
    let edited = edit.call(prepared, &ctx, None).await.unwrap();
    assert_eq!(
        edited.serializable.is_error,
        Some(false),
        "{:#?}",
        edited.serializable
    );
    assert_eq!(
        std::fs::read_to_string(&positive).unwrap(),
        "edited material"
    );

    for (tool, operation) in [
        (read.clone(), json!({})),
        (write.clone(), json!({"content":"escaped-write"})),
        (
            edit.clone(),
            json!({"oldString":"authorized material","newString":"escaped-edit"}),
        ),
        (
            multi,
            json!({"edits":[{"oldString":"authorized material","newString":"escaped-multi-edit"}]}),
        ),
        (
            write.clone(),
            json!({"content":"escaped-append","append":true,"continuation_context":"authorized material"}),
        ),
    ] {
        let target = directory.0.join("write/racing.txt");
        let original = directory.0.join("write/admitted.txt");
        let _ = std::fs::remove_file(&target);
        let _ = std::fs::remove_file(&original);
        std::fs::write(&target, "authorized material").unwrap();
        let first_read = scope
            .prepare_file_tool(read.as_ref(), &input(json!({"path":target})), &mut ctx)
            .unwrap()
            .unwrap();
        read.call(first_read, &ctx, None).await.unwrap();
        let mut operation = operation;
        operation
            .as_object_mut()
            .unwrap()
            .insert("path".into(), json!(target));
        let prepared = scope
            .prepare_file_tool(tool.as_ref(), &input(operation), &mut ctx)
            .unwrap()
            .unwrap();
        // A second actor changes the namespace after final permission admission
        // and before the real native tool opens its target. This is the exact
        // pathname-reopen window that a realpath-only guard leaves exposed.
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let worker_barrier = barrier.clone();
        let outside = directory.0.join("outside/private.txt");
        let worker = std::thread::spawn(move || {
            worker_barrier.wait();
            std::fs::rename(&target, &original).unwrap();
            symlink(outside, target).unwrap();
            worker_barrier.wait();
        });
        barrier.wait();
        barrier.wait();
        worker.join().unwrap();
        let result = tool.call(prepared, &ctx, None).await;
        assert!(
            result
                .as_ref()
                .map_or(true, |result| result.serializable.is_error == Some(true)),
            "{:?}",
            result.as_ref().map(|result| &result.serializable)
        );
        assert_eq!(
            std::fs::read_to_string(directory.0.join("outside/private.txt")).unwrap(),
            "outside-secret"
        );
        assert_eq!(
            std::fs::read_to_string(directory.0.join("write/admitted.txt")).unwrap(),
            "authorized material"
        );
    }
    let ls = klaus_agent_core::tools::builtins::ls::create_ls_tool();
    let child = directory.0.join("write/listing");
    std::fs::create_dir(&child).unwrap();
    let prepared = scope
        .prepare_file_tool(ls.as_ref(), &input(json!({"path":child})), &mut ctx)
        .unwrap()
        .unwrap();
    std::fs::rename(&child, directory.0.join("write/admitted-listing")).unwrap();
    symlink(directory.0.join("outside"), &child).unwrap();
    let result = ls.call(prepared, &ctx, None).await.unwrap();
    assert!(!format!("{:?}", result.serializable.output).contains("private.txt"));
}

#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "Live HTTPS verification; run explicitly when external networking is available"]
async fn task_resource_scope_real_https_requires_explicit_network_authorization() {
    let (directory, mut ctx) = fixture();
    for allowed in [false, true] {
        let mut definition = definition(&directory, TaskResourceMode::ScopedAuto);
        definition.network_allowed = allowed;
        let scope = CanonicalTaskResourceScope::bind(&definition, &ctx.cwd).unwrap();
        ctx.sandbox = Arc::new(
            TaskScopedSandbox::new(
                klaus_agent_core::sandbox::create_sandbox_for_mode(SandboxMode::WorkspaceWrite)
                    .unwrap(),
                scope,
            )
            .unwrap(),
        );
        let result = create_bash_tool().call(input(json!({"command":"/usr/bin/curl --silent --show-error --max-time 20 --connect-timeout 10 --head https://example.com", "timeout":25000})), &ctx, None).await.unwrap();
        assert_eq!(
            result.serializable.is_error,
            Some(!allowed),
            "{:#?}",
            result.serializable
        );
        if allowed {
            assert!(
                format!("{:?}", result.serializable).contains("HTTP/2 200"),
                "{:#?}",
                result.serializable
            );
        }
    }
}

#[tokio::test]
async fn task_resource_scope_three_modes_and_unscoped_baseline() {
    let (directory, mut ctx) = fixture();
    ctx.sandbox = Arc::new(CaptureSandbox::default());
    let read = create_read_tool();
    let write = create_write_tool();
    let bash = create_bash_tool();
    // The bound mode seeds the session's initial preset; the live approval
    // policy owns the ask level, so each mode's historical matrix now rides
    // the preset that mode seeds.
    for (mode, policy, reviewer) in [
        (
            TaskResourceMode::Manual,
            ApprovalPolicy::OnRequest,
            ApprovalsReviewer::User,
        ),
        (
            TaskResourceMode::AutoEdit,
            ApprovalPolicy::OnRequest,
            ApprovalsReviewer::Automatic,
        ),
        (
            TaskResourceMode::ScopedAuto,
            ApprovalPolicy::Never,
            ApprovalsReviewer::User,
        ),
    ] {
        ctx.approval_policy = Some(policy);
        ctx.approvals_reviewer = Some(reviewer);
        let mut config = default_config();
        config.task_resource_scope = Some(definition(&directory, mode));
        let engine = StaticPermissionEngine::new(&config);
        assert!(matches!(
            engine
                .evaluate(
                    read.as_ref(),
                    &input(json!({"path":directory.0.join("read/material.txt")})),
                    &ctx
                )
                .await,
            PermissionDecision::Allow { .. }
        ));
        let edit = engine
            .evaluate(
                write.as_ref(),
                &input(json!({"path":directory.0.join("write/new.txt"),"content":"approved"})),
                &ctx,
            )
            .await;
        assert_eq!(
            matches!(edit, PermissionDecision::Ask { .. }),
            mode == TaskResourceMode::Manual
        );
        assert_eq!(
            matches!(edit, PermissionDecision::Allow { .. }),
            mode != TaskResourceMode::Manual
        );
        let command = engine
            .evaluate(
                bash.as_ref(),
                &input(json!({"command":"echo approved"})),
                &ctx,
            )
            .await;
        if cfg!(target_os = "macos") {
            assert_eq!(
                matches!(command, PermissionDecision::Allow { .. }),
                mode == TaskResourceMode::ScopedAuto
            );
            assert_eq!(
                matches!(command, PermissionDecision::Ask { .. }),
                mode != TaskResourceMode::ScopedAuto
            );
        } else {
            assert!(matches!(command, PermissionDecision::Deny { .. }));
        }
    }
    ctx.sandbox_mode = Some(klaus_agent_core::types::permission::SandboxMode::DangerFullAccess);
    ctx.approval_policy = Some(klaus_agent_core::types::permission::ApprovalPolicy::Never);
    assert!(matches!(
        StaticPermissionEngine::new(&default_config())
            .evaluate(
                read.as_ref(),
                &input(json!({"path":directory.0.join("outside/private.txt")})),
                &ctx
            )
            .await,
        PermissionDecision::Allow { .. }
    ));
}

#[tokio::test]
async fn task_resource_scope_hard_denies_outside_and_read_only_even_with_global_grants() {
    let (directory, mut ctx) = fixture();
    ctx.sandbox_mode = Some(klaus_agent_core::types::permission::SandboxMode::DangerFullAccess);
    ctx.approval_policy = Some(klaus_agent_core::types::permission::ApprovalPolicy::Never);
    let mut config = default_config();
    config.task_resource_scope = Some(definition(&directory, TaskResourceMode::ScopedAuto));
    config.allow = vec!["Read".into(), "Write".into()];
    let durable = Arc::new(DurablePermissionLayer::default());
    durable.replace_stored(&DurableRules {
        allow: vec!["Read".into(), "Write".into()],
        ..Default::default()
    });
    let engine = StaticPermissionEngine::with_durable_rules(&config, durable.clone());
    let read = create_read_tool();
    let write = create_write_tool();
    let outside = input(json!({"path":directory.0.join("outside/private.txt")}));
    assert!(matches!(
        engine.evaluate(read.as_ref(), &outside, &ctx).await,
        PermissionDecision::Deny { .. }
    ));
    assert!(engine
        .check_task_resource_scope(read.as_ref(), &outside, &ctx)
        .await
        .is_err());
    for path in [
        directory.0.join("read/material.txt"),
        directory.0.join("outside/missing/child.txt"),
    ] {
        assert!(matches!(
            engine
                .evaluate(
                    write.as_ref(),
                    &input(json!({"path":path,"content":"forbidden"})),
                    &ctx
                )
                .await,
            PermissionDecision::Deny { .. }
        ));
    }
    durable.replace_stored(&DurableRules {
        deny: vec!["Read".into()],
        ..Default::default()
    });
    assert!(matches!(
        engine
            .evaluate(
                read.as_ref(),
                &input(json!({"path":directory.0.join("read/material.txt")})),
                &ctx
            )
            .await,
        PermissionDecision::Deny { .. }
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn task_resource_scope_symlinks_and_missing_targets_cannot_escape() {
    use std::os::unix::fs::symlink;
    let (directory, ctx) = fixture();
    let scope = definition(&directory, TaskResourceMode::ScopedAuto);
    symlink(directory.0.join("outside"), directory.0.join("write/link")).unwrap();
    let mut config = default_config();
    config.task_resource_scope = Some(scope);
    let engine = StaticPermissionEngine::new(&config);
    engine.task_scope(&ctx.cwd).unwrap();
    for path in [
        directory.0.join("write/link/private.txt"),
        directory.0.join("write/link/not-created/child.txt"),
    ] {
        assert!(matches!(
            engine
                .evaluate(
                    create_read_tool().as_ref(),
                    &input(json!({"path":path})),
                    &ctx
                )
                .await,
            PermissionDecision::Deny { .. }
        ));
        assert!(matches!(
            engine
                .evaluate(
                    create_write_tool().as_ref(),
                    &input(json!({"path":path,"content":"forbidden"})),
                    &ctx
                )
                .await,
            PermissionDecision::Deny { .. }
        ));
    }
    symlink(
        directory.0.join("outside/not-created"),
        directory.0.join("write/dangling"),
    )
    .unwrap();
    assert!(matches!(
        engine
            .evaluate(
                create_write_tool().as_ref(),
                &input(
                    json!({"path":directory.0.join("write/dangling/file"),"content":"forbidden"})
                ),
                &ctx
            )
            .await,
        PermissionDecision::Deny { .. }
    ));
}

#[test]
fn task_resource_scope_sandbox_removes_global_cwd_temp_and_unconfined_defaults() {
    let (directory, ctx) = fixture();
    let definition = definition(&directory, TaskResourceMode::ScopedAuto);
    let scope = CanonicalTaskResourceScope::bind(&definition, &ctx.cwd).unwrap();
    let backend = Arc::new(CaptureSandbox::default());
    let wrapper = TaskScopedSandbox::new(backend, scope.clone()).unwrap();
    let policy = wrapper.policy(&SandboxPolicy {
        writable_roots: vec!["/".into()],
        readable_roots: None,
        unconfined_writes: Some(true),
        ..Default::default()
    });
    assert_eq!(
        policy.writable_roots,
        scope
            .writable_roots()
            .into_iter()
            .map(RuntimePath::into_runtime_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(policy.unconfined_writes, Some(false));
    assert_eq!(policy.exclude_cwd, Some(true));
    assert_eq!(policy.exclude_slash_tmp, Some(true));
    assert_eq!(policy.exclude_tmpdir_env_var, Some(true));
    assert!(policy.readable_roots.is_some());
    assert!(!policy
        .readable_roots
        .unwrap()
        .contains(&RuntimeString::from("/")));
    assert!(
        TaskScopedSandbox::new(Arc::new(klaus_agent_core::sandbox::ProcessSandbox), scope).is_err()
    );
}

#[cfg(unix)]
#[test]
fn task_resource_scope_replaced_approved_root_needs_new_owner_approval() {
    use std::os::unix::fs::symlink;
    let (directory, ctx) = fixture();
    let definition = definition(&directory, TaskResourceMode::ScopedAuto);
    let scope = CanonicalTaskResourceScope::bind(&definition, &ctx.cwd).unwrap();
    std::fs::rename(directory.0.join("read"), directory.0.join("original-read")).unwrap();
    symlink(directory.0.join("outside"), directory.0.join("read")).unwrap();
    assert!(scope.validate_roots().is_err());
    assert!(CanonicalTaskResourceScope::bind(&definition, &ctx.cwd).is_err());
    let wrapper = TaskScopedSandbox::new(Arc::new(CaptureSandbox::default()), scope).unwrap();
    let request = ExecRequest {
        argv: vec!["/bin/sh".into(), "-c".into(), "echo forbidden".into()],
        cwd: ctx.cwd,
        env: None,
        timeout_ms: Some(1000),
    };
    assert!(wrapper
        .transform(&request, &SandboxPolicy::default())
        .file
        .is_empty());
}

#[test]
fn task_resource_scope_child_environment_drops_owner_and_explicit_credentials() {
    let (directory, ctx) = fixture();
    let definition = definition(&directory, TaskResourceMode::ScopedAuto);
    let scope = CanonicalTaskResourceScope::bind(&definition, &ctx.cwd).unwrap();
    let mut env = klaus_agent_core::runtime_process::RuntimeEnv::new();
    for key in [
        "OPENAI_API_KEY",
        "SSH_AUTH_SOCK",
        "GPG_AGENT_INFO",
        "DYLD_INSERT_LIBRARIES",
        "NODE_OPTIONS",
        "HTTP_PROXY",
    ] {
        env.insert(key.into(), "secret-owner-value".into());
    }
    env.insert("LANG".into(), "en_US.UTF-8".into());
    let wrapper =
        TaskScopedSandbox::new(Arc::new(CaptureSandbox::default()), scope.clone()).unwrap();
    let spawn = wrapper.transform(
        &ExecRequest {
            argv: vec!["/bin/sh".into()],
            cwd: ctx.cwd,
            env: Some(env),
            timeout_ms: None,
        },
        &SandboxPolicy::default(),
    );
    if !cfg!(target_os = "macos") {
        assert!(spawn.file.is_empty());
        return;
    }
    assert_eq!(
        spawn.env.get(&RuntimeString::from("HOME")),
        Some(&scope.scratch_root().into_runtime_string())
    );
    for key in [
        "OPENAI_API_KEY",
        "SSH_AUTH_SOCK",
        "GPG_AGENT_INFO",
        "DYLD_INSERT_LIBRARIES",
        "NODE_OPTIONS",
        "HTTP_PROXY",
    ] {
        assert!(
            !spawn.env.contains_key(&RuntimeString::from(key)),
            "{key} must not reach a friend command"
        );
    }
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn task_resource_scope_real_shell_denies_outside_reads_writes_and_read_only_edits() {
    let (directory, mut ctx) = fixture();
    let definition = definition(&directory, TaskResourceMode::ScopedAuto);
    let scope = CanonicalTaskResourceScope::bind(&definition, &ctx.cwd).unwrap();
    let backend =
        klaus_agent_core::sandbox::create_sandbox_for_mode(SandboxMode::WorkspaceWrite).unwrap();
    ctx.sandbox = Arc::new(TaskScopedSandbox::new(backend, scope).unwrap());
    // Even a caller's bypass request must stay inside the wrapper's roots.
    ctx.sandbox_mode = Some(klaus_agent_core::types::permission::SandboxMode::DangerFullAccess);
    ctx.approval_policy = Some(klaus_agent_core::types::permission::ApprovalPolicy::Never);
    let bash = create_bash_tool();
    let command = format!(
        "/bin/cat '{}'",
        directory.0.join("read/material.txt").display()
    );
    let allowed = bash
        .call(input(json!({"command":command})), &ctx, None)
        .await
        .unwrap();
    assert_eq!(
        allowed.serializable.is_error,
        Some(false),
        "{:#?}",
        allowed.serializable
    );
    let write = bash.call(input(json!({"command":format!("printf task-output > '{}'", directory.0.join("write/allowed.txt").display())})), &ctx, None).await.unwrap();
    assert_eq!(
        write.serializable.is_error,
        Some(false),
        "{:#?}",
        write.serializable.output
    );
    assert_eq!(
        std::fs::read_to_string(directory.0.join("write/allowed.txt")).unwrap(),
        "task-output"
    );
    for command in [
        format!(
            "/bin/cat '{}'",
            directory.0.join("outside/private.txt").display()
        ),
        format!(
            "echo escaped > '{}'",
            directory.0.join("outside/escaped.txt").display()
        ),
        format!(
            "echo escaped > '{}'",
            directory.0.join("read/material.txt").display()
        ),
    ] {
        let denied = bash
            .call(input(json!({"command":command})), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            denied.serializable.is_error,
            Some(true),
            "{:#?}",
            denied.serializable.output
        );
    }
    assert!(!directory.0.join("outside/escaped.txt").exists());
    assert_eq!(
        std::fs::read_to_string(directory.0.join("read/material.txt")).unwrap(),
        "authorized material"
    );
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn task_resource_scope_real_shell_environment_and_gui_automation_are_isolated() {
    let (directory, mut ctx) = fixture();
    let scope = CanonicalTaskResourceScope::bind(
        &definition(&directory, TaskResourceMode::ScopedAuto),
        &ctx.cwd,
    )
    .unwrap();
    ctx.sandbox = Arc::new(
        TaskScopedSandbox::new(
            klaus_agent_core::sandbox::create_sandbox_for_mode(SandboxMode::WorkspaceWrite)
                .unwrap(),
            scope,
        )
        .unwrap(),
    );
    ctx.shell_env = Some(Arc::new(|| {
        Box::pin(async {
            let mut env = klaus_agent_core::runtime_process::RuntimeEnv::new();
            for key in ["OPENAI_API_KEY", "SSH_AUTH_SOCK", "GPG_AGENT_INFO"] {
                env.insert(key.into(), "fake-owner-secret".into());
            }
            Some(env)
        })
    }));
    let bash = create_bash_tool();
    let env = bash
        .call(input(json!({"command":"/usr/bin/env"})), &ctx, None)
        .await
        .unwrap();
    assert_eq!(
        env.serializable.is_error,
        Some(false),
        "{:#?}",
        env.serializable
    );
    assert!(!format!("{:?}", env.serializable.output).contains("fake-owner-secret"));
    // Read-only AppleEvent: even if this failed guard regressed, it cannot run
    // an outer Terminal command or modify an application window.
    let gui = bash.call(input(json!({"command":"/usr/bin/osascript -e 'tell application \"System Events\" to get name of every process'","timeout":10000})), &ctx, None).await.unwrap();
    assert_eq!(
        gui.serializable.is_error,
        Some(true),
        "{:#?}",
        gui.serializable.output
    );
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn task_resource_scope_search_uses_real_sandbox_and_absolute_globs_cannot_escape() {
    let (directory, mut ctx) = fixture();
    let scope = CanonicalTaskResourceScope::bind(
        &definition(&directory, TaskResourceMode::ScopedAuto),
        &ctx.cwd,
    )
    .unwrap();
    ctx.sandbox = Arc::new(
        TaskScopedSandbox::new(
            klaus_agent_core::sandbox::create_sandbox_for_mode(SandboxMode::WorkspaceWrite)
                .unwrap(),
            scope,
        )
        .unwrap(),
    );
    let grep = klaus_agent_core::tools::builtins::grep::create_grep_tool();
    let result = grep
        .call(
            input(json!({"pattern":"authorized", "path":directory.0.join("read")})),
            &ctx,
            None,
        )
        .await
        .unwrap();
    assert_ne!(
        result.serializable.is_error,
        Some(true),
        "{:#?}",
        result.serializable
    );
    assert!(
        format!("{:?}", result.serializable.output).contains("material.txt"),
        "{:#?}",
        result.serializable
    );
    let glob = klaus_agent_core::tools::builtins::glob::create_glob_tool();
    assert!(glob
        .call(
            input(json!({"pattern":format!("{}/*.txt", directory.0.join("outside").display())})),
            &ctx,
            None
        )
        .await
        .is_err());
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn task_resource_scope_real_shell_network_does_not_authorize_localhost_or_unix_agents() {
    use std::net::TcpListener;
    use std::os::unix::net::UnixListener;
    let (directory, mut ctx) = fixture();
    let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
    let socket_directory = Directory(std::env::temp_dir().join(format!(
        "ts-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    )));
    std::fs::create_dir_all(&socket_directory.0).unwrap();
    let socket = socket_directory.0.join("agent");
    let _unix = UnixListener::bind(&socket).unwrap();
    let mut grant = definition(&directory, TaskResourceMode::ScopedAuto);
    grant.read_roots.push(RuntimeString::from(
        std::fs::canonicalize(&socket_directory.0)
            .unwrap()
            .to_str()
            .unwrap(),
    ));
    for enabled in [false, true] {
        grant.network_allowed = enabled;
        let scope = CanonicalTaskResourceScope::bind(&grant, &ctx.cwd).unwrap();
        ctx.sandbox = Arc::new(
            TaskScopedSandbox::new(
                klaus_agent_core::sandbox::create_sandbox_for_mode(SandboxMode::WorkspaceWrite)
                    .unwrap(),
                scope,
            )
            .unwrap(),
        );
        for command in [
            format!(
                "/usr/bin/nc -z -w 1 127.0.0.1 {}",
                tcp.local_addr().unwrap().port()
            ),
            format!("printf probe | /usr/bin/nc -w 1 -U '{}'", socket.display()),
        ] {
            let result = create_bash_tool()
                .call(input(json!({"command":command,"timeout":5000})), &ctx, None)
                .await
                .unwrap();
            assert_eq!(
                result.serializable.is_error,
                Some(true),
                "network={enabled}: {:#?}",
                result.serializable.output
            );
        }
    }
}
