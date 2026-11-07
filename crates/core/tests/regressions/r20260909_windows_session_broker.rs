//! Defect: a normal Windows managed-network Session required an external broker
//! context even though the desktop owns only the app-server process.
//! Root cause: synchronous Session construction had no owned broker path.
//! Regression for the 2026-09-09 session-owned Windows broker work item.
//!
//! The public Agent/Session surface must construct two live sessions for the same
//! owned cwd and confirm their shutdown. No target, AppContainer profile, system
//! ACL, loopback exemption, or WFP object is created by this case. It proves the
//! context-free construction boundary, not an end-to-end sandbox network grant.

#![cfg(windows)]

use std::fs;
use std::path::PathBuf;

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn managed_sessions_own_brokers_without_external_context_and_share_a_cwd() {
    // This case models the embedding binary's entry-point declaration solely
    // for Session construction. It never spawns a target; wrapper dispatch and
    // actual AppContainer networking require the separate product probes.
    klaus_agent_core::sandbox::windows::declare_wrapper_entry_point();
    let root = Scratch(
        std::env::temp_dir().join(format!("klaus-session-broker-{}", uuid::Uuid::new_v4())),
    );
    fs::create_dir_all(&root.0).expect("create owned cwd");
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.deps.call_model = Some(Box::new(|_, _| {
        panic!("broker construction must not call a model")
    }));
    let agent = Agent::new(options);
    let start = || StartSessionOptions {
        cwd: Some(root.0.to_string_lossy().to_string().into()),
        user_config_dir: Some(root.0.to_string_lossy().to_string().into()),
        config: Some(json!({
            "prewarm": false,
            "autoMemory": false,
            "autoDream": false,
            "agentMemory": false,
            "checkpointing": false,
            "sandbox": "windows-restricted-token",
            "requireRealSandbox": true,
            "sandboxNetworkAllowed": false,
            "networkProxy": { "enabled": true, "transport": "windows-broker" }
        })),
        ..Default::default()
    };

    let first = agent.try_start_session(start());
    let second = agent.try_start_session(start());
    let errors =
        [first.as_ref().err(), second.as_ref().err()].map(|error| error.map(ToString::to_string));
    let distinct = match (&first, &second) {
        (Ok(first), Ok(second)) => first.id != second.id,
        _ => false,
    };
    let mut closed = Vec::new();
    for session in [first, second].into_iter().flatten() {
        closed.push(session.shutdown_and_wait().await);
    }
    let runtime_closed = process.shutdown().await;
    let cleanup = fs::remove_dir_all(&root.0);
    eprintln!("session_broker construction={errors:?} distinct={distinct} closed={closed:?} runtime={runtime_closed:?} cleanup={cleanup:?}");

    cleanup.expect("clean owned cwd before assertions");
    runtime_closed.expect("runtime must drain");
    assert_eq!(
        errors,
        [None, None],
        "managed Session must own its missing broker"
    );
    assert!(
        distinct,
        "two live sessions must keep independent identities"
    );
    assert_eq!(closed.len(), 2);
    for result in closed {
        result.expect("session shutdown must confirm broker completion");
    }
}
