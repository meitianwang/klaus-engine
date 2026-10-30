use crate::engine::session::{resolve_config, resolve_config_default};
use crate::sandbox::create_sandbox_for_mode;
use crate::schemas::config::normalize_sandbox_config;
use crate::types::permission::SandboxMode;
use serde_json::{json, Value};

const SCENARIO: &str = "config-sandbox-defaults-validation";

pub fn config_sandbox_trace() -> Value {
    json!({
        "scenario": SCENARIO,
        "config": {
            "defaults": pick_config(resolve_config_default()),
            "partial": pick_config(resolve_config(json!({
                "model": "model-x",
                "memory": { "user": false },
                "mcpCatalog": { "enabled": false },
                "sandbox_mode": "workspace-write",
                "sandbox_workspace_write": { "network_access": true },
                "webSearch": {
                    "mode": "client",
                    "maxUses": 2,
                    "allowedDomains": ["example.com"],
                },
            })).unwrap()),
            "validation": {
                "memoryRecallNeedsAgentMemory": validation_result(json!({
                    "agentMemory": false,
                    "memoryRecall": true,
                }), "memoryRecall"),
                "autoMemoryNeedsAgentMemory": validation_result(json!({
                    "agentMemory": false,
                    "autoMemory": true,
                }), "autoMemory"),
                "autoDreamNeedsAgentMemory": validation_result(json!({
                    "agentMemory": false,
                    "autoDream": true,
                }), "autoDream"),
            },
            "migration": migration_result(),
        },
        "sandbox": {
            "readOnly": sandbox_result(SandboxMode::ReadOnly),
            "workspaceWrite": sandbox_result(SandboxMode::WorkspaceWrite),
            "fullAccess": sandbox_result(SandboxMode::DangerFullAccess),
        },
    })
}

fn pick_config(config: crate::schemas::config::EngineConfig) -> Value {
    let value = serde_json::to_value(config).unwrap();
    json!({
        "model": value["model"].clone(),
        "maxOutputTokens": value["maxOutputTokens"].clone(),
        "thinking": value["thinking"].clone(),
        "approval_policy": value["approval_policy"].clone(),
        "approvals_reviewer": value["approvals_reviewer"].clone(),
        "sandbox_mode": value["sandbox_mode"].clone(),
        "sandbox_workspace_write": value["sandbox_workspace_write"].clone(),
        "processHardening": value["processHardening"].clone(),
        "contextWindow": value["contextWindow"].clone(),
        "outputReserveTokens": value["outputReserveTokens"].clone(),
        "compactBufferTokens": value["compactBufferTokens"].clone(),
        "compactThreshold": value["compactThreshold"].clone(),
        "includeGitContext": value["includeGitContext"].clone(),
        "memory": value["memory"].clone(),
        "workflow": value["workflow"].clone(),
        "swarm": value["swarm"].clone(),
        "codeMode": value["codeMode"].clone(),
        "agentMemory": value["agentMemory"].clone(),
        "memoryRecall": value["memoryRecall"].clone(),
        "agentMemorySnapshot": value["agentMemorySnapshot"].clone(),
        "autoMemory": value["autoMemory"].clone(),
        "autoDream": value["autoDream"].clone(),
        "mcpCatalog": value["mcpCatalog"].clone(),
        "skillCatalog": value["skillCatalog"].clone(),
        "webSearch": value["webSearch"].clone(),
    })
}

fn validation_result(input: Value, path: &str) -> Value {
    match resolve_config(input) {
        Ok(_) => json!({ "ok": true }),
        Err(message) => json!({
            "ok": false,
            "message": message,
            "path": [path],
        }),
    }
}

fn migration_result() -> Value {
    let mut values = json!({
        "sandbox": "auto",
        "sandboxNetworkAllowed": true,
        "additionalWritableRoots": ["/workspace/extra"],
        "requireRealSandbox": false,
        "confineReads": true,
        "additionalReadableRoots": ["/private"]
    })
    .as_object()
    .unwrap()
    .clone();
    let warnings = normalize_sandbox_config(&mut values).unwrap();
    json!({
        "values": values,
        "warningCodes": warnings.iter().map(|warning| warning.code.clone()).collect::<Vec<_>>(),
        "migratedKeys": warnings.into_iter().flat_map(|warning| warning.keys).collect::<Vec<_>>(),
    })
}

fn sandbox_result(mode: SandboxMode) -> Value {
    match create_sandbox_for_mode(mode) {
        Ok(backend) => json!({
            "ok": true,
            "kind": backend.kind(),
        }),
        Err(message) => json!({
            "ok": false,
            "message": message,
        }),
    }
}
