//! Native preparation uses endpoint configuration and completes on the global event stream.

use super::*;
use crate::app_server::windows_sandbox::{Context, PreparationDriver, SetupPlan};
use crate::app_server::wire;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tokio::sync::{Notify, Semaphore};

type Capture = (
    wire::WindowsSandboxSetupMode,
    PathBuf,
    klaus_agent_core::types::sandbox::SandboxPolicy,
    bool,
    Option<klaus_agent_core::network_proxy::NetworkProxyConfig>,
);

struct Driver {
    complete: bool,
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
    captures: Arc<Mutex<Vec<Capture>>>,
    error: Option<String>,
}

impl PreparationDriver for Driver {
    fn is_windows(&self) -> bool {
        true
    }
    fn setup_is_complete(&self, home: &Path) -> bool {
        assert!(home.is_absolute());
        self.complete
    }
    fn prepare(
        &self,
        mode: wire::WindowsSandboxSetupMode,
        plan: SetupPlan,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        let captures = self.captures.clone();
        let entered = self.entered.clone();
        let release = self.release.clone();
        let error = self.error.clone();
        Box::pin(async move {
            {
                let mut captures = captures.lock().unwrap();
                assert!(captures.is_empty(), "one preparation per fixture");
                captures.push((
                    mode,
                    plan.cwd,
                    plan.policy,
                    plan.service_enabled,
                    plan.service_proxy,
                ));
            }
            entered.notify_one();
            release.acquire().await.unwrap().forget();
            error.map_or(Ok(()), Err)
        })
    }
}

fn start(context: Context, path: PathBuf) -> TestClient {
    let (client_side, server_side) = tokio::io::duplex(1 << 20);
    let (server_read, server_write) = tokio::io::split(server_side);
    let (client_read, client_write) = tokio::io::split(client_side);
    let server = tokio::spawn(run_app_server(
        server_read,
        server_write,
        AppServerOptions {
            windows_sandbox_context: Some(context),
            create_session: Arc::new(|_| {
                Box::pin(async { panic!("preparation never materializes a conversation") })
            }),
            credential_store: None,
            lifecycle: None,
            plugin_admin: None,
            permission_rules_path: None,
            config_path: Some(path),
            startup_init_ack_budget: None,
            session_unload_delay: None,
        },
    ));
    TestClient {
        writer: client_write,
        lines: BufReader::new(client_read).lines(),
        server,
        diagnostic: None,
    }
}

fn driver(complete: bool, error: Option<&str>) -> Arc<Driver> {
    Arc::new(Driver {
        complete,
        entered: Arc::new(Notify::new()),
        release: Arc::new(Semaphore::new(0)),
        captures: Arc::new(Mutex::new(Vec::new())),
        error: error.map(str::to_string),
    })
}

#[test]
fn windows_preparation_schema_preserves_optional_nullable_cwd_and_null_completion_error() {
    let schema = schema_bundle();
    for method in [
        "engine/windowsSandboxReadiness",
        "engine/startWindowsSandboxSetup",
    ] {
        assert!(schema["requests"][method].is_object());
    }
    let params = &schema["requests"]["engine/startWindowsSandboxSetup"]["params"];
    assert!(!params["required"]
        .as_array()
        .unwrap()
        .contains(&json!("cwd")));
    for value in [
        json!({"mode":"elevated"}),
        json!({"mode":"unelevated","cwd":null}),
    ] {
        let params: wire::WindowsSandboxSetupStartParams = serde_json::from_value(value).unwrap();
        assert!(params.cwd.is_none());
    }
    assert!(
        serde_json::from_value::<wire::WindowsSandboxSetupStartParams>(
            json!({"mode":"elevated","cwd":"relative"})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<wire::WindowsSandboxSetupStartParams>(json!({"mode":"mxc"}))
            .is_err()
    );
    let completed = serde_json::to_value(wire::WindowsSandboxSetupCompletedNotification {
        mode: wire::WindowsSandboxSetupMode::Elevated,
        success: true,
        error: None,
    })
    .unwrap();
    assert_eq!(
        completed,
        json!({"mode":"elevated","success":true,"error":null})
    );
    assert!(
        schema["notifications"]["engine/windowsSandboxSetupCompleted"]
            .get("requiresCapability")
            .is_none()
    );
}

#[tokio::test]
async fn readiness_uses_effective_selection_and_only_elevated_setup_state() {
    let root = tempfile::tempdir().unwrap();
    for (selection, complete, status) in [
        (None, false, "notConfigured"),
        (None, true, "notConfigured"),
        (Some("unelevated"), false, "ready"),
        (Some("mxc"), false, "ready"),
        (Some("elevated"), false, "updateRequired"),
        (Some("elevated"), true, "ready"),
    ] {
        let path = root.path().join("config.toml");
        let config = selection
            .map(|mode| format!("[windows]\nsandbox = \"{mode}\"\n"))
            .unwrap_or_default();
        std::fs::write(&path, &config).unwrap();
        let mut client = start(
            Context {
                cwd: Some(root.path().to_path_buf()),
                driver: Some(driver(complete, None)),
                ..Default::default()
            },
            path.clone(),
        );
        client.initialize().await;
        for (id, request) in [
            (2, json!({"id":2,"method":"engine/windowsSandboxReadiness"})),
            (
                3,
                json!({"id":3,"method":"engine/windowsSandboxReadiness","params":{}}),
            ),
        ] {
            client.send(request).await;
            let (reply, _) = client.response_for(id).await;
            assert!(reply.get("error").is_none(), "{reply}");
            assert_eq!(reply["result"], json!({"status":status}));
        }
        assert_eq!(std::fs::read_to_string(path).unwrap(), config);
        client.close().await;
    }
}

#[tokio::test]
async fn setup_ack_precedes_one_global_completion_and_never_writes_selected_mode() {
    let root = tempfile::tempdir().unwrap();
    for (label, cwd, error, mode) in [
        ("omitted", None, None, "elevated"),
        (
            "null",
            Some(Value::Null),
            Some("setup helper failed"),
            "unelevated",
        ),
        (
            "workspace",
            Some(json!(root.path().join("workspace"))),
            None,
            "elevated",
        ),
    ] {
        let path = root.path().join("config.toml");
        let original = "sandbox_mode = \"workspace-write\"\n[windows]\nsandbox = \"unelevated\"\n";
        std::fs::write(&path, original).unwrap();
        let native = driver(false, error);
        let mut client = start(
            Context {
                cwd: Some(root.path().to_path_buf()),
                driver: Some(native.clone()),
                ..Default::default()
            },
            path.clone(),
        );
        client.initialize().await;
        let mut params = json!({"mode":mode});
        if let Some(cwd) = cwd {
            params["cwd"] = cwd;
        }
        client
            .send(json!({"id":2,"method":"engine/startWindowsSandboxSetup","params":params}))
            .await;
        let (ack, frames) = client.response_for(2).await;
        assert_eq!(ack["result"], json!({"started":true}), "{ack}");
        assert!(!frames
            .iter()
            .any(|frame| frame["method"] == "engine/windowsSandboxSetupCompleted"));
        tokio::time::timeout(REPLY_TIMEOUT, native.entered.notified())
            .await
            .unwrap();
        let captured = native.captures.lock().unwrap().clone();
        let (actual_mode, command_cwd, policy, _, _) = &captured[0];
        assert_eq!(json!(actual_mode), json!(mode));
        if label == "workspace" {
            let workspace = root.path().join("workspace");
            assert_eq!(command_cwd, &workspace);
            assert_eq!(
                policy.writable_roots,
                vec![klaus_agent_core::runtime_values::RuntimeString::from(
                    workspace.to_str().unwrap()
                )]
            );
        } else {
            assert_eq!(command_cwd, root.path());
            assert!(
                policy.writable_roots.is_empty(),
                "no install-directory write grant"
            );
        }
        assert_eq!(policy.exclude_cwd, Some(true));
        native.release.add_permits(1);
        let completed = tokio::time::timeout(REPLY_TIMEOUT, async {
            loop {
                let message = client.next_message().await;
                if message["method"] == "engine/windowsSandboxSetupCompleted" {
                    break message;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            completed["params"],
            json!({"mode":mode,"success":error.is_none(),"error":error})
        );
        assert!(completed["params"].get("sessionId").is_none());
        assert_eq!(std::fs::read_to_string(path).unwrap(), original);
        client.close().await;
    }
}

#[tokio::test]
async fn invalid_setup_configuration_rejects_before_start_or_native_preparation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config.toml");
    std::fs::write(&path, "invalid = [").unwrap();
    let native = driver(false, None);
    let mut client = start(
        Context {
            cwd: Some(root.path().to_path_buf()),
            driver: Some(native.clone()),
            ..Default::default()
        },
        path,
    );
    client.initialize().await;
    client.send(json!({"id":2,"method":"engine/startWindowsSandboxSetup","params":{"mode":"unelevated"}})).await;
    let (reply, frames) = client.response_for(2).await;
    assert!(reply["error"]["message"]
        .as_str()
        .unwrap()
        .starts_with("Failed to load config:"));
    assert!(native.captures.lock().unwrap().is_empty());
    assert!(!frames
        .iter()
        .any(|frame| frame["method"] == "engine/windowsSandboxSetupCompleted"));
    client.close().await;
}
#[tokio::test]
async fn named_setup_captures_service_feature_and_explicit_proxy_presence() {
    let root = tempfile::tempdir().unwrap();
    for (proxy_table, endpoint_config, configured, enabled) in [
        ("", None, false, false),
        ("\n[networkProxy]\nenabled = false\n", None, true, false),
        ("\n[networkProxy]\n", None, true, false),
        (
            "",
            Some(json!({"networkProxy":{"enabled":false}})),
            true,
            false,
        ),
        ("\n[networkProxy]\nenabled = true\n", None, true, true),
    ] {
        let path = root.path().join("config.toml");
        let original = format!("[features]\nwindows_sandbox_service = true\n{proxy_table}");
        std::fs::write(&path, &original).unwrap();
        let native = driver(false, None);
        let mut client = start(
            Context {
                cwd: Some(root.path().to_path_buf()),
                config: endpoint_config,
                driver: Some(native.clone()),
            },
            path.clone(),
        );
        client.initialize().await;
        client.send(json!({"id":2,"method":"engine/startWindowsSandboxSetup","params":{"mode":"elevated"}})).await;
        let (reply, _) = client.response_for(2).await;
        assert_eq!(reply["result"], json!({"started":true}));
        tokio::time::timeout(REPLY_TIMEOUT, native.entered.notified())
            .await
            .unwrap();
        {
            let captures = native.captures.lock().unwrap();
            assert!(captures[0].3);
            assert_eq!(captures[0].4.is_some(), configured);
            if let Some(proxy) = &captures[0].4 {
                assert_eq!(proxy.enabled, enabled);
            }
        }
        native.release.add_permits(1);
        tokio::time::timeout(REPLY_TIMEOUT, async {
            loop {
                let message = client.next_message().await;
                if message["method"] == "engine/windowsSandboxSetupCompleted" {
                    assert_eq!(message["params"]["success"], true);
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(std::fs::read_to_string(path).unwrap(), original);
        client.close().await;
    }
}
