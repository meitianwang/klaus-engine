//! Defect: when any await inside session startup stalled (a credential read, a
//! catalog fetch, the publication gate), `session/start` answered NOTHING: the
//! host's 60s deadline expired as a silent timeout, the draft survived, and the
//! engine log carried no line naming the stalled step. Observed in the field as
//! "channel 'session:new' failed: app-server request timed out: session/start"
//! during a transient machine-state window, unreproducible once the window
//! closed, with no instrumentation to attribute it afterwards.
//! Root cause: the facade parked on the inner init_session ACK oneshot with no
//! budget of its own, and the startup path emitted no per-stage diagnostics.
//! Fix: the facade bounds the ACK wait (STARTUP_INIT_ACK_BUDGET_SECS by
//! default, injectable through AppServerOptions::startup_init_ack_budget) and
//! answers with a named ENGINE_ERROR; every startup stage prints one stderr
//! line so a stall names itself in the engine log.
//! Pin: a credential store whose read never returns stalls provider resolution
//! inside init_session. The facade must answer with the budget error within its
//! injected budget instead of hanging until the caller's own deadline expires.
//! Pre-fix this test sees no reply at all and dies on its outer timeout.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use klaus_agent_core::credential_store::{CredentialStore, CredentialStoreError};
use klaus_agent_core::engine::session::Session;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::super::{run_app_server, AppServerOptions};
use crate::lifecycle::{BoxError, CreateSession, SessionContext};

/// A store whose reads never settle: the stand-in for whatever external wait
/// (keychain prompt, hung filesystem, stalled network auth) froze startup.
struct StalledCredentialStore;

#[async_trait::async_trait]
impl CredentialStore for StalledCredentialStore {
    async fn get(&self, _: &str) -> Result<Option<String>, CredentialStoreError> {
        std::future::pending::<()>().await;
        Ok(None)
    }
    async fn set(&self, _: &str, _: &str) -> Result<(), CredentialStoreError> {
        unreachable!("the stalled startup must never write a credential")
    }
    async fn delete(&self, _: &str) -> Result<(), CredentialStoreError> {
        unreachable!("the stalled startup must never delete a credential")
    }
}

fn never_called_create_session() -> CreateSession {
    Arc::new(|_ctx: SessionContext| {
        Box::pin(async {
            Err("a startup that stalls before its ACK must never build a session".into())
        }) as Pin<Box<dyn Future<Output = Result<Arc<Session>, BoxError>> + Send>>
    })
}

#[tokio::test]
async fn a_stalled_startup_credential_read_fails_with_the_init_ack_budget_error() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (client_side, server_side) = tokio::io::duplex(1 << 20);
    let (server_read, server_write) = tokio::io::split(server_side);
    let (client_read, mut client_write) = tokio::io::split(client_side);
    let server = tokio::spawn(run_app_server(
        server_read,
        server_write,
        AppServerOptions {
            windows_sandbox_context: None,
            create_session: never_called_create_session(),
            credential_store: Some(Arc::new(StalledCredentialStore)),
            lifecycle: None,
            plugin_admin: None,
            permission_rules_path: Some(temp.path().join("rules.json")),
            config_path: Some(temp.path().join("config.toml")),
            startup_init_ack_budget: Some(Duration::from_millis(500)),
            session_unload_delay: None,
        },
    ));
    let mut lines = BufReader::new(client_read).lines();

    async fn send(
        client_write: &mut tokio::io::WriteHalf<tokio::io::DuplexStream>,
        message: Value,
    ) {
        let line = format!("{}\n", serde_json::to_string(&message).unwrap());
        client_write.write_all(line.as_bytes()).await.unwrap();
        client_write.flush().await.unwrap();
    }

    send(
        &mut client_write,
        json!({
            "id": 1,
            "method": "initialize",
            "params": { "clientInfo": { "name": "regression", "version": "0" } },
        }),
    )
    .await;
    let handshake_line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("initialize must answer in time")
        .expect("initialize read must not fail")
        .expect("initialize must answer before the transport closes");
    let handshake: Value = serde_json::from_str(&handshake_line).unwrap_or_default();
    assert_eq!(handshake["id"], 1, "{handshake}");

    // A stored-key auth makes init_session resolve its provider through the
    // credential store, which never returns: the inner ACK never lands.
    let started = Instant::now();
    send(
        &mut client_write,
        json!({
            "id": 2,
            "method": "session/start",
            "params": {
                "init": {
                    "cwd": ".",
                    "provider": {
                        "type": "anthropic",
                        "model": "fixture-model",
                        "baseUrl": "http://127.0.0.1:9",
                    },
                    "auth": { "type": "api_key_stored", "credentialId": "stalled" },
                },
                "resume": null,
                "fork": null,
            },
        }),
    )
    .await;

    let reply_line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect(
            "the facade must answer within its injected budget, not hang to the caller deadline",
        )
        .expect("the reply read must not fail")
        .expect("the facade must answer before the transport closes");
    let reply: Value = serde_json::from_str(&reply_line).unwrap_or_default();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the budget error took {:?}, the injected budget is 500ms: {reply}",
        started.elapsed()
    );
    assert_eq!(reply["id"], 2, "{reply}");
    let message = reply["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("did not acknowledge startup"),
        "the stalled startup must fail with the named budget error: {reply}"
    );
    server.abort();
}
