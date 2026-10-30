//! Defect: the model picker never received Kiro's consumption multiplier.
//! Root cause: `rateMultiplier`/`rateUnit` stayed inside `providerMetadata`
//! instead of being projected onto the shared `priceFactor` row field.
//! Fixing commit: b712de41.

use serde_json::{json, Value};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn default_config_root() -> PathBuf {
    std::env::var_os("KLAUS_CONFIG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".klaus")
        })
}

struct FixtureEnvironment {
    previous_root: PathBuf,
    variables: Vec<(&'static str, Option<OsString>)>,
}

impl FixtureEnvironment {
    fn new(root: &Path, management_url: &str) -> Self {
        let previous_root = default_config_root();
        crate::app_server::kiro::set_config_root(root);
        let mut variables = Vec::new();
        for (name, value) in [
            ("KLAUS_TEST", OsString::from("1")),
            ("KLAUS_KIRO_MANAGEMENT_URL", OsString::from(management_url)),
        ] {
            variables.push((name, std::env::var_os(name)));
            klaus_agent_core::runtime_process::set_process_environment_var(name, value);
        }
        Self {
            previous_root,
            variables,
        }
    }
}

impl Drop for FixtureEnvironment {
    fn drop(&mut self) {
        crate::app_server::kiro::set_config_root(&self.previous_root);
        for (name, previous) in self.variables.drain(..).rev() {
            if let Some(previous) = previous {
                klaus_agent_core::runtime_process::set_process_environment_var(name, previous);
            } else {
                klaus_agent_core::runtime_process::remove_process_environment_var(name);
            }
        }
    }
}

struct CatalogFixture {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CatalogFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn catalog_fixture() -> CatalogFixture {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let mut request = [0_u8; 8192];
            let _ = socket.read(&mut request).await;
            let body = json!({
                "defaultModel": {"modelId": "fixture-model"},
                "models": [{
                    "modelId": "fixture-model",
                    "modelName": "Fixture model",
                    "rateMultiplier": "0.5x",
                    "rateUnit": "credit",
                    "supportedInputTypes": ["TEXT"],
                    "tokenLimits": {"maxInputTokens": 200000, "maxOutputTokens": 64000}
                }]
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    CatalogFixture { url, task }
}

fn write_credential(root: &Path) {
    std::fs::write(
        root.join("kiro-auth.json"),
        json!({
            "generation": "fixture-generation",
            "credential": {
                "accessToken": "fixture-token",
                "authMethod": "social",
                "provider": "Google",
                "profileArn": "arn:aws:codewhisperer:us-east-1:123:profile/test"
            },
            "independentLogin": true,
            "localFollow": false
        })
        .to_string(),
    )
    .unwrap();
}

#[tokio::test]
async fn kiro_models_expose_rate_multiplier_as_price_factor() {
    let root = tempfile::tempdir().unwrap();
    write_credential(root.path());
    let fixture = catalog_fixture().await;
    let _environment = FixtureEnvironment::new(root.path(), &fixture.url);
    let response: Value = crate::app_server::kiro::models(false).await.unwrap();
    assert_eq!(response["models"][0]["key"], "fixture-model");
    assert_eq!(response["models"][0]["priceFactor"].as_f64(), Some(0.5));
    assert_eq!(
        response["models"][0]["providerMetadata"]["rateUnit"],
        "credit"
    );
}
