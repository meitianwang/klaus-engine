use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use klaus_agent_core::config_governance::ConfigGovernanceSource;
use klaus_agent_core::config_store;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeJson};
use klaus_agent_core::schemas::config::SandboxFeatureConfig;
use klaus_agent_core::store::session_store::{RolloutMigrationMode, RolloutMigrationRequest};
use klaus_agent_protocol::server::RolloutMaintenance;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::wire;

pub(super) struct Tasks {
    hooks: Option<Arc<RolloutMaintenance>>,
    cancelled: Arc<AtomicBool>,
    jobs: Mutex<Vec<JoinHandle<()>>>,
}

struct CompressionClaim(Arc<RolloutMaintenance>);
impl Drop for CompressionClaim {
    fn drop(&mut self) {
        self.0.compression_pending.store(false, Ordering::SeqCst);
    }
}

impl Tasks {
    pub(super) fn new(hooks: Option<Arc<RolloutMaintenance>>) -> Arc<Self> {
        Arc::new(Self {
            hooks,
            cancelled: Arc::new(AtomicBool::new(false)),
            jobs: Mutex::new(Vec::new()),
        })
    }

    /// Startup configuration and filesystem work never occupy the handshake path.
    pub(super) fn start(self: &Arc<Self>, path: PathBuf) {
        let Some(hooks) = &self.hooks else {
            return;
        };
        if hooks.startup_claimed.swap(true, Ordering::SeqCst) {
            return;
        }
        let tasks = self.clone();
        self.spawn(async move {
            let features = async {
                let document = config_store::load(&path)
                    .await
                    .map_err(|error| error.to_string())?;
                let view = config_store::resolve_view(&[
                    document.engine_layer(ConfigGovernanceSource::User)
                ])
                .map_err(|error| error.to_string())?;
                serde_json::from_value::<SandboxFeatureConfig>(
                    view.resolved
                        .get("features")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({})),
                )
                .map_err(|error| error.to_string())
            }
            .await;
            if tasks.cancelled.load(Ordering::SeqCst) {
                return;
            }
            match features {
                Ok(features) => {
                    if features.background_rollout_migration {
                        if let Err(error) = tasks
                            .migrate(RolloutMigrationRequest {
                                mode: RolloutMigrationMode::Apply,
                                expected_source_revisions: None,
                            })
                            .await
                        {
                            log_error("migration", &error);
                        }
                    }
                    if features.cold_rollout_compression {
                        if let Err(error) = tasks.kick_compression() {
                            log_error("compression", &error);
                        }
                    }
                }
                Err(error) => log_error("configuration", &error),
            }
        })
        .unwrap_or_else(|error| log_error("startup", &error));
    }

    /// A kick is only admission. Repeated kicks share one pending/running pass.
    pub(super) fn kick_compression(self: &Arc<Self>) -> Result<(), String> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Err("rollout maintenance is closing".into());
        }
        let hooks = self
            .hooks
            .clone()
            .ok_or("rollout compression requires a session store")?;
        let compress = hooks
            .compress
            .clone()
            .ok_or("the session store does not support rollout compression")?;
        if hooks.compression_pending.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let claim = CompressionClaim(hooks.clone());
        let tasks = self.clone();
        self.spawn(async move {
            let _claim = claim;
            let _lease = hooks.operation_gate.clone().lock_owned().await;
            if tasks.cancelled.load(Ordering::SeqCst) {
                return;
            }
            match std::panic::AssertUnwindSafe(compress(tasks.cancelled.clone()))
                .catch_unwind()
                .await
            {
                Ok(Ok(report)) => log_report("compression", &report),
                Ok(Err(error)) => log_error("compression", &error.to_string()),
                Err(_) => log_error("compression", "maintenance worker panicked"),
            }
        })?;
        Ok(())
    }

    /// The store transaction is retained even if the requesting RPC is dropped.
    pub(super) async fn migrate(
        self: &Arc<Self>,
        request: RolloutMigrationRequest,
    ) -> Result<RuntimeJson, String> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Err("rollout maintenance is closing".into());
        }
        let hooks = self
            .hooks
            .clone()
            .ok_or("rollout migration requires a session store")?;
        let migrate = hooks
            .migrate
            .clone()
            .ok_or("the session store does not support rollout migration")?;
        let cancelled = self.cancelled.clone();
        let (send, receive) = oneshot::channel();
        self.spawn(async move {
            let _lease = hooks.operation_gate.clone().lock_owned().await;
            let result: Result<RuntimeJson, String> = if cancelled.load(Ordering::SeqCst) {
                Err("rollout maintenance was cancelled before migration started".into())
            } else {
                match std::panic::AssertUnwindSafe(migrate(request, cancelled))
                    .catch_unwind()
                    .await
                {
                    Ok(result) => result.map_err(|error| error.to_string()),
                    Err(_) => Err("rollout migration worker panicked".into()),
                }
            };
            match &result {
                Ok(report) => log_report("migration", report),
                Err(error) => log_error("migration", error),
            }
            let _ = send.send(result);
        })?;
        receive
            .await
            .map_err(|_| "rollout migration owner ended without a result".to_string())?
    }

    fn spawn(
        &self,
        future: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), String> {
        let mut jobs = self.jobs.lock().unwrap();
        if self.cancelled.load(Ordering::SeqCst) {
            return Err("rollout maintenance is closing".into());
        }
        jobs.retain(|job| !job.is_finished());
        jobs.push(tokio::spawn(future));
        Ok(())
    }

    /// Signal cooperative cancellation, then retain ownership through atomic publication.
    pub(super) async fn shutdown(&self) {
        {
            let _admission = self.jobs.lock().unwrap();
            self.cancelled.store(true, Ordering::SeqCst);
        }
        loop {
            let jobs = std::mem::take(&mut *self.jobs.lock().unwrap());
            if jobs.is_empty() {
                break;
            }
            for job in jobs {
                if let Err(error) = job.await {
                    log_error("shutdown", &error.to_string());
                }
            }
        }
    }
}

pub(super) fn migration_request(
    params: wire::EngineMigrateRolloutsParams,
) -> Result<RolloutMigrationRequest, String> {
    let expected_source_revisions = params
        .expected_source_revisions
        .map(|raw| {
            let value = raw.to_runtime().map_err(|error| error.to_string())?.0;
            let object = value
                .as_object()
                .ok_or("expectedSourceRevisions must be an object keyed by source path")?;
            object
                .entries()
                .map(|(path, revision)| {
                    if path.is_empty() || revision.as_object().is_none() {
                        return Err(
                            "expectedSourceRevisions requires nonempty paths and revision objects"
                                .to_string(),
                        );
                    }
                    Ok((path.clone(), revision.clone()))
                })
                .collect::<Result<_, String>>()
        })
        .transpose()?;
    Ok(RolloutMigrationRequest {
        mode: match params.mode {
            wire::RolloutMigrationMode::DryRun => RolloutMigrationMode::DryRun,
            wire::RolloutMigrationMode::Apply => RolloutMigrationMode::Apply,
        },
        expected_source_revisions,
    })
}

fn log_error(kind: &str, error: &str) {
    eprintln!("[rollout-maintenance] kind={kind} error={error}");
}
fn log_report(kind: &str, report: &RuntimeJson) {
    match runtime_codec::encode_bytes(report) {
        Ok(bytes) => eprintln!(
            "[rollout-maintenance] kind={kind} report={}",
            String::from_utf8_lossy(&bytes)
        ),
        Err(error) => log_error(
            kind,
            &format!("maintenance report encoding failed: {error}"),
        ),
    }
}
