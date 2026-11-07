//! Native Windows preparation without creating a conversation or launching a workload.

use std::path::{Path, PathBuf};

use klaus_agent_core::config_governance::ConfigGovernanceSource;
use klaus_agent_core::config_store;
use klaus_agent_core::engine::session::{config_layer_from_value, resolve_config_layers};
use klaus_agent_core::network_proxy::NetworkProxyConfig;
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use klaus_agent_core::schemas::config::{EngineConfig, WindowsSandboxMode};
use klaus_agent_core::types::permission::SandboxMode;
use klaus_agent_core::types::sandbox::SandboxPolicy;
use serde_json::Value;

use super::wire::{
    WindowsSandboxReadiness, WindowsSandboxReadinessResult, WindowsSandboxSetupMode,
};

/// Startup state shared by every transport of one engine endpoint.
#[derive(Clone, Default)]
pub struct Context {
    pub cwd: Option<PathBuf>,
    pub config: Option<Value>,
    #[cfg(test)]
    pub(super) driver: Option<std::sync::Arc<dyn PreparationDriver>>,
}

pub(super) struct SetupPlan {
    pub cwd: PathBuf,
    pub config_home: PathBuf,
    pub policy: SandboxPolicy,
    pub service_enabled: bool,
    pub service_proxy: Option<NetworkProxyConfig>,
}

#[cfg(test)]
pub(super) trait PreparationDriver: Send + Sync {
    fn is_windows(&self) -> bool;
    fn setup_is_complete(&self, home: &Path) -> bool;
    fn prepare(
        &self,
        mode: WindowsSandboxSetupMode,
        plan: SetupPlan,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>;
}

impl Context {
    pub fn new(cwd: Option<PathBuf>, config: Option<Value>) -> Self {
        Self {
            cwd,
            config,
            #[cfg(test)]
            driver: None,
        }
    }

    pub(crate) fn capture_cwd(mut self) -> Self {
        if self.cwd.is_none() {
            self.cwd = std::env::current_dir().ok();
        }
        self
    }

    fn is_windows(&self) -> bool {
        #[cfg(test)]
        if let Some(driver) = &self.driver {
            return driver.is_windows();
        }
        cfg!(windows)
    }

    fn setup_is_complete(&self, home: &Path) -> bool {
        #[cfg(test)]
        if let Some(driver) = &self.driver {
            return driver.setup_is_complete(home);
        }
        #[cfg(windows)]
        {
            klaus_sandbox_win::elevated::setup_is_complete(home)
        }
        #[cfg(not(windows))]
        {
            unreachable!(
                "Native Windows preparation state is unavailable for {}",
                home.display()
            )
        }
    }

    pub(super) async fn readiness(
        &self,
        config_path: &Path,
    ) -> Result<WindowsSandboxReadinessResult, String> {
        if !self.is_windows() {
            return Ok(WindowsSandboxReadinessResult {
                status: WindowsSandboxReadiness::NotConfigured,
            });
        }
        let (config, _, config_home, _) = self.load_config(config_path, None).await?;
        let selection = klaus_agent_core::sandbox::effective_windows_config(&config)?
            .and_then(|windows| windows.sandbox);
        let status = match selection {
            Some(WindowsSandboxMode::Mxc | WindowsSandboxMode::Unelevated) => {
                WindowsSandboxReadiness::Ready
            }
            Some(WindowsSandboxMode::Elevated) if self.setup_is_complete(&config_home) => {
                WindowsSandboxReadiness::Ready
            }
            Some(WindowsSandboxMode::Elevated) => WindowsSandboxReadiness::UpdateRequired,
            None => WindowsSandboxReadiness::NotConfigured,
        };
        Ok(WindowsSandboxReadinessResult { status })
    }

    pub(super) async fn load_setup(
        &self,
        config_path: &Path,
        requested_cwd: Option<PathBuf>,
    ) -> Result<SetupPlan, String> {
        let has_workspace = requested_cwd.is_some();
        let (config, cwd, config_home, proxy_configured) =
            self.load_config(config_path, requested_cwd).await?;
        let policy = setup_policy(&config, &cwd, has_workspace)?;
        let plan = SetupPlan {
            cwd,
            config_home,
            policy,
            service_enabled: config.features.windows_sandbox_service,
            service_proxy: proxy_configured.then_some(config.network_proxy),
        };
        #[cfg(windows)]
        validate_native_setup(&plan)?;
        Ok(plan)
    }

    async fn load_config(
        &self,
        config_path: &Path,
        requested_cwd: Option<PathBuf>,
    ) -> Result<(EngineConfig, PathBuf, PathBuf, bool), String> {
        let cwd = requested_cwd
            .or_else(|| self.cwd.clone())
            .ok_or("Engine working directory is unavailable")?;
        if !cwd.is_absolute() {
            return Err("Engine working directory must be absolute".into());
        }
        let user = config_store::load(config_path)
            .await
            .map_err(|error| error.to_string())?;
        let mut layers = vec![user.engine_layer(ConfigGovernanceSource::User)];
        let project_path = config_store::project_config_path(&cwd);
        if project_path != config_path
            && tokio::fs::try_exists(&project_path)
                .await
                .map_err(|error| error.to_string())?
        {
            let project = config_store::load(&project_path)
                .await
                .map_err(|error| error.to_string())?;
            layers.push(project.engine_layer(ConfigGovernanceSource::Project));
        }
        if let Some(config) = &self.config {
            let mut config = config.clone();
            let fields = config
                .as_object_mut()
                .ok_or("Endpoint config must be an object")?;
            klaus_agent_core::schemas::config::normalize_sandbox_config(fields)?;
            klaus_agent_core::schemas::config::normalize_sandbox_workspace_roots(
                fields,
                Some(&cwd),
            )?;
            layers.push(config_layer_from_value(
                ConfigGovernanceSource::Env,
                config,
            )?);
        }
        // Capture explicit table presence before typed configuration defaults
        // erase the distinction between an omitted and disabled proxy.
        let proxy_configured = layers
            .iter()
            .any(|layer| layer.values.contains_key("networkProxy"));
        let config = resolve_config_layers(layers)?;
        let config_home = super::model_sources::config_root_of(config_path);
        Ok((config, cwd, config_home, proxy_configured))
    }

    pub(super) async fn prepare(
        &self,
        mode: WindowsSandboxSetupMode,
        plan: SetupPlan,
    ) -> Result<(), String> {
        #[cfg(test)]
        if let Some(driver) = &self.driver {
            return driver.prepare(mode, plan).await;
        }
        prepare_native(mode, plan).await
    }
}

fn path_string(path: &Path) -> Result<RuntimeString, String> {
    path.to_str()
        .map(RuntimeString::from)
        .ok_or_else(|| "Windows preparation path must be Unicode".into())
}

fn setup_policy(
    config: &EngineConfig,
    cwd: &Path,
    has_workspace: bool,
) -> Result<SandboxPolicy, String> {
    let mode = config.sandbox_mode.unwrap_or(SandboxMode::ReadOnly);
    let table = &config.sandbox_workspace_write;
    let mut roots = if has_workspace {
        table.writable_roots.clone()
    } else {
        Vec::new()
    };
    if has_workspace {
        let cwd = path_string(cwd)?;
        if !roots.contains(&cwd) {
            roots.push(cwd);
        }
    }
    let mut policy = SandboxPolicy {
        writable_roots: roots,
        exclude_cwd: Some(true),
        exclude_tmpdir_env_var: Some(table.exclude_tmpdir_env_var),
        exclude_slash_tmp: Some(table.exclude_slash_tmp),
        network_allowed: match mode {
            SandboxMode::ReadOnly => false,
            SandboxMode::WorkspaceWrite => table.network_access,
            SandboxMode::DangerFullAccess => true,
        },
        unconfined_writes: (mode == SandboxMode::DangerFullAccess).then_some(true),
        ..Default::default()
    };
    if mode == SandboxMode::ReadOnly {
        policy.filesystem_read_only = true;
        policy.writable_roots.clear();
        policy.exclude_tmpdir_env_var = Some(true);
        policy.exclude_slash_tmp = Some(true);
        policy.unconfined_writes = None;
    }
    if let Some(scope) = &config.task_resource_scope {
        let scope = klaus_agent_core::permission::task_scope::CanonicalTaskResourceScope::bind(
            scope,
            &RuntimePath::from(path_string(cwd)?),
        )?;
        policy.readable_roots = Some(
            scope
                .readable_roots()
                .into_iter()
                .map(RuntimePath::into_runtime_string)
                .collect(),
        );
        let allowed = scope
            .writable_roots()
            .into_iter()
            .map(RuntimePath::into_runtime_string)
            .collect::<Vec<_>>();
        policy.writable_roots = if policy.filesystem_read_only {
            Vec::new()
        } else if policy.unconfined_writes == Some(true) {
            allowed
        } else {
            intersect_roots(&policy.writable_roots, &allowed)?
        };
        policy.unconfined_writes = None;
        policy.exclude_tmpdir_env_var = Some(true);
        policy.exclude_slash_tmp = Some(true);
        policy.network_allowed &= scope.network_allowed;
    }
    Ok(policy)
}

fn intersect_roots(
    requested: &[RuntimeString],
    allowed: &[RuntimeString],
) -> Result<Vec<RuntimeString>, String> {
    let mut result: Vec<RuntimeString> = Vec::new();
    for requested in requested {
        let requested_path =
            PathBuf::from(requested.try_to_utf8().map_err(|error| error.to_string())?);
        for allowed in allowed {
            let allowed_path =
                PathBuf::from(allowed.try_to_utf8().map_err(|error| error.to_string())?);
            let root = if requested_path.starts_with(&allowed_path) {
                Some(requested)
            } else if allowed_path.starts_with(&requested_path) {
                Some(allowed)
            } else {
                None
            };
            if let Some(root) = root.filter(|root| !result.contains(*root)) {
                result.push(root.clone());
            }
        }
    }
    Ok(result)
}

#[cfg(windows)]
fn validate_native_setup(plan: &SetupPlan) -> Result<(), String> {
    klaus_sandbox_win::AbsPath::new(
        path_string(&plan.cwd)?
            .try_to_utf8()
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    for root in plan
        .policy
        .writable_roots
        .iter()
        .chain(plan.policy.readable_roots.iter().flatten())
    {
        klaus_sandbox_win::AbsPath::new(root.try_to_utf8().map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[cfg(windows)]
async fn prepare_native(mode: WindowsSandboxSetupMode, plan: SetupPlan) -> Result<(), String> {
    use klaus_agent_core::types::sandbox::ExecRequest;
    let request = klaus_agent_core::sandbox::windows::preparation_request(
        &ExecRequest {
            argv: Vec::new(),
            cwd: RuntimePath::from(path_string(&plan.cwd)?),
            env: None,
            timeout_ms: None,
        },
        &plan.policy,
    );
    match mode {
        WindowsSandboxSetupMode::Elevated => {
            if plan.service_enabled && plan.policy.unconfined_writes != Some(true) {
                let configuration = match &plan.service_proxy {
                    Some(proxy) => proxy.windows_sandbox_proxy_configuration(),
                    None => Ok(klaus_sandbox_win::elevated::provisioning_configuration(
                        &request,
                    )),
                };
                match configuration {
                    Ok((settings, listeners)) => {
                        klaus_sandbox_win::elevated::validate_request(&request)
                            .map_err(|error| error.to_string())?;
                        // Both service outcomes continue through shared setup;
                        // authenticated failures propagate before it can run.
                        klaus_sandbox_win::service::provision(&plan.config_home, settings, listeners)
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    Err(error) => eprintln!(
                        "Windows sandbox service does not support the configured proxy listeners; falling back to elevated setup: {error}"
                    ),
                }
            }
            klaus_sandbox_win::elevated::prepare(request, plan.config_home)
                .await
                .map_err(|error| error.to_string())
        }
        WindowsSandboxSetupMode::Unelevated => {
            klaus_sandbox_win::unelevated::prepare_native(request, plan.config_home).await
        }
    }
}

#[cfg(not(windows))]
async fn prepare_native(mode: WindowsSandboxSetupMode, plan: SetupPlan) -> Result<(), String> {
    let SetupPlan {
        cwd,
        config_home,
        policy,
        service_enabled,
        service_proxy,
    } = plan;
    drop((cwd, config_home, policy, service_enabled, service_proxy));
    match mode {
        WindowsSandboxSetupMode::Elevated | WindowsSandboxSetupMode::Unelevated => {
            Err("Windows sandbox setup is only supported on Windows".into())
        }
    }
}
