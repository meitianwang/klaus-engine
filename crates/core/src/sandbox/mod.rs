//! Platform sandbox selection from the effective filesystem mode.

pub mod bubblewrap;
pub mod coverage;
pub mod denial;
pub mod linux;
pub mod plan_readonly;
pub mod policy;
pub mod process;
pub mod process_hardening;
pub mod runtime_policy;
pub mod seatbelt;
pub mod task_scope;
pub mod windows;
pub mod windows_mxc;

pub use linux::LinuxSandbox;
pub use process::{scrub_env, ProcessSandbox};
pub use seatbelt::SeatbeltSandbox;
pub use windows::WindowsSandbox;
pub use windows_mxc::WindowsMxcSandbox;

use std::sync::Arc;

use crate::types::permission::SandboxMode;
use crate::types::sandbox::SandboxBackend;

/// Resolve a filesystem mode to its actual platform executor.
/// Enabled native execution never degrades when its executor is unavailable.
pub fn create_sandbox_for_mode(mode: SandboxMode) -> Result<Arc<dyn SandboxBackend>, String> {
    create_sandbox_for_mode_with_config(mode, None, None)
}

/// Resolve the known Windows preference after configuration loading. The raw
/// Windows mode remains unchanged for configuration inheritance.
pub fn create_sandbox_for_mode_with_engine_config(
    mode: SandboxMode,
    config: &crate::schemas::config::EngineConfig,
    config_home: Option<&crate::runtime_values::RuntimePath>,
) -> Result<Arc<dyn SandboxBackend>, String> {
    let effective_windows = effective_windows_config(config)?;
    let backend =
        create_sandbox_for_mode_with_config(mode, effective_windows.as_ref(), config_home)?;
    if cfg!(target_os = "linux")
        && config.features.use_legacy_landlock
        && backend.kind() != crate::types::sandbox::SandboxKind::Process
    {
        return Ok(Arc::new(LinuxSandbox::with_legacy_landlock()));
    }
    Ok(backend)
}

/// Resolve the local Windows selection for execution and preparation inspection.
/// A preference is eligible only after its opt-outs and native availability
/// have been checked; the configured mode itself is never rewritten.
pub fn effective_windows_config(
    config: &crate::schemas::config::EngineConfig,
) -> Result<Option<crate::schemas::config::WindowsConfig>, String> {
    use crate::schemas::config::WindowsSandboxMode;
    if config.windows.as_ref().is_some_and(|windows| {
        windows.sandbox == Some(WindowsSandboxMode::Mxc) && windows.allow_mxc == Some(false)
    }) {
        return Err(
            "windows.sandbox = \"mxc\" is not allowed when windows.allow_mxc = false".into(),
        );
    }
    let mut configured = config.windows.clone();
    if configured
        .as_ref()
        .is_none_or(|windows| windows.sandbox.is_none())
    {
        let legacy_mode = if config.features.elevated_windows_sandbox {
            Some(WindowsSandboxMode::Elevated)
        } else if config.features.experimental_windows_sandbox
            || config.features.enable_experimental_windows_sandbox
        {
            Some(WindowsSandboxMode::Unelevated)
        } else {
            None
        };
        if let Some(mode) = legacy_mode {
            configured.get_or_insert_with(Default::default).sandbox = Some(mode);
        }
    }
    Ok(resolve_windows_preference(
        configured.as_ref(),
        config.features.prefer_mxc,
        config.network_proxy.enabled,
        config.network_proxy.allow_local_binding,
        || {
            #[cfg(windows)]
            {
                klaus_sandbox_win::mxc::is_available()
            }
            #[cfg(not(windows))]
            {
                false
            }
        },
    ))
}

fn resolve_windows_preference(
    configured: Option<&crate::schemas::config::WindowsConfig>,
    prefer_mxc: bool,
    proxy_enabled: bool,
    allow_local_binding: Option<bool>,
    native_available: impl FnOnce() -> bool,
) -> Option<crate::schemas::config::WindowsConfig> {
    use crate::schemas::config::WindowsSandboxMode;
    if prefer_mxc
        && configured.is_none_or(|windows| windows.allow_mxc != Some(false))
        && !(proxy_enabled && allow_local_binding == Some(false))
        && native_available()
    {
        let mut effective = configured.cloned().unwrap_or_default();
        effective.sandbox = Some(WindowsSandboxMode::Mxc);
        Some(effective)
    } else {
        configured.cloned()
    }
}

/// Resolve the captured Windows setting separately from the filesystem mode.
/// With no Windows setting, native Windows confinement is disabled by default.
pub fn create_sandbox_for_mode_with_config(
    mode: SandboxMode,
    windows: Option<&crate::schemas::config::WindowsConfig>,
    config_home: Option<&crate::runtime_values::RuntimePath>,
) -> Result<Arc<dyn SandboxBackend>, String> {
    if mode == SandboxMode::DangerFullAccess {
        return Ok(Arc::new(ProcessSandbox::new()));
    }
    let backend: Arc<dyn SandboxBackend> = if cfg!(target_os = "macos") {
        Arc::new(SeatbeltSandbox::new())
    } else if cfg!(target_os = "linux") {
        Arc::new(LinuxSandbox::new())
    } else if cfg!(target_os = "windows") {
        use crate::schemas::config::WindowsSandboxMode;
        let Some(selection) = windows.and_then(|config| config.sandbox) else {
            eprintln!("Windows native sandboxing is disabled because windows.sandbox is not configured; commands run without operating-system confinement.");
            return Ok(Arc::new(ProcessSandbox::new()));
        };
        let level = match selection {
            WindowsSandboxMode::Unelevated => WindowsSandboxMode::Unelevated,
            WindowsSandboxMode::Elevated => WindowsSandboxMode::Elevated,
            WindowsSandboxMode::Mxc => {
                if windows.is_some_and(|config| config.allow_mxc == Some(false)) {
                    return Err(
                        "windows.sandbox = \"mxc\" is not allowed when windows.allow_mxc = false"
                            .into(),
                    );
                }
                let backend = Arc::new(WindowsMxcSandbox::new());
                if !backend.is_available() {
                    return Err("native MXC is unavailable on this executor".into());
                }
                return Ok(backend);
            }
        };
        Arc::new(WindowsSandbox::new_native(
            level,
            config_home.ok_or("Native Windows sandboxing requires a configuration home")?,
        )?)
    } else {
        return Err("This platform cannot enforce the requested filesystem sandbox.".into());
    };
    if !backend.is_available() {
        return Err(format!(
            "The {} sandbox executor is unavailable; confined execution cannot run without it.",
            std::env::consts::OS
        ));
    }
    Ok(backend)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::sandbox::SandboxKind;

    #[test]
    fn unrestricted_mode_uses_the_process_executor() {
        assert_eq!(
            create_sandbox_for_mode(SandboxMode::DangerFullAccess)
                .unwrap()
                .kind(),
            SandboxKind::Process
        );
    }

    #[test]
    fn enabled_platform_modes_never_return_an_unrestricted_executor() {
        if cfg!(windows) {
            return;
        }
        for mode in [SandboxMode::ReadOnly, SandboxMode::WorkspaceWrite] {
            if let Ok(backend) = create_sandbox_for_mode(mode) {
                assert_ne!(backend.kind(), SandboxKind::Process);
            }
        }
    }
    #[test]
    fn mxc_preference_requires_opt_in_eligibility_and_native_availability() {
        use crate::schemas::config::{WindowsConfig, WindowsSandboxMode};
        for selected in [
            None,
            Some(WindowsSandboxMode::Unelevated),
            Some(WindowsSandboxMode::Elevated),
            Some(WindowsSandboxMode::Mxc),
        ] {
            let configured = selected.map(|sandbox| WindowsConfig {
                sandbox: Some(sandbox),
                allow_mxc: None,
            });
            let effective =
                resolve_windows_preference(configured.as_ref(), true, true, None, || true).unwrap();
            assert_eq!(effective.sandbox, Some(WindowsSandboxMode::Mxc));
            assert_eq!(
                configured.as_ref().and_then(|windows| windows.sandbox),
                selected
            );
            assert_eq!(
                resolve_windows_preference(configured.as_ref(), true, true, None, || false),
                configured
            );
            assert_eq!(
                resolve_windows_preference(configured.as_ref(), false, true, None, || panic!(
                    "disabled preference must not probe"
                )),
                configured
            );
        }
    }

    #[test]
    fn mxc_preference_preserves_opt_out_and_explicit_binding_denial() {
        use crate::schemas::config::{WindowsConfig, WindowsSandboxMode};
        let configured = WindowsConfig {
            sandbox: Some(WindowsSandboxMode::Unelevated),
            allow_mxc: Some(false),
        };
        assert_eq!(
            resolve_windows_preference(Some(&configured), true, false, None, || panic!(
                "opt-out must not probe"
            )),
            Some(configured)
        );
        let configured = WindowsConfig {
            sandbox: Some(WindowsSandboxMode::Elevated),
            allow_mxc: None,
        };
        assert_eq!(
            resolve_windows_preference(Some(&configured), true, true, Some(false), || panic!(
                "binding denial must not probe"
            )),
            Some(configured.clone())
        );
        assert_eq!(
            resolve_windows_preference(Some(&configured), true, false, Some(false), || true)
                .unwrap()
                .sandbox,
            Some(WindowsSandboxMode::Mxc)
        );
    }
    #[test]
    fn effective_windows_selection_rejects_the_configured_mxc_opt_out_on_every_host() {
        use crate::schemas::config::{EngineConfig, WindowsConfig, WindowsSandboxMode};
        let config = EngineConfig {
            windows: Some(WindowsConfig {
                sandbox: Some(WindowsSandboxMode::Mxc),
                allow_mxc: Some(false),
            }),
            ..Default::default()
        };
        let error = effective_windows_config(&config).unwrap_err();
        assert_eq!(
            error,
            "windows.sandbox = \"mxc\" is not allowed when windows.allow_mxc = false"
        );
        assert!(create_sandbox_for_mode_with_engine_config(
            SandboxMode::DangerFullAccess,
            &config,
            None
        )
        .is_err());
    }

    #[test]
    fn execution_and_preparation_preserve_the_configured_mode_without_preference() {
        use crate::schemas::config::{EngineConfig, WindowsConfig, WindowsSandboxMode};
        let configured = WindowsConfig {
            sandbox: Some(WindowsSandboxMode::Elevated),
            allow_mxc: None,
        };
        let config = EngineConfig {
            windows: Some(configured.clone()),
            ..Default::default()
        };
        assert!(!config.features.prefer_mxc);
        assert_eq!(effective_windows_config(&config).unwrap(), Some(configured));
    }
}
