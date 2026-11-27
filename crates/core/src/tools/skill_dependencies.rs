//! Read-only dependency probes use the same recovered environment and command
//! resolver as shell tools. Only missing requirement names leave this module.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use super::shell::{build_shell_invocation, BuildShellOptions, ShellKind};
use super::shell_env_snapshot::{capture_login_shell_env, CaptureShellEnvOptions};
use crate::runtime_process::{
    ParentEnvSnapshot, RuntimeCompletedProcessRequest, RuntimeCompletedProcessSurface, RuntimeEnv,
    RuntimeProcessContext,
};
use crate::runtime_values::{RuntimePath, RuntimeString};

/// Endpoint-scoped, single-flight cache. Failed captures are retried.
#[derive(Default)]
pub struct SkillDependencyChecker {
    environments: Mutex<HashMap<String, CapturedEnvironment>>,
    #[cfg(test)]
    shell: Option<String>,
}

type CapturedEnvironment = Arc<Mutex<Option<RuntimeEnv>>>;

#[derive(Debug, PartialEq, Eq)]
pub struct SkillDependencyResult {
    pub missing_bins: Vec<String>,
    pub missing_env: Vec<String>,
    pub unsupported: bool,
}

impl SkillDependencyChecker {
    pub async fn check(
        &self,
        context: &RuntimeProcessContext,
        cwd: &str,
        bins: &[String],
        env: &[String],
        os: &[String],
    ) -> Result<SkillDependencyResult, String> {
        validate_requirements(cwd, bins, env, os)?;
        if bins.is_empty() && env.is_empty() {
            return Ok(SkillDependencyResult {
                missing_bins: Vec::new(),
                missing_env: Vec::new(),
                unsupported: !os.is_empty() && !os.iter().any(|value| value == platform()),
            });
        }
        let recovered = if cfg!(windows) {
            RuntimeEnv::new()
        } else {
            let cell = {
                let mut environments = self.environments.lock().await;
                if environments.len() >= 64 && !environments.contains_key(cwd) {
                    environments.clear();
                }
                environments.entry(cwd.to_owned()).or_default().clone()
            };
            let mut captured = cell.lock().await;
            if let Some(existing) = captured.as_ref() {
                existing.clone()
            } else {
                #[cfg(test)]
                let shell = self.shell.clone();
                #[cfg(not(test))]
                let shell = None;
                let recovered = capture_login_shell_env(
                    context,
                    &CaptureShellEnvOptions {
                        shell,
                        cwd: RuntimePath::from(cwd),
                        timeout_ms: None,
                    },
                )
                .await
                .filter(|environment| {
                    environment
                        .get(&RuntimeString::from("PATH"))
                        .is_some_and(|value| !value.is_empty())
                })
                .ok_or("Unable to recover the login-shell PATH for dependency checks")?;
                *captured = Some(recovered.clone());
                recovered
            }
        };
        check_with_environment(context, cwd, bins, env, os, recovered).await
    }
}

/// Validate the bounded, data-only facade inputs before starting any shell.
pub fn validate_requirements(
    cwd: &str,
    bins: &[String],
    env: &[String],
    os: &[String],
) -> Result<(), String> {
    if cwd.contains('\0')
        || !std::path::Path::new(cwd).is_absolute()
        || !std::path::Path::new(cwd).is_dir()
    {
        return Err("cwd must be an existing absolute directory".into());
    }
    if [bins, env, os].iter().any(|values| {
        values.len() > 256
            || values.iter().any(|value| {
                value.is_empty() || value.len() > 1024 || value.chars().any(char::is_control)
            })
    }) {
        return Err("dependency requirements must contain at most 256 bounded names".into());
    }
    if env.iter().any(|key| {
        !key.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphabetic() || (index > 0 && byte.is_ascii_digit())
        })
    }) {
        return Err("env entries must be shell variable names".into());
    }
    Ok(())
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn probe_script(bins: &[String], kind: ShellKind) -> Result<String, String> {
    let mut script = String::new();
    match kind {
        ShellKind::Bash => {
            for (index, bin) in bins.iter().enumerate() {
                // The resolver can return a non-executable fallback, so check its mode.
                script.push_str(&format!(
                    "resolved=$(type -P -- {} 2>/dev/null) && [ -f \"$resolved\" ] && [ -x \"$resolved\" ] || printf 'b{index}\\n';\n",
                    quote(bin)
                ));
            }
            script.push_str("printf 'done\\n'\n");
        }
        ShellKind::Pwsh | ShellKind::Powershell => {
            for (index, bin) in bins.iter().enumerate() {
                let literal = bin.replace('\'', "''");
                script.push_str(&format!(
                    "if (-not (Get-Command -Name ([WildcardPattern]::Escape('{literal}')) -CommandType Application,ExternalScript -ErrorAction SilentlyContinue)) {{ Write-Output 'b{index}' }}\n"
                ));
            }
            script.push_str("Write-Output 'done'\nexit 0\n");
        }
        ShellKind::Cmd => return Err("Dependency checks require Bash or PowerShell".into()),
    }
    Ok(script)
}

async fn check_with_environment(
    context: &RuntimeProcessContext,
    cwd: &str,
    bins: &[String],
    env: &[String],
    os: &[String],
    recovered: RuntimeEnv,
) -> Result<SkillDependencyResult, String> {
    let parent = ParentEnvSnapshot::capture();
    let environment = crate::sandbox::process::scrub_env(&parent, &recovered);
    let missing_env = env
        .iter()
        .filter(|key| {
            if cfg!(windows) {
                !environment.entries().any(|(name, _)| {
                    name.try_to_utf8()
                        .is_ok_and(|name| name.eq_ignore_ascii_case(key))
                })
            } else {
                let key = RuntimeString::from(key.as_str());
                !environment.contains_key(&key)
            }
        })
        .cloned()
        .collect();
    if bins.is_empty() {
        return Ok(SkillDependencyResult {
            missing_bins: Vec::new(),
            missing_env,
            unsupported: !os.is_empty() && !os.iter().any(|value| value == platform()),
        });
    }
    let kind = if cfg!(windows) {
        super::shell::resolve_windows_shell(&super::shell::process_env()).kind
    } else {
        ShellKind::Bash
    };
    let invocation =
        build_shell_invocation(&probe_script(bins, kind)?, BuildShellOptions::default());
    let output = context
        .exec_completed(
            RuntimeCompletedProcessRequest::new(
                RuntimeCompletedProcessSurface::SkillDependencyCheck,
                RuntimeString::from(invocation[0].as_str()),
            )
            .with_args(
                invocation[1..]
                    .iter()
                    .map(|value| RuntimeString::from(value.as_str()))
                    .collect(),
            )
            .with_cwd(RuntimePath::from(cwd))
            .with_environment(environment)
            .with_timeout(Duration::from_secs(10))
            .with_max_stdout_bytes(64 * 1024),
        )
        .await
        .map_err(|_| "Unable to execute the skill dependency probe".to_owned())?;
    if !output.status.success() {
        return Err("The skill dependency probe failed".into());
    }
    let mut result = parse_result(&output.stdout, bins, os)?;
    result.missing_env = missing_env;
    Ok(result)
}

fn parse_result(
    output: &[u8],
    bins: &[String],
    os: &[String],
) -> Result<SkillDependencyResult, String> {
    let text = std::str::from_utf8(output).map_err(|_| "Invalid skill dependency probe output")?;
    let mut lines = text.lines().collect::<Vec<_>>();
    if lines.pop() != Some("done") {
        return Err("Incomplete skill dependency probe output".into());
    }
    let mut result = SkillDependencyResult {
        missing_bins: Vec::new(),
        missing_env: Vec::new(),
        unsupported: !os.is_empty() && !os.iter().any(|value| value == platform()),
    };
    for line in lines {
        let (prefix, number) = line
            .split_at_checked(1)
            .ok_or("Invalid skill dependency probe output")?;
        let index = number
            .parse::<usize>()
            .map_err(|_| "Invalid skill dependency probe output")?;
        match prefix {
            "b" if index < bins.len() => result.missing_bins.push(bins[index].clone()),
            _ => return Err("Invalid skill dependency probe output".into()),
        }
    }
    Ok(result)
}

fn platform() -> &'static str {
    if cfg!(windows) {
        "win32"
    } else if cfg!(target_os = "macos") {
        "darwin"
    } else {
        std::env::consts::OS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    struct TempDir(std::path::PathBuf);

    #[cfg(unix)]
    impl TempDir {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("klaus-skill-deps-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    #[cfg(unix)]
    impl Drop for TempDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dependency_probe_uses_recovered_path_without_running_dependencies() {
        use std::os::unix::fs::PermissionsExt;
        let root = TempDir::new();
        let executable = root.path().join("skill-fixture");
        std::fs::write(&executable, "#!/bin/sh\nexit 99\n").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(root.path().join("not-executable"), "data").unwrap();
        let mut recovered = RuntimeEnv::new();
        recovered.insert(
            "PATH".into(),
            format!("{}:/bin:/usr/bin", root.path().display()).into(),
        );
        recovered.insert("SKILL_FIXTURE_ENV".into(), "private-value".into());
        let bins = vec![
            "skill-fixture".into(),
            "missing-skill-fixture".into(),
            "printf".into(),
            "not-executable".into(),
        ];
        let env = vec![
            "SKILL_FIXTURE_ENV".into(),
            "MISSING_SKILL_FIXTURE_ENV".into(),
        ];
        let result = check_with_environment(
            &crate::runtime_process::test_runtime_process_context(),
            root.path().to_str().unwrap(),
            &bins,
            &env,
            &["other-platform".into()],
            recovered,
        )
        .await
        .unwrap();
        assert_eq!(
            result.missing_bins,
            vec!["missing-skill-fixture", "not-executable"]
        );
        assert_eq!(result.missing_env, vec!["MISSING_SKILL_FIXTURE_ENV"]);
        assert!(result.unsupported);
    }

    #[test]
    fn dependency_probe_rejects_incomplete_and_unexpected_output() {
        for output in [
            b"".as_slice(),
            b"b0\n",
            b"secret-value\ndone\n",
            b"b99\ndone\n",
        ] {
            assert!(parse_result(output, &["node".into()], &[]).is_err());
        }
    }

    #[test]
    fn dependency_probe_quotes_binary_names_as_data() {
        let script =
            probe_script(&["tool'; touch /tmp/forbidden; '".into()], ShellKind::Bash).unwrap();
        assert!(script.contains("'tool'\\''; touch /tmp/forbidden; '\\'''"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dependency_checker_retries_failed_capture_and_reuses_successful_capture() {
        use std::os::unix::fs::PermissionsExt;
        let root = TempDir::new();
        let shell = root.path().join("capture-shell");
        std::fs::write(&shell, "#!/bin/sh\nexit 1\n").unwrap();
        std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o755)).unwrap();
        let checker = SkillDependencyChecker {
            shell: Some(shell.to_str().unwrap().to_owned()),
            ..Default::default()
        };
        let context = crate::runtime_process::test_runtime_process_context();
        let bins = vec!["missing-skill-fixture".into()];
        let cwd = root.path().to_str().unwrap();
        assert!(checker.check(&context, cwd, &bins, &[], &[]).await.is_err());
        let capture_log = root.path().join("capture-count");
        std::fs::write(
            &shell,
            format!(
                "#!/bin/sh\nprintf 'capture\\n' >> {}\nprintf 'PATH=/bin:/usr/bin\\000'\n",
                quote(capture_log.to_str().unwrap())
            ),
        )
        .unwrap();
        let (first, second) = tokio::join!(
            checker.check(&context, cwd, &bins, &[], &[]),
            checker.check(&context, cwd, &bins, &[], &[]),
        );
        assert_eq!(first.unwrap().missing_bins, bins);
        assert_eq!(second.unwrap().missing_bins, bins);
        assert_eq!(std::fs::read_to_string(&capture_log).unwrap(), "capture\n");
        std::fs::write(&shell, "#!/bin/sh\nexit 1\n").unwrap();
        assert_eq!(
            checker
                .check(&context, cwd, &bins, &[], &[])
                .await
                .unwrap()
                .missing_bins,
            bins
        );
    }
}
