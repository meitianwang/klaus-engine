//! The `process` sandbox backend and the shared environment scrubber.
//!
//! This backend performs env scrubbing + cwd + process-group management only.
//! It is NOT filesystem isolation — a `bash` command can read/write anywhere
//! the invoking user can. See docs/security-model.md.

use crate::runtime_process::{ParentEnvSnapshot, RuntimeEnv};
use crate::runtime_values::RuntimeString;
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use std::collections::HashSet;
use std::sync::LazyLock;

/// Env vars that are safe to pass through to child processes.
static SAFE_ENV_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    HashSet::from([
        // POSIX
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "TERM",
        "TMPDIR",
        "TZ",
        "PWD",
        "NODE_ENV",
        // Windows essentials — PowerShell/cmd will not launch or resolve commands
        // without SystemRoot/Path/PATHEXT/COMSPEC; the rest are commonly required by
        // tools. Windows env names are case-insensitive but the OS casing is
        // preserved, so both `Path` and `PATH` are listed.
        "SystemRoot",
        "windir",
        "Path",
        "PATHEXT",
        "COMSPEC",
        "ComSpec",
        "USERPROFILE",
        "TEMP",
        "TMP",
        "APPDATA",
        "LOCALAPPDATA",
        "PROGRAMDATA",
        "PROGRAMFILES",
        "HOMEDRIVE",
        "HOMEPATH",
        "NUMBER_OF_PROCESSORS",
        "PROCESSOR_ARCHITECTURE",
        "SYSTEMDRIVE",
        // Legitimate tooling vars that the secret heuristic below false-positives on
        // (they contain AUTH/SESSION). Dropping these breaks real workflows — e.g.
        // without SSH_AUTH_SOCK, `git push` / ssh inside a command cannot reach the
        // ssh-agent. The allowlist is checked first, so these survive the scrub.
        "SSH_AUTH_SOCK",
        "SSH_AGENT_PID",
        "SSH_CONNECTION",
        "SESSIONNAME",
    ])
});

/// ASCII case-insensitive secret-bearing fragments.
const SENSITIVE_FRAGMENTS: [&[u8]; 9] = [
    b"KEY",
    b"TOKEN",
    b"SECRET",
    b"PASSWORD",
    b"PASSWD",
    b"CREDENTIAL",
    b"AUTH",
    b"SESSION",
    b"COOKIE",
];

fn is_safe_key(key: &RuntimeString) -> bool {
    SAFE_ENV_KEYS.iter().any(|candidate| {
        key.as_utf16()
            .iter()
            .copied()
            .eq(candidate.as_bytes().iter().copied().map(u16::from))
    })
}

fn looks_sensitive(key: &RuntimeString) -> bool {
    let units = key.as_utf16();
    SENSITIVE_FRAGMENTS.iter().any(|fragment| {
        units.windows(fragment.len()).any(|window| {
            window.iter().zip(fragment.iter()).all(|(unit, expected)| {
                let unit = *unit;
                unit <= u16::from(u8::MAX) && (unit as u8).to_ascii_uppercase() == *expected
            })
        })
    })
}

/// Build the child environment: INHERIT the parent environment (so dev tooling —
/// JAVA_HOME, GOPATH, proxies, a customized PATH — keeps working, which an
/// `env_clear` model would break), MINUS anything that looks like a secret. We
/// always scrub secrets, since we run model-generated commands and must not leak
/// credentials to them. The allowlist is an override for legitimate vars that the
/// secret heuristic would otherwise false-positive (e.g. SSH_AUTH_SOCK). The
/// caller's explicit `extra` env is layered on top. Finally, PATH is
/// supplemented with the bundled ripgrep directory when `rg` does not already
/// resolve there, so shell searches work on machines without a system install.
pub fn scrub_env(base: &RuntimeEnv, extra: &RuntimeEnv) -> RuntimeEnv {
    let mut out = RuntimeEnv::new();
    for (key, value) in base.entries() {
        if is_safe_key(&key) || !looks_sensitive(&key) {
            out.insert(key, value.clone());
        }
    }
    for (key, value) in extra.entries() {
        out.insert(key, value.clone());
    }
    crate::fs::ripgrep::ensure_ripgrep_on_path(&mut out);
    out
}

/// The `process` backend: env scrubbing + cwd + process-group management only.
/// It is NOT filesystem isolation — a `bash` command can read/write anywhere the
/// user can. See docs/security-model.md.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessSandbox;

impl ProcessSandbox {
    pub fn new() -> Self {
        ProcessSandbox
    }
}

impl SandboxBackend for ProcessSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Process
    }

    fn is_available(&self) -> bool {
        true
    }

    fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
        let file = match req.argv.first() {
            Some(f) if !f.is_empty() => f.clone(),
            _ => panic!("ProcessSandbox: empty argv"),
        };
        let parent_env = ParentEnvSnapshot::capture();
        let empty = RuntimeEnv::new();
        let extra = req.env.as_ref().unwrap_or(&empty);
        SpawnSpec {
            file,
            args: req.argv[1..].to_vec(),
            cwd: req.cwd.clone(),
            env: scrub_env(&parent_env, extra),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimePath;

    fn env(entries: Vec<(RuntimeString, RuntimeString)>) -> RuntimeEnv {
        let mut environment = RuntimeEnv::new();
        for (key, value) in entries {
            environment.insert(key, value);
        }
        environment
    }

    /// A PATH entry that resolves `rg`, so the bundled-ripgrep supplement stays
    /// a no-op and exact-key expectations are machine-independent.
    fn path_with_resolvable_rg() -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "ae-scrub-rg-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let name = if cfg!(windows) { "rg.exe" } else { "rg" };
        let rg = dir.join(name);
        std::fs::write(&rg, b"#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&rg, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        dir
    }

    #[test]
    fn scrub_preserves_ecmascript_order_and_exact_keys() {
        let lone = RuntimeString::from_utf16(vec![0xd800]);
        let rg_path = path_with_resolvable_rg();
        let base = env(vec![
            (RuntimeString::from("B"), RuntimeString::from("1")),
            (
                RuntimeString::from("API_KEY"),
                RuntimeString::from("secret"),
            ),
            (lone.clone(), RuntimeString::from("exact")),
            (RuntimeString::from("A"), RuntimeString::from("2")),
            (
                RuntimeString::from("PATH"),
                RuntimeString::from(rg_path.to_str().unwrap()),
            ),
        ]);
        let extra = env(vec![
            (RuntimeString::from("B"), RuntimeString::from("override")),
            (RuntimeString::from("C"), RuntimeString::from("3")),
        ]);
        let scrubbed = scrub_env(&base, &extra);
        assert_eq!(
            scrubbed.entries().map(|(key, _)| key).collect::<Vec<_>>(),
            vec![
                RuntimeString::from("B"),
                lone.clone(),
                RuntimeString::from("A"),
                RuntimeString::from("PATH"),
                RuntimeString::from("C"),
            ]
        );
        assert_eq!(scrubbed.get(&lone), Some(&RuntimeString::from("exact")));
        assert!(!scrubbed.contains_key(&RuntimeString::from("API_KEY")));
    }

    #[test]
    fn process_transform_keeps_lone_surrogate_until_process_image() {
        let lone = RuntimeString::from_utf16(vec![0xd800]);
        let request = ExecRequest {
            argv: vec![RuntimeString::from("bash"), lone.clone()],
            cwd: RuntimePath::from(lone.clone()),
            env: Some(env(vec![(lone.clone(), lone.clone())])),
            timeout_ms: None,
        };
        let spec = ProcessSandbox.transform(&request, &SandboxPolicy::default());
        assert_eq!(spec.args, vec![lone.clone()]);
        assert_eq!(spec.cwd, RuntimePath::from(lone.clone()));
        assert_eq!(spec.env.get(&lone), Some(&lone));
    }
}
