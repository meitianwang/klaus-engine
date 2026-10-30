//! Detect whether a sandboxed command failed BECAUSE the sandbox blocked it (a
//! denied write / syscall / network) rather than failing on its own merits.

use crate::runtime_values::RuntimeString;
use crate::types::sandbox::SandboxKind;

/// Phrases a real sandbox (macOS Seatbelt / Linux Landlock+seccomp / bubblewrap)
/// emits when it BLOCKS a command — typically a write outside the writable roots
/// or a forbidden syscall. Matched case-insensitively against the command's
/// COMBINED stdout+stderr. (Note: a network block usually surfaces as
/// "connection refused" / "network is unreachable" / a timeout, which is
/// deliberately NOT in this set — adding those would false-positive on ordinary
/// connection failures.)
const DENIAL_KEYWORDS: [&str; 9] = [
    "operation not permitted",
    "permission denied",
    "read-only file system",
    "seccomp",
    "sandbox",
    "landlock",
    "failed to write file",
    // Windows restricted-token / ACL denials surface as ERROR_ACCESS_DENIED (5).
    "access is denied",
    "os error 5",
];

/// Exit codes that are KNOWN shell failures (2 = misuse, 126 = not executable,
/// 127 = not found), not sandbox denials — a quick-reject set. Only consulted
/// when NO denial keyword matched, so a keyword still wins over these.
const SHELL_FAILURE_EXIT_CODES: [i32; 3] = [2, 126, 127];

/// SIGSYS signal number on Linux — a forbidden syscall under a seccomp filter is
/// killed with it.
const SIGSYS: i32 = 31;

/// Heuristic: did a sandboxed command FAIL BECAUSE THE SANDBOX DENIED IT (a
/// blocked write / syscall / network) rather than failing on its own merits?
/// Adapted to the engine's captured fields — a single combined `output` string
/// and a `signal` name in place of the `128 + SIGSYS` exit encoding.
///
/// Order: no-real-sandbox / clean-exit short-circuit -> keyword match (wins) ->
/// known-shell-failure reject -> blocked-syscall signal -> default false.
///
/// Lets the bash tool tell the model/user "this is a sandbox restriction, not a
/// bug in your command" instead of surfacing an opaque non-zero exit — so the
/// model doesn't waste turns "fixing" a command that is actually fine. (The
/// engine does NOT auto-rerun unsandboxed off this signal; relaxing the sandbox
/// is an explicit user choice.)
pub fn is_likely_sandbox_denied(
    kind: SandboxKind,
    exit_code: Option<i32>,
    signal: Option<&RuntimeString>,
    output: &RuntimeString,
) -> bool {
    // 'process' is env-scrub only (no filesystem/syscall isolation), so an EPERM
    // under it is a REAL OS permission problem, not a sandbox artifact. A clean
    // exit is never a denial.
    if kind == SandboxKind::Process || exit_code == Some(0) {
        return false;
    }
    if DENIAL_KEYWORDS
        .iter()
        .any(|keyword| contains_ascii_case_insensitive(output, keyword))
    {
        return true;
    }
    // A keyword-less known shell failure is the command's own fault, not the
    // sandbox.
    if let Some(code) = exit_code {
        if SHELL_FAILURE_EXIT_CODES.contains(&code) {
            return false;
        }
    }
    // A forbidden syscall under the Linux syscall backend (macOS
    // Seatbelt has no SIGSYS and surfaces denials as the EPERM keywords above) is
    // killed with SIGSYS. Two shapes reach us, and the SECOND is the common one
    // because the engine always wraps the command in `bash -c`:
    //   - the spawned bash is itself signal-killed -> signal == "SIGSYS";
    //   - a GRANDCHILD bash is killed -> bash exits NORMALLY with the
    //     conventional 128 + signal code -> exit_code == 159 (128 + SIGSYS),
    //     signal == None. The numeric branch is what actually fires in practice.
    if matches!(kind, SandboxKind::NativeHelper | SandboxKind::Bubblewrap)
        && (signal.is_some_and(|signal| signal == "SIGSYS") || exit_code == Some(128 + SIGSYS))
    {
        return true;
    }
    false
}

fn contains_ascii_case_insensitive(value: &RuntimeString, needle: &str) -> bool {
    let needle = needle
        .encode_utf16()
        .map(ascii_lowercase)
        .collect::<Vec<_>>();
    needle.is_empty()
        || value.as_utf16().windows(needle.len()).any(|window| {
            window
                .iter()
                .copied()
                .map(ascii_lowercase)
                .eq(needle.iter().copied())
        })
}

fn ascii_lowercase(unit: u16) -> u16 {
    if (u16::from(b'A')..=u16::from(b'Z')).contains(&unit) {
        unit + u16::from(b'a' - b'A')
    } else {
        unit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_likely_sandbox_denied(
        kind: SandboxKind,
        exit_code: Option<i32>,
        signal: Option<&str>,
        output: &str,
    ) -> bool {
        let signal = signal.map(RuntimeString::from);
        super::is_likely_sandbox_denied(
            kind,
            exit_code,
            signal.as_ref(),
            &RuntimeString::from(output),
        )
    }

    #[test]
    fn never_flags_the_env_scrub_only_process_backend() {
        assert!(!is_likely_sandbox_denied(
            SandboxKind::Process,
            Some(1),
            None,
            "permission denied"
        ));
    }

    #[test]
    fn never_flags_a_clean_exit() {
        assert!(!is_likely_sandbox_denied(
            SandboxKind::Seatbelt,
            Some(0),
            None,
            "operation not permitted"
        ));
    }

    #[test]
    fn matches_the_denial_keyword_set_case_insensitively() {
        for kw in [
            "Operation Not Permitted",
            "permission denied",
            "Read-only file system",
            "seccomp violation",
            "sandbox: deny",
            "landlock blocked",
            "failed to write file",
        ] {
            assert!(is_likely_sandbox_denied(
                SandboxKind::Seatbelt,
                Some(1),
                None,
                &format!("bash: {kw}")
            ));
        }
    }

    #[test]
    fn rejects_a_keyword_less_known_shell_failure() {
        assert!(!is_likely_sandbox_denied(
            SandboxKind::Bubblewrap,
            Some(2),
            None,
            "syntax error"
        ));
        assert!(!is_likely_sandbox_denied(
            SandboxKind::Bubblewrap,
            Some(126),
            None,
            "cannot execute"
        ));
        assert!(!is_likely_sandbox_denied(
            SandboxKind::Bubblewrap,
            Some(127),
            None,
            "command not found"
        ));
    }

    #[test]
    fn lets_a_denial_keyword_win_over_a_known_shell_failure_exit_code() {
        // exit 127 would otherwise reject, but the keyword check runs first.
        assert!(is_likely_sandbox_denied(
            SandboxKind::Seatbelt,
            Some(127),
            None,
            "permission denied"
        ));
    }

    #[test]
    fn flags_a_blocked_syscall_under_the_seccomp_backend() {
        // The realistic case: a grandchild is SIGSYS-killed, bash exits NORMALLY
        // with 128 + 31 = 159.
        assert!(is_likely_sandbox_denied(
            SandboxKind::NativeHelper,
            Some(159),
            None,
            ""
        ));
        // The rare case: the spawned bash itself is signal-killed -> the signal is
        // reported.
        assert!(is_likely_sandbox_denied(
            SandboxKind::NativeHelper,
            None,
            Some("SIGSYS"),
            "killed"
        ));
    }

    #[test]
    fn gates_sigsys_detection_to_the_linux_seccomp_backend() {
        // macOS Seatbelt has no SIGSYS mechanism; a 159 there is the user
        // program's own, not a denial.
        assert!(!is_likely_sandbox_denied(
            SandboxKind::Seatbelt,
            Some(159),
            None,
            ""
        ));
        assert!(!is_likely_sandbox_denied(
            SandboxKind::Seatbelt,
            None,
            Some("SIGSYS"),
            "killed"
        ));
        assert!(is_likely_sandbox_denied(
            SandboxKind::Bubblewrap,
            Some(159),
            None,
            ""
        ));
    }

    #[test]
    fn flags_windows_access_denied_under_the_restricted_token_backend() {
        // ACL / capability-SID write denials from the Windows runner surface as
        // ERROR_ACCESS_DENIED text rather than a distinct exit code.
        assert!(is_likely_sandbox_denied(
            SandboxKind::WindowsRestrictedToken,
            Some(1),
            None,
            "Access is denied."
        ));
        assert!(is_likely_sandbox_denied(
            SandboxKind::WindowsRestrictedToken,
            Some(1),
            None,
            "failed to open file (os error 5)"
        ));
    }

    #[test]
    fn does_not_flag_an_ordinary_non_zero_exit_with_no_denial_signal() {
        assert!(!is_likely_sandbox_denied(
            SandboxKind::Seatbelt,
            Some(1),
            None,
            "error: file not found"
        ));
        assert!(!is_likely_sandbox_denied(
            SandboxKind::NativeHelper,
            Some(1),
            Some("SIGTERM"),
            "terminated"
        ));
    }
}
