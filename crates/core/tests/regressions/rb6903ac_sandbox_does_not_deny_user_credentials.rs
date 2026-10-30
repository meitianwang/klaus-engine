//! Defect: with the sandbox in its default posture, every command that
//! authenticates as the user failed inside it. `git push`, `git ls-remote` and
//! `ssh` died with `Operation not permitted` on `~/.ssh/known_hosts`, which
//! surfaces as `Host key verification failed`. Because macOS spells a seatbelt
//! denial and a TCC denial identically, the failure was repeatedly misread as a
//! Full Disk Access problem, and the suggested remedy (grant FDA, restart) could
//! never work — FDA was already granted.
//! Root cause: `compute_read_denies` subtracted a built-in credential deny-list
//! (`~/.ssh`, `~/.aws`, `~/.git-credentials`, …) from an otherwise full-disk read
//! grant. The deny-list had no escape hatch: the engine deliberately forwards
//! `SSH_AUTH_SOCK` so a command can reach ssh-agent instead of reading keys, but
//! the matching unix-socket rule was only emitted when a managed network proxy
//! was configured, so in the default posture both paths to a signature were shut.
//! The subtraction also bought nothing: the agent's own file tools do not run
//! through this sandbox, so a deny-list here does not stop exfiltration.
//! Fixed by: commit b6903ac (align the read policy with the upstream design the
//! seatbelt backend was forked from — a broad read grant stays broad, and read
//! denials come only from the caller via `deny_read_subpaths`).

use klaus_agent_core::runtime_process::RuntimeEnv;
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use klaus_agent_core::sandbox::policy::compute_read_denies;
use klaus_agent_core::sandbox::seatbelt::build_seatbelt_profile;
use klaus_agent_core::types::sandbox::SandboxPolicy;

fn home_env() -> RuntimeEnv {
    let mut env = RuntimeEnv::new();
    env.insert(
        RuntimeString::from("HOME"),
        RuntimeString::from("/Users/tester"),
    );
    env
}

/// The workspace-write posture the desktop client actually ships: writes scoped
/// to the project, reads unrestricted, network on.
fn default_posture() -> SandboxPolicy {
    SandboxPolicy {
        writable_roots: vec![RuntimeString::from("/work/project")],
        network_allowed: true,
        ..SandboxPolicy::default()
    }
}

#[test]
fn the_default_posture_denies_no_credential_path() {
    let denies = compute_read_denies(&default_posture(), &home_env());
    assert!(
        denies.is_empty(),
        "the engine must not invent read denials: {denies:?}"
    );
}

#[test]
fn the_generated_profile_leaves_ssh_readable() {
    let (profile, params) = build_seatbelt_profile(
        &default_posture(),
        &RuntimePath::from("/work/project"),
        &home_env(),
    );

    // The read grant is global, exactly as the upstream seatbelt design has it.
    assert!(
        profile.contains("(allow file-read*)"),
        "reads must be granted filesystem-wide"
    );
    // Nothing may re-deny a read on top of that grant by default. This is the
    // assertion that fails on the pre-fix engine, where eleven `~/.<cred>` paths
    // were emitted as last-wins `(deny file-read*)` rules.
    assert!(
        !profile.contains("deny file-read*"),
        "no read denials in the default posture: {profile:?}"
    );
    for (key, value) in &params {
        assert!(
            !key.starts_with("DENY_READ_"),
            "no deny-read parameter may be defined: {key:?}={value:?}"
        );
        assert!(
            !value.contains(".ssh"),
            "no parameter may point at the user's ssh directory: {key:?}={value:?}"
        );
    }
}

#[test]
fn a_caller_can_still_deny_reads_explicitly() {
    // Removing the built-in list must not remove the mechanism: a caller that
    // wants a path unreadable can still say so, and it still lands in the profile.
    let policy = SandboxPolicy {
        deny_read_subpaths: Some(vec![RuntimeString::from("/work/project/secrets")]),
        ..default_posture()
    };

    let denies = compute_read_denies(&policy, &home_env());
    assert_eq!(
        denies,
        vec![RuntimeString::from("/work/project/secrets")],
        "an explicit denial is the caller's own instruction and must survive"
    );

    let (profile, params) =
        build_seatbelt_profile(&policy, &RuntimePath::from("/work/project"), &home_env());
    assert!(
        profile.contains("deny file-read*"),
        "an explicit denial must reach the profile: {profile:?}"
    );
    assert!(
        params
            .iter()
            .any(|(key, value)| key.starts_with("DENY_READ_")
                && value == &RuntimeString::from("/work/project/secrets")),
        "the denied path must be passed out-of-band as a parameter: {params:?}"
    );
}
