//! Testable platform sandbox coverage inventory.

use crate::types::sandbox::SandboxKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxCoveragePlatform {
    All,
    Macos,
    Linux,
    Windows,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxCoverageStatus {
    AlwaysAvailable,
    RuntimeProbe,
    OutOfScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxIsolation {
    None,
    PolicyEnforced,
    HelperEnforced,
    OutOfScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxCoverageEntry {
    pub name: &'static str,
    pub kind: Option<SandboxKind>,
    pub platform: SandboxCoveragePlatform,
    pub status: SandboxCoverageStatus,
    pub runtime_dependency: Option<&'static str>,
    pub filesystem: SandboxIsolation,
    pub network: SandboxIsolation,
    pub syscall: SandboxIsolation,
    pub note: &'static str,
}

pub fn sandbox_coverage_matrix() -> Vec<SandboxCoverageEntry> {
    vec![
        SandboxCoverageEntry {
            name: "process",
            kind: Some(SandboxKind::Process),
            platform: SandboxCoveragePlatform::All,
            status: SandboxCoverageStatus::AlwaysAvailable,
            runtime_dependency: None,
            filesystem: SandboxIsolation::None,
            network: SandboxIsolation::None,
            syscall: SandboxIsolation::None,
            note: "env-scrub and process management only; NO filesystem, network, or syscall isolation",
        },
        SandboxCoverageEntry {
            name: "process-hardening",
            kind: None,
            platform: SandboxCoveragePlatform::All,
            status: SandboxCoverageStatus::AlwaysAvailable,
            runtime_dependency: None,
            filesystem: SandboxIsolation::None,
            network: SandboxIsolation::None,
            syscall: SandboxIsolation::None,
            note: "strips DYLD_*, LD_*, and NODE_OPTIONS from the engine environment; does not defend against pre-main injection or disable core dumps",
        },
        SandboxCoverageEntry {
            name: "seatbelt",
            kind: Some(SandboxKind::Seatbelt),
            platform: SandboxCoveragePlatform::Macos,
            status: SandboxCoverageStatus::RuntimeProbe,
            runtime_dependency: Some("/usr/bin/sandbox-exec"),
            filesystem: SandboxIsolation::PolicyEnforced,
            network: SandboxIsolation::PolicyEnforced,
            syscall: SandboxIsolation::None,
            note: "macOS Seatbelt profile generated from SandboxPolicy; sandbox-exec is probed at runtime",
        },
        SandboxCoverageEntry {
            name: "linux-sandbox",
            kind: Some(SandboxKind::Bubblewrap),
            platform: SandboxCoveragePlatform::Linux,
            status: SandboxCoverageStatus::RuntimeProbe,
            runtime_dependency: Some("engine helper entry point with embedded bubblewrap"),
            filesystem: SandboxIsolation::PolicyEnforced,
            network: SandboxIsolation::PolicyEnforced,
            syscall: SandboxIsolation::HelperEnforced,
            note: "Linux execution uses embedded bubblewrap namespaces followed by no-new-privileges and seccomp; setup failures never execute the command unrestricted",
        },
        SandboxCoverageEntry {
            name: "windows-restricted-token",
            kind: Some(SandboxKind::WindowsRestrictedToken),
            platform: SandboxCoveragePlatform::Windows,
            status: SandboxCoverageStatus::RuntimeProbe,
            runtime_dependency: Some("engine wrapper; elevated selection also requires native setup and command helpers"),
            filesystem: SandboxIsolation::HelperEnforced,
            network: SandboxIsolation::HelperEnforced,
            syscall: SandboxIsolation::None,
            note: "The selected native token level controls preparation and launch. Unelevated execution restricts writes and uses environment-only network mitigation; elevated execution uses provisioned accounts and native network enforcement. Internal task ceilings retain an explicitly scoped container.",
        },
        SandboxCoverageEntry {
            name: "windows-mxc",
            kind: Some(SandboxKind::WindowsMxc),
            platform: SandboxCoveragePlatform::Windows,
            status: SandboxCoverageStatus::RuntimeProbe,
            runtime_dependency: Some("native Windows process security environment and engine early launcher dispatch"),
            filesystem: SandboxIsolation::PolicyEnforced,
            network: SandboxIsolation::PolicyEnforced,
            syscall: SandboxIsolation::None,
            note: "Explicit native process-security execution probes the kernel facility and never downgrades to ACL fallback execution. Managed networking requires bidirectional host loopback; unavailable facilities and unsupported policy shapes return errors.",
        },
        SandboxCoverageEntry {
            name: "other-platform-real-sandbox",
            kind: None,
            platform: SandboxCoveragePlatform::Other,
            status: SandboxCoverageStatus::OutOfScope,
            runtime_dependency: None,
            filesystem: SandboxIsolation::OutOfScope,
            network: SandboxIsolation::OutOfScope,
            syscall: SandboxIsolation::OutOfScope,
            note: "no dedicated real sandbox backend is implemented for non-macOS/non-Linux/non-Windows targets",
        },
    ]
}

pub fn sandbox_coverage_for_kind(kind: SandboxKind) -> Option<SandboxCoverageEntry> {
    sandbox_coverage_matrix()
        .into_iter()
        .find(|entry| entry.kind == Some(kind))
}

pub fn sandbox_coverage_by_name(name: &str) -> Option<SandboxCoverageEntry> {
    sandbox_coverage_matrix()
        .into_iter()
        .find(|entry| entry.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn find(name: &str) -> SandboxCoverageEntry {
        sandbox_coverage_matrix()
            .into_iter()
            .find(|entry| entry.name == name)
            .unwrap_or_else(|| panic!("missing sandbox coverage entry: {name}"))
    }

    #[test]
    fn matrix_covers_every_current_backend_kind_once() {
        let matrix = sandbox_coverage_matrix();
        for kind in [
            SandboxKind::Process,
            SandboxKind::Seatbelt,
            SandboxKind::Bubblewrap,
            SandboxKind::WindowsRestrictedToken,
            SandboxKind::WindowsMxc,
        ] {
            let count = matrix
                .iter()
                .filter(|entry| entry.kind == Some(kind))
                .count();
            assert_eq!(count, 1, "expected one coverage row for {kind:?}");
        }
    }

    #[test]
    fn process_backend_is_documented_as_no_isolation() {
        let process = find("process");

        assert_eq!(process.platform, SandboxCoveragePlatform::All);
        assert_eq!(process.status, SandboxCoverageStatus::AlwaysAvailable);
        assert_eq!(process.filesystem, SandboxIsolation::None);
        assert_eq!(process.network, SandboxIsolation::None);
        assert_eq!(process.syscall, SandboxIsolation::None);
        assert!(process.note.contains("env"));
        assert!(process.note.contains("NO filesystem"));
    }

    #[test]
    fn macos_seatbelt_entry_tracks_runtime_probe_and_policy_coverage() {
        let seatbelt = find("seatbelt");

        assert_eq!(seatbelt.kind, Some(SandboxKind::Seatbelt));
        assert_eq!(seatbelt.platform, SandboxCoveragePlatform::Macos);
        assert_eq!(seatbelt.status, SandboxCoverageStatus::RuntimeProbe);
        assert_eq!(seatbelt.runtime_dependency, Some("/usr/bin/sandbox-exec"));
        assert_eq!(seatbelt.filesystem, SandboxIsolation::PolicyEnforced);
        assert_eq!(seatbelt.network, SandboxIsolation::PolicyEnforced);
    }

    #[test]
    fn linux_executor_is_shipped_and_enforces_syscall_policy() {
        let linux = find("linux-sandbox");
        assert_eq!(linux.kind, Some(SandboxKind::Bubblewrap));
        assert_eq!(linux.platform, SandboxCoveragePlatform::Linux);
        assert_eq!(linux.syscall, SandboxIsolation::HelperEnforced);
        assert_eq!(
            linux.runtime_dependency,
            Some("engine helper entry point with embedded bubblewrap")
        );
    }

    #[test]
    fn windows_restricted_token_sandbox_is_implemented_via_runner() {
        let windows = find("windows-restricted-token");

        assert_eq!(windows.kind, Some(SandboxKind::WindowsRestrictedToken));
        assert_eq!(windows.platform, SandboxCoveragePlatform::Windows);
        assert_eq!(windows.status, SandboxCoverageStatus::RuntimeProbe);
        assert_eq!(windows.filesystem, SandboxIsolation::HelperEnforced);
        assert_eq!(windows.network, SandboxIsolation::HelperEnforced);
        assert_eq!(windows.syscall, SandboxIsolation::None);
        assert!(windows.runtime_dependency.is_some());
        assert!(windows.note.contains("Unelevated"));
    }
}
