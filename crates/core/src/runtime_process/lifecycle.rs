use super::{NodeSpawnPlan, ProcessEnvironmentPlan};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessTarget {
    Linux,
    MacOs,
    Windows,
}

impl ProcessTarget {
    fn is_unix(self) -> bool {
        matches!(self, Self::Linux | Self::MacOs)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessTopology {
    Inherit,
    UnixNewProcessGroup,
    UnixNewSession,
    UnixControllingPty,
    WindowsDetached,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KillTarget {
    Child,
    UnixProcessGroup,
    WindowsProcessTree,
    WindowsPrivateJob,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessImageDialect {
    Node20Libuv146,
    Rust193,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnvironmentImageDialect {
    Node20ParentSnapshot,
    NativeParentInherit,
    Rust193CommandOverlay,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessEnvironmentGuard {
    None,
    SealedNodeNoPreloadV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowsJobPolicy {
    LibuvGlobal,
    LibuvGlobalWithControlTracker,
    BrowserSidecarPrivate,
    None,
}

/// Structural pre-spawn compatibility selected before the generated
/// per-surface manifest binds ownership, stdio, and lifecycle policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreSpawnPolicy {
    target: ProcessTarget,
    image_dialect: ProcessImageDialect,
    environment_dialect: EnvironmentImageDialect,
    environment_guard: ProcessEnvironmentGuard,
    windows_job_policy: WindowsJobPolicy,
    topology: ProcessTopology,
    kill_target: KillTarget,
}

impl PreSpawnPolicy {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        target: ProcessTarget,
        image_dialect: ProcessImageDialect,
        environment_dialect: EnvironmentImageDialect,
        environment_guard: ProcessEnvironmentGuard,
        windows_job_policy: WindowsJobPolicy,
        topology: ProcessTopology,
        kill_target: KillTarget,
    ) -> Self {
        Self {
            target,
            image_dialect,
            environment_dialect,
            environment_guard,
            windows_job_policy,
            topology,
            kill_target,
        }
    }

    pub(crate) fn validate(&self, plan: &NodeSpawnPlan) -> Result<(), PreSpawnValidationError> {
        if !topology_matches_target(self.target, self.topology) {
            return Err(PreSpawnValidationError::TopologyTargetMismatch {
                target: self.target,
                topology: self.topology,
            });
        }

        let expected_detached = match self.topology {
            ProcessTopology::UnixNewSession if self.target.is_unix() => true,
            ProcessTopology::WindowsDetached if self.target == ProcessTarget::Windows => true,
            _ => false,
        };
        if plan.detached() != expected_detached {
            return Err(PreSpawnValidationError::DetachedTopologyMismatch {
                target: self.target,
                detached: plan.detached(),
                topology: self.topology,
            });
        }

        let environment_kind = ProcessEnvironmentKind::of(plan.environment());
        if !environment_matches_dialect(environment_kind, self.environment_dialect) {
            return Err(PreSpawnValidationError::EnvironmentPlanMismatch {
                dialect: self.environment_dialect,
                environment: environment_kind,
            });
        }

        if !image_matches_environment(self.image_dialect, environment_kind) {
            return Err(PreSpawnValidationError::ImageEnvironmentMismatch {
                image: self.image_dialect,
                environment: environment_kind,
            });
        }

        if self.environment_guard == ProcessEnvironmentGuard::SealedNodeNoPreloadV1
            && (self.image_dialect != ProcessImageDialect::Rust193
                || environment_kind == ProcessEnvironmentKind::NodeRuntime)
        {
            return Err(PreSpawnValidationError::GuardMismatch {
                image: self.image_dialect,
                environment: environment_kind,
                guard: self.environment_guard,
            });
        }

        if self.target.is_unix() && self.windows_job_policy != WindowsJobPolicy::None {
            return Err(PreSpawnValidationError::WindowsJobTargetMismatch {
                target: self.target,
                policy: self.windows_job_policy,
            });
        }

        match self.windows_job_policy {
            WindowsJobPolicy::LibuvGlobal | WindowsJobPolicy::LibuvGlobalWithControlTracker
                if self.image_dialect != ProcessImageDialect::Node20Libuv146 =>
            {
                return Err(PreSpawnValidationError::WindowsJobImageMismatch {
                    policy: self.windows_job_policy,
                    image: self.image_dialect,
                });
            }
            WindowsJobPolicy::BrowserSidecarPrivate
                if self.image_dialect != ProcessImageDialect::Rust193 =>
            {
                return Err(PreSpawnValidationError::WindowsJobImageMismatch {
                    policy: self.windows_job_policy,
                    image: self.image_dialect,
                });
            }
            _ => {}
        }

        if (self.windows_job_policy == WindowsJobPolicy::BrowserSidecarPrivate)
            != (self.kill_target == KillTarget::WindowsPrivateJob)
        {
            return Err(PreSpawnValidationError::PrivateJobMismatch {
                kill_target: self.kill_target,
                guard: self.environment_guard,
                environment: environment_kind,
                topology: self.topology,
            });
        }

        if self.windows_job_policy == WindowsJobPolicy::BrowserSidecarPrivate
            && (self.target != ProcessTarget::Windows
                || self.image_dialect != ProcessImageDialect::Rust193
                || environment_kind != ProcessEnvironmentKind::Rust193Overlay
                || self.environment_guard != ProcessEnvironmentGuard::SealedNodeNoPreloadV1
                || self.topology != ProcessTopology::Inherit
                || plan.detached())
        {
            return Err(PreSpawnValidationError::PrivateJobMismatch {
                kill_target: self.kill_target,
                guard: self.environment_guard,
                environment: environment_kind,
                topology: self.topology,
            });
        }

        if !kill_target_matches(self.target, self.topology, self.kill_target) {
            return Err(PreSpawnValidationError::KillTargetMismatch {
                target: self.target,
                topology: self.topology,
                kill_target: self.kill_target,
            });
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessEnvironmentKind {
    NodeRuntime,
    NativeParentInherit,
    Rust193Overlay,
}

impl ProcessEnvironmentKind {
    fn of(environment: &ProcessEnvironmentPlan) -> Self {
        match environment {
            ProcessEnvironmentPlan::NodeRuntime(_) => Self::NodeRuntime,
            ProcessEnvironmentPlan::NativeParentInherit => Self::NativeParentInherit,
            ProcessEnvironmentPlan::Rust193Overlay { .. } => Self::Rust193Overlay,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum PreSpawnValidationError {
    TopologyTargetMismatch {
        target: ProcessTarget,
        topology: ProcessTopology,
    },
    DetachedTopologyMismatch {
        target: ProcessTarget,
        detached: bool,
        topology: ProcessTopology,
    },
    EnvironmentPlanMismatch {
        dialect: EnvironmentImageDialect,
        environment: ProcessEnvironmentKind,
    },
    ImageEnvironmentMismatch {
        image: ProcessImageDialect,
        environment: ProcessEnvironmentKind,
    },
    GuardMismatch {
        image: ProcessImageDialect,
        environment: ProcessEnvironmentKind,
        guard: ProcessEnvironmentGuard,
    },
    WindowsJobTargetMismatch {
        target: ProcessTarget,
        policy: WindowsJobPolicy,
    },
    WindowsJobImageMismatch {
        policy: WindowsJobPolicy,
        image: ProcessImageDialect,
    },
    PrivateJobMismatch {
        kill_target: KillTarget,
        guard: ProcessEnvironmentGuard,
        environment: ProcessEnvironmentKind,
        topology: ProcessTopology,
    },
    KillTargetMismatch {
        target: ProcessTarget,
        topology: ProcessTopology,
        kill_target: KillTarget,
    },
}

fn topology_matches_target(target: ProcessTarget, topology: ProcessTopology) -> bool {
    match topology {
        ProcessTopology::Inherit => true,
        ProcessTopology::UnixNewProcessGroup
        | ProcessTopology::UnixNewSession
        | ProcessTopology::UnixControllingPty => target.is_unix(),
        ProcessTopology::WindowsDetached => target == ProcessTarget::Windows,
    }
}

fn image_matches_environment(
    image: ProcessImageDialect,
    environment: ProcessEnvironmentKind,
) -> bool {
    matches!(
        (image, environment),
        (
            ProcessImageDialect::Node20Libuv146,
            ProcessEnvironmentKind::NodeRuntime
        ) | (
            ProcessImageDialect::Rust193,
            ProcessEnvironmentKind::NativeParentInherit | ProcessEnvironmentKind::Rust193Overlay
        )
    )
}

fn environment_matches_dialect(
    environment: ProcessEnvironmentKind,
    dialect: EnvironmentImageDialect,
) -> bool {
    matches!(
        (environment, dialect),
        (
            ProcessEnvironmentKind::NodeRuntime,
            EnvironmentImageDialect::Node20ParentSnapshot
        ) | (
            ProcessEnvironmentKind::NativeParentInherit,
            EnvironmentImageDialect::NativeParentInherit
        ) | (
            ProcessEnvironmentKind::Rust193Overlay,
            EnvironmentImageDialect::Rust193CommandOverlay
        )
    )
}

fn kill_target_matches(
    target: ProcessTarget,
    topology: ProcessTopology,
    kill_target: KillTarget,
) -> bool {
    match kill_target {
        KillTarget::Child => true,
        KillTarget::UnixProcessGroup => {
            target.is_unix()
                && matches!(
                    topology,
                    ProcessTopology::UnixNewProcessGroup
                        | ProcessTopology::UnixNewSession
                        | ProcessTopology::UnixControllingPty
                )
        }
        KillTarget::WindowsProcessTree | KillTarget::WindowsPrivateJob => {
            target == ProcessTarget::Windows
                && (kill_target != KillTarget::WindowsPrivateJob
                    || topology == ProcessTopology::Inherit)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime_process::{
        EnvironmentImageDialect, KillTarget, NodeSpawnPlan, PreSpawnPolicy,
        PreSpawnValidationError, ProcessAtom, ProcessEnvironmentGuard, ProcessEnvironmentPlan,
        ProcessImageDialect, ProcessTarget, ProcessTopology, RuntimeEnv, Rust193EnvMutation,
        WindowsJobPolicy,
    };
    use crate::runtime_values::{runtime_codec, RuntimeJson, RuntimeObject, RuntimeString};

    fn atom(value: &str) -> ProcessAtom {
        ProcessAtom::Runtime(RuntimeString::from_utf8(value))
    }

    fn empty_runtime_env() -> RuntimeEnv {
        runtime_codec::decode_value(&RuntimeJson::Object(RuntimeObject::new()))
            .expect("empty object is an exact runtime environment")
    }

    fn plan(environment: ProcessEnvironmentPlan, detached: bool) -> NodeSpawnPlan {
        NodeSpawnPlan::from_normalized_parts(
            atom("node"),
            atom("node"),
            Vec::new(),
            None,
            environment,
            detached,
            false,
            false,
        )
        .expect("valid normalized fixture")
    }

    fn native_plan(detached: bool) -> NodeSpawnPlan {
        plan(ProcessEnvironmentPlan::NativeParentInherit, detached)
    }

    fn overlay_plan(detached: bool) -> NodeSpawnPlan {
        plan(
            ProcessEnvironmentPlan::Rust193Overlay {
                mutations: vec![Rust193EnvMutation::RuntimeSet {
                    key: RuntimeString::from_utf8("KEY"),
                    value: RuntimeString::from_utf8("value"),
                }],
            },
            detached,
        )
    }

    fn policy(
        target: ProcessTarget,
        image_dialect: ProcessImageDialect,
        environment_dialect: EnvironmentImageDialect,
        environment_guard: ProcessEnvironmentGuard,
        windows_job_policy: WindowsJobPolicy,
        topology: ProcessTopology,
        kill_target: KillTarget,
    ) -> PreSpawnPolicy {
        PreSpawnPolicy::new(
            target,
            image_dialect,
            environment_dialect,
            environment_guard,
            windows_job_policy,
            topology,
            kill_target,
        )
    }

    fn rust_native_policy(target: ProcessTarget, guard: ProcessEnvironmentGuard) -> PreSpawnPolicy {
        policy(
            target,
            ProcessImageDialect::Rust193,
            EnvironmentImageDialect::NativeParentInherit,
            guard,
            WindowsJobPolicy::None,
            ProcessTopology::Inherit,
            KillTarget::Child,
        )
    }

    fn rust_overlay_policy(
        target: ProcessTarget,
        guard: ProcessEnvironmentGuard,
    ) -> PreSpawnPolicy {
        policy(
            target,
            ProcessImageDialect::Rust193,
            EnvironmentImageDialect::Rust193CommandOverlay,
            guard,
            WindowsJobPolicy::None,
            ProcessTopology::Inherit,
            KillTarget::Child,
        )
    }

    #[test]
    fn structural_matrix_accepts_exact_node_environment_family() {
        let node_plan = plan(
            ProcessEnvironmentPlan::NodeRuntime(empty_runtime_env()),
            false,
        );
        for target in [
            ProcessTarget::Linux,
            ProcessTarget::MacOs,
            ProcessTarget::Windows,
        ] {
            let node = policy(
                target,
                ProcessImageDialect::Node20Libuv146,
                EnvironmentImageDialect::Node20ParentSnapshot,
                ProcessEnvironmentGuard::None,
                WindowsJobPolicy::None,
                ProcessTopology::Inherit,
                KillTarget::Child,
            );
            assert_eq!(node.validate(&node_plan), Ok(()));
        }

        for windows_job_policy in [
            WindowsJobPolicy::LibuvGlobal,
            WindowsJobPolicy::LibuvGlobalWithControlTracker,
        ] {
            let node = policy(
                ProcessTarget::Windows,
                ProcessImageDialect::Node20Libuv146,
                EnvironmentImageDialect::Node20ParentSnapshot,
                ProcessEnvironmentGuard::None,
                windows_job_policy,
                ProcessTopology::Inherit,
                KillTarget::WindowsProcessTree,
            );
            assert_eq!(node.validate(&node_plan), Ok(()));
        }
    }

    #[test]
    fn structural_matrix_accepts_both_rust_environment_families_and_sealed_guard() {
        for target in [
            ProcessTarget::Linux,
            ProcessTarget::MacOs,
            ProcessTarget::Windows,
        ] {
            assert_eq!(
                rust_native_policy(target, ProcessEnvironmentGuard::None)
                    .validate(&native_plan(false)),
                Ok(())
            );
            assert_eq!(
                rust_overlay_policy(target, ProcessEnvironmentGuard::None)
                    .validate(&overlay_plan(false)),
                Ok(())
            );
            assert_eq!(
                rust_native_policy(target, ProcessEnvironmentGuard::SealedNodeNoPreloadV1)
                    .validate(&native_plan(false)),
                Ok(())
            );
            assert_eq!(
                rust_overlay_policy(target, ProcessEnvironmentGuard::SealedNodeNoPreloadV1)
                    .validate(&overlay_plan(false)),
                Ok(())
            );
        }
    }

    #[test]
    fn structural_matrix_accepts_exact_browser_private_job_row() {
        let browser = policy(
            ProcessTarget::Windows,
            ProcessImageDialect::Rust193,
            EnvironmentImageDialect::Rust193CommandOverlay,
            ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
            WindowsJobPolicy::BrowserSidecarPrivate,
            ProcessTopology::Inherit,
            KillTarget::WindowsPrivateJob,
        );

        assert_eq!(browser.validate(&overlay_plan(false)), Ok(()));
    }

    #[test]
    fn structural_matrix_rejects_browser_private_job_variants() {
        let detached = policy(
            ProcessTarget::Windows,
            ProcessImageDialect::Rust193,
            EnvironmentImageDialect::Rust193CommandOverlay,
            ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
            WindowsJobPolicy::BrowserSidecarPrivate,
            ProcessTopology::WindowsDetached,
            KillTarget::WindowsPrivateJob,
        );
        assert!(matches!(
            detached.validate(&overlay_plan(true)),
            Err(PreSpawnValidationError::PrivateJobMismatch { .. })
        ));

        let unsealed = policy(
            ProcessTarget::Windows,
            ProcessImageDialect::Rust193,
            EnvironmentImageDialect::Rust193CommandOverlay,
            ProcessEnvironmentGuard::None,
            WindowsJobPolicy::BrowserSidecarPrivate,
            ProcessTopology::Inherit,
            KillTarget::WindowsPrivateJob,
        );
        assert!(matches!(
            unsealed.validate(&overlay_plan(false)),
            Err(PreSpawnValidationError::PrivateJobMismatch { .. })
        ));

        let unpaired = policy(
            ProcessTarget::Windows,
            ProcessImageDialect::Rust193,
            EnvironmentImageDialect::Rust193CommandOverlay,
            ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
            WindowsJobPolicy::None,
            ProcessTopology::Inherit,
            KillTarget::WindowsPrivateJob,
        );
        assert!(matches!(
            unpaired.validate(&overlay_plan(false)),
            Err(PreSpawnValidationError::PrivateJobMismatch { .. })
        ));
    }

    #[test]
    fn structural_matrix_rejects_group_kill_without_a_group_topology() {
        let invalid = policy(
            ProcessTarget::Linux,
            ProcessImageDialect::Rust193,
            EnvironmentImageDialect::NativeParentInherit,
            ProcessEnvironmentGuard::None,
            WindowsJobPolicy::None,
            ProcessTopology::Inherit,
            KillTarget::UnixProcessGroup,
        );

        assert_eq!(
            invalid.validate(&native_plan(false)),
            Err(PreSpawnValidationError::KillTargetMismatch {
                target: ProcessTarget::Linux,
                topology: ProcessTopology::Inherit,
                kill_target: KillTarget::UnixProcessGroup,
            })
        );
    }

    #[test]
    fn structural_matrix_accepts_all_group_creating_unix_topologies() {
        for (topology, detached) in [
            (ProcessTopology::UnixNewProcessGroup, false),
            (ProcessTopology::UnixNewSession, true),
            (ProcessTopology::UnixControllingPty, false),
        ] {
            let group = policy(
                ProcessTarget::Linux,
                ProcessImageDialect::Rust193,
                EnvironmentImageDialect::NativeParentInherit,
                ProcessEnvironmentGuard::None,
                WindowsJobPolicy::None,
                topology,
                KillTarget::UnixProcessGroup,
            );
            assert_eq!(group.validate(&native_plan(detached)), Ok(()));
        }
    }

    #[test]
    fn structural_matrix_rejects_environment_image_and_target_mismatches() {
        let wrong_dialect = policy(
            ProcessTarget::Linux,
            ProcessImageDialect::Rust193,
            EnvironmentImageDialect::NativeParentInherit,
            ProcessEnvironmentGuard::None,
            WindowsJobPolicy::None,
            ProcessTopology::Inherit,
            KillTarget::Child,
        );
        assert!(matches!(
            wrong_dialect.validate(&overlay_plan(false)),
            Err(PreSpawnValidationError::EnvironmentPlanMismatch { .. })
        ));

        let unix_job = policy(
            ProcessTarget::Linux,
            ProcessImageDialect::Node20Libuv146,
            EnvironmentImageDialect::Node20ParentSnapshot,
            ProcessEnvironmentGuard::None,
            WindowsJobPolicy::LibuvGlobal,
            ProcessTopology::Inherit,
            KillTarget::Child,
        );
        let node_plan = plan(
            ProcessEnvironmentPlan::NodeRuntime(empty_runtime_env()),
            false,
        );
        assert!(matches!(
            unix_job.validate(&node_plan),
            Err(PreSpawnValidationError::WindowsJobTargetMismatch { .. })
        ));

        let wrong_image = policy(
            ProcessTarget::Linux,
            ProcessImageDialect::Rust193,
            EnvironmentImageDialect::Node20ParentSnapshot,
            ProcessEnvironmentGuard::None,
            WindowsJobPolicy::None,
            ProcessTopology::Inherit,
            KillTarget::Child,
        );
        assert!(matches!(
            wrong_image.validate(&node_plan),
            Err(PreSpawnValidationError::ImageEnvironmentMismatch { .. })
        ));

        let invalid_guard = policy(
            ProcessTarget::Linux,
            ProcessImageDialect::Node20Libuv146,
            EnvironmentImageDialect::Node20ParentSnapshot,
            ProcessEnvironmentGuard::SealedNodeNoPreloadV1,
            WindowsJobPolicy::None,
            ProcessTopology::Inherit,
            KillTarget::Child,
        );
        assert!(matches!(
            invalid_guard.validate(&node_plan),
            Err(PreSpawnValidationError::GuardMismatch { .. })
        ));

        let wrong_target = policy(
            ProcessTarget::Linux,
            ProcessImageDialect::Rust193,
            EnvironmentImageDialect::NativeParentInherit,
            ProcessEnvironmentGuard::None,
            WindowsJobPolicy::None,
            ProcessTopology::WindowsDetached,
            KillTarget::Child,
        );
        assert!(matches!(
            wrong_target.validate(&native_plan(true)),
            Err(PreSpawnValidationError::TopologyTargetMismatch { .. })
        ));

        let wrong_detached = policy(
            ProcessTarget::Windows,
            ProcessImageDialect::Rust193,
            EnvironmentImageDialect::NativeParentInherit,
            ProcessEnvironmentGuard::None,
            WindowsJobPolicy::None,
            ProcessTopology::Inherit,
            KillTarget::Child,
        );
        assert!(matches!(
            wrong_detached.validate(&native_plan(true)),
            Err(PreSpawnValidationError::DetachedTopologyMismatch { .. })
        ));
    }
}
