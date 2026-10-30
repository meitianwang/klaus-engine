//! Shared runtime spawn shadow.
//!
//! The adapter is connected to the project-owned Unix unpublished transaction
//! and shared reaper, but no production call site is connected. The default
//! shadow initializer remains fail-closed; Linux callers can explicitly build
//! the initialized SIGCHLD/pidfd variant before any descriptor/fork effect.

use std::fmt;

use tokio::runtime::Handle;

#[cfg(target_os = "linux")]
use super::linux_notifier::LinuxNotifierInitializationError;
#[cfg(target_os = "macos")]
use super::macos_notifier::MacOsNotifierInitializationError;
#[cfg(target_os = "macos")]
use super::macos_posix_spawn::{
    spawn_node20_libuv146_unregistered, spawn_node20_pty_unregistered, spawn_rust193_unregistered,
    MacOsPosixSpawnError, MacOsPtySize, Node20MacOsSpawnFeatures,
};
use super::reaper::{
    ReaperAdmissionError, ReaperCommitError, RuntimeChild, RuntimeDropAction, SharedUnixReaper,
    UnpublishedUnixCleanupOutcome,
};
use super::unix::UnixExecImage;
#[cfg(test)]
use super::unix_spawn::UnixSpawnTestControl;
#[cfg(target_os = "linux")]
use super::unix_spawn::{prepare_node20_pty_stdio, SHADOW_FORK_DESCRIPTOR_LOCK};
use super::unix_spawn::{
    ParentStdio, ParentStdioAttachmentError, Rust193DirectPipeShadowPlan,
    Rust193ForkFallbackShadowSpawner, UnixShadowSpawnError, UnixStdioPlan,
};
use super::{KillTarget, ProcessImageDialect, ProcessTopology};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SharedSpawnPolicy {
    image_dialect: ProcessImageDialect,
    topology: ProcessTopology,
    drop_action: RuntimeDropAction,
}

impl SharedSpawnPolicy {
    pub(super) const fn rust193_inherit(drop_action: RuntimeDropAction) -> Self {
        Self {
            image_dialect: ProcessImageDialect::Rust193,
            topology: ProcessTopology::Inherit,
            drop_action,
        }
    }

    pub(super) const fn node20(topology: ProcessTopology, drop_action: RuntimeDropAction) -> Self {
        Self {
            image_dialect: ProcessImageDialect::Node20Libuv146,
            topology,
            drop_action,
        }
    }

    #[cfg(target_os = "macos")]
    pub(super) const fn node20_macos(
        topology: ProcessTopology,
        drop_action: RuntimeDropAction,
    ) -> Self {
        Self::node20(topology, drop_action)
    }

    #[cfg(test)]
    pub(super) const fn for_test(
        image_dialect: ProcessImageDialect,
        topology: ProcessTopology,
        drop_action: RuntimeDropAction,
    ) -> Self {
        Self {
            image_dialect,
            topology,
            drop_action,
        }
    }

    fn validate(&self, image: &UnixExecImage) -> Result<(), SharedSpawnPolicyError> {
        if self.image_dialect != ProcessImageDialect::Rust193 {
            return Err(SharedSpawnPolicyError::UnsupportedImageDialect {
                requested: self.image_dialect,
            });
        }
        if image.image_dialect() != self.image_dialect {
            return Err(SharedSpawnPolicyError::ImageDialectMismatch {
                image: image.image_dialect(),
                requested: self.image_dialect,
            });
        }
        if self.topology != ProcessTopology::Inherit {
            return Err(SharedSpawnPolicyError::UnsupportedTopology {
                requested: self.topology,
            });
        }
        if let RuntimeDropAction::HardKill(target) = self.drop_action {
            if target != KillTarget::Child {
                return Err(SharedSpawnPolicyError::UnsupportedDropTarget { target });
            }
        }
        Ok(())
    }

    fn kill_target(&self) -> KillTarget {
        match self.topology {
            ProcessTopology::Inherit => KillTarget::Child,
            ProcessTopology::UnixNewProcessGroup
            | ProcessTopology::UnixNewSession
            | ProcessTopology::UnixControllingPty => KillTarget::UnixProcessGroup,
            ProcessTopology::WindowsDetached => KillTarget::Child,
        }
    }

    #[cfg(target_os = "macos")]
    fn validate_node20_macos(&self, image: &UnixExecImage) -> Result<(), SharedSpawnPolicyError> {
        self.validate_node20(image)
    }

    fn validate_node20(&self, image: &UnixExecImage) -> Result<(), SharedSpawnPolicyError> {
        if self.image_dialect != ProcessImageDialect::Node20Libuv146 {
            return Err(SharedSpawnPolicyError::UnsupportedImageDialect {
                requested: self.image_dialect,
            });
        }
        if image.image_dialect() != self.image_dialect {
            return Err(SharedSpawnPolicyError::ImageDialectMismatch {
                image: image.image_dialect(),
                requested: self.image_dialect,
            });
        }
        if self.topology == ProcessTopology::WindowsDetached {
            return Err(SharedSpawnPolicyError::UnsupportedTopology {
                requested: self.topology,
            });
        }
        if let RuntimeDropAction::HardKill(target) = self.drop_action {
            let expected = match self.topology {
                ProcessTopology::Inherit => KillTarget::Child,
                ProcessTopology::UnixNewProcessGroup
                | ProcessTopology::UnixNewSession
                | ProcessTopology::UnixControllingPty => KillTarget::UnixProcessGroup,
                ProcessTopology::WindowsDetached => unreachable!(),
            };
            if target != expected {
                return Err(SharedSpawnPolicyError::UnsupportedDropTarget { target });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SharedSpawnPolicyError {
    ImageDialectMismatch {
        image: ProcessImageDialect,
        requested: ProcessImageDialect,
    },
    UnsupportedImageDialect {
        requested: ProcessImageDialect,
    },
    UnsupportedTopology {
        requested: ProcessTopology,
    },
    UnsupportedDropTarget {
        target: KillTarget,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SharedSpawnError {
    Policy(SharedSpawnPolicyError),
    Reaper(ReaperAdmissionError),
    ReaperCommit(ReaperCommitError),
    ParentStdioAttachment(ParentStdioAttachmentError),
    ParentStdioAttachmentCleanup {
        original_error: ParentStdioAttachmentError,
        cleanup: UnpublishedUnixCleanupOutcome,
    },
    Unix(UnixShadowSpawnError),
    #[cfg(target_os = "macos")]
    MacOs(MacOsPosixSpawnError),
}

impl fmt::Display for SharedSpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy(error) => write!(formatter, "shared spawn policy rejected: {error:?}"),
            Self::Reaper(error) => write!(formatter, "shared reaper rejected spawn: {error}"),
            Self::ReaperCommit(error) => {
                write!(
                    formatter,
                    "shared reaper retained hidden cleanup: {error:?}"
                )
            }
            Self::ParentStdioAttachment(error) => {
                write!(formatter, "parent stdio attachment failed: {error:?}")
            }
            Self::ParentStdioAttachmentCleanup {
                original_error,
                cleanup,
            } => write!(
                formatter,
                "parent stdio attachment failed ({original_error:?}); cleanup retained: {cleanup:?}"
            ),
            Self::Unix(error) => write!(formatter, "Unix spawn failed: {error}"),
            #[cfg(target_os = "macos")]
            Self::MacOs(error) => write!(formatter, "macOS spawn failed: {error:?}"),
        }
    }
}

impl std::error::Error for SharedSpawnError {}

pub(super) struct SharedSpawnedRuntimeProcess {
    child: RuntimeChild,
    stdio: ParentStdio,
}

impl SharedSpawnedRuntimeProcess {
    pub(super) fn into_parts(self) -> (RuntimeChild, ParentStdio) {
        (self.child, self.stdio)
    }
}

#[derive(Clone)]
pub(super) struct SharedRuntimeSpawner {
    reaper: SharedUnixReaper,
    runtime: Option<Handle>,
}

impl SharedRuntimeSpawner {
    pub(super) fn initialize_shadow() -> Self {
        Self {
            reaper: SharedUnixReaper::initialize_shadow(),
            runtime: None,
        }
    }

    #[cfg(target_os = "linux")]
    pub(super) fn initialize_linux() -> Result<Self, LinuxNotifierInitializationError> {
        let runtime = Handle::try_current()
            .map_err(|_| LinuxNotifierInitializationError::TokioRuntimeUnavailable)?;
        Self::initialize_linux_with_runtime(runtime)
    }

    #[cfg(target_os = "linux")]
    pub(super) fn initialize_linux_with_runtime(
        runtime: Handle,
    ) -> Result<Self, LinuxNotifierInitializationError> {
        Ok(Self {
            reaper: SharedUnixReaper::initialize_linux_with_runtime(runtime.clone())?,
            runtime: Some(runtime),
        })
    }

    #[cfg(target_os = "macos")]
    pub(super) fn initialize_macos() -> Result<Self, MacOsNotifierInitializationError> {
        let runtime = Handle::try_current()
            .map_err(|_| MacOsNotifierInitializationError::TokioRuntimeUnavailable)?;
        Self::initialize_macos_with_runtime(runtime)
    }

    #[cfg(target_os = "macos")]
    pub(super) fn initialize_macos_with_runtime(
        runtime: Handle,
    ) -> Result<Self, MacOsNotifierInitializationError> {
        Ok(Self {
            reaper: SharedUnixReaper::initialize_macos_with_runtime(runtime.clone())?,
            runtime: Some(runtime),
        })
    }

    pub(super) async fn shutdown(&self) -> Result<(), super::reaper::ReaperShutdownError> {
        self.reaper.shutdown().await
    }

    #[cfg(test)]
    pub(super) fn with_reaper_for_test(reaper: SharedUnixReaper) -> Self {
        Self {
            reaper,
            runtime: Some(Handle::try_current().expect("test spawner requires a Tokio runtime")),
        }
    }

    fn runtime(&self) -> &Handle {
        self.runtime
            .as_ref()
            .unwrap_or_else(|| std::process::abort())
    }

    pub(super) async fn spawn_rust193_fork_shadow(
        &self,
        image: UnixExecImage,
        stdio: UnixStdioPlan,
        policy: SharedSpawnPolicy,
    ) -> Result<SharedSpawnedRuntimeProcess, SharedSpawnError> {
        policy.validate(&image).map_err(SharedSpawnError::Policy)?;
        let mut permit = self
            .reaper
            .admit_spawn_with_target(policy.drop_action, policy.kill_target())
            .map_err(SharedSpawnError::Reaper)?;
        let spawned = Rust193ForkFallbackShadowSpawner::spawn_unregistered(
            permit
                .authorize_unix_spawn()
                .map_err(SharedSpawnError::Reaper)?,
            image,
            Rust193DirectPipeShadowPlan::rust193_fork_fallback(stdio),
        )
        .map_err(SharedSpawnError::Unix)?;
        let (unregistered_child, stdio) = spawned.into_parts();
        let stdio = match stdio.attach(self.runtime()) {
            Ok(stdio) => stdio,
            Err(original_error) => {
                let cleanup = permit
                    .cleanup_unpublished_unix(unregistered_child, || {})
                    .await;
                return Err(parent_stdio_attachment_error(original_error, cleanup));
            }
        };
        let mut stdio = Some(stdio);
        let child = permit
            .commit_unix(unregistered_child, || drop(stdio.take()))
            .await
            .map_err(SharedSpawnError::ReaperCommit)?;
        let stdio = stdio.expect("successful commit retains parent endpoints");
        Ok(SharedSpawnedRuntimeProcess { child, stdio })
    }

    #[cfg(target_os = "linux")]
    pub(super) async fn spawn_node20_fork_shadow(
        &self,
        image: UnixExecImage,
        stdio: UnixStdioPlan,
        policy: SharedSpawnPolicy,
    ) -> Result<SharedSpawnedRuntimeProcess, SharedSpawnError> {
        policy
            .validate_node20(&image)
            .map_err(SharedSpawnError::Policy)?;
        if policy.topology == ProcessTopology::UnixControllingPty {
            return Err(SharedSpawnError::Policy(
                SharedSpawnPolicyError::UnsupportedTopology {
                    requested: policy.topology,
                },
            ));
        }
        let mut permit = self
            .reaper
            .admit_spawn_with_target(policy.drop_action, policy.kill_target())
            .map_err(SharedSpawnError::Reaper)?;
        let spawned = Rust193ForkFallbackShadowSpawner::spawn_unregistered(
            permit
                .authorize_unix_spawn()
                .map_err(SharedSpawnError::Reaper)?,
            image,
            Rust193DirectPipeShadowPlan::node20_fork(stdio, policy.topology),
        )
        .map_err(SharedSpawnError::Unix)?;
        let (unregistered_child, stdio) = spawned.into_parts();
        let stdio = match stdio.attach(self.runtime()) {
            Ok(stdio) => stdio,
            Err(original_error) => {
                let cleanup = permit
                    .cleanup_unpublished_unix(unregistered_child, || {})
                    .await;
                return Err(parent_stdio_attachment_error(original_error, cleanup));
            }
        };
        let mut stdio = Some(stdio);
        let child = permit
            .commit_unix(unregistered_child, || drop(stdio.take()))
            .await
            .map_err(SharedSpawnError::ReaperCommit)?;
        Ok(SharedSpawnedRuntimeProcess {
            child,
            stdio: stdio.expect("successful commit retains parent endpoints"),
        })
    }

    #[cfg(target_os = "linux")]
    pub(super) async fn spawn_node20_pty_fork_shadow(
        &self,
        image: UnixExecImage,
        rows: u16,
        columns: u16,
        pixel_width: u16,
        pixel_height: u16,
        policy: SharedSpawnPolicy,
    ) -> Result<SharedSpawnedRuntimeProcess, SharedSpawnError> {
        use std::sync::PoisonError;

        policy
            .validate_node20(&image)
            .map_err(SharedSpawnError::Policy)?;
        if policy.topology != ProcessTopology::UnixControllingPty {
            return Err(SharedSpawnError::Policy(
                SharedSpawnPolicyError::UnsupportedTopology {
                    requested: policy.topology,
                },
            ));
        }
        let mut permit = self
            .reaper
            .admit_spawn_with_target(policy.drop_action, policy.kill_target())
            .map_err(SharedSpawnError::Reaper)?;
        let fork_guard = SHADOW_FORK_DESCRIPTOR_LOCK
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let prepared = prepare_node20_pty_stdio(rows, columns, pixel_width, pixel_height)
            .map_err(SharedSpawnError::Unix)?;
        let stdio_plan = UnixStdioPlan::new(
            super::unix_spawn::UnixStdioDisposition::Pipe,
            super::unix_spawn::UnixStdioDisposition::Pipe,
            super::unix_spawn::UnixStdioDisposition::Pipe,
        )
        .as_pty();
        let spawned = Rust193ForkFallbackShadowSpawner::spawn_unregistered_with_prepared_and_guard(
            permit
                .authorize_unix_spawn()
                .map_err(SharedSpawnError::Reaper)?,
            image,
            Rust193DirectPipeShadowPlan::node20_fork(stdio_plan, policy.topology),
            prepared,
            fork_guard,
        )
        .map_err(SharedSpawnError::Unix)?;
        let (unregistered_child, stdio) = spawned.into_parts();
        let stdio = match stdio.attach(self.runtime()) {
            Ok(stdio) => stdio,
            Err(original_error) => {
                let cleanup = permit
                    .cleanup_unpublished_unix(unregistered_child, || {})
                    .await;
                return Err(parent_stdio_attachment_error(original_error, cleanup));
            }
        };
        let mut stdio = Some(stdio);
        let child = permit
            .commit_unix(unregistered_child, || drop(stdio.take()))
            .await
            .map_err(SharedSpawnError::ReaperCommit)?;
        Ok(SharedSpawnedRuntimeProcess {
            child,
            stdio: stdio.expect("successful commit retains PTY master"),
        })
    }

    #[cfg(target_os = "macos")]
    pub(super) async fn spawn_node20_macos_shadow(
        &self,
        image: UnixExecImage,
        stdio: UnixStdioPlan,
        policy: SharedSpawnPolicy,
    ) -> Result<SharedSpawnedRuntimeProcess, SharedSpawnError> {
        policy
            .validate_node20_macos(&image)
            .map_err(SharedSpawnError::Policy)?;
        if policy.topology == ProcessTopology::UnixControllingPty {
            return Err(SharedSpawnError::Policy(
                SharedSpawnPolicyError::UnsupportedTopology {
                    requested: policy.topology,
                },
            ));
        }
        let mut permit = self
            .reaper
            .admit_spawn_with_target(policy.drop_action, policy.kill_target())
            .map_err(SharedSpawnError::Reaper)?;
        let spawned = spawn_node20_libuv146_unregistered(
            permit
                .authorize_unix_spawn()
                .map_err(SharedSpawnError::Reaper)?,
            image,
            stdio,
            Node20MacOsSpawnFeatures::ordinary(policy.topology),
        )
        .map_err(SharedSpawnError::MacOs)?;
        let (unregistered_child, stdio) = spawned.into_parts();
        let stdio = match stdio.attach(self.runtime()) {
            Ok(stdio) => stdio,
            Err(original_error) => {
                let cleanup = permit
                    .cleanup_unpublished_unix(unregistered_child, || {})
                    .await;
                return Err(parent_stdio_attachment_error(original_error, cleanup));
            }
        };
        let mut stdio = Some(stdio);
        let child = permit
            .commit_unix(unregistered_child, || drop(stdio.take()))
            .await
            .map_err(SharedSpawnError::ReaperCommit)?;
        Ok(SharedSpawnedRuntimeProcess {
            child,
            stdio: stdio.expect("successful commit retains parent endpoints"),
        })
    }

    #[cfg(target_os = "macos")]
    pub(super) async fn spawn_node20_macos_pty_shadow(
        &self,
        image: UnixExecImage,
        size: MacOsPtySize,
        policy: SharedSpawnPolicy,
    ) -> Result<SharedSpawnedRuntimeProcess, SharedSpawnError> {
        policy
            .validate_node20_macos(&image)
            .map_err(SharedSpawnError::Policy)?;
        if policy.topology != ProcessTopology::UnixControllingPty {
            return Err(SharedSpawnError::Policy(
                SharedSpawnPolicyError::UnsupportedTopology {
                    requested: policy.topology,
                },
            ));
        }
        let mut permit = self
            .reaper
            .admit_spawn_with_target(policy.drop_action, policy.kill_target())
            .map_err(SharedSpawnError::Reaper)?;
        let spawned = spawn_node20_pty_unregistered(
            permit
                .authorize_unix_spawn()
                .map_err(SharedSpawnError::Reaper)?,
            image,
            size,
        )
        .map_err(SharedSpawnError::MacOs)?;
        let (unregistered_child, stdio) = spawned.into_parts();
        let stdio = match stdio.attach(self.runtime()) {
            Ok(stdio) => stdio,
            Err(original_error) => {
                let cleanup = permit
                    .cleanup_unpublished_unix(unregistered_child, || {})
                    .await;
                return Err(parent_stdio_attachment_error(original_error, cleanup));
            }
        };
        let mut stdio = Some(stdio);
        let child = permit
            .commit_unix(unregistered_child, || drop(stdio.take()))
            .await
            .map_err(SharedSpawnError::ReaperCommit)?;
        Ok(SharedSpawnedRuntimeProcess {
            child,
            stdio: stdio.expect("successful commit retains the PTY master endpoints"),
        })
    }

    #[cfg(target_os = "macos")]
    pub(super) async fn spawn_rust193_macos_shadow(
        &self,
        image: UnixExecImage,
        stdio: UnixStdioPlan,
        policy: SharedSpawnPolicy,
    ) -> Result<SharedSpawnedRuntimeProcess, SharedSpawnError> {
        policy.validate(&image).map_err(SharedSpawnError::Policy)?;
        let mut permit = self
            .reaper
            .admit_spawn_with_target(policy.drop_action, policy.kill_target())
            .map_err(SharedSpawnError::Reaper)?;
        let spawned = spawn_rust193_unregistered(
            permit
                .authorize_unix_spawn()
                .map_err(SharedSpawnError::Reaper)?,
            image,
            stdio,
        )
        .map_err(SharedSpawnError::MacOs)?;
        let (unregistered_child, stdio) = spawned.into_parts();
        let stdio = match stdio.attach(self.runtime()) {
            Ok(stdio) => stdio,
            Err(original_error) => {
                let cleanup = permit
                    .cleanup_unpublished_unix(unregistered_child, || {})
                    .await;
                return Err(parent_stdio_attachment_error(original_error, cleanup));
            }
        };
        let mut stdio = Some(stdio);
        let child = permit
            .commit_unix(unregistered_child, || drop(stdio.take()))
            .await
            .map_err(SharedSpawnError::ReaperCommit)?;
        Ok(SharedSpawnedRuntimeProcess {
            child,
            stdio: stdio.expect("successful commit retains parent endpoints"),
        })
    }

    #[cfg(test)]
    pub(super) async fn spawn_rust193_fork_with_test_control(
        &self,
        image: UnixExecImage,
        stdio: UnixStdioPlan,
        policy: SharedSpawnPolicy,
        control: &mut UnixSpawnTestControl,
    ) -> Result<SharedSpawnedRuntimeProcess, SharedSpawnError> {
        policy.validate(&image).map_err(SharedSpawnError::Policy)?;
        let mut permit = self
            .reaper
            .admit_spawn_with_target(policy.drop_action, policy.kill_target())
            .map_err(SharedSpawnError::Reaper)?;
        let spawned = Rust193ForkFallbackShadowSpawner::spawn_unregistered_with_test_control(
            permit
                .authorize_unix_spawn()
                .map_err(SharedSpawnError::Reaper)?,
            image,
            Rust193DirectPipeShadowPlan::rust193_fork_fallback(stdio),
            control,
        )
        .map_err(SharedSpawnError::Unix)?;
        let (unregistered_child, stdio) = spawned.into_parts();
        let stdio = match stdio.attach(self.runtime()) {
            Ok(stdio) => stdio,
            Err(original_error) => {
                let cleanup = permit
                    .cleanup_unpublished_unix(unregistered_child, || {})
                    .await;
                return Err(parent_stdio_attachment_error(original_error, cleanup));
            }
        };
        let mut stdio = Some(stdio);
        let child = permit
            .commit_unix(unregistered_child, || drop(stdio.take()))
            .await
            .map_err(SharedSpawnError::ReaperCommit)?;
        let stdio = stdio.expect("successful commit retains parent endpoints");
        Ok(SharedSpawnedRuntimeProcess { child, stdio })
    }
}

fn parent_stdio_attachment_error(
    original_error: ParentStdioAttachmentError,
    cleanup: UnpublishedUnixCleanupOutcome,
) -> SharedSpawnError {
    match cleanup {
        UnpublishedUnixCleanupOutcome::Reaped { .. } => {
            SharedSpawnError::ParentStdioAttachment(original_error)
        }
        cleanup @ (UnpublishedUnixCleanupOutcome::ProcessStillLive { .. }
        | UnpublishedUnixCleanupOutcome::RetainedInvariant { .. }) => {
            SharedSpawnError::ParentStdioAttachmentCleanup {
                original_error,
                cleanup,
            }
        }
    }
}
