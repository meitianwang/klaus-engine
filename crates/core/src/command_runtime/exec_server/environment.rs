use super::backend::ExecBackend;
use super::client::{ExecServerClient, RemoteExecBackend};
use super::protocol::EnvironmentInfo;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvironmentStatus {
    Connecting,
    Ready,
    Disconnected { reason: String },
}

#[derive(Clone)]
pub struct ExecEnvironment {
    pub id: String,
    pub info: EnvironmentInfo,
    pub status: EnvironmentStatus,
    pub backend: Arc<dyn ExecBackend>,
}

#[derive(Default)]
pub struct ExecEnvironmentRegistry {
    entries: Mutex<HashMap<String, ExecEnvironment>>,
}

impl ExecEnvironmentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_remote_client(
        &self,
        id: impl Into<String>,
        info: EnvironmentInfo,
        client: ExecServerClient,
    ) -> Arc<dyn ExecBackend> {
        let id = id.into();
        let backend: Arc<dyn ExecBackend> = Arc::new(RemoteExecBackend::new(client));
        let environment = ExecEnvironment {
            id: id.clone(),
            info,
            status: EnvironmentStatus::Ready,
            backend: Arc::clone(&backend),
        };
        self.entries().insert(id, environment);
        backend
    }

    pub fn get(&self, id: &str) -> Option<ExecEnvironment> {
        self.entries().get(id).cloned()
    }

    pub fn list(&self) -> Vec<ExecEnvironment> {
        let mut entries = self.entries().values().cloned().collect::<Vec<_>>();
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        entries
    }

    pub fn mark_disconnected(&self, id: &str, reason: impl Into<String>) -> bool {
        let mut entries = self.entries();
        let Some(entry) = entries.get_mut(id) else {
            return false;
        };
        entry.status = EnvironmentStatus::Disconnected {
            reason: reason.into(),
        };
        true
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<String, ExecEnvironment>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_runtime::exec_server::backend::LocalExecBackend;
    use crate::command_runtime::exec_server::client::{ExecServerClient, InMemoryTransport};
    use crate::command_runtime::exec_server::handler::ExecServerHandler;
    use crate::command_runtime::exec_server::protocol::ShellInfo;

    #[tokio::test]
    async fn registry_tracks_remote_backends_and_status() {
        let handler = ExecServerHandler::new(LocalExecBackend::new(
            crate::runtime_process::test_runtime_process_context(),
        ));
        let client = ExecServerClient::connect(InMemoryTransport::new(handler), "env-test")
            .await
            .unwrap();
        let registry = ExecEnvironmentRegistry::new();
        let backend = registry.register_remote_client(
            "env-1",
            EnvironmentInfo {
                shell: ShellInfo {
                    name: "sh".into(),
                    path: "/bin/sh".into(),
                },
                cwd: None,
            },
            client,
        );

        let entry = registry.get("env-1").unwrap();
        assert_eq!(entry.status, EnvironmentStatus::Ready);
        assert!(Arc::ptr_eq(&entry.backend, &backend));
        assert!(registry.mark_disconnected("env-1", "socket closed"));
        assert_eq!(
            registry.get("env-1").unwrap().status,
            EnvironmentStatus::Disconnected {
                reason: "socket closed".to_string()
            }
        );
    }
}
