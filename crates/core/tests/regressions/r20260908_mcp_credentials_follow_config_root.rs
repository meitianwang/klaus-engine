//! Defect: an isolated endpoint's MCP OAuth data was written to the real user profile.
//! Root cause: FileCredentialStore bypassed the engine's config-home resolver.
//! Fixed for the 2026-09-08 Electron MCP profile-isolation work item.

use klaus_agent_core::mcp::credential_store::{CredentialStore, FileCredentialStore};

struct ProfileRoots(std::path::PathBuf);

impl Drop for ProfileRoots {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn explicit_profile_credentials_reopen_and_revoke_without_cross_profile_fallback() {
    let scratch = ProfileRoots(std::env::temp_dir().join(format!(
        "klaus-credential-profiles-{}",
        uuid::Uuid::new_v4()
    )));
    let first_root = scratch.0.join("first");
    let second_root = scratch.0.join("second");
    let first = FileCredentialStore::at_config_root(&first_root);
    let second = FileCredentialStore::at_config_root(&second_root);
    assert!(!first_root.exists());
    assert!(!second_root.exists());

    for part in ["discovery", "client", "tokens"] {
        let key = format!("mcp:oauth:profile-fixture|0000000000000000:{part}");
        first.set(&key, "first-profile-value").await.unwrap();
        assert_eq!(second.get(&key).await.unwrap(), None);
        second.set(&key, "second-profile-value").await.unwrap();
    }
    let second_bytes = std::fs::read(second_root.join("credentials.enc")).unwrap();
    let second_key = std::fs::read(second_root.join("cred.key")).unwrap();
    assert_eq!(second_key.len(), 32);
    assert_ne!(
        std::fs::read(first_root.join("cred.key")).unwrap(),
        second_key,
        "independent profiles must own independent data keys"
    );
    let reopened = FileCredentialStore::at_config_root(&first_root);
    for part in ["discovery", "client", "tokens"] {
        let key = format!("mcp:oauth:profile-fixture|0000000000000000:{part}");
        assert_eq!(
            reopened.get(&key).await.unwrap().as_deref(),
            Some("first-profile-value")
        );
        reopened.delete(&key).await.unwrap();
        assert_eq!(first.get(&key).await.unwrap(), None);
        assert_eq!(
            second.get(&key).await.unwrap().as_deref(),
            Some("second-profile-value")
        );
    }
    assert_eq!(
        std::fs::read(second_root.join("credentials.enc")).unwrap(),
        second_bytes
    );
    assert_eq!(
        std::fs::read(second_root.join("cred.key")).unwrap(),
        second_key
    );
}
