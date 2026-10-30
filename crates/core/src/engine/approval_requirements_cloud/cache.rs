// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to persist native subscription requirements with local identity binding.

use super::{Bundle, CacheRevision, ManagedReviewCloudIdentity};
use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const CACHE_VERSION: u32 = 1;
const CACHE_FILENAME: &str = "cloud-config-bundle-cache.json";
const CACHE_HMAC_KEY: &[u8] =
    b"klaus-cloud-config-bundle-cache-v1-6160ae70-bcfd-4ca8-a99b-40f73b3b072e";

pub(super) struct BundleCache {
    path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SignedFile {
    signed_payload: Payload,
    signature: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Payload {
    version: u32,
    cached_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    chatgpt_user_id: Option<String>,
    account_id: Option<String>,
    bundle: Bundle,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn sign(payload: &[u8]) -> io::Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(CACHE_HMAC_KEY)
        .map_err(|_| invalid("invalid cloud cache key"))?;
    mac.update(payload);
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn verified(payload: &[u8], signature: &str) -> bool {
    let Ok(signature) = base64::engine::general_purpose::STANDARD.decode(signature) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(CACHE_HMAC_KEY) else {
        return false;
    };
    mac.update(payload);
    mac.verify_slice(&signature).is_ok()
}

impl BundleCache {
    pub(super) fn new(home: PathBuf) -> Self {
        Self {
            path: home.join(CACHE_FILENAME),
        }
    }

    pub(super) async fn load(&self, identity: &ManagedReviewCloudIdentity) -> io::Result<Bundle> {
        let (Some(user), Some(account)) = (&identity.chatgpt_user_id, &identity.account_id) else {
            return Err(invalid("cloud cache auth identity is incomplete"));
        };
        let bytes = tokio::fs::read(&self.path).await?;
        let file: SignedFile =
            serde_json::from_slice(&bytes).map_err(|_| invalid("cloud cache JSON is invalid"))?;
        let payload = serde_json::to_vec(&file.signed_payload)
            .map_err(|_| invalid("cloud cache payload is invalid"))?;
        if !verified(&payload, &file.signature) {
            return Err(invalid("cloud cache signature is invalid"));
        }
        if file.signed_payload.version != CACHE_VERSION {
            return Err(invalid("cloud cache version is unsupported"));
        }
        if file.signed_payload.chatgpt_user_id.as_ref() != Some(user)
            || file.signed_payload.account_id.as_ref() != Some(account)
        {
            return Err(invalid(
                "cloud cache belongs to another subscription identity",
            ));
        }
        if file.signed_payload.expires_at <= Utc::now() {
            return Err(invalid("cloud cache has expired"));
        }
        Ok(file.signed_payload.bundle)
    }

    pub(super) async fn store(
        &self,
        identity: &ManagedReviewCloudIdentity,
        bundle: Bundle,
        revision: CacheRevision,
    ) -> io::Result<()> {
        let now = Utc::now();
        let payload = Payload {
            version: CACHE_VERSION,
            cached_at: now,
            expires_at: now
                .checked_add_signed(Duration::hours(1))
                .ok_or_else(|| invalid("cloud cache expiry overflow"))?,
            chatgpt_user_id: identity.chatgpt_user_id.clone(),
            account_id: identity.account_id.clone(),
            bundle,
        };
        let payload_bytes = serde_json::to_vec(&payload)
            .map_err(|_| invalid("cloud cache payload cannot be serialized"))?;
        let bytes = serde_json::to_vec_pretty(&SignedFile {
            signature: sign(&payload_bytes)?,
            signed_payload: payload,
        })
        .map_err(|_| invalid("cloud cache cannot be serialized"))?;
        let destination = self.path.clone();
        tokio::task::spawn_blocking(move || atomic_write(&destination, &bytes, revision))
            .await
            .map_err(|error| io::Error::other(format!("cloud cache write failed: {error}")))?
    }
}

fn atomic_write(destination: &Path, bytes: &[u8], revision: CacheRevision) -> io::Result<()> {
    let parent = destination
        .parent()
        .ok_or_else(|| invalid("cloud cache has no parent directory"))?;
    std::fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".cloud-config-bundle-cache-{}.tmp",
        uuid::Uuid::new_v4()
    ));
    struct RemoveOnDrop(PathBuf);
    impl Drop for RemoveOnDrop {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let cleanup = RemoveOnDrop(temporary.clone());
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    file.write_all(bytes)?;
    drop(file);
    revision.commit(|| replace_file(&temporary, destination))?;
    drop(cleanup);
    Ok(())
}

#[cfg(not(windows))]
fn replace_file(source: &Path, destination: &Path) -> io::Result<()> {
    std::fs::rename(source, destination)
}

#[cfg(windows)]
fn replace_file(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let destination: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    if unsafe {
        windows_sys::Win32::Storage::FileSystem::MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            windows_sys::Win32::Storage::FileSystem::MOVEFILE_REPLACE_EXISTING,
        )
    } == 0
    {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn edited_payload_cannot_reuse_its_signature() {
        let payload = b"original";
        let signature = sign(payload).unwrap();
        assert!(verified(payload, &signature));
        assert!(!verified(b"modified", &signature));
        assert!(!verified(payload, "invalid base64"));
    }

    fn identity() -> ManagedReviewCloudIdentity {
        ManagedReviewCloudIdentity {
            chatgpt_user_id: Some("user".into()),
            account_id: Some("account".into()),
            plan_type: Some("business".into()),
            backend_base_url: "https://chatgpt.com/backend-api".into(),
            owner_generation: 1,
        }
    }

    struct TemporaryHome(PathBuf);
    impl TemporaryHome {
        fn new() -> Self {
            let path = std::env::temp_dir()
                .join(format!("klaus-cloud-cache-test-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TemporaryHome {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn signed_file(payload: Payload) -> Vec<u8> {
        let signature = sign(&serde_json::to_vec(&payload).unwrap()).unwrap();
        serde_json::to_vec(&SignedFile {
            signed_payload: payload,
            signature,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn signed_cache_requires_version_identity_and_unexpired_timestamp() {
        let home = TemporaryHome::new();
        let cache = BundleCache::new(home.0.clone());
        let payload = Payload {
            version: CACHE_VERSION,
            cached_at: Utc::now(),
            expires_at: Utc::now() + Duration::hours(1),
            chatgpt_user_id: Some("user".into()),
            account_id: Some("account".into()),
            bundle: Bundle::default(),
        };
        tokio::fs::write(&cache.path, signed_file(payload.clone()))
            .await
            .unwrap();
        assert_eq!(cache.load(&identity()).await.unwrap(), Bundle::default());
        let mut wrong_user = identity();
        wrong_user.chatgpt_user_id = Some("other".into());
        assert!(cache.load(&wrong_user).await.is_err());
        let mut wrong_account = identity();
        wrong_account.account_id = Some("other".into());
        assert!(cache.load(&wrong_account).await.is_err());
        let mut incomplete = identity();
        incomplete.chatgpt_user_id = None;
        assert!(cache.load(&incomplete).await.is_err());
        for invalid_payload in [
            Payload {
                version: CACHE_VERSION + 1,
                ..payload.clone()
            },
            Payload {
                expires_at: Utc::now() - Duration::seconds(1),
                ..payload.clone()
            },
            Payload {
                chatgpt_user_id: None,
                ..payload.clone()
            },
            Payload {
                account_id: None,
                ..payload
            },
        ] {
            tokio::fs::write(&cache.path, signed_file(invalid_payload))
                .await
                .unwrap();
            assert!(cache.load(&identity()).await.is_err());
        }
    }

    #[tokio::test]
    async fn cache_write_atomically_replaces_existing_snapshot_and_is_private() {
        use super::super::ObservedBundle;
        let home = TemporaryHome::new();
        let cache = BundleCache::new(home.0.clone());
        let observed = std::sync::Arc::new(std::sync::Mutex::new(ObservedBundle::default()));
        let revision = CacheRevision {
            observed,
            revision: 0,
        };
        cache
            .store(&identity(), Bundle::default(), revision.clone())
            .await
            .unwrap();
        let bundle = Bundle {
            requirements_toml: super::super::FragmentBucket {
                enterprise_managed: vec![super::super::Fragment {
                    id: "new".into(),
                    name: "New".into(),
                    contents: "guardian_policy_config='new'".into(),
                }],
            },
            ..Default::default()
        };
        cache
            .store(&identity(), bundle.clone(), revision)
            .await
            .unwrap();
        assert_eq!(cache.load(&identity()).await.unwrap(), bundle);
        assert_eq!(std::fs::read_dir(&home.0).unwrap().count(), 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&cache.path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
}
