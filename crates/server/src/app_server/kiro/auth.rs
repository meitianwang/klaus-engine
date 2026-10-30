//! Private, generation-bound connection credentials.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    sync::OnceLock,
};
use tokio::sync::{watch, Mutex};

#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Credential {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub profile_arn: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
    pub auth_method: String,
    pub provider: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_secret: Option<String>,
    #[serde(default)]
    pub account: Option<String>,
}
#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct Stored {
    generation: String,
    credential: Credential,
    independent_login: bool,
    #[serde(default)]
    local_follow: bool,
}
fn lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
fn refresh_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
fn updates() -> &'static watch::Sender<u64> {
    static TX: OnceLock<watch::Sender<u64>> = OnceLock::new();
    TX.get_or_init(|| watch::channel(0).0)
}
pub fn changes() -> watch::Receiver<u64> {
    updates().subscribe()
}
pub(super) fn notify_changed() {
    updates().send_modify(|v| *v = v.wrapping_add(1));
}
pub fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
fn read() -> Option<Stored> {
    let mut b = Vec::new();
    std::fs::File::open(super::config_root().join("kiro-auth.json"))
        .ok()?
        .take(131073)
        .read_to_end(&mut b)
        .ok()?;
    if b.len() > 131072 {
        return None;
    }
    serde_json::from_slice(&b).ok()
}
fn write(s: &Stored) -> Result<(), String> {
    let root = super::config_root();
    std::fs::create_dir_all(&root).map_err(|_| "kiro_store_unavailable")?;
    let tmp = root.join(format!(".kiro-auth-{}.tmp", uuid::Uuid::new_v4()));
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let result = (|| {
        let b = serde_json::to_vec(s).map_err(|_| "kiro_credential_invalid")?;
        let mut f = opts.open(&tmp).map_err(|_| "kiro_store_unavailable")?;
        f.write_all(&b).map_err(|_| "kiro_store_unavailable")?;
        f.sync_all().map_err(|_| "kiro_store_unavailable")?;
        std::fs::rename(&tmp, root.join("kiro-auth.json")).map_err(|_| "kiro_store_unavailable")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(tmp);
    }
    result
}
fn clean(v: &str, max: usize) -> bool {
    !v.is_empty() && v.len() <= max && v.trim() == v && !v.chars().any(char::is_control)
}
fn validate(c: &Credential) -> Result<(), String> {
    if !clean(&c.access_token, 32768)
        || !clean(&c.auth_method, 32)
        || !clean(&c.provider, 64)
        || c.refresh_token.as_deref().is_some_and(|v| !clean(v, 32768))
        || c.profile_arn.as_deref().is_some_and(|v| !clean(v, 2048))
        || c.expires_at
            .as_deref()
            .is_some_and(|v| chrono::DateTime::parse_from_rfc3339(v).is_err())
    {
        return Err("kiro_credential_invalid".into());
    }
    if let Some(region) = &c.region {
        if !region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || region.len() > 32
        {
            return Err("kiro_region_invalid".into());
        }
    }
    Ok(())
}
fn expiry(c: &Credential) -> Option<i64> {
    c.expires_at
        .as_deref()
        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
        .map(|v| v.timestamp_millis())
}
fn same_local_account(left: &Credential, right: &Credential) -> bool {
    left.profile_arn
        .as_deref()
        .is_some_and(|profile| !profile.is_empty())
        && left.profile_arn == right.profile_arn
        && left.provider == right.provider
        && left.auth_method == right.auth_method
        && super::region(left) == super::region(right)
}
fn local_credential() -> Result<Credential, String> {
    let test_home = (std::env::var("KLAUS_TEST").as_deref() == Ok("1"))
        .then(|| std::env::var_os("KLAUS_KIRO_LOCAL_HOME"))
        .flatten();
    let home = test_home
        .or_else(|| std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" }))
        .map(std::path::PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or("kiro_local_unreadable")?;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let mut file = options
        .open(home.join(".aws/sso/cache/kiro-auth-token.json"))
        .map_err(|_| "kiro_local_unreadable")?;
    let meta = file.metadata().map_err(|_| "kiro_local_unreadable")?;
    if !meta.is_file() || meta.len() > 131072 {
        return Err("kiro_local_unreadable".into());
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take(131073)
        .read_to_end(&mut bytes)
        .map_err(|_| "kiro_local_unreadable")?;
    if bytes.len() > 131072 {
        return Err("kiro_local_unreadable".into());
    }
    let raw: Value = serde_json::from_slice(&bytes).map_err(|_| "kiro_local_unreadable")?;
    let mut allowed = serde_json::Map::new();
    for key in [
        "accessToken",
        "refreshToken",
        "profileArn",
        "expiresAt",
        "authMethod",
        "provider",
        "region",
        "clientId",
        "clientSecret",
    ] {
        if let Some(value) = raw.get(key) {
            allowed.insert(key.into(), value.clone());
        }
    }
    let credential: Credential =
        serde_json::from_value(Value::Object(allowed)).map_err(|_| "kiro_local_unreadable")?;
    validate(&credential)?;
    if expiry(&credential).is_none() {
        return Err("kiro_local_unreadable".into());
    }
    Ok(credential)
}
pub fn status() -> Value {
    let Some(s) = read() else {
        return json!({"ok":true,"signedIn":false,"loggedIn":false});
    };
    let local = (s.local_follow && !s.independent_login)
        .then(local_credential)
        .transpose()
        .ok()
        .flatten();
    let account_changed = local
        .as_ref()
        .is_some_and(|local| !same_local_account(&s.credential, local));
    let current_local = local.as_ref().filter(|local| {
        same_local_account(&s.credential, local)
            && expiry(local).is_some_and(|at| at > now())
            && expiry(local) >= expiry(&s.credential)
    });
    let needs_login = account_changed
        || current_local.is_none()
            && expiry(&s.credential).is_some_and(|v| v <= now())
            && (!s.independent_login || s.credential.refresh_token.is_none());
    json!({"ok":true,"signedIn":true,"loggedIn":true,"account":s.credential.account.as_deref().unwrap_or(&s.credential.provider),"expiresAt":current_local.and_then(|v|v.expires_at.as_ref()).or(s.credential.expires_at.as_ref()),"needsLogin":needs_login,"needsReimport":account_changed,"localFollow":s.local_follow,"canRefresh":s.independent_login&&s.credential.refresh_token.is_some(),"imported":!s.independent_login,"authMethod":s.credential.auth_method,"provider":s.credential.provider})
}
pub fn generation() -> Option<String> {
    read().map(|s| s.generation)
}
pub fn is_current(g: &str) -> bool {
    generation().as_deref() == Some(g)
}
pub async fn adopt(c: Credential, local_follow: bool) -> Result<Value, String> {
    validate(&c)?;
    let (root, epoch) = {
        let _gate = super::login::ownership_gate();
        (super::config_root(), super::login::epoch())
    };
    let _g = lock().lock().await;
    let _gate = super::login::ownership_gate();
    if super::config_root() != root || !super::login::is_epoch(epoch) {
        return Err("kiro_credential_changed".into());
    }
    if local_follow && !same_local_account(&c, &local_credential()?) {
        return Err("kiro_local_account_changed".into());
    }
    super::login::invalidate_pending_locked();
    store(c, false, local_follow)?;
    Ok(status())
}
fn store(c: Credential, independent_login: bool, local_follow: bool) -> Result<String, String> {
    validate(&c)?;
    let generation = uuid::Uuid::new_v4().to_string();
    write(&Stored {
        generation: generation.clone(),
        credential: c,
        independent_login,
        local_follow,
    })?;
    super::forget_catalog();
    Ok(generation)
}
pub(super) async fn store_login(c: Credential, epoch: u64) -> Result<String, String> {
    let _g = lock().lock().await;
    let _gate = super::login::ownership_gate();
    if !super::login::is_epoch(epoch) {
        return Err("kiro_login_cancelled".into());
    }
    store(c, true, false)
}
pub async fn logout() -> Result<Value, String> {
    let _g = lock().lock().await;
    super::login::with_invalidation(|| {
        match std::fs::remove_file(super::config_root().join("kiro-auth.json")) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err("kiro_store_unavailable".into()),
        }
        super::forget_catalog();
        Ok(status())
    })
}
fn usable_at(stored: &Stored, now: i64) -> bool {
    let buffer = if stored.independent_login { 60000 } else { 0 };
    expiry(&stored.credential).is_none_or(|expiry| expiry > now + buffer)
}
pub async fn credential(expected: Option<&str>) -> Result<Credential, String> {
    let (root, epoch) = {
        let _gate = super::login::ownership_gate();
        (super::config_root(), super::login::epoch())
    };
    let _refresh = refresh_lock().lock().await;
    let s = {
        let _gate = super::login::ownership_gate();
        if super::config_root() != root || !super::login::is_epoch(epoch) {
            return Err("kiro_credential_changed".into());
        }
        read().ok_or("kiro_not_signed_in")?
    };
    if expected.is_some_and(|v| v != s.generation) {
        return Err("kiro_credential_changed".into());
    }
    if s.local_follow && !s.independent_login {
        let _g = lock().lock().await;
        let _gate = super::login::ownership_gate();
        let current = read().ok_or("kiro_credential_changed")?;
        if super::config_root() != root
            || current.generation != s.generation
            || !current.local_follow
            || current.independent_login
        {
            return Err("kiro_credential_changed".into());
        }
        // Read after acquiring ownership so a queued request cannot publish an old snapshot.
        if let Ok(mut local) = local_credential() {
            if !same_local_account(&current.credential, &local) {
                return Err("kiro_local_account_changed".into());
            }
            if expiry(&local).is_some_and(|at| at > now())
                && expiry(&local) >= expiry(&current.credential)
            {
                local.account = current.credential.account.clone();
                // Same-account renewal preserves replay and publication ownership.
                if local.access_token != current.credential.access_token
                    || local.refresh_token != current.credential.refresh_token
                    || local.expires_at != current.credential.expires_at
                {
                    write(&Stored {
                        credential: local.clone(),
                        ..current
                    })?;
                }
                return Ok(local);
            }
        }
    }
    if usable_at(&s, now()) {
        return Ok(s.credential);
    }
    if !s.independent_login {
        return Err("kiro_login_expired".into());
    }
    if s.credential.auth_method != "social" {
        return Err("kiro_refresh_method_unsupported".into());
    }
    let token = s
        .credential
        .refresh_token
        .as_deref()
        .ok_or("kiro_login_expired")?;
    let response = super::client()
        .post(format!("{}/refreshToken", super::auth_base()))
        .header("user-agent", "KiroIDE-1.4.26-Klaus")
        .json(&json!({"refreshToken":token}))
        .send()
        .await
        .map_err(|_| "kiro_refresh_transport_failed")?;
    let value = super::bounded_json(response).await?;
    let mut credential = s.credential.clone();
    credential.access_token = value["accessToken"]
        .as_str()
        .ok_or("kiro_refresh_invalid")?
        .to_owned();
    if let Some(v) = value["refreshToken"].as_str() {
        credential.refresh_token = Some(v.into())
    }
    let seconds = value["expiresIn"]
        .as_i64()
        .filter(|v| *v > 0 && *v <= 604800)
        .ok_or("kiro_refresh_invalid")?;
    credential.expires_at =
        Some((chrono::Utc::now() + chrono::Duration::seconds(seconds)).to_rfc3339());
    validate(&credential)?;
    let _g = lock().lock().await;
    let _gate = super::login::ownership_gate();
    if super::config_root() != root || !is_current(&s.generation) {
        return Err("kiro_credential_changed".into());
    }
    write(&Stored {
        credential: credential.clone(),
        ..s
    })?;
    Ok(credential)
}

#[cfg(test)]
mod tests {
    use super::*;
    struct LocalFixture {
        _root: tempfile::TempDir,
        local: std::path::PathBuf,
        store: std::path::PathBuf,
        previous_root: std::path::PathBuf,
        env: Vec<(&'static str, Option<std::ffi::OsString>)>,
    }
    impl LocalFixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let home = root.path().join("external");
            let local = home.join(".aws/sso/cache/kiro-auth-token.json");
            std::fs::create_dir_all(local.parent().unwrap()).unwrap();
            let mut env = Vec::new();
            for (key, value) in [
                ("KLAUS_TEST", std::ffi::OsString::from("1")),
                ("KLAUS_KIRO_LOCAL_HOME", home.into_os_string()),
            ] {
                env.push((key, std::env::var_os(key)));
                std::env::set_var(key, value);
            }
            let previous_root = super::super::config_root();
            let store = root.path().join("klaus");
            super::super::set_config_root(&store);
            Self {
                _root: root,
                local,
                store,
                previous_root,
                env,
            }
        }
        fn write_local(&self, token: &str, expires: &str, profile: &str) -> Credential {
            let value = json!({"accessToken":token,"refreshToken":"externally-owned-refresh","expiresAt":expires,"profileArn":profile,"authMethod":"social","provider":"Google","extraPrivateField":"not-copied"});
            std::fs::write(&self.local, serde_json::to_vec(&value).unwrap()).unwrap();
            local_credential().unwrap()
        }
    }
    impl Drop for LocalFixture {
        fn drop(&mut self) {
            super::super::set_config_root(&self.previous_root);
            for (key, value) in self.env.drain(..) {
                if let Some(value) = value {
                    std::env::set_var(key, value);
                } else {
                    std::env::remove_var(key);
                }
            }
        }
    }
    #[tokio::test]
    async fn local_follow_renews_only_the_bound_account_without_rotating_external_state() {
        let fixture = LocalFixture::new();
        let profile = "arn:aws:codewhisperer:us-east-1:123:profile/local-fixture";
        let mut first = fixture.write_local("first", "2030-01-01T00:00:00Z", profile);
        first.account = Some("Fixture account".into());
        adopt(first.clone(), true).await.unwrap();
        let generation = generation().unwrap();
        let changes = changes();
        let before_change = *changes.borrow();
        let mut expired = read().unwrap();
        expired.credential.expires_at = Some("2020-01-01T00:00:00Z".into());
        write(&expired).unwrap();
        fixture.write_local("renewed", "2031-01-01T00:00:00Z", profile);
        let external = std::fs::read(&fixture.local).unwrap();
        assert_eq!(status()["needsLogin"], false);
        let renewed = credential(Some(&generation)).await.unwrap();
        assert_eq!(renewed.access_token, "renewed");
        assert_eq!(renewed.account.as_deref(), Some("Fixture account"));
        assert_eq!(super::generation().as_deref(), Some(generation.as_str()));
        assert_eq!(*changes.borrow(), before_change);
        assert_eq!(std::fs::read(&fixture.local).unwrap(), external);
        std::fs::write(&fixture.local, b"{").unwrap();
        assert_eq!(
            credential(Some(&generation)).await.unwrap().access_token,
            "renewed"
        );
        fixture.write_local("stale", "2020-01-01T00:00:00Z", profile);
        assert_eq!(
            credential(Some(&generation)).await.unwrap().access_token,
            "renewed"
        );
        fixture.write_local(
            "another-account",
            "2032-01-01T00:00:00Z",
            "arn:aws:codewhisperer:us-east-1:123:profile/other",
        );
        assert_eq!(status()["needsReimport"], true);
        assert!(
            matches!(credential(Some(&generation)).await, Err(error) if error == "kiro_local_account_changed")
        );
        assert_eq!(read().unwrap().credential.access_token, "renewed");
        adopt(first.clone(), false).await.unwrap();
        assert_eq!(credential(None).await.unwrap().access_token, "first");
        store(first, true, false).unwrap();
        assert_eq!(credential(None).await.unwrap().access_token, "first");
    }
    #[tokio::test]
    async fn delayed_local_follow_cannot_write_after_the_config_root_changes() {
        let fixture = LocalFixture::new();
        let profile = "arn:aws:codewhisperer:us-east-1:123:profile/local-fixture";
        let first = fixture.write_local("first", "2030-01-01T00:00:00Z", profile);
        adopt(first, true).await.unwrap();
        let generation = generation().unwrap();
        fixture.write_local("renewed", "2031-01-01T00:00:00Z", profile);
        let lock = lock().lock().await;
        let mut pending = Box::pin(credential(Some(&generation)));
        assert!(futures::poll!(&mut pending).is_pending());
        let replacement = fixture._root.path().join("replacement");
        super::super::set_config_root(&replacement);
        drop(lock);
        assert!(matches!(pending.await, Err(error) if error == "kiro_credential_changed"));
        assert!(!replacement.join("kiro-auth.json").exists());
        let original: Value =
            serde_json::from_slice(&std::fs::read(fixture.store.join("kiro-auth.json")).unwrap())
                .unwrap();
        assert_eq!(original["credential"]["accessToken"], "first");
    }
    #[tokio::test]
    async fn delayed_local_follow_reads_the_latest_account_after_acquiring_ownership() {
        let fixture = LocalFixture::new();
        let profile = "arn:aws:codewhisperer:us-east-1:123:profile/local-fixture";
        let first = fixture.write_local("first", "2030-01-01T00:00:00Z", profile);
        adopt(first, true).await.unwrap();
        let generation = generation().unwrap();
        let guard = lock().lock().await;
        let mut pending = Box::pin(credential(Some(&generation)));
        assert!(futures::poll!(&mut pending).is_pending());
        fixture.write_local(
            "other",
            "2031-01-01T00:00:00Z",
            "arn:aws:codewhisperer:us-east-1:123:profile/other",
        );
        drop(guard);
        assert!(matches!(pending.await, Err(error) if error == "kiro_local_account_changed"));
        assert_eq!(read().unwrap().credential.access_token, "first");
    }
    #[tokio::test]
    async fn delayed_adoption_cannot_write_into_a_replacement_config_root() {
        let fixture = LocalFixture::new();
        let first = fixture.write_local(
            "first",
            "2030-01-01T00:00:00Z",
            "arn:aws:codewhisperer:us-east-1:123:profile/local-fixture",
        );
        let guard = lock().lock().await;
        let mut pending = Box::pin(adopt(first, true));
        assert!(futures::poll!(&mut pending).is_pending());
        let replacement = fixture._root.path().join("replacement");
        super::super::set_config_root(&replacement);
        drop(guard);
        assert!(matches!(pending.await, Err(error) if error == "kiro_credential_changed"));
        assert!(!replacement.join("kiro-auth.json").exists());
    }
    #[test]
    fn rejects_invalid_expiry_and_control_tokens() {
        let mut c: Credential = serde_json::from_value(
            json!({"accessToken":"token","authMethod":"social","provider":"Google"}),
        )
        .unwrap();
        assert!(validate(&c).is_ok());
        c.expires_at = Some("bad".into());
        assert!(validate(&c).is_err());
        c.expires_at = None;
        c.access_token = "secret\nheader".into();
        assert!(validate(&c).is_err());
    }
    #[test]
    fn imported_token_is_valid_until_its_exact_expiry() {
        let mut c:Credential=serde_json::from_value(json!({"accessToken":"token","authMethod":"social","provider":"Google","expiresAt":"2030-01-01T00:00:00Z"})).unwrap();
        c.refresh_token = Some("external-refresh".into());
        let expiry = super::expiry(&c).unwrap();
        let mut stored = Stored {
            generation: "fixture".into(),
            credential: c,
            independent_login: false,
            local_follow: false,
        };
        assert!(usable_at(&stored, expiry - 1));
        assert!(usable_at(&stored, expiry - 59999));
        assert!(!usable_at(&stored, expiry));
        stored.independent_login = true;
        assert!(!usable_at(&stored, expiry - 59999));
        assert!(usable_at(&stored, expiry - 60001));
    }
}
