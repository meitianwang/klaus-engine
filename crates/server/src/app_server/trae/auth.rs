//! Private account storage and generation-bound token renewal.

use super::{config_root, http_json};
use base64::{engine::general_purpose::STANDARD, Engine};
use ring::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_ASN1_SIGNING};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::sync::OnceLock;
use tokio::sync::Mutex;

pub(super) const CLIENT_ID: &str = "ono9krqynydwx5";
pub(super) const EXCHANGE_PATH: &str = "/trae/api/v3/oauth/ExchangeToken";

#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Credential {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: Option<i64>,
    pub refresh_expires_at: Option<i64>,
    pub account: Option<String>,
    pub user_id: Option<String>,
    pub device_id: String,
    pub machine_id: String,
    pub device_private_key: Option<String>,
    pub device_public_key: Option<String>,
    pub client_version: String,
    pub region: String,
}

/// Device proof is separate from access grants and never carries external tokens.
#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocalDeviceProof {
    pub device_id: String,
    pub machine_id: String,
    pub device_private_key: String,
    pub device_public_key: String,
}

pub(super) fn validate_device_proof(proof: &LocalDeviceProof) -> Result<(), String> {
    if proof.device_id.is_empty()
        || proof.device_id.len() > 32
        || !proof.device_id.bytes().all(|byte| byte.is_ascii_digit())
        || proof.device_id.bytes().all(|byte| byte == b'0')
        || !clean(&proof.machine_id, 256)
        || proof.device_private_key.len() > 4096
        || proof.device_public_key.len() > 4096
    {
        return Err("trae_device_key_invalid".into());
    }
    let key = signing_key(&proof.device_private_key)?;
    let public = STANDARD
        .decode(
            proof
                .device_public_key
                .lines()
                .filter(|line| !line.starts_with("-----"))
                .collect::<String>(),
        )
        .map_err(|_| "trae_device_key_invalid")?;
    const SPKI_PREFIX: &[u8] = &[
        0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08,
        0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
    ];
    if public.len() != SPKI_PREFIX.len() + key.public_key().as_ref().len()
        || !public.starts_with(SPKI_PREFIX)
        || public[SPKI_PREFIX.len()..] != *key.public_key().as_ref()
    {
        return Err("trae_device_key_mismatch".into());
    }
    Ok(())
}

#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct Stored {
    generation: String,
    credential: Credential,
    // An imported refresh token belongs to the external login. Renew only a
    // connection established independently by this application.
    independent_login: bool,
}

pub(super) fn lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn refresh_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn read() -> Option<Stored> {
    let mut bytes = Vec::new();
    std::fs::File::open(config_root().join("trae-auth.json"))
        .ok()?
        .take(128 * 1024 + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() > 128 * 1024 {
        return None;
    }
    serde_json::from_slice(&bytes).ok()
}

fn write(stored: &Stored) -> Result<(), String> {
    let root = config_root();
    std::fs::create_dir_all(&root).map_err(|_| "trae_store_unavailable")?;
    let path = root.join(format!(".trae-auth-{}.tmp", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| {
        let bytes = serde_json::to_vec(stored).map_err(|_| "trae_credential_invalid")?;
        if bytes.len() > 128 * 1024 {
            return Err("trae_credential_invalid".to_string());
        }
        let mut file = options.open(&path).map_err(|_| "trae_store_unavailable")?;
        file.write_all(&bytes)
            .map_err(|_| "trae_store_unavailable")?;
        file.sync_all().map_err(|_| "trae_store_unavailable")?;
        std::fs::rename(&path, root.join("trae-auth.json"))
            .map_err(|_| "trae_store_unavailable")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(path);
    }
    result
}

fn clean(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn validate(c: &Credential) -> Result<(), String> {
    if c.region != "cn" {
        return Err("trae_region_unsupported".into());
    }
    if !clean(&c.access_token, 32768)
        || !clean(&c.device_id, 256)
        || !clean(&c.machine_id, 256)
        || !clean(&c.client_version, 64)
        || c.account.as_deref().is_some_and(|value| !clean(value, 320))
        || c.user_id.as_deref().is_some_and(|value| !clean(value, 256))
        || c.refresh_token
            .as_deref()
            .is_some_and(|value| !clean(value, 32768))
        || c.expires_at.is_some_and(|value| value <= 0)
        || c.refresh_expires_at.is_some_and(|value| value <= 0)
    {
        return Err("trae_credential_invalid".into());
    }
    if let Some(key) = &c.device_private_key {
        if key.len() > 4096 {
            return Err("trae_device_key_invalid".into());
        }
        signing_key(key)?;
    }
    Ok(())
}

pub fn status() -> Value {
    let Some(stored) = read() else {
        return json!({"ok":true,"signedIn":false,"loggedIn":false});
    };
    let needs_login = stored
        .credential
        .expires_at
        .is_some_and(|expiry| expiry <= now())
        && (!stored.independent_login
            || stored.credential.refresh_token.is_none()
            || stored
                .credential
                .refresh_expires_at
                .is_some_and(|expiry| expiry <= now()));
    json!({"ok":true,"signedIn":true,"loggedIn":true,"account":stored.credential.account,"userId":stored.credential.user_id,
        "expiresAt":stored.credential.expires_at,"needsLogin":needs_login,
        "canRefresh":stored.independent_login && stored.credential.refresh_token.is_some(),
        "imported":!stored.independent_login,
        "authMethod":if stored.independent_login {"browser"} else {"local"}})
}

pub async fn adopt(credential: Credential) -> Result<Value, String> {
    validate(&credential)?;
    let _lock = lock().lock().await;
    super::login::invalidate_pending();
    write(&Stored {
        generation: uuid::Uuid::new_v4().to_string(),
        credential,
        independent_login: false,
    })?;
    super::forget_catalog();
    Ok(status())
}

pub(super) fn store_login(credential: Credential) -> Result<String, String> {
    validate(&credential)?;
    let generation = uuid::Uuid::new_v4().to_string();
    write(&Stored {
        generation: generation.clone(),
        credential,
        independent_login: true,
    })?;
    super::forget_catalog();
    Ok(generation)
}

pub async fn logout() -> Result<Value, String> {
    let _lock = lock().lock().await;
    super::login::invalidate_pending();
    match std::fs::remove_file(config_root().join("trae-auth.json")) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("trae_store_unavailable".into()),
    }
    super::forget_catalog();
    Ok(status())
}

pub fn generation() -> Option<String> {
    Some(read()?.generation)
}
pub(super) fn current_device_id() -> Option<String> {
    Some(read()?.credential.device_id)
}
pub fn is_current(generation: &str) -> bool {
    read().is_some_and(|stored| stored.generation == generation)
}
pub fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
pub(super) fn account_scope() -> Option<String> {
    use sha2::{Digest, Sha256};
    let stored = read()?;
    let identity = stored
        .credential
        .user_id
        .as_deref()
        .unwrap_or(&stored.generation);
    Some(format!(
        "{:x}",
        Sha256::digest(format!("cn:{identity}").as_bytes())
    ))
}
fn changes_sender() -> &'static tokio::sync::watch::Sender<Option<String>> {
    static CHANGES: OnceLock<tokio::sync::watch::Sender<Option<String>>> = OnceLock::new();
    CHANGES.get_or_init(|| tokio::sync::watch::channel(generation()).0)
}
pub(super) fn changes() -> tokio::sync::watch::Receiver<Option<String>> {
    changes_sender().subscribe()
}
pub(super) fn notify_changed() {
    REVISION.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    changes_sender().send_replace(generation());
}
static REVISION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub(super) fn revision() -> u64 {
    REVISION.load(std::sync::atomic::Ordering::Acquire)
}

pub async fn credential(expected: Option<&str>) -> Result<Credential, String> {
    let saved = read().ok_or("trae_not_signed_in")?;
    if expected.is_some_and(|value| value != saved.generation) {
        return Err("trae_credential_changed".into());
    }
    if !saved.independent_login
        && saved
            .credential
            .expires_at
            .is_none_or(|expiry| expiry > now())
    {
        return Ok(saved.credential);
    }
    if saved
        .credential
        .expires_at
        .is_none_or(|expiry| expiry > now() + 120_000)
    {
        return Ok(saved.credential);
    }
    if !saved.independent_login {
        return Err("trae_login_expired".into());
    }
    let _refresh = refresh_lock().lock().await;
    let current = read().ok_or("trae_not_signed_in")?;
    if current.generation != saved.generation {
        return Err("trae_credential_changed".into());
    }
    if current
        .credential
        .expires_at
        .is_none_or(|expiry| expiry > now() + 120_000)
    {
        return Ok(current.credential);
    }
    let refresh_token = current
        .credential
        .refresh_token
        .as_deref()
        .ok_or("trae_login_expired")?;
    if current
        .credential
        .refresh_expires_at
        .is_some_and(|expiry| expiry <= now())
    {
        return Err("trae_login_expired".into());
    }
    let timestamp = chrono::Utc::now().timestamp();
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let input =
        format!("POST\n{EXCHANGE_PATH}\n{CLIENT_ID}\n{refresh_token}\n{timestamp}\n{nonce}");
    let key = signing_key(
        current
            .credential
            .device_private_key
            .as_deref()
            .ok_or("trae_device_key_unavailable")?,
    )?;
    let signature = key
        .sign(&ring::rand::SystemRandom::new(), input.as_bytes())
        .map_err(|_| "trae_device_proof_failed")?;
    let response = http_json(&super::account_base(), EXCHANGE_PATH, reqwest::Method::POST,
        Some(json!({"ClientID":CLIENT_ID,"ClientSecret":"","RefreshToken":refresh_token,
            "DeviceInfo":device_info(&current.credential),"DeviceProof":{"Signature":STANDARD.encode(signature.as_ref()),"Timestamp":timestamp,"Nonce":nonce},
            "IDEVersion":current.credential.client_version})), Some((&current.credential.access_token, true))).await?;
    let refreshed = exchanged(&response, current.credential)?;
    let _lock = lock().lock().await;
    if !is_current(&current.generation) {
        return Err("trae_credential_changed".into());
    }
    write(&Stored {
        generation: current.generation,
        credential: refreshed.clone(),
        independent_login: true,
    })?;
    Ok(refreshed)
}

pub(super) fn signing_key(pem: &str) -> Result<EcdsaKeyPair, String> {
    let bytes = STANDARD
        .decode(
            pem.lines()
                .filter(|line| !line.starts_with("-----"))
                .collect::<String>(),
        )
        .map_err(|_| "trae_device_key_invalid")?;
    EcdsaKeyPair::from_pkcs8(
        &ECDSA_P256_SHA256_ASN1_SIGNING,
        &bytes,
        &ring::rand::SystemRandom::new(),
    )
    .map_err(|_| "trae_device_key_invalid".into())
}

pub(super) fn generated_keys() -> Result<(String, String), String> {
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng)
        .map_err(|_| "trae_device_key_unavailable")?;
    let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8.as_ref(), &rng)
        .map_err(|_| "trae_device_key_unavailable")?;
    let mut spki = vec![
        0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08,
        0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
    ];
    spki.extend_from_slice(pair.public_key().as_ref());
    fn pem(label: &str, bytes: &[u8]) -> String {
        let encoded = STANDARD.encode(bytes);
        let lines = encoded
            .as_bytes()
            .chunks(64)
            .map(|line| std::str::from_utf8(line).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");
        format!("-----BEGIN {label}-----\n{lines}\n-----END {label}-----\n")
    }
    Ok((pem("PRIVATE KEY", pkcs8.as_ref()), pem("PUBLIC KEY", &spki)))
}

pub(super) fn device_info(c: &Credential) -> Value {
    json!({"DeviceID":c.device_id,"MachineID":c.machine_id,"PlatformCode":"IDE_PC","DeviceType":"PC",
        "DeviceName":"Klaus","DeviceModel":"","ClientVersion":c.client_version,
        "DevicePublicKey":c.device_public_key,"DeviceBrand":"","DeviceCPU":std::env::consts::ARCH,
        "OSInfo":if cfg!(target_os="macos") {"MacOS"} else {std::env::consts::OS},"OSVersion":super::device::os_version()})
}

fn parsed_time(value: &Value) -> Option<i64> {
    value
        .as_str()
        .and_then(|time| chrono::DateTime::parse_from_rfc3339(time).ok())
        .map(|time| time.timestamp_millis())
        .or_else(|| {
            value.as_i64().filter(|time| *time > 0).map(|time| {
                if time < 100_000_000_000 {
                    time * 1000
                } else {
                    time
                }
            })
        })
}

pub(super) fn exchanged(
    response: &Value,
    mut credential: Credential,
) -> Result<Credential, String> {
    let result = response
        .get("Result")
        .ok_or("trae_token_exchange_invalid")?;
    credential.access_token = result["Token"]
        .as_str()
        .ok_or("trae_token_exchange_invalid")?
        .into();
    credential.refresh_token = result["RefreshToken"]
        .as_str()
        .map(str::to_owned)
        .or(credential.refresh_token);
    credential.expires_at = parsed_time(&result["TokenExpireAt"]);
    credential.refresh_expires_at =
        parsed_time(&result["RefreshExpireAt"]).or(credential.refresh_expires_at);
    if credential.expires_at.is_none() {
        return Err("trae_token_exchange_invalid".into());
    }
    validate(&credential)?;
    Ok(credential)
}
