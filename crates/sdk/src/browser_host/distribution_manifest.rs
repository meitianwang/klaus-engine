//! Fail-closed admission of the Rust browser native-host binary.
//!
//! The distribution manifest must name an independently approved producer
//! (see `distribution_source`). Its complete bytes are pinned into the SDK
//! build; fields in the manifest never establish trust by themselves. A source
//! distribution configures no producer, so every manifest is refused.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use klaus_agent_core::runtime_values::{NodeBufferUtf8, RuntimePath, RuntimeString};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::distribution_source::NATIVE_HOST_PRODUCER;
use super::plan::VerifiedNativeHostBinary;
use super::result::BrowserPlatform;

const DISTRIBUTION_MANIFEST_FILE: &str = "klaus-browser-native-host.distribution.json";
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const MISSING_TRUST_ANCHOR: &str =
    "Native host distribution trust anchor is unavailable; refusing installation.";
const INVALID_DISTRIBUTION: &str =
    "Native host distribution manifest verification failed; refusing installation.";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NativeHostDistributionManifest {
    version: u8,
    producer: String,
    target: String,
    absolute_path: String,
    size: u64,
    sha256: String,
    mode: String,
    windows_signer_certificate_sha256: RequiredNullableString,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RequiredNullableString {
    Value(String),
    Null(()),
}

/// Resolve only the fixed sibling manifest and admit its binary only after the
/// approved producer, independently supplied build-time trust anchors, complete
/// manifest hash, absolute path, file hash, and platform mode have all been
/// verified.
pub(super) fn verified_native_host_from_distribution(
    platform: BrowserPlatform,
) -> Result<VerifiedNativeHostBinary, RuntimeString> {
    let expected_manifest_sha256 = option_env!("KLAUS_BROWSER_NATIVE_HOST_MANIFEST_SHA256")
        .ok_or_else(|| RuntimeString::from(MISSING_TRUST_ANCHOR))?;
    let expected_manifest_sha256 = parse_sha256(expected_manifest_sha256)
        .ok_or_else(|| RuntimeString::from(MISSING_TRUST_ANCHOR))?;
    let expected_windows_signer = match platform {
        BrowserPlatform::Windows => Some(
            option_env!("KLAUS_BROWSER_NATIVE_HOST_SIGNER_SHA256")
                .and_then(parse_sha256)
                .ok_or_else(|| RuntimeString::from(MISSING_TRUST_ANCHOR))?,
        ),
        BrowserPlatform::MacOs | BrowserPlatform::Linux => None,
    };
    let executable =
        std::env::current_exe().map_err(|_| RuntimeString::from(INVALID_DISTRIBUTION))?;
    let manifest_path = executable
        .parent()
        .ok_or_else(|| RuntimeString::from(INVALID_DISTRIBUTION))?
        .join(DISTRIBUTION_MANIFEST_FILE);
    let expected_target =
        current_distribution_target().ok_or_else(|| RuntimeString::from(INVALID_DISTRIBUTION))?;
    verify_native_host_distribution(
        platform,
        expected_target,
        NATIVE_HOST_PRODUCER,
        &manifest_path,
        expected_manifest_sha256,
        expected_windows_signer,
    )
    .map_err(|_| RuntimeString::from(INVALID_DISTRIBUTION))
}

fn verify_native_host_distribution(
    platform: BrowserPlatform,
    expected_target: &str,
    expected_producer: Option<&str>,
    manifest_path: &Path,
    expected_manifest_sha256: [u8; 32],
    expected_windows_signer: Option<[u8; 32]>,
) -> Result<VerifiedNativeHostBinary, ()> {
    let expected_producer = expected_producer
        .filter(|value| !value.is_empty())
        .ok_or(())?;
    let manifest_metadata = std::fs::symlink_metadata(manifest_path).map_err(|_| ())?;
    if !manifest_metadata.file_type().is_file()
        || manifest_metadata.file_type().is_symlink()
        || manifest_metadata.len() == 0
        || manifest_metadata.len() > MAX_MANIFEST_BYTES
    {
        return Err(());
    }
    let manifest_bytes = std::fs::read(manifest_path).map_err(|_| ())?;
    if manifest_bytes.len() as u64 != manifest_metadata.len()
        || sha256(&manifest_bytes) != expected_manifest_sha256
    {
        return Err(());
    }
    let manifest: NativeHostDistributionManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|_| ())?;
    if manifest.version != 1
        || manifest.producer != expected_producer
        || manifest.target != expected_target
        || manifest.size == 0
    {
        return Err(());
    }
    let expected_host_sha256 = parse_sha256(&manifest.sha256).ok_or(())?;
    let native_path = PathBuf::from(&manifest.absolute_path);
    let verified = VerifiedNativeHostBinary::from_attested_distribution_path(
        platform,
        runtime_path_from_native(&native_path),
    )
    .map_err(|_| ())?;

    let host_metadata = std::fs::symlink_metadata(&native_path).map_err(|_| ())?;
    if !host_metadata.file_type().is_file()
        || host_metadata.file_type().is_symlink()
        || host_metadata.len() != manifest.size
    {
        return Err(());
    }
    match platform {
        BrowserPlatform::MacOs | BrowserPlatform::Linux => {
            if manifest.mode != "100755"
                || !matches!(
                    manifest.windows_signer_certificate_sha256,
                    RequiredNullableString::Null(())
                )
                || !unix_mode_is_0755(&host_metadata)
            {
                return Err(());
            }
        }
        BrowserPlatform::Windows => {
            let expected_signer = expected_windows_signer.ok_or(())?;
            let RequiredNullableString::Value(observed_signer) =
                manifest.windows_signer_certificate_sha256
            else {
                return Err(());
            };
            let observed_signer = parse_sha256(&observed_signer).ok_or(())?;
            if manifest.mode != "100644" || observed_signer != expected_signer {
                return Err(());
            }
        }
    }
    let mut host = File::open(&native_path).map_err(|_| ())?;
    if sha256_reader(&mut host).map_err(|_| ())? != expected_host_sha256 {
        return Err(());
    }
    Ok(verified)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn sha256_reader(reader: &mut impl Read) -> std::io::Result<[u8; 32]> {
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize().into())
}

fn parse_sha256(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return None;
    }
    let mut output = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(chunk[0])? << 4) | hex_nibble(chunk[1])?;
    }
    Some(output)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(unix)]
fn unix_mode_is_0755(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.mode() & 0o7777 == 0o755
}

#[cfg(not(unix))]
fn unix_mode_is_0755(_metadata: &std::fs::Metadata) -> bool {
    false
}

fn runtime_path_from_native(value: &Path) -> RuntimePath {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        RuntimePath::from_runtime_string(NodeBufferUtf8::decode(value.as_os_str().as_bytes()))
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        RuntimePath::from_runtime_string(RuntimeString::from_utf16(
            value.as_os_str().encode_wide().collect::<Vec<_>>(),
        ))
    }
}

fn current_distribution_target() -> Option<&'static str> {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        Some("darwin-arm64")
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        Some("darwin-x64")
    } else if cfg!(all(target_os = "windows", target_arch = "aarch64")) {
        Some("win32-arm64")
    } else if cfg!(all(target_os = "windows", target_arch = "x86")) {
        Some("win32-ia32")
    } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        Some("win32-x64")
    } else if cfg!(all(target_os = "linux", target_arch = "arm")) {
        Some("linux-arm")
    } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
        Some("linux-arm64")
    } else if cfg!(all(target_os = "linux", target_arch = "x86")) {
        Some("linux-ia32")
    } else if cfg!(all(target_os = "linux", target_arch = "powerpc64")) {
        Some("linux-ppc64")
    } else if cfg!(all(target_os = "linux", target_arch = "riscv64")) {
        Some("linux-riscv64")
    } else if cfg!(all(target_os = "linux", target_arch = "s390x")) {
        Some("linux-s390x")
    } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        Some("linux-x64")
    } else {
        None
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    const FIXTURE_PRODUCER: &str = "synthetic-fixture-producer";

    #[test]
    fn signed_manifest_binds_absolute_path_hash_and_exact_unix_mode() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "klaus-native-host-distribution-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let host_path = root.join("klaus-browser-native-host");
        let host_bytes = b"signed native host";
        std::fs::write(&host_path, host_bytes).unwrap();
        std::fs::set_permissions(&host_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let target = current_distribution_target().unwrap();
        let manifest = serde_json::json!({
            "version": 1,
            "producer": FIXTURE_PRODUCER,
            "target": target,
            "absolutePath": host_path.to_str().unwrap(),
            "size": host_bytes.len(),
            "sha256": format!("{:x}", Sha256::digest(host_bytes)),
            "mode": "100755",
            "windowsSignerCertificateSha256": null
        });
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        let manifest_path = root.join(DISTRIBUTION_MANIFEST_FILE);
        std::fs::write(&manifest_path, &manifest_bytes).unwrap();
        let platform = if cfg!(target_os = "macos") {
            BrowserPlatform::MacOs
        } else {
            BrowserPlatform::Linux
        };

        let verified = verify_native_host_distribution(
            platform,
            target,
            Some(FIXTURE_PRODUCER),
            &manifest_path,
            sha256(&manifest_bytes),
            None,
        )
        .unwrap();
        assert_eq!(
            verified.absolute_path(),
            &runtime_path_from_native(&host_path)
        );

        std::fs::set_permissions(&host_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(verify_native_host_distribution(
            platform,
            target,
            Some(FIXTURE_PRODUCER),
            &manifest_path,
            sha256(&manifest_bytes),
            None,
        )
        .is_err());
        std::fs::set_permissions(&host_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(verify_native_host_distribution(
            platform,
            target,
            Some(FIXTURE_PRODUCER),
            &manifest_path,
            [0_u8; 32],
            None,
        )
        .is_err());

        // An unconfigured, empty or mismatched producer refuses even a manifest
        // whose bytes, path, hash and mode are otherwise valid.
        for producer in [None, Some(""), Some("wrong-producer"), NATIVE_HOST_PRODUCER] {
            assert!(verify_native_host_distribution(
                platform,
                target,
                producer,
                &manifest_path,
                sha256(&manifest_bytes),
                None,
            )
            .is_err());
        }

        std::fs::remove_dir_all(root).unwrap();
    }
}
