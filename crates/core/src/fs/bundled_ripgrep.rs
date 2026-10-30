//! Verified materialization of the target-specific `@vscode/ripgrep` asset.

use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use sha2::{Digest, Sha256};
use xz2::read::XzDecoder;

struct EmbeddedAsset {
    target_id: &'static str,
    file_name: &'static str,
    compressed: &'static [u8],
    compressed_length: usize,
    compressed_sha256: &'static str,
    raw_length: usize,
    raw_sha256: &'static str,
}

macro_rules! asset_from_crate {
    ($crate_name:ident) => {
        EmbeddedAsset {
            target_id: $crate_name::TARGET_ID,
            file_name: $crate_name::FILE_NAME,
            compressed: $crate_name::RG_XZ,
            compressed_length: $crate_name::RG_XZ_LEN,
            compressed_sha256: $crate_name::RG_XZ_SHA256,
            raw_length: $crate_name::RG_RAW_LEN,
            raw_sha256: $crate_name::RG_RAW_SHA256,
        }
    };
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_darwin_arm64))
}

#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_darwin_x64))
}

#[cfg(all(target_os = "linux", target_arch = "arm"))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_linux_arm))
}

#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_linux_arm64))
}

#[cfg(all(target_os = "linux", target_arch = "x86"))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_linux_ia32))
}

#[cfg(all(
    target_os = "linux",
    target_arch = "powerpc64",
    target_endian = "little"
))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_linux_ppc64))
}

#[cfg(all(target_os = "linux", target_arch = "riscv64"))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_linux_riscv64))
}

#[cfg(all(target_os = "linux", target_arch = "s390x"))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_linux_s390x))
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_linux_x64))
}

#[cfg(all(target_os = "windows", target_arch = "aarch64"))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_win32_arm64))
}

#[cfg(all(target_os = "windows", target_arch = "x86"))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_win32_ia32))
}

#[cfg(all(target_os = "windows", target_arch = "x86_64"))]
fn selected_asset() -> Option<EmbeddedAsset> {
    Some(asset_from_crate!(klaus_agent_ripgrep_win32_x64))
}

#[cfg(not(any(
    all(target_os = "macos", target_arch = "aarch64"),
    all(target_os = "macos", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "arm"),
    all(target_os = "linux", target_arch = "aarch64"),
    all(target_os = "linux", target_arch = "x86"),
    all(
        target_os = "linux",
        target_arch = "powerpc64",
        target_endian = "little"
    ),
    all(target_os = "linux", target_arch = "riscv64"),
    all(target_os = "linux", target_arch = "s390x"),
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "windows", target_arch = "aarch64"),
    all(target_os = "windows", target_arch = "x86"),
    all(target_os = "windows", target_arch = "x86_64"),
)))]
fn selected_asset() -> Option<EmbeddedAsset> {
    None
}

fn decompress_xz_bounded(compressed: &[u8], max_length: usize) -> Result<Vec<u8>, String> {
    let decoder = XzDecoder::new(compressed);
    let mut decoder = decoder.take((max_length as u64).saturating_add(1));
    let mut data = Vec::with_capacity(max_length);
    decoder
        .read_to_end(&mut data)
        .map_err(|error| format!("failed to decompress bundled ripgrep: {error}"))?;
    if data.len() > max_length {
        return Err(format!(
            "bundled ripgrep exceeds the {max_length}-byte decompression limit"
        ));
    }
    Ok(data)
}

fn verified_bytes(asset: &EmbeddedAsset) -> Result<Vec<u8>, String> {
    if asset.compressed.len() != asset.compressed_length {
        return Err(format!(
            "bundled ripgrep XZ for {} has length {}, expected {}",
            asset.target_id,
            asset.compressed.len(),
            asset.compressed_length
        ));
    }
    let compressed_digest = format!("{:x}", Sha256::digest(asset.compressed));
    if compressed_digest != asset.compressed_sha256 {
        return Err(format!(
            "bundled ripgrep XZ for {} has SHA-256 {compressed_digest}, expected {}",
            asset.target_id, asset.compressed_sha256
        ));
    }
    let data = decompress_xz_bounded(asset.compressed, asset.raw_length)?;
    if data.len() != asset.raw_length {
        return Err(format!(
            "bundled ripgrep for {} has length {}, expected {}",
            asset.target_id,
            data.len(),
            asset.raw_length
        ));
    }
    let raw_digest = format!("{:x}", Sha256::digest(&data));
    if raw_digest != asset.raw_sha256 {
        return Err(format!(
            "bundled ripgrep for {} has SHA-256 {raw_digest}, expected {}",
            asset.target_id, asset.raw_sha256
        ));
    }
    Ok(data)
}

fn absolute_environment_path(
    variable: &str,
    value: Option<OsString>,
) -> Result<Option<PathBuf>, String> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(format!(
            "{variable} must be an absolute path for the bundled-ripgrep executable cache"
        ));
    }
    Ok(Some(path))
}

#[cfg(not(windows))]
fn absolute_home_path(home: Option<PathBuf>) -> Result<PathBuf, String> {
    let Some(home) = home.filter(|path| !path.as_os_str().is_empty() && path.is_absolute()) else {
        return Err(
            "HOME must be a non-empty absolute path for the bundled-ripgrep executable cache; set KLAUS_RIPGREP_CACHE_DIR"
                .to_string(),
        );
    };
    Ok(home)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn unix_cache_root<F>(xdg: Option<OsString>, home: F) -> Result<PathBuf, String>
where
    F: FnOnce() -> Option<PathBuf>,
{
    // XDG_CACHE_HOME is authoritative and does not depend on HOME/passwd being
    // available. The XDG specification requires it to be absolute.
    if let Some(xdg) = absolute_environment_path("XDG_CACHE_HOME", xdg)? {
        return Ok(xdg.join("klaus-agent").join("ripgrep"));
    }
    Ok(absolute_home_path(home())?
        .join(".cache")
        .join("klaus-agent")
        .join("ripgrep"))
}

#[cfg(windows)]
fn windows_cache_root(local_app_data: Option<OsString>) -> Result<PathBuf, String> {
    let local = absolute_environment_path("LOCALAPPDATA", local_app_data)?.ok_or_else(|| {
        "LOCALAPPDATA must be a non-empty absolute path for the bundled-ripgrep executable cache; set KLAUS_RIPGREP_CACHE_DIR to a trusted absolute directory"
            .to_string()
    })?;
    Ok(local.join("klaus-agent").join("ripgrep"))
}

fn executable_cache_root() -> Result<PathBuf, String> {
    if let Some(configured) = absolute_environment_path(
        "KLAUS_RIPGREP_CACHE_DIR",
        std::env::var_os("KLAUS_RIPGREP_CACHE_DIR"),
    )? {
        return Ok(configured);
    }

    #[cfg(windows)]
    {
        // Rust's standard filesystem API cannot prove the owner SID, DACL, or
        // absence of reparse points in every ancestor. Keep the implicit
        // Windows cache inside the user's absolute LOCALAPPDATA directory;
        // KLAUS_RIPGREP_CACHE_DIR is the explicit trusted-directory escape
        // hatch handled above.
        windows_cache_root(std::env::var_os("LOCALAPPDATA"))
    }

    #[cfg(target_os = "macos")]
    {
        let home = absolute_home_path(super::ripgrep::platform_home_dir())?;
        Ok(home
            .join("Library")
            .join("Caches")
            .join("klaus-agent")
            .join("ripgrep"))
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        return unix_cache_root(std::env::var_os("XDG_CACHE_HOME"), || {
            super::ripgrep::platform_home_dir()
        });
    }

    #[cfg(all(not(unix), not(windows)))]
    {
        let home = absolute_home_path(super::ripgrep::platform_home_dir())?;
        Ok(home.join(".klaus-agent").join("cache").join("ripgrep"))
    }
}

fn ensure_private_directory(directory: &Path) -> Result<(), String> {
    if let Ok(metadata) = fs::symlink_metadata(directory) {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(format!(
                "bundled-ripgrep cache path is not a private directory: {}",
                directory.display()
            ));
        }
    } else {
        fs::create_dir_all(directory).map_err(|error| {
            format!(
                "failed to create bundled-ripgrep cache directory {}: {error}",
                directory.display()
            )
        })?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = fs::symlink_metadata(directory).map_err(|error| {
            format!("failed to inspect bundled-ripgrep cache directory: {error}")
        })?;
        if metadata.file_type().is_symlink() || metadata.uid() != unsafe { libc::geteuid() } {
            return Err(format!(
                "bundled-ripgrep cache directory is not owned by the current user: {}",
                directory.display()
            ));
        }
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).map_err(|error| {
            format!("failed to secure bundled-ripgrep cache directory: {error}")
        })?;
    }
    Ok(())
}

fn existing_executable_is_valid(path: &Path, asset: &EmbeddedAsset) -> Result<bool, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(format!(
                "failed to inspect cached bundled ripgrep {}: {error}",
                path.display()
            ))
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "cached bundled ripgrep must not be a symlink: {}",
            path.display()
        ));
    }
    if !metadata.is_file() || metadata.len() != asset.raw_length as u64 {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(format!(
                "cached bundled ripgrep is not owned by the current user: {}",
                path.display()
            ));
        }
        if metadata.permissions().mode() & 0o777 != 0o700 {
            return Ok(false);
        }
    }
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("failed to read cached bundled ripgrep: {error}")),
    };
    Ok(format!("{:x}", Sha256::digest(data)) == asset.raw_sha256)
}

fn remove_stale_cached_file(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        // Another materializer may have removed the corrupt final after our
        // metadata check. That is a successful transition to the installable
        // state, not a process-lifetime failure.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "failed to replace cached bundled ripgrep {}: {error}",
            path.display()
        )),
    }
}

const MAX_MATERIALIZATION_ATTEMPTS: usize = 3;

fn materialize_in(asset: &EmbeddedAsset, root: &Path) -> Result<PathBuf, String> {
    ensure_private_directory(root)?;
    let directory = root.join(format!("{}-{}", asset.target_id, asset.raw_sha256));
    ensure_private_directory(&directory)?;
    let path = directory.join(asset.file_name);
    let mut verified_data = None;

    for attempt in 0..MAX_MATERIALIZATION_ATTEMPTS {
        if existing_executable_is_valid(&path, asset)? {
            return Ok(path);
        }
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(format!(
                        "cached bundled ripgrep must not be a symlink: {}",
                        path.display()
                    ));
                }
                remove_stale_cached_file(&path)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "failed to inspect cached bundled ripgrep {}: {error}",
                    path.display()
                ));
            }
        }

        if verified_data.is_none() {
            verified_data = Some(verified_bytes(asset)?);
        }
        let data = verified_data
            .as_deref()
            .expect("verified data was initialized");
        let temporary = directory.join(format!(
            "{}.tmp-{}-{:032x}",
            asset.file_name,
            std::process::id(),
            rand::random::<u128>()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o700);
        }
        let attempt_result = (|| -> Result<bool, String> {
            let mut file = options
                .open(&temporary)
                .map_err(|error| format!("failed to create bundled ripgrep executable: {error}"))?;
            file.write_all(data)
                .map_err(|error| format!("failed to write bundled ripgrep executable: {error}"))?;
            file.sync_all()
                .map_err(|error| format!("failed to sync bundled ripgrep executable: {error}"))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&temporary, fs::Permissions::from_mode(0o700)).map_err(
                    |error| format!("failed to mark bundled ripgrep executable: {error}"),
                )?;
            }
            match fs::rename(&temporary, &path) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
                    ) =>
                {
                    // Windows cannot replace an existing final with rename.
                    // A peer may have installed it, or may still be deleting a
                    // corrupt predecessor. Verify now and retry only the latter.
                    return existing_executable_is_valid(&path, asset);
                }
                Err(error) => {
                    return Err(format!(
                        "failed to atomically install bundled ripgrep executable: {error}"
                    ));
                }
            }
            existing_executable_is_valid(&path, asset)
        })();
        let _ = fs::remove_file(&temporary);

        match attempt_result {
            Ok(true) => return Ok(path),
            Ok(false) if attempt + 1 < MAX_MATERIALIZATION_ATTEMPTS => continue,
            Ok(false) => {
                return Err(format!(
                    "cached bundled ripgrep failed post-install verification after {MAX_MATERIALIZATION_ATTEMPTS} attempts"
                ));
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("materialization loop returns on its final attempt")
}

fn materialize(asset: &EmbeddedAsset) -> Result<PathBuf, String> {
    materialize_in(asset, &executable_cache_root()?)
}

fn cache_successful_materialization<F>(
    cache: &OnceLock<PathBuf>,
    materialize: F,
) -> Result<&Path, String>
where
    F: FnOnce() -> Result<PathBuf, String>,
{
    if let Some(path) = cache.get() {
        return Ok(path.as_path());
    }
    let path = materialize()?;
    // A concurrent caller may win. Both paths use the same target/hash cache
    // identity, so the cache winner is the canonical return value.
    let _ = cache.set(path);
    Ok(cache
        .get()
        .expect("a successful materialization initialized the cache")
        .as_path())
}

fn verified_cached_materialization<'a, F>(
    cache: &'a OnceLock<PathBuf>,
    asset: &EmbeddedAsset,
    materialize_first: F,
) -> Result<&'a Path, String>
where
    F: FnOnce() -> Result<PathBuf, String>,
{
    if let Some(path) = cache.get() {
        if existing_executable_is_valid(path, asset)? {
            return Ok(path.as_path());
        }
        let root = path
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| "cached bundled ripgrep path has no cache root".to_string())?;
        let repaired = materialize_in(asset, root)?;
        if repaired != *path {
            return Err("repaired bundled ripgrep path changed cached identity".to_string());
        }
        return Ok(path.as_path());
    }
    cache_successful_materialization(cache, materialize_first)
}

/// Return the verified target asset, or `None` when the pinned asset lock did
/// not contain an asset for this Rust target. Corrupt assets fail closed.
pub(super) fn bundled_ripgrep_command() -> Result<Option<&'static Path>, String> {
    let Some(asset) = selected_asset() else {
        return Ok(None);
    };
    static MATERIALIZED: OnceLock<PathBuf> = OnceLock::new();
    verified_cached_materialization(&MATERIALIZED, &asset, || materialize(&asset)).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::io::Write;
    use std::sync::OnceLock;

    #[test]
    fn explicit_cache_override_must_be_absolute() {
        let error = absolute_environment_path(
            "KLAUS_RIPGREP_CACHE_DIR",
            Some(OsString::from("relative-cache")),
        )
        .unwrap_err();
        assert!(error.contains("KLAUS_RIPGREP_CACHE_DIR must be an absolute path"));
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn absolute_xdg_cache_does_not_require_home_or_passwd() {
        assert_eq!(
            unix_cache_root(Some(OsString::from("/var/tmp/xdg")), || {
                panic!("absolute XDG_CACHE_HOME must be selected before HOME/passwd")
            })
            .unwrap(),
            PathBuf::from("/var/tmp/xdg/klaus-agent/ripgrep")
        );
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn relative_xdg_cache_fails_closed_instead_of_using_home() {
        let error = unix_cache_root(Some(OsString::from("relative-xdg")), || {
            Some(PathBuf::from("/home/example"))
        })
        .unwrap_err();
        assert!(error.contains("XDG_CACHE_HOME must be an absolute path"));
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn empty_home_cannot_create_a_relative_dot_cache() {
        let error = unix_cache_root(None, || Some(PathBuf::new())).unwrap_err();
        assert!(error.contains("HOME must be a non-empty absolute path"));
    }

    #[cfg(windows)]
    #[test]
    fn implicit_windows_cache_requires_absolute_local_app_data() {
        assert!(
            windows_cache_root(Some(OsString::from("relative-local-app-data")))
                .unwrap_err()
                .contains("LOCALAPPDATA must be an absolute path")
        );
        assert!(windows_cache_root(Some(OsString::new()))
            .unwrap_err()
            .contains("LOCALAPPDATA must be a non-empty absolute path"));
        assert_eq!(
            windows_cache_root(Some(OsString::from(r"C:\Users\example\AppData\Local"))).unwrap(),
            PathBuf::from(r"C:\Users\example\AppData\Local\klaus-agent\ripgrep")
        );
    }

    #[test]
    fn removing_a_stale_file_accepts_a_competing_not_found() {
        let root = std::env::temp_dir().join(format!(
            "klaus-bundled-ripgrep-remove-race-{}-{:032x}",
            std::process::id(),
            rand::random::<u128>()
        ));
        fs::create_dir_all(&root).unwrap();
        let missing = root.join("already-removed-rg");
        remove_stale_cached_file(&missing).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_failed_materialization_is_not_cached_permanently() {
        let cache = OnceLock::new();
        let first =
            cache_successful_materialization(
                &cache,
                || Err("simulated transient race".to_string()),
            );
        assert_eq!(first.unwrap_err(), "simulated transient race");

        let expected = PathBuf::from("/verified/rg");
        let second = cache_successful_materialization(&cache, || Ok(expected.clone())).unwrap();
        assert_eq!(second, expected.as_path());
    }

    #[test]
    fn a_cached_executable_is_rehashed_and_repaired_before_reuse() {
        let root = std::env::temp_dir().join(format!(
            "klaus-bundled-ripgrep-revalidate-{}-{:032x}",
            std::process::id(),
            rand::random::<u128>()
        ));
        fs::create_dir_all(&root).unwrap();
        let asset = selected_asset().expect("the test host must have a fixed-lock asset");
        let cache = OnceLock::new();
        let first =
            verified_cached_materialization(&cache, &asset, || materialize_in(&asset, &root))
                .unwrap()
                .to_path_buf();

        fs::write(&first, b"corrupt").unwrap();
        let second = verified_cached_materialization(&cache, &asset, || {
            panic!("an initialized path must use the repair path")
        })
        .unwrap();

        assert_eq!(second, first);
        assert!(existing_executable_is_valid(second, &asset).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_cached_symlink_fails_closed_before_reuse() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "klaus-bundled-ripgrep-symlink-{}-{:032x}",
            std::process::id(),
            rand::random::<u128>()
        ));
        fs::create_dir_all(&root).unwrap();
        let asset = selected_asset().expect("the test host must have a fixed-lock asset");
        let cache = OnceLock::new();
        let path =
            verified_cached_materialization(&cache, &asset, || materialize_in(&asset, &root))
                .unwrap()
                .to_path_buf();
        fs::remove_file(&path).unwrap();
        symlink("/dev/null", &path).unwrap();

        let error = verified_cached_materialization(&cache, &asset, || {
            panic!("an initialized path must not rematerialize through the first-call closure")
        })
        .unwrap_err();

        assert!(error.contains("must not be a symlink"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn selected_asset_passes_bounded_length_and_hash_validation() {
        let asset = selected_asset().expect("the test host must have a fixed-lock asset");
        let data = verified_bytes(&asset).unwrap();
        assert_eq!(data.len(), asset.raw_length);
        if cfg!(target_os = "macos") {
            assert!(matches!(&data[..4], [0xcf, 0xfa, 0xed, 0xfe]));
        } else if cfg!(target_os = "windows") {
            assert_eq!(&data[..2], b"MZ");
        } else {
            assert_eq!(&data[..4], b"\x7fELF");
        }
    }

    #[test]
    fn bounded_decompression_rejects_expansion_past_the_raw_identity() {
        let mut encoder = xz2::write::XzEncoder::new(Vec::new(), 9);
        encoder.write_all(&[0_u8; 17]).unwrap();
        let compressed = encoder.finish().unwrap();
        assert_eq!(
            decompress_xz_bounded(&compressed, 16).unwrap_err(),
            "bundled ripgrep exceeds the 16-byte decompression limit"
        );
    }

    #[test]
    fn materialized_asset_is_private_and_executable() {
        let path = bundled_ripgrep_command().unwrap().unwrap();
        assert!(path.is_file());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(path.parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
        let output = std::process::Command::new(path)
            .arg("--version")
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).starts_with("ripgrep 15.0.0"));
    }

    #[test]
    fn deterministic_cache_reuses_one_verified_file_across_materializations() {
        let root = std::env::temp_dir().join(format!(
            "klaus-bundled-ripgrep-test-{}-{:032x}",
            std::process::id(),
            rand::random::<u128>()
        ));
        let asset = selected_asset().unwrap();
        let first = materialize_in(&asset, &root).unwrap();
        let second = materialize_in(&asset, &root).unwrap();
        assert_eq!(first, second);
        let entries = fs::read_dir(first.parent().unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 1);
        fs::remove_dir_all(root).unwrap();
    }
}
