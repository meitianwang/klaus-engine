//! Install a plugin from a remote source.
//!
//! A spec string is classified into a git clone / npm pack / local tarball
//! source, fetched into a private staging directory, validated as a plugin, and
//! (when valid) installed into the versioned plugin store under
//! `~/.klaus/plugins/cache/{marketplace}/{plugin}/{version}` before being
//! loaded. Every external command runs directly (no shell), and the staging
//! directory is always cleaned up.

use super::identity::PluginId;
use super::installed::record_installed_plugin_package;
use super::loader::{load_one_plugin, path_join, LoadedPlugin};
use super::store::{
    plugin_store_host_utf8_boundary, PluginInstallResult, PluginStore, DEFAULT_PLUGIN_VERSION,
};
use crate::extensions::command_loader::CommandLoadOptions;
use crate::extensions::plugins::archive::{unpack_tar_gz_file, MAX_PLUGIN_BUNDLE_EXTRACT_BYTES};
use crate::runtime_process::{
    RuntimeCompletedProcessRequest, RuntimeCompletedProcessSurface, RuntimeProcessContext,
};
use crate::runtime_values::{RuntimePath, RuntimeString};
use std::io;

/// A concrete plugin source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginSource {
    /// A git repository (optionally at a specific ref/branch).
    Git { url: String, ref_: Option<String> },
    /// An npm package spec (`name`, `name@version`, …).
    Npm { pkg: String },
    /// A local `.tgz` / `.tar.gz` tarball path.
    Tarball { path: String },
}

/// A plugin to install: either a spec string to classify, or an explicit source.
pub enum PluginSpec {
    Str(String),
    Source(PluginSource),
}

/// Classify a plugin spec string into a concrete source.
pub fn parse_plugin_source(spec: &str) -> PluginSource {
    if let Some(rest) = spec.strip_prefix("git+") {
        return PluginSource::Git {
            url: rest.to_string(),
            ref_: None,
        };
    }
    if let Some(rest) = spec.strip_prefix("npm:") {
        return PluginSource::Npm {
            pkg: rest.to_string(),
        };
    }
    if spec.ends_with(".tgz") || spec.ends_with(".tar.gz") {
        return PluginSource::Tarball {
            path: spec.to_string(),
        };
    }
    if spec.ends_with(".git") {
        return PluginSource::Git {
            url: spec.to_string(),
            ref_: None,
        };
    }
    if spec.starts_with("http://") || spec.starts_with("https://") {
        return PluginSource::Git {
            url: spec.to_string(),
            ref_: None,
        };
    }
    PluginSource::Npm {
        pkg: spec.to_string(),
    }
}

fn source_record(source: &PluginSource) -> String {
    match source {
        PluginSource::Git { url, ref_ } => match ref_ {
            Some(ref_) => format!("git+{url}#{ref_}"),
            None => format!("git+{url}"),
        },
        PluginSource::Npm { pkg } => format!("npm:{pkg}"),
        PluginSource::Tarball { path } => path.clone(),
    }
}

fn source_marketplace(source: &PluginSource) -> &'static str {
    match source {
        PluginSource::Git { .. } => "git",
        PluginSource::Npm { .. } => "npm",
        PluginSource::Tarball { .. } => "local",
    }
}

/// The current user's home directory ("" when it cannot be determined). Note
/// that the plugin home follows the OS home (not any config-dir override).
fn home_dir() -> String {
    #[allow(deprecated)] // fine on modern toolchains; `home_dir` is un-deprecated
    std::env::home_dir()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn plugin_home() -> String {
    path_join(&[&home_dir(), ".klaus", "plugins"])
}

/// Run an external command directly (no shell). Returns captured stdout on
/// success; a non-zero exit is an error (carrying stderr for diagnostics).
async fn exec(
    process: &RuntimeProcessContext,
    program: &str,
    args: &[&str],
    cwd: Option<&str>,
) -> io::Result<String> {
    let mut request = RuntimeCompletedProcessRequest::new(
        RuntimeCompletedProcessSurface::ExtensionExecFile,
        RuntimeString::from(program),
    )
    .with_args(args.iter().copied().map(RuntimeString::from).collect());
    if let Some(dir) = cwd {
        request = request.with_cwd(RuntimePath::from(dir));
    }
    let output = process
        .exec_completed(request)
        .await
        .map_err(io::Error::other)?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "{} exited with code {:?}, signal {:?}: {}",
            program,
            output.status.code(),
            output.status.signal(),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Create a fresh, unique staging directory under the OS temp dir.
async fn make_staging_dir() -> io::Result<String> {
    let base = std::env::temp_dir();
    loop {
        let suffix: u64 = rand::random();
        let path = base.join(format!("ae-plugin-{suffix:016x}"));
        match tokio::fs::create_dir(&path).await {
            Ok(()) => return Ok(path.to_string_lossy().into_owned()),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
}

/// If a dir contains exactly one subdirectory (and no root manifest), descend
/// into it.
async fn unwrap_single_dir(dir: &str) -> io::Result<String> {
    if load_one_plugin(dir, CommandLoadOptions::default())
        .await
        .is_some()
    {
        return Ok(dir.to_string());
    }
    let mut rd = tokio::fs::read_dir(dir).await?;
    let mut dir_names: Vec<String> = Vec::new();
    let mut has_manifest = false;
    while let Some(entry) = rd.next_entry().await? {
        let ft = entry.file_type().await?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if ft.is_dir() {
            dir_names.push(name);
        } else if ft.is_file() && name == "plugin.json" {
            has_manifest = true;
        }
    }
    if !has_manifest && dir_names.len() == 1 {
        return Ok(path_join(&[dir, &dir_names[0]]));
    }
    Ok(dir.to_string())
}

/// The staged-extract → validate → install body, kept separate so the staging
/// dir can always be cleaned up afterward (regardless of success or error).
async fn install_inner(
    process: &RuntimeProcessContext,
    source: &PluginSource,
    home: &str,
    staging: &str,
) -> io::Result<Option<(LoadedPlugin, PluginId, PluginInstallResult)>> {
    let extracted: String = match source {
        PluginSource::Git { url, ref_ } => {
            let mut args: Vec<String> = vec!["clone".into(), "--depth".into(), "1".into()];
            if let Some(r) = ref_ {
                args.push("--branch".into());
                args.push(r.clone());
            }
            args.push(url.clone());
            args.push(staging.to_string());
            let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
            exec(process, "git", &arg_refs, None).await?;
            staging.to_string()
        }
        PluginSource::Tarball { path } => {
            unpack_tar_gz_file(path, staging, MAX_PLUGIN_BUNDLE_EXTRACT_BYTES).await?;
            unwrap_single_dir(staging).await?
        }
        PluginSource::Npm { pkg } => {
            let stdout = exec(process, "npm", &["pack", pkg, "--silent"], Some(staging)).await?;
            let tgz = stdout.trim().split('\n').next_back().unwrap_or("");
            if tgz.is_empty() {
                return Ok(None);
            }
            let tgz_path = path_join(&[staging, tgz]);
            unpack_tar_gz_file(&tgz_path, staging, MAX_PLUGIN_BUNDLE_EXTRACT_BYTES).await?;
            // npm tarballs root their contents at `package/`.
            path_join(&[staging, "package"])
        }
    };

    let probe = match load_one_plugin(&extracted, CommandLoadOptions::default()).await {
        Some(p) => p,
        None => return Ok(None),
    };
    let marketplace = source_marketplace(source);
    let plugin_id = match PluginId::new(probe.name.clone(), marketplace) {
        Ok(id) => id,
        Err(err) => {
            tracing::warn!(
                "[extensions] refusing to install plugin with unsafe identity {}@{}: {}",
                probe.name,
                marketplace,
                err
            );
            return Ok(None);
        }
    };
    let version = probe
        .version
        .as_deref()
        .unwrap_or(DEFAULT_PLUGIN_VERSION)
        .to_string();
    let store = PluginStore::new(home);
    let installed = store
        .install_dir(&plugin_id, Some(&version), &extracted)
        .await?;
    let installed_path = plugin_store_host_utf8_boundary(&installed.installed_path);
    let plugin = match load_one_plugin(&installed_path, CommandLoadOptions::default()).await {
        Some(plugin) => plugin,
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "installed plugin package no longer has a loadable manifest",
            ))
        }
    };
    if plugin.name != probe.name {
        tracing::warn!(
            "[extensions] installed plugin name changed from {} to {}",
            probe.name,
            plugin.name
        );
        return Ok(None);
    }
    Ok(Some((plugin, plugin_id, installed)))
}

/// Install a plugin from a remote source (git clone / npm pack / tarball),
/// extract it, validate its manifest, install it into the versioned plugin store
/// rooted at `~/.klaus/plugins` (or `dir` when supplied), and return the loaded
/// plugin. Returns `Ok(None)` when
/// the fetched content has no valid plugin manifest.
pub async fn install_plugin(
    process: &RuntimeProcessContext,
    spec: PluginSpec,
    dir: Option<String>,
) -> io::Result<Option<LoadedPlugin>> {
    let source = match spec {
        PluginSpec::Str(s) => parse_plugin_source(&s),
        PluginSpec::Source(src) => src,
    };
    let home = dir.unwrap_or_else(plugin_home);
    tokio::fs::create_dir_all(&home).await?;
    let staging = make_staging_dir().await?;

    let result = install_inner(process, &source, &home, &staging).await;
    let out = match result {
        Ok(Some((plugin, plugin_id, installed))) => {
            record_installed_plugin_package(
                &home,
                &plugin,
                &plugin_id,
                installed.version,
                Some(installed.data_path),
                source_record(&source),
            )
            .await?;
            Ok(Some(plugin))
        }
        Ok(None) => Ok(None),
        Err(err) => Err(err),
    };
    // Always clean up the staging directory (best-effort, like a force remove).
    let _ = tokio::fs::remove_dir_all(&staging).await;
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimeString;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-remote-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn parse_plugin_source_classifies_every_form() {
        assert_eq!(
            parse_plugin_source("git+https://x/y.git"),
            PluginSource::Git {
                url: "https://x/y.git".to_string(),
                ref_: None
            }
        );
        assert_eq!(
            parse_plugin_source("npm:@scope/pkg"),
            PluginSource::Npm {
                pkg: "@scope/pkg".to_string()
            }
        );
        assert_eq!(
            parse_plugin_source("/tmp/x.tgz"),
            PluginSource::Tarball {
                path: "/tmp/x.tgz".to_string()
            }
        );
        assert_eq!(
            parse_plugin_source("/tmp/x.tar.gz"),
            PluginSource::Tarball {
                path: "/tmp/x.tar.gz".to_string()
            }
        );
        assert_eq!(
            parse_plugin_source("https://x/y.git"),
            PluginSource::Git {
                url: "https://x/y.git".to_string(),
                ref_: None
            }
        );
        assert_eq!(
            parse_plugin_source("https://x/y"),
            PluginSource::Git {
                url: "https://x/y".to_string(),
                ref_: None
            }
        );
        assert_eq!(
            parse_plugin_source("just-a-package"),
            PluginSource::Npm {
                pkg: "just-a-package".to_string()
            }
        );
    }

    #[tokio::test]
    async fn unwrap_single_dir_descends_only_for_a_lone_subdir_without_a_root_manifest() {
        // Exactly one subdir, no root manifest → descend.
        let a = temp_dir();
        std::fs::create_dir_all(a.join("only")).unwrap();
        assert_eq!(
            unwrap_single_dir(a.to_str().unwrap()).await.unwrap(),
            path_join(&[a.to_str().unwrap(), "only"])
        );

        // Root manifest present → stay.
        let b = temp_dir();
        std::fs::create_dir_all(b.join("only")).unwrap();
        std::fs::write(b.join("plugin.json"), "{}").unwrap();
        assert_eq!(
            unwrap_single_dir(b.to_str().unwrap()).await.unwrap(),
            b.to_string_lossy()
        );

        // Two subdirs → stay.
        let c = temp_dir();
        std::fs::create_dir_all(c.join("one")).unwrap();
        std::fs::create_dir_all(c.join("two")).unwrap();
        assert_eq!(
            unwrap_single_dir(c.to_str().unwrap()).await.unwrap(),
            c.to_string_lossy()
        );
    }

    #[tokio::test]
    async fn installs_a_plugin_from_a_local_tarball() {
        let process = crate::runtime_process::test_runtime_process_context();
        let base = temp_dir();
        // Build a source plugin tree: <base>/pkgroot/myplugin/{plugin.json,commands/hi.md}.
        let pkgroot = base.join("pkgroot");
        let src = pkgroot.join("myplugin");
        std::fs::create_dir_all(src.join("commands")).unwrap();
        std::fs::write(src.join("plugin.json"), r#"{"name":"myplugin"}"#).unwrap();
        std::fs::write(
            src.join("commands").join("hi.md"),
            "---\ndescription: hi\n---\nHello",
        )
        .unwrap();

        // Archive the single top-level `myplugin/` dir so unwrap descends into it.
        let tgz = base.join("pkg.tgz");
        let status = std::process::Command::new("tar")
            .arg("-czf")
            .arg(&tgz)
            .arg("-C")
            .arg(&pkgroot)
            .arg("myplugin")
            .status()
            .expect("tar runs");
        assert!(status.success());

        let home = base.join("home");
        let plugin = install_plugin(
            &process,
            PluginSpec::Str(tgz.to_string_lossy().into_owned()),
            Some(home.to_string_lossy().into_owned()),
        )
        .await
        .expect("install ok")
        .expect("plugin present");

        assert_eq!(plugin.name, "myplugin");
        assert_eq!(plugin.commands.len(), 1);
        let expected_dir = PathBuf::from(&plugin.dir);
        assert!(expected_dir.join("plugin.json").exists());
        let installed = crate::extensions::plugins::installed::read_installed_plugins(&home)
            .await
            .unwrap();
        let record = installed
            .plugins
            .get(&RuntimeString::from("myplugin@local"))
            .unwrap();
        assert_eq!(record.name, "myplugin");
        assert_eq!(
            record.plugin_id.as_ref(),
            Some(&RuntimeString::from("myplugin@local"))
        );
        assert_eq!(
            record.marketplace.as_ref(),
            Some(&RuntimeString::from("local"))
        );
        assert_eq!(
            record.version.as_ref(),
            Some(&RuntimeString::from(DEFAULT_PLUGIN_VERSION))
        );
        assert_eq!(
            record.install_path,
            RuntimeString::from(expected_dir.to_string_lossy().as_ref())
        );
        let expected_data = home
            .join("data")
            .join("myplugin-local")
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            record.data_path.as_ref(),
            Some(&RuntimeString::from(expected_data))
        );
        assert_eq!(
            record.source,
            RuntimeString::from(tgz.to_string_lossy().as_ref())
        );
    }
}
