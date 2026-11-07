// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for native request paths, helper packaging, and execution lifecycle.

//! Resolve sandbox helpers and materialize legacy executables with inherited sandbox ACLs.

mod copy;
use copy::copy_from_source_if_needed;
use copy::CopyOutcome;

use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use crate::elevated::logging::log_note;
use crate::elevated::sandbox_bin_dir;

const DEV_BUILD_VERSION_SENTINEL: &str = "0.0.0";
const COMMAND_RUNNER_EXE: &str = "klaus-command-runner.exe";
pub(crate) const BIN_DIRNAME: &str = "bin";
pub(crate) const RESOURCES_DIRNAME: &str = "klaus-resources";

pub(crate) fn helper_bin_dir(config_home: &Path) -> PathBuf {
    sandbox_bin_dir(config_home)
}

fn legacy_lookup() -> PathBuf {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(candidate) = bundled_executable_path_for_exe(&exe, COMMAND_RUNNER_EXE) {
            return candidate;
        }
    }
    PathBuf::from(COMMAND_RUNNER_EXE)
}

pub(crate) fn resolve_command_runner(
    config_home: &Path,
    log_dir: Option<&Path>,
) -> Result<PathBuf> {
    Ok(match copy_runner_if_needed(config_home, log_dir) {
        Ok(path) => {
            log_note(
                &format!(
                    "helper launch resolution: using copied command-runner path {}",
                    path.display()
                ),
                log_dir,
            );
            path
        }
        Err(err) => {
            let fallback = legacy_lookup();
            log_note(
                &format!(
                    "helper copy failed for command-runner: {err:#}; falling back to legacy path {}",
                    fallback.display()
                ),
                log_dir,
            );
            fallback
        }
    })
}

fn copy_runner_if_needed(config_home: &Path, log_dir: Option<&Path>) -> Result<PathBuf> {
    let source = sibling_source_path()?;
    let suffix = helper_version_suffix(&source)?;
    let destination = helper_bin_dir(config_home).join(materialized_file_name(&suffix));
    log_note(
        &format!(
            "helper copy: validating command-runner source={} destination={}",
            source.display(),
            destination.display()
        ),
        log_dir,
    );
    let outcome = copy_from_source_if_needed(&source, &destination)?;
    let action = match outcome {
        CopyOutcome::Reused => "reused",
        CopyOutcome::ReCopied => "recopied",
    };
    log_note(
        &format!(
            "helper copy: {} command-runner source={} destination={}",
            action,
            source.display(),
            destination.display()
        ),
        log_dir,
    );
    Ok(destination)
}

fn sibling_source_path() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("resolve current executable for helper lookup")?;
    bundled_executable_path_for_exe(&exe, COMMAND_RUNNER_EXE).ok_or_else(|| {
        anyhow!(
            "helper not found next to current executable or under {RESOURCES_DIRNAME}: {}",
            exe.display()
        )
    })
}

pub(crate) fn bundled_executable_path_for_exe(exe: &Path, file_name: &str) -> Option<PathBuf> {
    let find = |exe: &Path| {
        let dir = exe.parent()?;
        let direct_candidate = dir.join(file_name);
        if direct_candidate.is_file() {
            return Some(direct_candidate);
        }

        if dir.file_name() == Some(OsStr::new(BIN_DIRNAME)) {
            if let Some(package_dir) = dir.parent() {
                let package_resource_candidate =
                    package_dir.join(RESOURCES_DIRNAME).join(file_name);
                if package_resource_candidate.is_file() {
                    return Some(package_resource_candidate);
                }
            }
        }

        let resource_candidate = dir.join(RESOURCES_DIRNAME).join(file_name);
        resource_candidate.is_file().then_some(resource_candidate)
    };

    // Installer bin directories can be junctions, so retry beside the real executable once.
    find(exe).or_else(|| find(&dunce::canonicalize(exe).ok()?))
}

fn materialized_file_name(suffix: &str) -> String {
    format!("klaus-command-runner-{suffix}.exe")
}

fn helper_version_suffix(source: &Path) -> Result<String> {
    let version = env!("CARGO_PKG_VERSION");
    if version == DEV_BUILD_VERSION_SENTINEL {
        dev_build_suffix(source)
    } else {
        Ok(version.to_string())
    }
}

fn dev_build_suffix(source: &Path) -> Result<String> {
    let metadata = fs::metadata(source)
        .with_context(|| format!("read helper source metadata {}", source.display()))?;
    let modified = metadata
        .modified()
        .with_context(|| format!("read helper source mtime {}", source.display()))?;
    let duration = modified
        .duration_since(UNIX_EPOCH)
        .with_context(|| format!("convert helper source mtime {}", source.display()))?;
    Ok(format!("{}-{:x}", metadata.len(), duration.as_secs(),))
}
