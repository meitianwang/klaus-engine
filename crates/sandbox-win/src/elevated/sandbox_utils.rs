// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for native request paths, helper packaging, and execution lifecycle.

//! Shared helper utilities for Windows sandbox setup.
//!
//! These helpers centralize small pieces of setup logic used across both legacy and
//! elevated paths, including unified_exec sessions and capture flows. They cover
//! configuration home directory creation and git safe.directory injection so sandboxed
//! users can run git inside a repo owned by the primary user.

use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;

/// Walk upward from `start` to locate the git worktree root for `safe.directory`.
fn find_git_worktree_root_for_safe_directory(start: &Path) -> Option<std::path::PathBuf> {
    let mut cur = dunce::canonicalize(start).ok()?;
    loop {
        if cur.join(".git").exists() {
            return Some(cur);
        }
        let parent = cur.parent()?;
        if parent == cur {
            return None;
        }
        cur = parent.to_path_buf();
    }
}

/// Ensure the sandbox configuration home directory exists.
pub fn ensure_config_home_exists(p: &Path) -> Result<()> {
    std::fs::create_dir_all(p)?;
    Ok(())
}

/// Adds git safe.directory entries for the repository and nested repositories.
/// git will not otherwise allow the Sandbox user to run git commands on repositories
/// which are owned by the primary user.
pub fn inject_git_safe_directory(env_map: &mut HashMap<String, String>, cwd: &Path) {
    if let Some(git_root) = find_git_worktree_root_for_safe_directory(cwd) {
        let mut cfg_count: usize = env_map
            .get("GIT_CONFIG_COUNT")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let git_path = git_root.to_string_lossy().replace("\\\\", "/");
        let nested_git_path = format!("{git_path}/*");
        for git_path in [git_path, nested_git_path] {
            env_map.insert(
                format!("GIT_CONFIG_KEY_{cfg_count}"),
                "safe.directory".to_string(),
            );
            env_map.insert(format!("GIT_CONFIG_VALUE_{cfg_count}"), git_path);
            cfg_count += 1;
        }
        env_map.insert("GIT_CONFIG_COUNT".to_string(), cfg_count.to_string());
    }
}
