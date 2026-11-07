// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for native request paths, helper packaging, and execution lifecycle.

use crate::elevated::resolved_permissions::ResolvedWindowsSandboxPermissions;
use dunce::canonicalize;
use std::collections::HashSet;
use std::path::PathBuf;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct AllowDenyPaths {
    pub allow: HashSet<PathBuf>,
    pub deny: HashSet<PathBuf>,
}

pub(crate) fn compute_allow_paths_for_permissions(
    permissions: &ResolvedWindowsSandboxPermissions,
) -> AllowDenyPaths {
    let mut allow: HashSet<PathBuf> = HashSet::new();
    let mut deny: HashSet<PathBuf> = HashSet::new();

    let mut add_allow_path = |p: PathBuf| {
        if p.exists() {
            allow.insert(p);
        }
    };
    let mut add_deny_path = |p: PathBuf| {
        if p.exists() {
            deny.insert(p);
        }
    };

    for writable_root in permissions.writable_roots() {
        let canonical = canonicalize(&writable_root.root).unwrap_or(writable_root.root);
        add_allow_path(canonical);
        for read_only_subpath in writable_root.read_only_subpaths {
            add_deny_path(read_only_subpath);
        }
    }

    AllowDenyPaths { allow, deny }
}
