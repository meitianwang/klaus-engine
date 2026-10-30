//! Task-owned filesystem boundaries, independent of reusable permission grants.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::fs::node_path::{project_runtime_path, runtime_path_from_native};
use crate::runtime_values::RuntimeJson;
use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::schemas::config::{TaskResourceMode, TaskResourceScope};
use crate::types::tool::{PermissionPathKind, Tool, ToolContext};

pub(crate) fn task_scope_builtin_allowed(tool: &dyn Tool) -> bool {
    [
        "read",
        "write",
        "edit",
        "multi_edit",
        "ls",
        "glob",
        "grep",
        "bash",
        "think",
        "ask_user",
        "ask_user_async",
        // Search only exposes the already filtered registry's deferred tools.
        "tool_search",
    ]
    .iter()
    .any(|name| tool.name() == *name)
}

pub(crate) fn task_scope_tool_allowed(tool: &dyn Tool) -> bool {
    task_scope_builtin_allowed(tool)
        || tool.task_scope_host_capability()
        // Task sessions only register explicitly injected, per-task MCP tools.
        || tool.mcp_server().is_some()
}

/// Canonical roots are pinned when a task starts, rather than following aliases
/// again when a later tool asks to use them.
#[derive(Debug)]
pub struct CanonicalTaskResourceScope {
    pub mode: TaskResourceMode,
    pub network_allowed: bool,
    scratch: PathBuf,
    read_roots: Vec<PathBuf>,
    write_roots: Vec<PathBuf>,
    pinned_roots: Vec<Arc<crate::memory::pinned_root::PinnedMemoryRoot>>,
}

fn canonical_target(path: &Path) -> Result<PathBuf, String> {
    match crate::fs::node_path::canonicalize_host(path) {
        Ok(path) => return Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("Cannot resolve task resource path: {error}")),
    }
    let mut parent = path.to_path_buf();
    let mut tail = Vec::new();
    loop {
        // A dangling link must not be treated as an ordinary missing child.
        if std::fs::symlink_metadata(&parent)
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            return Err("Task resource path contains a dangling symbolic link".into());
        }
        let name = parent
            .file_name()
            .ok_or("Task resource path has no existing ancestor")?
            .to_os_string();
        tail.push(name);
        if !parent.pop() {
            return Err("Task resource path has no existing ancestor".into());
        }
        match crate::fs::node_path::canonicalize_host(&parent) {
            Ok(mut canonical) => {
                for name in tail.iter().rev() {
                    canonical.push(name);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("Cannot resolve task resource ancestor: {error}")),
        }
    }
}

fn root(path: &RuntimeString, already_canonical: bool) -> Result<PathBuf, String> {
    let native = project_runtime_path(&RuntimePath::from(path.clone()), "task resource root")
        .map_err(|error| format!("Invalid task resource root: {error:?}"))?;
    if !native.is_absolute() {
        return Err("Task resource roots must be absolute directories".into());
    }
    let canonical = crate::fs::node_path::canonicalize_host(&native)
        .map_err(|error| format!("Cannot resolve task resource root: {error}"))?;
    if already_canonical && canonical != native {
        return Err("Approved task resource directory changed before task initialization".into());
    }
    if !canonical.is_dir() {
        return Err("Task resource roots must be existing directories".into());
    }
    Ok(canonical)
}

impl CanonicalTaskResourceScope {
    pub fn bind(scope: &TaskResourceScope, cwd: &RuntimePath) -> Result<Arc<Self>, String> {
        scope.validate()?;
        let scratch = root(cwd.as_runtime_string(), false)?;
        let mut read_roots = scope
            .read_roots
            .iter()
            .map(|path| root(path, true))
            .collect::<Result<Vec<_>, _>>()?;
        let mut write_roots = scope
            .write_roots
            .iter()
            .map(|path| root(path, true))
            .collect::<Result<Vec<_>, _>>()?;
        write_roots.push(scratch.clone());
        read_roots.extend(write_roots.iter().cloned());
        read_roots.sort();
        read_roots.dedup();
        write_roots.sort();
        write_roots.dedup();
        let pinned_roots = read_roots
            .iter()
            .map(|root| {
                crate::memory::pinned_root::PinnedMemoryRoot::pin(root)
                    .map_err(|error| format!("Cannot pin task resource root: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(Self {
            mode: scope.mode,
            network_allowed: scope.network_allowed,
            scratch,
            read_roots,
            write_roots,
            pinned_roots,
        }))
    }

    pub fn writable_roots(&self) -> Vec<RuntimePath> {
        self.write_roots
            .iter()
            .map(|path| runtime_path_from_native(path))
            .collect()
    }

    pub fn validate_roots(&self) -> Result<(), String> {
        for pinned in &self.pinned_roots {
            pinned
                .validate_root_namespace()
                .map_err(|error| format!("Approved task resource directory changed: {error}"))?;
        }
        for root in &self.read_roots {
            if !crate::fs::node_path::canonicalize_host(root).is_ok_and(|current| current == *root)
            {
                return Err(
                    "Approved task resource directory changed; fresh owner approval is required"
                        .into(),
                );
            }
        }
        Ok(())
    }

    /// Bind native file I/O to an admitted handle before its first await.
    /// Tools without a handle-backed implementation are rejected by the tool
    /// allowlist instead of relying on a pathname check followed by a reopen.
    pub fn prepare_file_tool(
        &self,
        tool: &dyn Tool,
        input: &RuntimeJson,
        ctx: &mut ToolContext,
    ) -> Result<Option<RuntimeJson>, String> {
        if !["read", "write", "edit", "multi_edit", "ls"]
            .iter()
            .any(|name| tool.name() == *name)
        {
            return Ok(None);
        }
        self.check_tool(tool, input, ctx)?;
        let raw = input
            .get("path")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_else(|| ctx.cwd.as_runtime_string().clone());
        let absolute = crate::fs::node_path::resolve_runtime(&ctx.cwd, &[RuntimePath::from(raw)])
            .map_err(|error| format!("Invalid task file path: {error:?}"))?;
        let projected = project_runtime_path(&absolute, "task file target")
            .map_err(|error| format!("Invalid task file target: {error:?}"))?;
        let canonical = canonical_target(&projected)?;
        let root = self
            .pinned_roots
            .iter()
            .filter(|root| canonical.starts_with(root.canonical_root()))
            .max_by_key(|root| root.canonical_root().components().count())
            .ok_or("Task file target has no pinned resource root")?;
        ctx.permission.pinned_memory_root = Some(root.clone());
        ctx.permission.additional_writable_roots = self.writable_roots();
        let mut updated = input.clone();
        updated
            .as_object_mut()
            .ok_or("Task file input must be an object")?
            .insert(
                "path",
                RuntimeJson::String(runtime_path_from_native(&canonical).into_runtime_string()),
            );
        Ok(Some(updated))
    }

    pub fn readable_roots(&self) -> Vec<RuntimePath> {
        self.read_roots
            .iter()
            .map(|path| runtime_path_from_native(path))
            .collect()
    }

    pub fn check_path(
        &self,
        path: &RuntimePath,
        cwd: &RuntimePath,
        write: bool,
    ) -> Result<(), String> {
        let absolute = crate::fs::node_path::resolve_runtime(cwd, std::slice::from_ref(path))
            .map_err(|error| format!("Invalid task resource target: {error:?}"))?;
        let projected = project_runtime_path(&absolute, "task resource target")
            .map_err(|error| format!("Invalid task resource target: {error:?}"))?;
        let target = canonical_target(&projected)?;
        let roots = if write {
            &self.write_roots
        } else {
            &self.read_roots
        };
        // If an authorized root was itself replaced with a link, do not follow
        // its new destination and accidentally authorize a different tree.
        if roots.iter().any(|root| {
            target.starts_with(root)
                && crate::fs::node_path::canonicalize_host(root)
                    .is_ok_and(|current| current == *root)
        }) {
            return Ok(());
        }
        Err(format!(
            "Task resource scope denies {} access outside authorized directories",
            if write { "write" } else { "read" }
        ))
    }

    pub fn check_tool(
        &self,
        tool: &dyn Tool,
        input: &RuntimeJson,
        ctx: &ToolContext,
    ) -> Result<(), String> {
        self.validate_roots()?;
        if !task_scope_tool_allowed(tool) {
            return Err(format!(
                "Task resource scope denies unsupported tool {:?}",
                tool.name()
            ));
        }
        let kind = tool.permission_path_kind();
        if let Some(raw) = tool.get_raw_path(input) {
            let paths = if raw.is_array() {
                raw.into_array().expect("array shape was checked")
            } else {
                vec![raw]
            };
            for path in paths {
                let path = path
                    .as_string()
                    .ok_or("Task resource paths must be strings")?;
                self.check_path(
                    &RuntimePath::from(path.clone()),
                    &ctx.cwd,
                    kind == Some(PermissionPathKind::Edit)
                        || kind.is_none() && !tool.is_read_only(input),
                )?;
            }
        } else if kind.is_some() {
            self.check_path(&ctx.cwd, &ctx.cwd, kind == Some(PermissionPathKind::Edit))?;
        }
        if tool.name() == "bash" || tool.name() == "exec_command" {
            if !cfg!(target_os = "macos") {
                return Err("Isolated task commands are not supported on this platform".into());
            }
            let cwd = input
                .get("cwd")
                .and_then(RuntimeJson::as_string)
                .map(|value| RuntimePath::from(value.clone()))
                .unwrap_or_else(|| ctx.cwd.clone());
            self.check_path(&cwd, &ctx.cwd, false)?;
            if ctx.sandbox.kind() == crate::types::sandbox::SandboxKind::Process {
                return Err("Task shell execution requires a real filesystem sandbox".into());
            }
        }
        Ok(())
    }

    pub fn scratch_root(&self) -> RuntimePath {
        runtime_path_from_native(&self.scratch)
    }
}
