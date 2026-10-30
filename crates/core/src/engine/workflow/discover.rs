//! Disk-registered named workflows (the `.klaus/workflows/` convention). A
//! workflow is a bare `.js` file whose first statement is
//! `export const meta = { name, description, ... }`; the runtime name is
//! `meta.name` VERBATIM — the filename only matters for saving. Loading is
//! synchronous so it can sit behind the engine's sync `workflows` thunk (which
//! also buys live reload: the thunk re-reads on each evaluation).

use crate::engine::workflow::meta::parse_workflow;
use crate::engine::workflow::WorkflowError;
use crate::fs::paths::{config_home, find_project_root};
use crate::permission::bash::node_path::{dirname, posix_rendered, resolve};
use crate::runtime_values::{NodeBufferUtf8, RuntimeString};
use crate::tools::builtins::workflow::WorkflowRef;
use std::path::Path;
use tokio::io::AsyncWriteExt;

/// Max size of a workflow program file; larger files are skipped with a warning.
const MAX_WORKFLOW_FILE_BYTES: u64 = 524_288;
const WORKFLOW_FILE_EXT: &str = ".js";

/// The directories a session loads named workflows from, in precedence order
/// (first hit wins on a name collision):
///   1. every existing `<ancestor>/.klaus/workflows` walking UP from `cwd`
///      (nearest first — the innermost project dir shadows outer ones),
///   2. the user dir `<config_home>/workflows`.
///
/// The ancestor walk stops at the home directory (exclusive) so the user scope
/// stays a distinct tier rather than being picked up mid-walk.
pub fn workflow_dirs(cwd: &str) -> Vec<String> {
    let mut dirs: Vec<String> = Vec::new();
    let home = home_dir();
    let mut dir = resolve(&[cwd]);
    loop {
        if dir == home {
            break;
        }
        let candidate = join(&[&dir, ".klaus", "workflows"]);
        if Path::new(&candidate).exists() {
            dirs.push(candidate);
        }
        let parent = dirname(&dir);
        if parent == dir {
            break;
        }
        dir = parent;
    }
    dirs.push(join(&[&config_home(), "workflows"]));
    dirs
}

/// Load every valid workflow script in one directory. Only bare `.js` files
/// are considered; oversized or unparsable files are skipped with a warning so
/// one bad file can never break discovery.
pub fn load_workflows_from_dir(dir: &str) -> Vec<WorkflowRef> {
    let entries = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<WorkflowRef> = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let fname = entry.file_name().to_string_lossy().into_owned();
        if !fname.ends_with(WORKFLOW_FILE_EXT) {
            continue;
        }
        let file = join(&[dir, &fname]);
        match load_one_workflow(&file) {
            LoadOutcome::Loaded(wf) => out.push(wf),
            LoadOutcome::Skip => {}
            LoadOutcome::Invalid(msg) => {
                tracing::warn!(
                    "workflow {} has invalid meta: {} — skipping",
                    file,
                    runtime_diagnostic(&msg)
                );
            }
        }
    }
    out
}

enum LoadOutcome {
    Loaded(WorkflowRef),
    /// Not a regular file, or oversized (already warned): skipped silently here.
    Skip,
    /// A stat/read/parse failure: warned as "has invalid meta".
    Invalid(RuntimeString),
}

fn load_one_workflow(file: &str) -> LoadOutcome {
    let meta = match std::fs::metadata(file) {
        Ok(m) => m,
        Err(e) => return LoadOutcome::Invalid(e.to_string().into()),
    };
    if !meta.is_file() {
        return LoadOutcome::Skip;
    }
    if meta.len() > MAX_WORKFLOW_FILE_BYTES {
        tracing::warn!(
            "workflow {} exceeds {} bytes — skipping",
            file,
            MAX_WORKFLOW_FILE_BYTES
        );
        return LoadOutcome::Skip;
    }
    let bytes = match std::fs::read(file) {
        Ok(b) => b,
        Err(e) => return LoadOutcome::Invalid(e.to_string().into()),
    };
    let script = NodeBufferUtf8::decode(&bytes);
    match parse_workflow(&script) {
        Ok(parsed) => LoadOutcome::Loaded(WorkflowRef {
            name: parsed.meta.name,
            description: Some(parsed.meta.description),
            script,
        }),
        Err(e) => LoadOutcome::Invalid(e.message),
    }
}

/// Discover all disk-registered workflows visible from `cwd`, deduplicated by
/// `meta.name` with first-wins across [`workflow_dirs`] order (nearest project
/// dir > outer project dirs > user dir).
pub fn discover_workflows(cwd: &str) -> Vec<WorkflowRef> {
    let mut seen: std::collections::HashSet<RuntimeString> = std::collections::HashSet::new();
    let mut out: Vec<WorkflowRef> = Vec::new();
    for dir in workflow_dirs(cwd) {
        for wf in load_workflows_from_dir(&dir) {
            if seen.contains(&wf.name) {
                continue;
            }
            seen.insert(wf.name.clone());
            out.push(wf);
        }
    }
    out
}

/// Reduce a workflow name to a safe filename slug.
pub fn workflow_file_slug(name: &RuntimeString) -> String {
    let mut slug = String::new();
    let mut pending_dash = false;
    for point in name.code_points() {
        let Some(character) = char::from_u32(point) else {
            pending_dash = !slug.is_empty();
            continue;
        };
        for lowered in character.to_lowercase() {
            if lowered.is_ascii_lowercase() || lowered.is_ascii_digit() {
                if pending_dash && !slug.is_empty() {
                    slug.push('-');
                }
                pending_dash = false;
                slug.push(lowered);
            } else {
                pending_dash = !slug.is_empty();
            }
        }
    }
    if slug.is_empty() {
        "workflow".to_string()
    } else {
        slug
    }
}

/// Where a saved workflow lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowScope {
    User,
    Project,
}

/// Options for [`save_workflow`].
#[derive(Debug, Clone)]
pub struct SaveWorkflowOptions {
    pub name: RuntimeString,
    pub scope: WorkflowScope,
    pub script: RuntimeString,
    /// Overwrite an existing file (default false — saving over one errors).
    pub overwrite: bool,
    /// Required for project scope (resolves the target `.klaus/workflows`).
    pub cwd: Option<String>,
}

/// The saved workflow's on-disk path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaveWorkflowResult {
    pub path: String,
}

/// Persist a workflow program into the named-workflow registry: user scope →
/// `<config_home>/workflows/<slug>.js`; project scope → the nearest
/// existing `<ancestor>/.klaus/workflows` else
/// `<project_root>/.klaus/workflows`. The program is fully validated first so
/// a broken body is rejected at save time rather than at first run. Dir mode
/// 0o700 / file mode 0o600; without `overwrite` an existing file is an error.
pub async fn save_workflow(opts: SaveWorkflowOptions) -> Result<SaveWorkflowResult, WorkflowError> {
    parse_workflow(&opts.script).map_err(|e| WorkflowError::new(e.message))?;

    let dir = match opts.scope {
        WorkflowScope::User => join(&[&config_home(), "workflows"]),
        WorkflowScope::Project => {
            let cwd = opts.cwd.clone().unwrap_or_else(current_dir);
            let mut project_tiers = workflow_dirs(&cwd);
            project_tiers.pop(); // drop the user dir (last) → project tiers only
            match project_tiers.into_iter().next() {
                Some(d) => d,
                None => join(&[&find_project_root(&cwd), ".klaus", "workflows"]),
            }
        }
    };
    let path = join(&[
        &dir,
        &format!("{}{}", workflow_file_slug(&opts.name), WORKFLOW_FILE_EXT),
    ]);
    // The scope roots are host paths and the tiers below them are joined with `/`, so on
    // Windows the result mixed both separators in one path (`C:\Users\me\.klaus-config` +
    // `/workflows/flow.js`). What the caller and the duplicate-name error name is one
    // spelling: POSIX, the same as every other path the engine reports.
    let reported_path = posix_rendered(&path);

    let mut builder = tokio::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        builder.mode(0o700);
    }
    builder
        .create(&dir)
        .await
        .map_err(|e| WorkflowError::new(e.to_string()))?;

    let mut open_opts = tokio::fs::OpenOptions::new();
    open_opts.write(true);
    if opts.overwrite {
        open_opts.create(true).truncate(true);
    } else {
        open_opts.create_new(true);
    }
    #[cfg(unix)]
    {
        open_opts.mode(0o600);
    }
    match open_opts.open(&path).await {
        Ok(mut file) => {
            file.write_all(&opts.script.to_node_utf8())
                .await
                .map_err(|e| WorkflowError::new(e.to_string()))?;
            file.flush()
                .await
                .map_err(|e| WorkflowError::new(e.to_string()))?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(WorkflowError::new(
                RuntimeString::from("workflow \"")
                    .concat(&opts.name)
                    .concat(&RuntimeString::from(format!(
                        "\" already exists at {reported_path} — use a different name or overwrite"
                    ))),
            ));
        }
        Err(e) => return Err(WorkflowError::new(e.to_string())),
    }
    Ok(SaveWorkflowResult {
        path: reported_path,
    })
}

fn runtime_diagnostic(value: &RuntimeString) -> String {
    let mut output = String::new();
    for point in value.code_points() {
        match char::from_u32(point) {
            Some(character) => output.push(character),
            None => output.push_str(&format!("\\u{point:04x}")),
        }
    }
    output
}

// ---- path helpers -----------------------------------------------------------

/// The current user's home directory ("" when it cannot be determined).
fn home_dir() -> String {
    #[allow(deprecated)]
    std::env::home_dir()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// The process working directory (fallback root when it cannot be read).
fn current_dir() -> String {
    std::env::current_dir()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "/".to_string())
}

/// POSIX `path.join`: concatenate the non-empty segments and normalize.
fn join(parts: &[&str]) -> String {
    let mut joined: Option<String> = None;
    for part in parts {
        if part.is_empty() {
            continue;
        }
        joined = Some(match joined {
            None => (*part).to_string(),
            Some(j) => format!("{j}/{part}"),
        });
    }
    match joined {
        None => ".".to_string(),
        Some(j) => normalize(&j),
    }
}

/// Normalize a POSIX path: collapse `.`/`..` and duplicate slashes, preserving a
/// leading `/` (absolute) and a trailing `/`.
fn normalize(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let is_abs = path.starts_with('/');
    let trailing = path.ends_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in path.split('/') {
        match seg {
            "" | "." => {}
            ".." => match out.last() {
                Some(&last) if last != ".." => {
                    out.pop();
                }
                _ => {
                    if !is_abs {
                        out.push("..");
                    }
                }
            },
            s => out.push(s),
        }
    }
    let mut joined = out.join("/");
    if joined.is_empty() {
        if is_abs {
            return "/".to_string();
        }
        return if trailing {
            "./".to_string()
        } else {
            ".".to_string()
        };
    }
    if trailing {
        joined.push('/');
    }
    if is_abs {
        format!("/{joined}")
    } else {
        joined
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::CONFIG_ENV_LOCK as ENV_LOCK;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(value: &str) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prev = std::env::var("KLAUS_CONFIG_DIR").ok();
            crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", value);
            EnvGuard { _lock: lock, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => {
                    crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", v)
                }
                None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
            }
        }
    }

    fn make_temp_dir(tag: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let uniq = format!(
            "ae-workflow-{tag}-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = base.join(uniq);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    const VALID_JS: &str =
        "export const meta = { name: 'demo', description: 'A demo' }\nreturn null";

    #[test]
    fn slug_cases() {
        assert_eq!(
            workflow_file_slug(&RuntimeString::from("Deep Research!")),
            "deep-research"
        );
        assert_eq!(workflow_file_slug(&RuntimeString::from("  ")), "workflow");
        assert_eq!(workflow_file_slug(&RuntimeString::from("a__b--c")), "a-b-c");
        assert_eq!(
            workflow_file_slug(&RuntimeString::from("already-ok-123")),
            "already-ok-123"
        );
        assert_eq!(workflow_file_slug(&RuntimeString::from("@@@")), "workflow");
    }

    #[test]
    fn load_dir_skips_bad_and_oversized_files() {
        let dir = make_temp_dir("loaddir");
        let d = dir.to_string_lossy().into_owned();
        std::fs::write(dir.join("good.js"), VALID_JS).unwrap();
        std::fs::write(dir.join("bad.js"), "no meta here").unwrap();
        std::fs::write(
            dir.join("ignore.flow.json"),
            r#"{"meta":{"name":"flow","description":"Flow"},"body":[{"return":null}]}"#,
        )
        .unwrap();
        std::fs::write(dir.join("ignore.txt"), VALID_JS).unwrap();
        // Oversized file is skipped.
        let big = format!("{VALID_JS}{}", "a".repeat(600_000));
        std::fs::write(dir.join("big.js"), big).unwrap();

        let loaded = load_workflows_from_dir(&d);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "demo");
        assert_eq!(loaded[0].description.as_ref(), Some(&"A demo".into()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_missing_dir_is_empty() {
        assert!(load_workflows_from_dir("/nonexistent/ae/workflows").is_empty());
    }

    #[tokio::test]
    async fn save_user_scope_then_discover_round_trips() {
        let cfg = make_temp_dir("savecfg");
        let _g = EnvGuard::set(&cfg.to_string_lossy());
        // A distinct cwd with no project dirs so discovery only sees the user tier.
        let work = make_temp_dir("savework");
        let cwd = work.to_string_lossy().into_owned();

        let saved = save_workflow(SaveWorkflowOptions {
            name: "My Flow".into(),
            scope: WorkflowScope::User,
            script: VALID_JS.into(),
            overwrite: false,
            cwd: Some(cwd.clone()),
        })
        .await
        .unwrap();
        assert!(saved.path.ends_with("/workflows/my-flow.js"));

        // Saving again without overwrite errors with the precise message.
        let err = save_workflow(SaveWorkflowOptions {
            name: "My Flow".into(),
            scope: WorkflowScope::User,
            script: VALID_JS.into(),
            overwrite: false,
            cwd: Some(cwd.clone()),
        })
        .await
        .unwrap_err();
        assert!(err
            .message
            .starts_with("workflow \"My Flow\" already exists at"));
        assert!(err.message.ends_with("use a different name or overwrite"));

        // Overwrite succeeds.
        save_workflow(SaveWorkflowOptions {
            name: "My Flow".into(),
            scope: WorkflowScope::User,
            script: VALID_JS.into(),
            overwrite: true,
            cwd: Some(cwd.clone()),
        })
        .await
        .unwrap();

        let discovered = discover_workflows(&cwd);
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].name, "demo");

        let _ = std::fs::remove_dir_all(&cfg);
        let _ = std::fs::remove_dir_all(&work);
    }

    #[tokio::test]
    async fn save_rejects_invalid_script_before_writing() {
        let cfg = make_temp_dir("savebad");
        let _g = EnvGuard::set(&cfg.to_string_lossy());
        let err = save_workflow(SaveWorkflowOptions {
            name: "broken".into(),
            scope: WorkflowScope::User,
            script: "not a workflow".into(),
            overwrite: false,
            cwd: None,
        })
        .await
        .unwrap_err();
        assert_eq!(
            err.message,
            "workflow script must begin with `export const meta = { ... }`"
        );
        let _ = std::fs::remove_dir_all(&cfg);
    }
}
