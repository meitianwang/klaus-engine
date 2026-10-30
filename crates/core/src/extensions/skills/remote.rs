//! Install a single skill from a remote or archive source into the user-level
//! skills root. A skill spec is classified (by pattern, no I/O) into one of a
//! handful of source kinds — a GitHub repo, an arbitrary git URL, a `.tar.gz`
//! archive (local path or http URL), an npm package, or a local directory — and
//! then materialized to disk, validated (a `SKILL.md` must load), and copied
//! into `<configHome>/skills/<name>`.
//!
//! Safety contract mirrored here: the destination name is the basename of the
//! subpath/repo (or an explicit override), NOT the frontmatter `name`; a subpath
//! may not escape the source root; the destination name must stay under the
//! skills home; validation happens BEFORE any overwrite so a bad source can never
//! destroy an already-installed skill; and an existing destination is refused
//! unless `force` is set.

use crate::extensions::skills::loader::{load_one_skill, Skill};
use crate::fs::paths::config_home;
use crate::permission::bash::node_path::{is_absolute, resolve};
use crate::runtime_process::{
    RuntimeCompletedProcessRequest, RuntimeCompletedProcessSurface, RuntimeProcessContext,
};
use crate::runtime_values::{RuntimePath, RuntimeString};
use regex::Regex;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// A boxed, thread-safe error carrying a human-readable message.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A concrete, classified skill source. `subpath` pulls a skill out of a
/// repo/archive subdirectory; `git_ref` pins a branch/tag/commit for the git
/// sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkillSource {
    /// A GitHub `<owner>/<repo>` (fetched as a tarball, git clone on failure).
    Github {
        owner: String,
        repo: String,
        git_ref: Option<String>,
        subpath: Option<String>,
    },
    /// An arbitrary git URL (shallow clone).
    Git {
        url: String,
        git_ref: Option<String>,
        subpath: Option<String>,
    },
    /// A `.tgz` / `.tar.gz` archive — a local path OR an http(s) URL.
    Tarball {
        src: String,
        subpath: Option<String>,
    },
    /// An npm package (`npm pack`).
    Npm {
        pkg: String,
        subpath: Option<String>,
    },
    /// A local directory already on disk.
    Local { path: String },
}

/// A spec accepted by [`install_skill`]: either a raw string classified via
/// [`parse_skill_source`], or an already-built [`SkillSource`].
pub enum SkillSpec {
    Str(String),
    Source(SkillSource),
}

impl From<&str> for SkillSpec {
    fn from(s: &str) -> Self {
        SkillSpec::Str(s.to_string())
    }
}
impl From<String> for SkillSpec {
    fn from(s: String) -> Self {
        SkillSpec::Str(s)
    }
}
impl From<SkillSource> for SkillSpec {
    fn from(s: SkillSource) -> Self {
        SkillSpec::Source(s)
    }
}

/// Options controlling a single install. All fields default to "unset".
#[derive(Default, Clone)]
pub struct InstallOptions {
    /// Branch/tag/commit override for git sources.
    pub git_ref: Option<String>,
    /// Subdirectory within the source that holds the skill.
    pub subpath: Option<String>,
    /// Explicit destination directory name (overrides the derived basename).
    pub name: Option<String>,
    /// Skills root override (defaults to the user skills dir). For tests.
    pub dir: Option<String>,
    /// Overwrite an already-installed skill of the same name.
    pub force: bool,
}

/// The user-level skills root (`<configHome>/skills`), where installed skills land.
pub fn user_skills_dir() -> String {
    path_join(&[&config_home(), "skills"])
}

// GitHub tree/blob URL with an embedded ref + subpath.
static TREE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?/(?:tree|blob)/([^/]+)/(.+)$")
        .unwrap()
});
// A bare GitHub repo URL (optionally `.git`, optional trailing slash).
static REPO_URL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?/?$").unwrap());
// scp-like git remote: `git@host:owner/repo.git`.
static SCP_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[^/\s]+@[^/\s]+:").unwrap());
// Strip a leading npm scope (`@scope/`).
static NPM_SCOPE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^@[^/]+/").unwrap());
// Strip a trailing npm version (`@1.2.3` and anything after the first `@`).
static NPM_VERSION_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"@.*$").unwrap());

/// Classify a skill spec string into a concrete source (pattern-based, no I/O):
///   - `https://github.com/<o>/<r>/tree|blob/<ref>/<subpath>` → github (ref + subpath)
///   - `https://github.com/<o>/<r>[.git]` → github
///   - `npm:<pkg>` → npm
///   - `*.tgz` / `*.tar.gz` (local path OR http(s) URL) → tarball
///   - `git+<url>` / `file://…` / `git@host:…` / `*.git` / other http(s) → git
///   - `/abs`, `./rel`, `../rel`, `~/…` → local
///   - `<owner>/<repo>` (bare, single slash) → github
///   - anything else → local (best-effort)
///
/// [`install_skill`] additionally short-circuits to `local` when the spec is an
/// existing directory on disk, so an unprefixed local path still works.
pub fn parse_skill_source(spec: &str) -> SkillSource {
    if let Some(caps) = TREE_RE.captures(spec) {
        return SkillSource::Github {
            owner: caps[1].to_string(),
            repo: caps[2].to_string(),
            git_ref: Some(caps[3].to_string()),
            subpath: Some(caps[4].to_string()),
        };
    }
    if let Some(caps) = REPO_URL_RE.captures(spec) {
        return SkillSource::Github {
            owner: caps[1].to_string(),
            repo: caps[2].to_string(),
            git_ref: None,
            subpath: None,
        };
    }
    if let Some(pkg) = spec.strip_prefix("npm:") {
        return SkillSource::Npm {
            pkg: pkg.to_string(),
            subpath: None,
        };
    }
    // Tarball BEFORE the git/http checks, so a `.tar.gz` URL isn't mistaken for a git repo.
    if spec.ends_with(".tgz") || spec.ends_with(".tar.gz") {
        return SkillSource::Tarball {
            src: spec.to_string(),
            subpath: None,
        };
    }
    if let Some(url) = spec.strip_prefix("git+") {
        return SkillSource::Git {
            url: url.to_string(),
            git_ref: None,
            subpath: None,
        };
    }
    if spec.starts_with("file://")
        || SCP_RE.is_match(spec)
        || spec.ends_with(".git")
        || is_http_url(spec)
    {
        return SkillSource::Git {
            url: spec.to_string(),
            git_ref: None,
            subpath: None,
        };
    }
    if spec.starts_with('/')
        || spec.starts_with("./")
        || spec.starts_with("../")
        || spec.starts_with('~')
    {
        return SkillSource::Local {
            path: spec.to_string(),
        };
    }
    let parts: Vec<&str> = spec.split('/').collect();
    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
        return SkillSource::Github {
            owner: parts[0].to_string(),
            repo: parts[1].to_string(),
            git_ref: None,
            subpath: None,
        };
    }
    SkillSource::Local {
        path: spec.to_string(),
    }
}

// --- shared fetch/extract helpers -------------------------------------------

/// Download a URL to a file. Adds a UA + optional token, but the token only for
/// GitHub hosts (never leak a token to a third-party tarball host).
async fn download_file(url: &str, dest_file: &str) -> Result<(), BoxError> {
    // `GITHUB_TOKEN` wins; a *set-but-empty* value still suppresses the fallback.
    let token = match std::env::var("GITHUB_TOKEN") {
        Ok(v) => Some(v),
        Err(_) => std::env::var("GH_TOKEN").ok(),
    };
    let host = reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()))
        .unwrap_or_default();

    let client = reqwest::Client::new();
    let mut req = client
        .get(url)
        .header("User-Agent", "klaus-agent-skill-install");
    if let Some(t) = token.as_deref() {
        if !t.is_empty()
            && (host == "api.github.com" || host == "codeload.github.com" || host == "github.com")
        {
            req = req.header("Authorization", format!("Bearer {t}"));
        }
    }
    let res = req.send().await?;
    if !res.status().is_success() {
        return Err(format!("download failed: HTTP {} for {url}", res.status().as_u16()).into());
    }
    let bytes = res.bytes().await?;
    tokio::fs::write(dest_file, &bytes).await?;
    Ok(())
}

/// Extract a `.tar.gz` into `dest_dir` (must exist). Runs `tar` directly (no shell).
async fn extract_tar_gz(
    process: &RuntimeProcessContext,
    tgz: &str,
    dest_dir: &str,
) -> Result<(), BoxError> {
    run_command(process, "tar", &["-xzf", tgz, "-C", dest_dir], None).await?;
    Ok(())
}

/// The single subdirectory of `dir`, or `None` if there isn't exactly one.
async fn sole_subdir(dir: &str) -> Option<String> {
    let mut rd = tokio::fs::read_dir(dir).await.ok()?;
    let mut dirs: Vec<String> = Vec::new();
    while let Ok(Some(entry)) = rd.next_entry().await {
        if let Ok(ft) = entry.file_type().await {
            if ft.is_dir() {
                dirs.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    if dirs.len() == 1 {
        Some(path_join(&[dir, &dirs[0]]))
    } else {
        None
    }
}

async fn is_dir(path: &str) -> bool {
    tokio::fs::metadata(path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
}

async fn is_file(path: &str) -> bool {
    tokio::fs::metadata(path)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false)
}

fn is_http_url(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://")
}

/// Download + extract a GitHub tarball into `staging`, returning the content root
/// (GitHub tarballs wrap everything in a single `<owner>-<repo>-<sha>/` dir).
async fn github_tarball(
    process: &RuntimeProcessContext,
    api: &str,
    staging: &str,
) -> Result<String, BoxError> {
    let tgz = path_join(&[staging, "gh.tar.gz"]);
    download_file(api, &tgz).await?;
    let ex = path_join(&[staging, "gh"]);
    tokio::fs::create_dir_all(&ex).await?;
    extract_tar_gz(process, &tgz, &ex).await?;
    Ok(sole_subdir(&ex).await.unwrap_or(ex))
}

/// Materialize a remote/archive source into a directory on disk (the "content
/// root" against which `subpath` is resolved), using `staging` as scratch space.
/// GitHub uses a tarball download (no local `git` needed) and falls back to a
/// shallow `git clone` on any failure. Never called for a local source.
async fn materialize(
    process: &RuntimeProcessContext,
    source: &SkillSource,
    git_ref: Option<&str>,
    staging: &str,
) -> Result<(String, String), BoxError> {
    match source {
        SkillSource::Github { owner, repo, .. } => {
            let api = format!(
                "https://api.github.com/repos/{owner}/{repo}/tarball{}",
                git_ref.map(|r| format!("/{r}")).unwrap_or_default()
            );
            match github_tarball(process, &api, staging).await {
                Ok(root) => Ok((root, repo.clone())),
                Err(_) => {
                    // Fallback: shallow clone (needs `git`, but works for private
                    // repos via the user's credentials).
                    let clone = path_join(&[staging, "gh-git"]);
                    let url = format!("https://github.com/{owner}/{repo}.git");
                    let mut args: Vec<&str> = vec!["clone", "--depth", "1"];
                    if let Some(r) = git_ref {
                        args.push("--branch");
                        args.push(r);
                    }
                    args.push(url.as_str());
                    args.push(clone.as_str());
                    run_command(process, "git", &args, None).await?;
                    Ok((clone, repo.clone()))
                }
            }
        }
        SkillSource::Git { url, .. } => {
            let clone = path_join(&[staging, "git"]);
            let mut args: Vec<&str> = vec!["clone", "--depth", "1"];
            if let Some(r) = git_ref {
                args.push("--branch");
                args.push(r);
            }
            args.push(url.as_str());
            args.push(clone.as_str());
            run_command(process, "git", &args, None).await?;
            let stripped = strip_git_suffix(url).trim_end_matches('/');
            let base = basename(stripped);
            let base = if base.is_empty() {
                "skill".to_string()
            } else {
                base
            };
            Ok((clone, base))
        }
        SkillSource::Tarball { src, .. } => {
            let tgz = if is_http_url(src) {
                let t = path_join(&[staging, "archive.tar.gz"]);
                download_file(src, &t).await?;
                t
            } else {
                resolve(&[src.as_str()])
            };
            let ex = path_join(&[staging, "tar"]);
            tokio::fs::create_dir_all(&ex).await?;
            extract_tar_gz(process, &tgz, &ex).await?;
            // Descend into a single wrapper dir unless SKILL.md is already at the top.
            let mut root = ex.clone();
            if !is_file(&path_join(&[&ex, "SKILL.md"])).await {
                root = sole_subdir(&ex).await.unwrap_or(ex);
            }
            let base = basename(strip_tar_suffix(src));
            let base = if base.is_empty() {
                "skill".to_string()
            } else {
                base
            };
            Ok((root, base))
        }
        SkillSource::Npm { pkg, .. } => {
            // `npm pack` produces a `<name>-<version>.tgz` rooted at `package/`.
            let out =
                run_command(process, "npm", &["pack", pkg, "--silent"], Some(staging)).await?;
            let stdout = String::from_utf8_lossy(&out.stdout);
            let file = stdout.trim().split('\n').next_back().unwrap_or("");
            if file.is_empty() {
                return Err(format!("npm pack produced no tarball for {pkg}").into());
            }
            extract_tar_gz(process, &path_join(&[staging, file]), staging).await?;
            let base = npm_base_name(pkg);
            let base = if base.is_empty() {
                "skill".to_string()
            } else {
                base
            };
            Ok((path_join(&[staging, "package"]), base))
        }
        SkillSource::Local { .. } => unreachable!("materialize is never called for a local source"),
    }
}

/// Reject a skill dir name that isn't a single safe path segment (never dot-prefixed).
fn is_valid_skill_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('\\')
        && name != "."
        && name != ".."
        && !name.starts_with('.')
}

/// Install a single skill from a GitHub / git / tarball / npm / local source into
/// `<configHome>/skills/<name>`, and return the loaded skill.
///
/// Returns `Ok(None)` (with a warning) on any rejection: bad name, path escape,
/// missing `SKILL.md`, or an existing destination without `force`. Propagates
/// `Err` on materialize/copy failures (network, missing `git`, I/O).
pub async fn install_skill(
    process: &RuntimeProcessContext,
    spec: impl Into<SkillSpec>,
    opts: InstallOptions,
) -> Result<Option<Skill>, BoxError> {
    let spec = spec.into();
    let mut source = match &spec {
        SkillSpec::Str(s) => parse_skill_source(s),
        SkillSpec::Source(src) => src.clone(),
    };
    // An unprefixed spec that happens to be a real directory is a local install.
    if let SkillSpec::Str(s) = &spec {
        if matches!(source, SkillSource::Github { .. }) && is_dir(s).await {
            source = SkillSource::Local { path: s.clone() };
        }
    }

    let subpath = opts.subpath.clone().or_else(|| source_subpath(&source));
    let git_ref = opts.git_ref.clone().or_else(|| match &source {
        SkillSource::Github { git_ref, .. } | SkillSource::Git { git_ref, .. } => git_ref.clone(),
        _ => None,
    });
    let skill_home = opts.dir.clone().unwrap_or_else(user_skills_dir);

    let mut staging: Option<String> = None;
    let result = install_body(
        process,
        &source,
        subpath.as_deref(),
        git_ref.as_deref(),
        &skill_home,
        &opts,
        &mut staging,
    )
    .await;
    if let Some(s) = &staging {
        let _ = tokio::fs::remove_dir_all(s).await;
    }
    result
}

/// The body wrapped by the staging-cleanup guard in [`install_skill`].
async fn install_body(
    process: &RuntimeProcessContext,
    source: &SkillSource,
    subpath: Option<&str>,
    git_ref: Option<&str>,
    skill_home: &str,
    opts: &InstallOptions,
    staging: &mut Option<String>,
) -> Result<Option<Skill>, BoxError> {
    // 1. Obtain the content root (clone/download for remote, resolve for local).
    let (src_root, default_name) = if let SkillSource::Local { path } = source {
        let root = resolve(&[path.as_str()]);
        let name = basename(&root);
        (root, name)
    } else {
        let s = make_temp_dir("ae-skill-").await?;
        *staging = Some(s.clone());
        materialize(process, source, git_ref, &s).await?
    };

    // 2. Resolve the skill dir within the source, guarding against subpath escape.
    let src_dir = match subpath {
        Some(sp) => path_join(&[&src_root, sp]),
        None => src_root.clone(),
    };
    let rel_sub = relative(&src_root, &src_dir);
    if rel_sub.starts_with("..") || is_absolute(&rel_sub) {
        tracing::warn!(
            "[extensions] refusing skill subpath escaping source: {}",
            subpath.unwrap_or("")
        );
        return Ok(None);
    }

    // 3. Destination name = basename(subpath ?? repo/local) or explicit override.
    let name = match &opts.name {
        Some(n) => n.clone(),
        None => basename(
            subpath
                .unwrap_or(default_name.as_str())
                .trim_end_matches('/'),
        ),
    };
    if !is_valid_skill_name(&name) {
        tracing::warn!(
            "[extensions] refusing to install skill with unsafe name: {}",
            name
        );
        return Ok(None);
    }

    // 4. Destination path, guarded to stay under the skills home.
    let dest = path_join(&[skill_home, &name]);
    let rel_dest = relative(skill_home, &dest);
    if rel_dest.is_empty() || rel_dest.starts_with("..") || is_absolute(&rel_dest) {
        tracing::warn!(
            "[extensions] refusing to install skill with unsafe name: {}",
            name
        );
        return Ok(None);
    }

    // 5. Validate the source BEFORE touching the destination — a bad source must
    //    never destroy an already-installed skill.
    let probe = load_one_skill(&path_join(&[&src_dir, "SKILL.md"]), &name).await;
    if probe.is_none() {
        tracing::warn!(
            "[extensions] no valid SKILL.md in skill source: {}",
            src_dir
        );
        return Ok(None);
    }

    // 6. Refuse to overwrite unless forced.
    if is_dir(&dest).await {
        if !opts.force {
            tracing::warn!(
                "[extensions] skill already installed (pass force to overwrite): {}",
                name
            );
            return Ok(None);
        }
        tokio::fs::remove_dir_all(&dest).await?;
    }

    // 7. Land it, then re-load from the installed location.
    tokio::fs::create_dir_all(skill_home).await?;
    copy_dir_recursive(&src_dir, &dest).await?;
    Ok(load_one_skill(&path_join(&[&dest, "SKILL.md"]), &name).await)
}

/// The subpath carried by a non-local source (`None` for a local source).
fn source_subpath(source: &SkillSource) -> Option<String> {
    match source {
        SkillSource::Github { subpath, .. }
        | SkillSource::Git { subpath, .. }
        | SkillSource::Tarball { subpath, .. }
        | SkillSource::Npm { subpath, .. } => subpath.clone(),
        SkillSource::Local { .. } => None,
    }
}

fn strip_git_suffix(s: &str) -> &str {
    s.strip_suffix(".git").unwrap_or(s)
}

fn strip_tar_suffix(s: &str) -> &str {
    if let Some(r) = s.strip_suffix(".tar.gz") {
        r
    } else if let Some(r) = s.strip_suffix(".tgz") {
        r
    } else {
        s
    }
}

/// `<pkg>` with any leading `@scope/` and trailing `@version` removed.
fn npm_base_name(pkg: &str) -> String {
    let no_scope = NPM_SCOPE_RE.replace(pkg, "");
    NPM_VERSION_RE.replace(&no_scope, "").into_owned()
}

/// Run a child process (no shell), erroring on a spawn failure or non-zero exit.
async fn run_command(
    process: &RuntimeProcessContext,
    program: &str,
    args: &[&str],
    cwd: Option<&str>,
) -> Result<crate::runtime_process::RuntimeExecFileOutput, BoxError> {
    let mut request = RuntimeCompletedProcessRequest::new(
        RuntimeCompletedProcessSurface::ExtensionExecFile,
        RuntimeString::from(program),
    )
    .with_args(args.iter().copied().map(RuntimeString::from).collect());
    if let Some(c) = cwd {
        request = request.with_cwd(RuntimePath::from(c));
    }
    let out = process.exec_completed(request).await?;
    if !out.status.success() {
        return Err(format!(
            "command `{program}` failed with code {:?}, signal {:?}",
            out.status.code(),
            out.status.signal()
        )
        .into());
    }
    Ok(out)
}

/// Create a fresh, uniquely-named scratch directory under the system temp dir.
async fn make_temp_dir(prefix: &str) -> Result<String, BoxError> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let name = format!("{prefix}{}-{}-{}", std::process::id(), nanos, n);
    let dir = std::env::temp_dir().join(name);
    tokio::fs::create_dir_all(&dir).await?;
    Ok(dir.to_string_lossy().into_owned())
}

/// Recursively copy the contents of `src` into `dest` (creating `dest`).
fn copy_dir_recursive<'a>(
    src: &'a str,
    dest: &'a str,
) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'a>> {
    Box::pin(async move {
        tokio::fs::create_dir_all(dest).await?;
        let mut rd = tokio::fs::read_dir(src).await?;
        while let Some(entry) = rd.next_entry().await? {
            let ft = entry.file_type().await?;
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            let s = path_join(&[src, &name]);
            let d = path_join(&[dest, &name]);
            if ft.is_dir() {
                copy_dir_recursive(&s, &d).await?;
            } else {
                tokio::fs::copy(&s, &d).await?;
            }
        }
        Ok(())
    })
}

// --- host path helpers -------------------------------------------------------
//
// These address a real directory on disk (the source the skill is installed from and the
// destination under the skills root), so they follow the host dialect. A POSIX-only copy
// used to live here, whose `basename` returned the entire `C:\\path\\to\\my-skill` string
// on Windows — the installer then had no usable skill name and installed nothing.

use crate::permission::bash::node_path::{basename, join as path_join, relative};

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command as StdCommand, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A uniquely-named scratch directory that is removed on drop.
    struct Tmp {
        path: std::path::PathBuf,
    }

    impl Tmp {
        fn new(prefix: &str) -> Self {
            let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let uniq = format!(
                "{prefix}-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                n
            );
            let path = std::env::temp_dir().join(uniq);
            std::fs::create_dir_all(&path).unwrap();
            Tmp { path }
        }
        fn path(&self) -> &std::path::Path {
            &self.path
        }
        fn str(&self) -> String {
            self.path.to_string_lossy().into_owned()
        }
    }

    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn join_str(p: &std::path::Path, child: &str) -> String {
        p.join(child).to_string_lossy().into_owned()
    }

    /// Write a `<parent>/<name>/SKILL.md` with the given frontmatter name + body.
    fn write_skill(parent: &std::path::Path, dir_name: &str, fm_name: &str) -> std::path::PathBuf {
        let dir = parent.join(dir_name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("SKILL.md"),
            format!("---\nname: {fm_name}\ndescription: a test skill\n---\n\nDo the thing.\n"),
        )
        .unwrap();
        dir
    }

    fn git(args: &[&str], cwd: &std::path::Path) {
        StdCommand::new("git")
            .args(args)
            .current_dir(cwd)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("git runs");
    }

    fn tar(args: &[&str]) {
        StdCommand::new("tar")
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("tar runs");
    }

    // ---- parse_skill_source ----------------------------------------------------

    #[test]
    fn classifies_github_owner_repo_tree_git_and_local() {
        assert_eq!(
            parse_skill_source("openai/skills"),
            SkillSource::Github {
                owner: "openai".into(),
                repo: "skills".into(),
                git_ref: None,
                subpath: None,
            }
        );
        assert_eq!(
            parse_skill_source("https://github.com/openai/skills/tree/main/skills/.curated/pdf"),
            SkillSource::Github {
                owner: "openai".into(),
                repo: "skills".into(),
                git_ref: Some("main".into()),
                subpath: Some("skills/.curated/pdf".into()),
            }
        );
        assert_eq!(
            parse_skill_source("https://example.com/x/y.git"),
            SkillSource::Git {
                url: "https://example.com/x/y.git".into(),
                git_ref: None,
                subpath: None,
            }
        );
        assert_eq!(
            parse_skill_source("./local/thing"),
            SkillSource::Local {
                path: "./local/thing".into(),
            }
        );
    }

    #[test]
    fn classifies_npm_and_tarball_and_not_git() {
        assert_eq!(
            parse_skill_source("npm:@scope/pkg"),
            SkillSource::Npm {
                pkg: "@scope/pkg".into(),
                subpath: None,
            }
        );
        // A tarball URL must NOT be classified as git.
        assert_eq!(
            parse_skill_source("https://host/x/y.tar.gz"),
            SkillSource::Tarball {
                src: "https://host/x/y.tar.gz".into(),
                subpath: None,
            }
        );
        assert_eq!(
            parse_skill_source("./bundle.tgz"),
            SkillSource::Tarball {
                src: "./bundle.tgz".into(),
                subpath: None,
            }
        );
    }

    // ---- install_skill ---------------------------------------------------------

    #[tokio::test]
    async fn installs_from_local_dir_named_by_basename() {
        let process = crate::runtime_process::test_runtime_process_context();
        let src = Tmp::new("ae-src");
        write_skill(src.path(), "myskill", "My Display Name");
        let home = Tmp::new("ae-home");

        let skill = install_skill(
            &process,
            join_str(src.path(), "myskill"),
            InstallOptions {
                dir: Some(home.str()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert!(skill.is_some());
        // Directory name = basename of the source path.
        assert!(home.path().join("myskill").join("SKILL.md").exists());
        // Display name still comes from frontmatter.
        assert_eq!(skill.unwrap().name, "My Display Name");
    }

    #[tokio::test]
    async fn installs_a_subdir_out_of_a_git_repo() {
        let _external = crate::test_support::external_process_lock().await;
        let process = crate::runtime_process::test_runtime_process_context();
        let repo = Tmp::new("ae-repo");
        // create_dir_all is recursive, so this creates repo/pkgs/foo/SKILL.md.
        write_skill(&repo.path().join("pkgs"), "foo", "Foo Skill");
        git(&["init"], repo.path());
        git(
            &["-c", "user.email=t@t", "-c", "user.name=t", "add", "."],
            repo.path(),
        );
        git(
            &[
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-m",
                "init",
            ],
            repo.path(),
        );
        let home = Tmp::new("ae-home");

        let skill = install_skill(
            &process,
            SkillSource::Git {
                url: format!("file://{}", repo.str()),
                git_ref: None,
                subpath: None,
            },
            InstallOptions {
                subpath: Some("pkgs/foo".into()),
                dir: Some(home.str()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert!(skill.is_some());
        assert!(home.path().join("foo").join("SKILL.md").exists());
    }

    #[tokio::test]
    async fn installs_from_a_local_tar_gz_descending_into_the_wrapper_dir() {
        let _external = crate::test_support::external_process_lock().await;
        let process = crate::runtime_process::test_runtime_process_context();
        // Pack a tarball `bundle.tgz` whose sole top entry is the wrapper dir `inner/`.
        let work = Tmp::new("ae-pack");
        write_skill(work.path(), "inner", "Tar Skill");
        let tgz = work.path().join("bundle.tgz");
        tar(&[
            "-czf",
            tgz.to_string_lossy().as_ref(),
            "-C",
            work.str().as_str(),
            "inner",
        ]);
        let home = Tmp::new("ae-home");

        let skill = install_skill(
            &process,
            tgz.to_string_lossy().into_owned(),
            InstallOptions {
                dir: Some(home.str()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert!(skill.is_some());
        assert_eq!(skill.unwrap().name, "Tar Skill"); // display name from frontmatter
                                                      // Dir name comes from the archive filename ("bundle"), not the wrapper dir.
        assert!(home.path().join("bundle").join("SKILL.md").exists());
    }

    #[tokio::test]
    async fn refuses_to_overwrite_an_existing_skill_unless_force_is_set() {
        let process = crate::runtime_process::test_runtime_process_context();
        let src = Tmp::new("ae-src");
        write_skill(src.path(), "dup", "Dup");
        let home = Tmp::new("ae-home");
        let dup = join_str(src.path(), "dup");

        assert!(install_skill(
            &process,
            dup.clone(),
            InstallOptions {
                dir: Some(home.str()),
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_some());
        // Drop a marker into the installed dir to detect whether it gets replaced.
        std::fs::write(home.path().join("dup").join("MARKER"), "x").unwrap();

        // No force → rejected, marker survives.
        assert!(install_skill(
            &process,
            dup.clone(),
            InstallOptions {
                dir: Some(home.str()),
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_none());
        assert!(home.path().join("dup").join("MARKER").exists());

        // Force → replaces the dir (marker gone).
        assert!(install_skill(
            &process,
            dup.clone(),
            InstallOptions {
                dir: Some(home.str()),
                force: true,
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_some());
        assert!(!home.path().join("dup").join("MARKER").exists());
    }

    #[tokio::test]
    async fn rejects_a_subpath_that_escapes_the_source() {
        let process = crate::runtime_process::test_runtime_process_context();
        let src = Tmp::new("ae-src");
        write_skill(src.path(), "real", "Real");
        let home = Tmp::new("ae-home");

        let skill = install_skill(
            &process,
            src.str(),
            InstallOptions {
                subpath: Some("../real".into()),
                dir: Some(home.str()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(skill.is_none());
    }

    #[tokio::test]
    async fn rejects_an_unsafe_explicit_name() {
        let process = crate::runtime_process::test_runtime_process_context();
        let src = Tmp::new("ae-src");
        write_skill(src.path(), "ok", "Ok");
        let home = Tmp::new("ae-home");
        let ok = join_str(src.path(), "ok");

        assert!(install_skill(
            &process,
            ok.clone(),
            InstallOptions {
                name: Some("a/b".into()),
                dir: Some(home.str()),
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_none());
        assert!(install_skill(
            &process,
            ok.clone(),
            InstallOptions {
                name: Some("..".into()),
                dir: Some(home.str()),
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_none());
        assert!(install_skill(
            &process,
            ok.clone(),
            InstallOptions {
                name: Some(".hidden".into()),
                dir: Some(home.str()),
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_none());
    }

    #[tokio::test]
    async fn rejects_a_source_with_no_skill_md_nothing_lands() {
        let process = crate::runtime_process::test_runtime_process_context();
        let src = Tmp::new("ae-src");
        std::fs::create_dir_all(src.path().join("empty")).unwrap();
        let home = Tmp::new("ae-home");

        let skill = install_skill(
            &process,
            join_str(src.path(), "empty"),
            InstallOptions {
                dir: Some(home.str()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(skill.is_none());
        assert!(!home.path().join("empty").exists());
    }
}
