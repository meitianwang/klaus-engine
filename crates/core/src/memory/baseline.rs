//! Snapshot baseline and change report for a persistent memory directory.
//!
//! After each successful consolidation the engine snapshots the memory files
//! under `.dream/baseline`. Before the next consolidation it diffs that
//! baseline against the live directory and writes a bounded, git-style report
//! to `.dream/workspace_diff.md` so the consolidation pass can spend its review
//! budget on what actually changed (added, modified, deleted notes) instead of
//! re-scanning every file from scratch. Everything lives under the `.dream`
//! dot-directory so ordinary memory scans never treat these artifacts as topic
//! notes, and the engine (not the restricted fork) owns all writes here.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Hidden state directory inside the memory root.
pub const DREAM_DIR: &str = ".dream";
/// Baseline snapshot directory inside [`DREAM_DIR`].
pub const BASELINE_DIR: &str = "baseline";
/// Generated change-report filename inside [`DREAM_DIR`].
pub const WORKSPACE_DIFF_FILE: &str = "workspace_diff.md";

/// Total byte bound for the rendered report.
const MAX_DIFF_TOTAL_BYTES: usize = 60_000;
/// Per-file bound on rendered diff lines.
const MAX_DIFF_LINES_PER_FILE: usize = 200;
/// Files longer than this (either side) skip the inline line diff and report
/// sizes only; the LCS table stays bounded.
const MAX_DIFF_FILE_LINES: usize = 1_500;

fn dream_dir(dir: &str) -> PathBuf {
    Path::new(dir).join(DREAM_DIR)
}

fn baseline_dir(dir: &str) -> PathBuf {
    dream_dir(dir).join(BASELINE_DIR)
}

fn diff_path(dir: &str) -> PathBuf {
    dream_dir(dir).join(WORKSPACE_DIFF_FILE)
}

/// Collect every regular file under `root` keyed by relative POSIX-style path,
/// skipping dot-files and dot-directories at every level (locks, ledgers, the
/// dream state itself).
fn collect_files(
    root: &Path,
    prefix: &str,
    out: &mut BTreeMap<String, PathBuf>,
) -> std::io::Result<()> {
    let read_dir = match std::fs::read_dir(root) {
        Ok(rd) => rd,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    for entry in read_dir {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let relative = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}/{name}")
        };
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_files(&path, &relative, out)?;
        } else if file_type.is_file() {
            out.insert(relative, path);
        }
    }
    Ok(())
}

fn read_lines(path: &Path) -> Vec<String> {
    match std::fs::read_to_string(path) {
        Ok(text) => text.lines().map(str::to_string).collect(),
        // Binary-ish or unreadable: treat as one opaque marker line so the
        // report still lists the change without inlining content.
        Err(_) => vec!["(unreadable as text)".to_string()],
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tag {
    Equal,
    Delete,
    Insert,
}

/// LCS-based line edit script. Bounded by [`MAX_DIFF_FILE_LINES`] on both sides.
fn line_diff(old: &[String], new: &[String]) -> Vec<(Tag, String)> {
    let n = old.len();
    let m = new.len();
    // dp[i][j] = LCS length of old[i..] and new[j..]
    let mut dp = vec![vec![0u32; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            dp[i][j] = if old[i] == new[j] {
                dp[i + 1][j + 1] + 1
            } else {
                dp[i + 1][j].max(dp[i][j + 1])
            };
        }
    }
    let mut script = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < n && j < m {
        if old[i] == new[j] {
            script.push((Tag::Equal, old[i].clone()));
            i += 1;
            j += 1;
        } else if dp[i + 1][j] >= dp[i][j + 1] {
            script.push((Tag::Delete, old[i].clone()));
            i += 1;
        } else {
            script.push((Tag::Insert, new[j].clone()));
            j += 1;
        }
    }
    while i < n {
        script.push((Tag::Delete, old[i].clone()));
        i += 1;
    }
    while j < m {
        script.push((Tag::Insert, new[j].clone()));
        j += 1;
    }
    script
}

/// Render the script as unified-style hunks with 3 lines of context.
fn render_hunks(script: &[(Tag, String)]) -> Vec<String> {
    const CONTEXT: usize = 3;
    let mut out = Vec::new();
    let mut old_line = 1usize;
    let mut new_line = 1usize;

    let mut idx = 0usize;
    while idx < script.len() {
        if script[idx].0 == Tag::Equal {
            old_line += 1;
            new_line += 1;
            idx += 1;
            continue;
        }
        // Start of a change run: include up to CONTEXT equal lines before.
        let hunk_start_old = old_line.saturating_sub(CONTEXT.min(old_line - 1));
        let hunk_start_new = new_line.saturating_sub(CONTEXT.min(new_line - 1));
        let mut hunk: Vec<String> = Vec::new();
        let mut back = idx;
        let mut taken = 0usize;
        while back > 0 && taken < CONTEXT && script[back - 1].0 == Tag::Equal {
            back -= 1;
            taken += 1;
        }
        for (_, line) in &script[back..idx] {
            hunk.push(format!(" {}", line));
        }
        let mut old_count = taken;
        let mut new_count = taken;
        let mut k = idx;
        let mut trailing_equal = 0usize;
        while k < script.len() {
            match script[k].0 {
                Tag::Delete => {
                    hunk.push(format!("-{}", script[k].1));
                    old_count += 1;
                    old_line += 1;
                    trailing_equal = 0;
                }
                Tag::Insert => {
                    hunk.push(format!("+{}", script[k].1));
                    new_count += 1;
                    new_line += 1;
                    trailing_equal = 0;
                }
                Tag::Equal => {
                    trailing_equal += 1;
                    if trailing_equal > CONTEXT * 2 {
                        break;
                    }
                    hunk.push(format!(" {}", script[k].1));
                    old_count += 1;
                    new_count += 1;
                    old_line += 1;
                    new_line += 1;
                }
            }
            k += 1;
        }
        // Trim trailing context beyond CONTEXT lines after the last change.
        let last_change = hunk
            .iter()
            .rposition(|line| line.starts_with('-') || line.starts_with('+'))
            .unwrap_or(hunk.len().saturating_sub(1));
        let keep = (last_change + 1 + CONTEXT).min(hunk.len());
        let dropped = hunk.len() - keep;
        hunk.truncate(keep);
        // Rewind counters for dropped trailing context so the next hunk starts right.
        for _ in 0..dropped {
            old_line = old_line.saturating_sub(1);
            new_line = new_line.saturating_sub(1);
            old_count = old_count.saturating_sub(1);
            new_count = new_count.saturating_sub(1);
        }
        out.push(format!(
            "@@ -{hunk_start_old},{old_count} +{hunk_start_new},{new_count} @@"
        ));
        out.extend(hunk);
        idx = k.saturating_sub(dropped).max(idx + 1);
    }
    out
}

fn push_bounded(sink: &mut String, lines: &[String]) {
    for line in lines {
        if sink.len() + line.len() + 1 > MAX_DIFF_TOTAL_BYTES {
            sink.push_str("\n(report truncated at the total size bound)\n");
            return;
        }
        sink.push_str(line);
        sink.push('\n');
    }
}

/// Render the full change report between `baseline` and `current` file maps.
fn render_report(
    current: &BTreeMap<String, PathBuf>,
    baseline: &BTreeMap<String, PathBuf>,
) -> String {
    let mut added: Vec<&String> = Vec::new();
    let mut modified: Vec<&String> = Vec::new();
    let mut deleted: Vec<&String> = Vec::new();

    for (relative, path) in current {
        match baseline.get(relative) {
            None => added.push(relative),
            Some(base_path) => {
                let a = std::fs::read(path).unwrap_or_default();
                let b = std::fs::read(base_path).unwrap_or_default();
                if a != b {
                    modified.push(relative);
                }
            }
        }
    }
    for relative in baseline.keys() {
        if !current.contains_key(relative) {
            deleted.push(relative);
        }
    }

    let mut report = String::from(
        "# Memory Workspace Diff\n\n\
Generated before consolidation. Read this file first and do not edit it.\n\n\
## Status\n",
    );
    if added.is_empty() && modified.is_empty() && deleted.is_empty() {
        report.push_str("- none\n");
        return report;
    }
    for relative in &added {
        report.push_str(&format!("- added {relative}\n"));
    }
    for relative in &modified {
        report.push_str(&format!("- modified {relative}\n"));
    }
    for relative in &deleted {
        report.push_str(&format!("- deleted {relative}\n"));
    }
    report.push_str("\n## Diff\n\n```diff\n");

    for relative in &added {
        let Some(path) = current.get(*relative) else {
            continue;
        };
        let lines = read_lines(path);
        report.push_str(&format!("--- /dev/null\n+++ b/{relative}\n"));
        let capped: Vec<String> = lines
            .iter()
            .take(MAX_DIFF_LINES_PER_FILE)
            .map(|line| format!("+{line}"))
            .collect();
        push_bounded(&mut report, &capped);
        if lines.len() > MAX_DIFF_LINES_PER_FILE {
            report.push_str(&format!(
                "+(... {} more lines truncated ...)\n",
                lines.len() - MAX_DIFF_LINES_PER_FILE
            ));
        }
    }
    for relative in &modified {
        let (Some(cur_path), Some(base_path)) = (current.get(*relative), baseline.get(*relative))
        else {
            continue;
        };
        let new_lines = read_lines(cur_path);
        let old_lines = read_lines(base_path);
        report.push_str(&format!("--- a/{relative}\n+++ b/{relative}\n"));
        if old_lines.len() > MAX_DIFF_FILE_LINES || new_lines.len() > MAX_DIFF_FILE_LINES {
            report.push_str(&format!(
                "(file too large for an inline diff: {} -> {} lines)\n",
                old_lines.len(),
                new_lines.len()
            ));
            continue;
        }
        let script = line_diff(&old_lines, &new_lines);
        let mut hunks = render_hunks(&script);
        if hunks.len() > MAX_DIFF_LINES_PER_FILE {
            hunks.truncate(MAX_DIFF_LINES_PER_FILE);
            hunks.push("(... diff truncated ...)".to_string());
        }
        push_bounded(&mut report, &hunks);
    }
    for relative in &deleted {
        let Some(path) = baseline.get(*relative) else {
            continue;
        };
        let lines = read_lines(path);
        report.push_str(&format!("--- a/{relative}\n+++ /dev/null\n"));
        let capped: Vec<String> = lines
            .iter()
            .take(MAX_DIFF_LINES_PER_FILE)
            .map(|line| format!("-{line}"))
            .collect();
        push_bounded(&mut report, &capped);
        if lines.len() > MAX_DIFF_LINES_PER_FILE {
            report.push_str(&format!(
                "-(... {} more lines truncated ...)\n",
                lines.len() - MAX_DIFF_LINES_PER_FILE
            ));
        }
    }
    report.push_str("```\n");
    report
}

/// Compute the baseline-vs-live change report and write it to
/// `.dream/workspace_diff.md`. A missing baseline (first consolidation) yields
/// a report listing every current file as added.
pub async fn prepare_workspace_diff(dir: &str) -> std::io::Result<()> {
    tokio::fs::create_dir_all(dream_dir(dir)).await?;
    let mut current = BTreeMap::new();
    collect_files(Path::new(dir), "", &mut current)?;
    let mut baseline = BTreeMap::new();
    collect_files(&baseline_dir(dir), "", &mut baseline)?;
    let report = render_report(&current, &baseline);
    let tmp = diff_path(dir).with_extension("md.tmp");
    tokio::fs::write(&tmp, report.as_bytes()).await?;
    tokio::fs::rename(&tmp, diff_path(dir)).await
}

/// Replace the baseline snapshot with the current live memory files. Called
/// only after a consolidation pass succeeded, so the next report covers just
/// the changes that arrive after it.
pub async fn commit_baseline(dir: &str) -> std::io::Result<()> {
    let baseline = baseline_dir(dir);
    match tokio::fs::remove_dir_all(&baseline).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    tokio::fs::create_dir_all(&baseline).await?;
    let mut current = BTreeMap::new();
    collect_files(Path::new(dir), "", &mut current)?;
    for (relative, path) in current {
        let target = baseline.join(&relative);
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::copy(path, target).await?;
    }
    Ok(())
}

/// Remove the generated change report (it is a prompt artifact, not memory).
pub async fn remove_workspace_diff(dir: &str) -> std::io::Result<()> {
    match tokio::fs::remove_file(diff_path(dir)).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "klaus-memory-baseline-{tag}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn first_diff_lists_every_file_as_added() {
        let dir = temp_dir("first");
        let dir_s = dir.to_string_lossy().into_owned();
        std::fs::write(dir.join("MEMORY.md"), "# index\n- a\n").unwrap();
        std::fs::write(dir.join("alpha.md"), "line one\nline two\n").unwrap();
        prepare_workspace_diff(&dir_s).await.unwrap();
        let report = std::fs::read_to_string(diff_path(&dir_s)).unwrap();
        assert!(report.contains("- added MEMORY.md"), "{report}");
        assert!(report.contains("- added alpha.md"), "{report}");
        assert!(report.contains("+line one"), "{report}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn diff_reports_added_modified_deleted_after_baseline_commit() {
        let dir = temp_dir("changes");
        let dir_s = dir.to_string_lossy().into_owned();
        std::fs::write(dir.join("keep.md"), "same\n").unwrap();
        std::fs::write(dir.join("edit.md"), "old line\ncontext\n").unwrap();
        std::fs::write(dir.join("gone.md"), "deleted content\n").unwrap();
        commit_baseline(&dir_s).await.unwrap();

        // Live changes since the baseline.
        std::fs::write(dir.join("edit.md"), "new line\ncontext\n").unwrap();
        std::fs::remove_file(dir.join("gone.md")).unwrap();
        std::fs::write(dir.join("fresh.md"), "brand new\n").unwrap();

        prepare_workspace_diff(&dir_s).await.unwrap();
        let report = std::fs::read_to_string(diff_path(&dir_s)).unwrap();
        assert!(report.contains("- modified edit.md"), "{report}");
        assert!(report.contains("- deleted gone.md"), "{report}");
        assert!(report.contains("- added fresh.md"), "{report}");
        assert!(report.contains("-old line"), "{report}");
        assert!(report.contains("+new line"), "{report}");
        assert!(!report.contains("- modified keep.md"), "{report}");

        // After committing the new baseline the next report is clean.
        commit_baseline(&dir_s).await.unwrap();
        prepare_workspace_diff(&dir_s).await.unwrap();
        let report = std::fs::read_to_string(diff_path(&dir_s)).unwrap();
        assert!(report.contains("## Status\n- none"), "{report}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn dot_files_and_ledger_are_excluded_from_diff_and_baseline() {
        let dir = temp_dir("dots");
        let dir_s = dir.to_string_lossy().into_owned();
        std::fs::write(dir.join("topic.md"), "content\n").unwrap();
        std::fs::write(dir.join(".memory-usage.json"), "{}").unwrap();
        std::fs::write(dir.join(".consolidate-lock"), "").unwrap();
        commit_baseline(&dir_s).await.unwrap();
        prepare_workspace_diff(&dir_s).await.unwrap();
        let report = std::fs::read_to_string(diff_path(&dir_s)).unwrap();
        assert!(!report.contains("memory-usage"), "{report}");
        assert!(!report.contains("consolidate-lock"), "{report}");
        assert!(!baseline_dir(&dir_s).join(".memory-usage.json").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn remove_workspace_diff_tolerates_absence() {
        let dir = temp_dir("remove");
        let dir_s = dir.to_string_lossy().into_owned();
        remove_workspace_diff(&dir_s).await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }
}
