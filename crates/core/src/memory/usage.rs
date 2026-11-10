//! Recall-usage ledger for a persistent memory directory.
//!
//! A small JSON ledger (`.memory-usage.json`, a dot-file so ordinary memory
//! scans never treat it as a topic note) records how often each topic file has
//! been surfaced into a live turn, and when that last happened. Consolidation
//! reads these statistics to separate proven core memories from cold notes
//! that have not been consulted in a long time, so pruning decisions are
//! evidence-based instead of guesswork.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// Ledger filename inside the memory directory.
pub const LEDGER_FILE: &str = ".memory-usage.json";
/// A topic file not recalled within this window is a stale candidate that the
/// consolidation pass should re-examine against current evidence.
pub const STALE_AFTER_MS: i64 = 30 * 24 * 60 * 60 * 1000;

/// One topic file's recall statistics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UsageEntry {
    #[serde(rename = "usageCount", default)]
    pub usage_count: u64,
    #[serde(rename = "lastUsedAt", default)]
    pub last_used_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Ledger {
    #[serde(default = "ledger_version")]
    version: u32,
    #[serde(default)]
    entries: BTreeMap<String, UsageEntry>,
}

fn ledger_version() -> u32 {
    1
}

fn ledger_path(dir: &str) -> std::path::PathBuf {
    Path::new(dir).join(LEDGER_FILE)
}

/// Load the ledger; any read/parse failure yields an empty map (the ledger is
/// best-effort statistics, never a hard dependency).
pub async fn load_ledger(dir: &str) -> BTreeMap<String, UsageEntry> {
    let raw = match tokio::fs::read(ledger_path(dir)).await {
        Ok(raw) => raw,
        Err(_) => return BTreeMap::new(),
    };
    serde_json::from_slice::<Ledger>(&raw)
        .map(|ledger| ledger.entries)
        .unwrap_or_default()
}

async fn save_ledger(dir: &str, entries: BTreeMap<String, UsageEntry>) -> std::io::Result<()> {
    let ledger = Ledger {
        version: 1,
        entries,
    };
    let raw = serde_json::to_vec_pretty(&ledger)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let path = ledger_path(dir);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension("json.tmp");
    tokio::fs::write(&tmp, &raw).await?;
    tokio::fs::rename(&tmp, &path).await
}

/// Record one recall hit per stem (filename without extension). Entries whose
/// topic file no longer exists are pruned on write so the ledger cannot grow
/// without bound across renames and deletions.
pub async fn record_usage(dir: &str, stems: &[String], now_ms: i64) {
    if stems.is_empty() {
        return;
    }
    let mut entries = load_ledger(dir).await;
    for stem in stems {
        if stem.is_empty() || stem == "MEMORY" {
            continue;
        }
        let entry = entries.entry(stem.clone()).or_default();
        entry.usage_count = entry.usage_count.saturating_add(1);
        entry.last_used_at = now_ms;
    }
    // Prune orphaned entries whose topic file is gone.
    entries.retain(|stem, _| {
        if !Path::new(dir).join(format!("{stem}.md")).exists() {
            return false;
        }
        true
    });
    let _ = save_ledger(dir, entries).await;
}

fn humanize_age(now_ms: i64, then_ms: i64) -> String {
    if then_ms <= 0 {
        return "never".to_string();
    }
    let delta = now_ms.saturating_sub(then_ms);
    let minutes = delta / 60_000;
    if minutes < 1 {
        return "just now".to_string();
    }
    if minutes < 60 {
        return format!("{minutes} min ago");
    }
    let hours = minutes / 60;
    if hours < 24 {
        return format!("{hours} h ago");
    }
    let days = hours / 24;
    format!("{days} days ago")
}

/// Build the usage/staleness report the consolidation pass receives. Lists
/// every top-level topic file (excluding the index) with its recall count,
/// last recall, and modification age; files not recalled within
/// [`STALE_AFTER_MS`] and not recently modified are flagged as stale
/// candidates. Returns an empty string when there are no topic files.
pub async fn build_health_report(dir: &str, now_ms: i64) -> String {
    let entries = load_ledger(dir).await;
    let mut rows: Vec<String> = Vec::new();
    let mut stale: Vec<String> = Vec::new();

    let mut read_dir = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return String::new(),
    };
    let mut files: Vec<(String, i64)> = Vec::new();
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".md") || name.starts_with('.') || name == "MEMORY.md" {
            continue;
        }
        let mtime_ms = entry
            .metadata()
            .await
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let stem = name.trim_end_matches(".md").to_string();
        files.push((stem, mtime_ms));
    }
    files.sort();

    for (stem, mtime_ms) in &files {
        let entry = entries.get(stem);
        let usage_count = entry.map(|e| e.usage_count).unwrap_or(0);
        let last_used_at = entry.map(|e| e.last_used_at).unwrap_or(0);
        let is_stale = now_ms.saturating_sub(last_used_at.max(*mtime_ms)) >= STALE_AFTER_MS;
        let mut row = format!(
            "| {}.md | {} | {} | {} |",
            stem,
            usage_count,
            humanize_age(now_ms, last_used_at),
            humanize_age(now_ms, *mtime_ms)
        );
        if is_stale {
            row.push_str(" <- STALE CANDIDATE");
            stale.push(format!("{stem}.md"));
        }
        rows.push(row);
    }

    if rows.is_empty() {
        return String::new();
    }

    let mut report = String::from(
        "## Memory usage & staleness report\n\n\
Recall statistics for the topic files in this memory directory, gathered from live turns:\n\
- High-recall files are proven core memories: preserve their substance and keep their wording searchable.\n\
- Files marked STALE CANDIDATE have not been recalled for 30+ days (or never) and were not modified recently. \
Verify each against current evidence: delete or rewrite it when its content is superseded or obsolete, but do \
not delete a durable constraint merely because it is old and quiet.\n\n\
| file | recalls | last recall | modified |\n\
|---|---|---|---|\n",
    );
    for row in &rows {
        report.push_str(row);
        report.push('\n');
    }
    if !stale.is_empty() {
        report.push_str(&format!(
            "\nStale candidates this pass: {}\n",
            stale.join(", ")
        ));
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "klaus-memory-usage-{tag}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn record_usage_accumulates_counts_and_prunes_orphans() {
        let dir = temp_dir("record");
        let dir_s = dir.to_string_lossy().into_owned();
        tokio::fs::write(dir.join("alpha.md"), "# alpha")
            .await
            .unwrap();
        tokio::fs::write(dir.join("beta.md"), "# beta")
            .await
            .unwrap();

        record_usage(&dir_s, &["alpha".to_string()], 1_000).await;
        record_usage(&dir_s, &["alpha".to_string(), "beta".to_string()], 2_000).await;
        let entries = load_ledger(&dir_s).await;
        assert_eq!(entries["alpha"].usage_count, 2);
        assert_eq!(entries["alpha"].last_used_at, 2_000);
        assert_eq!(entries["beta"].usage_count, 1);

        // Deleting the topic file prunes its entry on the next write.
        tokio::fs::remove_file(dir.join("beta.md")).await.unwrap();
        record_usage(&dir_s, &["alpha".to_string()], 3_000).await;
        let entries = load_ledger(&dir_s).await;
        assert!(!entries.contains_key("beta"));
        assert_eq!(entries["alpha"].usage_count, 3);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn health_report_flags_stale_candidates_and_skips_index() {
        let dir = temp_dir("report");
        let dir_s = dir.to_string_lossy().into_owned();
        tokio::fs::write(dir.join("MEMORY.md"), "# index")
            .await
            .unwrap();
        tokio::fs::write(dir.join("core.md"), "# core")
            .await
            .unwrap();
        tokio::fs::write(dir.join("oldnote.md"), "# old")
            .await
            .unwrap();
        let now = 100 * 24 * 60 * 60 * 1000_i64;
        // core.md was recalled recently; oldnote.md never was.
        record_usage(&dir_s, &["core".to_string()], now - 60_000).await;
        // Backdate both files' ledger-independent mtime signal by using an old last_used for none.
        let report = build_health_report(&dir_s, now).await;
        assert!(report.contains("core.md"));
        assert!(report.contains("oldnote.md"));
        assert!(!report.contains("| MEMORY.md |"));
        assert!(report.contains("oldnote.md | 0 | never"), "{report}");
        // oldnote.md has a fresh mtime, so it is not stale yet on mtime alone;
        // backdate the file to trigger the flag.
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(40 * 24 * 3600);
        std::fs::OpenOptions::new()
            .write(true)
            .open(dir.join("oldnote.md"))
            .unwrap()
            .set_modified(old)
            .unwrap();
        let report = build_health_report(&dir_s, now).await;
        assert!(report.contains("STALE CANDIDATE"), "{report}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn missing_ledger_is_not_an_error() {
        let dir = temp_dir("missing");
        let dir_s = dir.to_string_lossy().into_owned();
        assert!(load_ledger(&dir_s).await.is_empty());
        assert_eq!(build_health_report(&dir_s, 1).await, "");
        std::fs::remove_dir_all(&dir).ok();
    }
}
