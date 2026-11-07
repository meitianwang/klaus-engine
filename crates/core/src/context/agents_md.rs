//! Project guidance discovery: walk from the working directory up to the
//! filesystem root, collecting per-directory `AGENTS.md` (falling back to
//! `KLAUS.md`) guidance files for the system prompt.

use crate::fs::node_path::{
    dirname_runtime, host_path_dialect, project_runtime_path, resolve_runtime,
    runtime_path_from_native,
};
use crate::runtime_process::path_join;
use crate::runtime_values::{NodeBufferUtf8, RuntimePath, RuntimeString};

/// Options for [`load_agents_md`].
#[derive(Debug, Clone, Default)]
pub struct LoadAgentsMdOptions {
    /// Candidate file names, tried in order within each directory. Defaults to
    /// `AGENTS.md` then `KLAUS.md`.
    pub names: Option<Vec<String>>,
    /// Size budget for the combined output, counted in UTF-16 code units.
    /// Defaults to 32 KiB.
    pub max_bytes: Option<usize>,
}

/// Walk from cwd up to the filesystem root collecting AGENTS.md (falling back to
/// KLAUS.md) from each directory. Ordered root-first so the nearest directory's
/// guidance lands last (closest to the end of the prompt).
pub async fn load_agents_md(cwd: &RuntimePath, opts: LoadAgentsMdOptions) -> Option<RuntimeString> {
    let names = opts
        .names
        .unwrap_or_else(|| vec!["AGENTS.md".to_string(), "KLAUS.md".to_string()]);
    let max_bytes = opts.max_bytes.unwrap_or(32 * 1024);

    let process_cwd = std::env::current_dir()
        .map(|path| runtime_path_from_native(&path))
        .unwrap_or_else(|_| RuntimePath::from("."));
    let mut cur = resolve_runtime(&process_cwd, std::slice::from_ref(cwd)).ok()?;
    let mut dirs: Vec<RuntimePath> = Vec::new();
    loop {
        dirs.push(cur.clone());
        let parent = dirname_runtime(&cur);
        if parent == cur {
            break;
        }
        cur = parent;
    }
    dirs.reverse();

    let mut chunks: Vec<RuntimeString> = Vec::new();
    let mut total: usize = 0;
    for d in &dirs {
        for name in &names {
            let path = path_join(
                host_path_dialect(),
                &[d.clone(), RuntimePath::from(name.as_str())],
            );
            // Decode lossily (invalid byte sequences become U+FFFD); a missing
            // or unreadable candidate just isn't present in this dir.
            let Ok(native_path) = project_runtime_path(&path, "agents guidance path") else {
                continue;
            };
            if let Ok(bytes) = tokio::fs::read(native_path).await {
                let content = trim_runtime(&NodeBufferUtf8::decode(&bytes));
                if !content.is_empty() {
                    let piece = RuntimeString::from("# ")
                        .concat(path.as_runtime_string())
                        .concat(&RuntimeString::from("\n"))
                        .concat(&content);
                    total += piece.len_utf16();
                    chunks.push(piece);
                    break; // one file per directory
                }
            }
        }
        if total >= max_bytes {
            break;
        }
    }

    if chunks.is_empty() {
        return None;
    }
    let mut combined = runtime_join(chunks, "\n\n");
    if combined.len_utf16() > max_bytes {
        combined = combined
            .slice_utf16(..max_bytes)
            .concat(&RuntimeString::from("\n…[truncated]"));
    }
    Some(combined)
}

/// The whitespace set used when trimming file content: tab/VT/FF, line
/// terminators, Unicode space separators, and the zero-width no-break space —
/// so a stray byte-order mark at the top of a file counts as blank.
fn is_trimmable(c: char) -> bool {
    matches!(
        c,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

fn runtime_join(values: impl IntoIterator<Item = RuntimeString>, separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut values = values.into_iter();
    let Some(mut joined) = values.next() else {
        return RuntimeString::default();
    };
    for value in values {
        joined = joined.concat(&separator).concat(&value);
    }
    joined
}

fn trim_runtime(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let mut start = 0;
    while start < units.len() {
        let (point, width) = code_point_at(units, start);
        if !char::from_u32(point).is_some_and(is_trimmable) {
            break;
        }
        start += width;
    }

    let mut end = units.len();
    while end > start {
        let candidate = if end >= 2
            && (0xdc00..=0xdfff).contains(&units[end - 1])
            && (0xd800..=0xdbff).contains(&units[end - 2])
        {
            end - 2
        } else {
            end - 1
        };
        let (point, _) = code_point_at(units, candidate);
        if !char::from_u32(point).is_some_and(is_trimmable) {
            break;
        }
        end = candidate;
    }
    value.slice_utf16(start..end)
}

fn code_point_at(units: &[u16], index: usize) -> (u32, usize) {
    let high = units[index];
    if (0xd800..=0xdbff).contains(&high) {
        if let Some(low) = units
            .get(index + 1)
            .copied()
            .filter(|low| (0xdc00..=0xdfff).contains(low))
        {
            let point = 0x10000 + ((u32::from(high) - 0xd800) << 10) + (u32::from(low) - 0xdc00);
            return (point, 2);
        }
    }
    (u32::from(high), 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Process-unique suffix: the wall clock alone can collide across
    /// concurrently running tests, so mix in an atomic counter.
    fn uniq() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!(
            "{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!("ae-agentsmd-{}", uniq()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A file name no real ancestor directory could contain, so walks stay
    /// hermetic above the temp dir.
    fn unique_name(tag: &str) -> String {
        format!("AE-AGENTS-{tag}-{}.md", uniq())
    }

    fn opts(names: Vec<String>, max_bytes: Option<usize>) -> LoadAgentsMdOptions {
        LoadAgentsMdOptions {
            names: Some(names),
            max_bytes,
        }
    }

    /// The path exactly as the loader spells it in a chunk header.
    ///
    /// The loader joins with [`host_path_dialect`], so the separator is the host's.
    /// These tests used to write `format!("{dir}/{name}")`, which made every one of
    /// them assert a POSIX path and fail on Windows against a correct `\` result.
    fn header_path(dir: &std::path::Path, name: &str) -> String {
        dir.join(name).to_string_lossy().into_owned()
    }

    #[tokio::test]
    async fn returns_none_when_no_guidance_file_exists() {
        let dir = temp_dir();
        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            opts(vec![unique_name("none")], None),
        )
        .await;
        assert_eq!(out, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn root_only_walk_terminates_and_returns_none() {
        // cwd at the filesystem root: the walk visits exactly one dir and stops.
        let out = load_agents_md(
            &RuntimePath::from("/"),
            opts(vec![unique_name("root")], None),
        )
        .await;
        assert_eq!(out, None);
    }

    #[tokio::test]
    async fn collects_root_first_so_nearest_directory_lands_last() {
        let base = temp_dir();
        let leaf = base.join("mid").join("leaf");
        std::fs::create_dir_all(&leaf).unwrap();
        let name = unique_name("order");
        std::fs::write(base.join(&name), "outer guidance").unwrap();
        std::fs::write(leaf.join(&name), "inner guidance").unwrap();

        let out = load_agents_md(
            &RuntimePath::from(leaf.to_str().unwrap()),
            opts(vec![name.clone()], None),
        )
        .await;
        // Outer chunk first, inner last, joined by a blank line; `mid` (no
        // file) contributes nothing.
        let expected = format!(
            "# {}\nouter guidance\n\n# {}\ninner guidance",
            header_path(&base, &name),
            header_path(&leaf, &name),
        );
        assert_eq!(out, Some(expected.into()));
        std::fs::remove_dir_all(&base).ok();
    }

    #[tokio::test]
    async fn first_matching_name_wins_per_directory() {
        let dir = temp_dir();
        let n1 = unique_name("first");
        let n2 = unique_name("second");
        std::fs::write(dir.join(&n1), "primary").unwrap();
        std::fs::write(dir.join(&n2), "secondary").unwrap();

        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            opts(vec![n1.clone(), n2.clone()], None),
        )
        .await;
        assert_eq!(
            out,
            Some(format!("# {}\nprimary", header_path(&dir, &n1)).into())
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn blank_first_candidate_falls_back_to_second() {
        let dir = temp_dir();
        let n1 = unique_name("blank");
        let n2 = unique_name("fallback");
        std::fs::write(dir.join(&n1), "  \n\t ").unwrap();
        std::fs::write(dir.join(&n2), "fallback").unwrap();

        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            opts(vec![n1.clone(), n2.clone()], None),
        )
        .await;
        let d = header_path(&dir, &n2);
        assert_eq!(out, Some(format!("# {d}\nfallback").into()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn whitespace_only_file_yields_none() {
        let dir = temp_dir();
        let name = unique_name("ws");
        std::fs::write(dir.join(&name), " \n\t\r\n ").unwrap();
        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            opts(vec![name], None),
        )
        .await;
        assert_eq!(out, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn content_is_trimmed_and_a_leading_bom_is_stripped() {
        let dir = temp_dir();
        let name = unique_name("bom");
        let mut bytes = vec![0xEF, 0xBB, 0xBF]; // UTF-8 byte-order mark
        bytes.extend_from_slice("hello\n".as_bytes());
        std::fs::write(dir.join(&name), &bytes).unwrap();

        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            opts(vec![name.clone()], None),
        )
        .await;
        let d = header_path(&dir, &name);
        assert_eq!(out, Some(format!("# {d}\nhello").into()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn bom_only_file_counts_as_blank() {
        let dir = temp_dir();
        let name = unique_name("bomonly");
        std::fs::write(dir.join(&name), [0xEF, 0xBB, 0xBF]).unwrap();
        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            opts(vec![name], None),
        )
        .await;
        assert_eq!(out, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn invalid_utf8_decodes_lossily() {
        let dir = temp_dir();
        let name = unique_name("lossy");
        std::fs::write(dir.join(&name), b"ok \xFF end").unwrap();

        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            opts(vec![name.clone()], None),
        )
        .await;
        let d = header_path(&dir, &name);
        assert_eq!(out, Some(format!("# {d}\nok \u{FFFD} end").into()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn directory_named_like_a_candidate_is_ignored() {
        let dir = temp_dir();
        let n1 = unique_name("isdir");
        let n2 = unique_name("real");
        std::fs::create_dir_all(dir.join(&n1)).unwrap();
        std::fs::write(dir.join(&n2), "real").unwrap();

        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            opts(vec![n1.clone(), n2.clone()], None),
        )
        .await;
        let d = header_path(&dir, &n2);
        assert_eq!(out, Some(format!("# {d}\nreal").into()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn truncates_combined_output_past_the_budget() {
        let dir = temp_dir();
        let name = unique_name("trunc");
        std::fs::write(dir.join(&name), "x".repeat(200)).unwrap();

        let d = header_path(&dir, &name);
        let full = format!("# {d}\n{}", "x".repeat(200));
        let max = 100usize;
        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            opts(vec![name.clone()], Some(max)),
        )
        .await;
        assert_eq!(
            out,
            Some(
                RuntimeString::from(full)
                    .slice_utf16(..max)
                    .concat(&RuntimeString::from("\n…[truncated]"))
            )
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn output_exactly_at_the_budget_is_not_truncated() {
        // Truncation only fires strictly past the budget.
        let dir = temp_dir();
        let name = unique_name("exact");
        std::fs::write(dir.join(&name), "abc").unwrap();

        let d = header_path(&dir, &name);
        let full = format!("# {d}\nabc");
        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            opts(
                vec![name.clone()],
                Some(RuntimeString::from(&full).len_utf16()),
            ),
        )
        .await;
        assert_eq!(out, Some(full.into()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn utf16_truncation_preserves_split_surrogates_and_parsed_lone_surrogates() {
        let astral_half = RuntimeString::from("😀x").slice_utf16(..1);
        assert_eq!(
            crate::runtime_values::runtime_json::stringify(
                &crate::runtime_values::RuntimeJson::String(astral_half)
            ),
            r#""\ud83d""#
        );

        let parsed =
            crate::runtime_values::runtime_json::parse(&RuntimeString::from(r#""\ud800x""#))
                .unwrap();
        let lone = parsed.as_str().unwrap().slice_utf16(..1);
        assert_eq!(
            crate::runtime_values::runtime_json::stringify(
                &crate::runtime_values::RuntimeJson::String(lone)
            ),
            r#""\ud800""#
        );
    }

    #[tokio::test]
    async fn stops_collecting_once_the_budget_is_reached() {
        let base = temp_dir();
        let leaf = base.join("leaf");
        std::fs::create_dir_all(&leaf).unwrap();
        let name = unique_name("budget");
        std::fs::write(base.join(&name), "A".repeat(300)).unwrap();
        std::fs::write(leaf.join(&name), "LEAF").unwrap();

        // The outer (root-side) chunk alone exhausts the budget, so the nearer
        // directory is never read.
        let out = load_agents_md(
            &RuntimePath::from(leaf.to_str().unwrap()),
            opts(vec![name.clone()], Some(200)),
        )
        .await
        .unwrap();
        let base_d = header_path(&base, &name);
        assert!(out.starts_with(&format!("# {base_d}\n")));
        assert!(out.ends_with("\n…[truncated]"));
        assert!(!out.contains("LEAF"));
        std::fs::remove_dir_all(&base).ok();
    }

    #[tokio::test]
    async fn default_names_prefer_agents_md_over_klaus_md() {
        let dir = temp_dir();
        std::fs::write(dir.join("AGENTS.md"), "agents guidance").unwrap();
        std::fs::write(dir.join("KLAUS.md"), "klaus guidance").unwrap();

        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            LoadAgentsMdOptions::default(),
        )
        .await
        .unwrap();
        let d = header_path(&dir, "AGENTS.md");
        // The nearest directory lands last regardless of what ancestors add.
        assert!(out.ends_with(&format!("# {d}\nagents guidance")));
        assert!(!out.contains("klaus guidance"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn default_names_fall_back_to_klaus_md() {
        let dir = temp_dir();
        std::fs::write(dir.join("KLAUS.md"), "klaus guidance").unwrap();

        let out = load_agents_md(
            &RuntimePath::from(dir.to_str().unwrap()),
            LoadAgentsMdOptions::default(),
        )
        .await
        .unwrap();
        let d = header_path(&dir, "KLAUS.md");
        assert!(out.ends_with(&format!("# {d}\nklaus guidance")));
        std::fs::remove_dir_all(&dir).ok();
    }
}
