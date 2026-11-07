//! The nested-memory attachments utility as a [`ReminderProvider`]. Each
//! turn it drains the per-agent trigger buffer and, for every touched file, computes the
//! contextual memory ([`get_nested_memory_for_file`]) and injects any file NOT already seen.
//!
//! Two-level dedup:
//!   1. `loaded_paths` contains the path — non-evicting cross-turn dedup (skip if injected
//!      before).
//!   2. `file_state.has(path)` — skip a file the agent already read/loaded (its content is in
//!      context).
//!
//! On inject, BOTH are updated: a `loaded_paths` insert + a mtime-only `file_state.record`.
//!
//! The render is a `nested_memory` attachment: `Contents of {path}:\n\n{content}` — no
//! per-tier description, no trim. Rather than wrapping EACH attachment in its own `<system-reminder>`, the
//! engine wraps the whole provider output in one, so the per-file blocks are joined with a blank
//! line inside that single reminder (content-identical, one wrapper).
//!
//! `file_state` marking (honest note): a fuller scheme records the RAW disk bytes with a partial-view
//! marker when the injected (frontmatter-stripped) body differs from disk. The engine's
//! `FileStateEntry` has no such marker, so recording stripped content would make the edited-files
//! reminder diff it as an out-of-band change. We instead record mtime-ONLY (no content): the
//! edited-files reminder skips content-less entries, so there's no spurious diff, while
//! read-before-write is still satisfied (the path is in `file_state`) and stale-write detection
//! still fires (a later external edit bumps the mtime). The only missing effect is
//! mid-session content-diffing of an injected memory file — which the engine's edited-files
//! reminder can't do for content-less entries anyway.

use crate::context::memory_files::MemoryGates;
use crate::context::nested_memory::{get_nested_memory_for_file, NestedMemoryOptions};
use crate::fs::node_path::{project_runtime_path, runtime_path_from_native};
use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::types::file_state::{FileStateCache, RecordOptions};
use crate::types::reminder::{ReminderContext, ReminderProvider};
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

/// Per-agent state + config the nested-memory reminder needs.
pub struct NestedMemoryReminderDeps {
    /// Drain (return AND clear) the per-agent buffer of file paths touched since the last turn.
    /// These trigger paths accumulate as files are touched (added by the Read tool) and are
    /// cleared after each drain. The engine feeds the buffer from the
    /// file-touch signal.
    pub drain_triggers: Box<dyn Fn() -> Vec<RuntimePath> + Send + Sync>,
    /// Per-agent, cross-turn dedup of already-injected memory paths: a NON-evicting
    /// set, so a memory file/rule injected once is
    /// never re-injected for this agent. Must be FRESH per fork (a fork independently re-surfaces
    /// memory it needs).
    pub loaded_paths: Arc<Mutex<HashSet<RuntimePath>>>,
    /// The agent's file-state cache — for the `has` dedup and to mark injected files as seen.
    pub file_state: Arc<dyn FileStateCache>,
    /// Per-tier gates (reused from the eager loader).
    pub gates: Option<MemoryGates>,
    /// User config home override (tests).
    pub user_config_dir: Option<RuntimePath>,
}

/// The nested-memory reminder provider. Hold ONE instance per agent.
pub struct NestedMemoryReminderProvider {
    deps: NestedMemoryReminderDeps,
}

/// The legacy nested-memory loader performs real filesystem discovery and
/// still accepts a scalar host path. Keep the logical path exact until this
/// immediate Node-compatible filesystem projection boundary.
fn nested_memory_filesystem_host_path(path: &RuntimePath, label: &str) -> Option<String> {
    let native = project_runtime_path(path, label).ok()?;
    let projected = runtime_path_from_native(&native);
    String::from_utf8(projected.as_runtime_string().to_node_utf8()).ok()
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

#[async_trait]
impl ReminderProvider for NestedMemoryReminderProvider {
    async fn reminder(&self, ctx: &ReminderContext) -> Option<RuntimeString> {
        let triggers = (self.deps.drain_triggers)();
        if triggers.is_empty() {
            return None;
        }

        let mut blocks: Vec<RuntimeString> = Vec::new();
        let user_config_dir = match self.deps.user_config_dir.as_ref() {
            Some(path) => Some(nested_memory_filesystem_host_path(
                path,
                "nested-memory user config directory",
            )?),
            None => None,
        };
        let opts = NestedMemoryOptions {
            gates: self.deps.gates.clone(),
            user_config_dir,
        };
        let cwd_host = nested_memory_filesystem_host_path(&ctx.cwd, "nested-memory cwd")?;
        for file_path in &triggers {
            // Best-effort: the loader absorbs every per-file failure internally, so one bad
            // trigger can never break turn entry.
            let Some(file_path_host) =
                nested_memory_filesystem_host_path(file_path, "nested-memory trigger")
            else {
                continue;
            };
            let files = get_nested_memory_for_file(&file_path_host, &cwd_host, &opts).await;
            for f in files {
                // The legacy loader returns the projected scalar filesystem
                // path and scalar markdown body. Re-enter exact carriers at
                // that named API boundary and keep them exact thereafter.
                let memory_path = f.path;
                let memory_content = f.content;
                if self
                    .deps
                    .loaded_paths
                    .lock()
                    .unwrap()
                    .contains(&memory_path)
                {
                    continue; // cross-turn dedup (non-evicting)
                }
                if self.deps.file_state.has(&memory_path) {
                    continue; // already read/loaded — content is in context
                }
                blocks.push(
                    RuntimeString::from("Contents of ")
                        .concat(memory_path.as_runtime_string())
                        .concat(&RuntimeString::from(":\n\n"))
                        .concat(&memory_content),
                );
                self.deps
                    .loaded_paths
                    .lock()
                    .unwrap()
                    .insert(memory_path.clone());
                // Mark as seen (read-before-write baseline) at the file's REAL mtime so a later
                // external edit is detected as stale. mtime-only (see module docs). A stat
                // failure just skips the mark — loaded_paths still dedups the injection; the file
                // is unreadable now, so it's left out of file_state.
                let Ok(native_path) =
                    project_runtime_path(&memory_path, "nested-memory loaded path")
                else {
                    continue;
                };
                if let Ok(meta) = tokio::fs::metadata(&native_path).await {
                    if let Ok(modified) = meta.modified() {
                        let mtime_ms = match modified.duration_since(UNIX_EPOCH) {
                            Ok(d) => d.as_secs_f64() * 1000.0,
                            Err(e) => -(e.duration().as_secs_f64() * 1000.0),
                        };
                        self.deps.file_state.record(
                            &memory_path,
                            mtime_ms,
                            RecordOptions::default(),
                        );
                    }
                }
            }
        }

        if blocks.is_empty() {
            None
        } else {
            Some(join_runtime_strings(&blocks, "\n\n"))
        }
    }
}

/// Build the nested-memory reminder provider over the per-agent deps.
pub fn create_nested_memory_reminder_provider(
    deps: NestedMemoryReminderDeps,
) -> NestedMemoryReminderProvider {
    NestedMemoryReminderProvider { deps }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::file_state::FileStateEntry;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::time::SystemTime;

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-nestedrem-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Join a `/`-separated relative path onto `base`, one component at a time.
    ///
    /// `PathBuf::join("a/b")` keeps the literal `/` on Windows, which makes the path a
    /// test builds differ from the normalized one the code under test produces.
    fn at(base: &Path, relative: &str) -> PathBuf {
        let mut path = base.to_path_buf();
        for segment in relative.split('/') {
            path.push(segment);
        }
        path
    }

    fn write(dir: &Path, rel: &str, content: &str) {
        let f = at(dir, rel);
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(f, content).unwrap();
    }

    fn ctx(dir: &Path, cwd_rel: &str) -> ReminderContext {
        ReminderContext {
            messages: vec![],
            cwd: runtime_path_from_native(&at(dir, cwd_rel)),
            task_list_id: None,
            task_plan: None,
            signal: None,

            plan_file_path: None,
            plan_file_exists: None,
            has_task_tool: false,
        }
    }

    fn contains_runtime(haystack: &RuntimeString, needle: &RuntimeString) -> bool {
        needle.is_empty()
            || haystack
                .as_utf16()
                .windows(needle.len_utf16())
                .any(|window| window == needle.as_utf16())
    }

    #[test]
    fn keeps_trigger_and_loaded_path_identity_exact_until_filesystem_projection() {
        let mut lone_units = "/tmp/memory-".encode_utf16().collect::<Vec<_>>();
        lone_units.push(0xd800);
        let lone = RuntimePath::from_runtime_string(RuntimeString::from_utf16(lone_units));
        let replacement = RuntimePath::from("/tmp/memory-�");

        let mut loaded = HashSet::new();
        loaded.insert(lone.clone());
        loaded.insert(replacement.clone());
        assert_eq!(loaded.len(), 2);
        assert_ne!(lone, replacement);

        // Node/V8 intentionally aliases both only at the real filesystem/API
        // projection selected by the legacy nested-memory loader.
        assert_eq!(
            nested_memory_filesystem_host_path(&lone, "test path"),
            nested_memory_filesystem_host_path(&replacement, "test path")
        );
    }

    /// A simple in-memory file-state cache for the tests.
    #[derive(Default)]
    struct TestFileStateCache {
        map: Mutex<HashMap<RuntimePath, FileStateEntry>>,
    }

    impl FileStateCache for TestFileStateCache {
        fn get(&self, path: &RuntimePath) -> Option<FileStateEntry> {
            self.map.lock().unwrap().get(path).cloned()
        }
        fn record(&self, path: &RuntimePath, mtime_ms: f64, opts: RecordOptions) {
            self.map.lock().unwrap().insert(
                path.clone(),
                FileStateEntry {
                    mtime_ms,
                    size: opts.size,
                    content: opts.content,
                    encoding: opts.encoding,
                },
            );
        }
        fn has(&self, path: &RuntimePath) -> bool {
            self.map.lock().unwrap().contains_key(path)
        }
        fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
            self.map
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
        fn delete(&self, path: &RuntimePath) {
            self.map.lock().unwrap().remove(path);
        }
        fn clear(&self) {
            self.map.lock().unwrap().clear();
        }
        fn clone_box(&self) -> Box<dyn FileStateCache> {
            Box::new(TestFileStateCache {
                map: Mutex::new(self.map.lock().unwrap().clone()),
            })
        }
    }

    struct Setup {
        provider: NestedMemoryReminderProvider,
        loaded_paths: Arc<Mutex<HashSet<RuntimePath>>>,
        file_state: Arc<TestFileStateCache>,
        buffer: Arc<Mutex<Vec<RuntimePath>>>,
    }

    /// Build a provider over a mutable trigger buffer + the given file state.
    fn setup(dir: &Path, triggers: &[RuntimePath], file_state: Arc<TestFileStateCache>) -> Setup {
        let loaded_paths = Arc::new(Mutex::new(HashSet::new()));
        let buffer = Arc::new(Mutex::new(triggers.to_vec()));
        let drain_buffer = buffer.clone();
        let provider = create_nested_memory_reminder_provider(NestedMemoryReminderDeps {
            drain_triggers: Box::new(move || {
                let mut b = drain_buffer.lock().unwrap();
                let out = b.clone();
                b.clear();
                out
            }),
            loaded_paths: loaded_paths.clone(),
            file_state: file_state.clone(),
            gates: None,
            user_config_dir: Some(runtime_path_from_native(&dir.join("userhome"))),
        });
        Setup {
            provider,
            loaded_paths,
            file_state,
            buffer,
        }
    }

    #[tokio::test]
    async fn renders_contents_of_path_for_a_nested_agents_md_and_marks_loaded_paths_and_file_state()
    {
        let dir = temp_dir();
        write(&dir, "proj/sub/AGENTS.md", "SUB-MEM");
        let file = runtime_path_from_native(&at(&dir, "proj/sub/file.ts"));
        let mem_path = runtime_path_from_native(&at(&dir, "proj/sub/AGENTS.md"));
        let st = setup(&dir, &[file], Arc::new(TestFileStateCache::default()));

        let out = st.provider.reminder(&ctx(&dir, "proj")).await.unwrap();
        let expected = RuntimeString::from("Contents of ")
            .concat(mem_path.as_runtime_string())
            .concat(&RuntimeString::from(":"));
        assert!(contains_runtime(&out, &expected));
        assert!(out.contains("SUB-MEM"));
        assert!(st.loaded_paths.lock().unwrap().contains(&mem_path));
        assert!(st.file_state.has(&mem_path));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn marks_file_state_with_mtime_but_no_content() {
        // The edited-files reminder skips content-less entries.
        let dir = temp_dir();
        write(&dir, "proj/sub/AGENTS.md", "SUB-MEM");
        let file = runtime_path_from_native(&at(&dir, "proj/sub/file.ts"));
        let mem_path = runtime_path_from_native(&at(&dir, "proj/sub/AGENTS.md"));
        let st = setup(&dir, &[file], Arc::new(TestFileStateCache::default()));

        st.provider.reminder(&ctx(&dir, "proj")).await;
        let entry = st.file_state.get(&mem_path);
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert!(entry.mtime_ms > 0.0);
        assert!(entry.content.is_none()); // mtime-only: no spurious out-of-band diff later
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn does_not_reinject_on_a_later_turn() {
        // loaded_paths cross-turn dedup.
        let dir = temp_dir();
        write(&dir, "proj/sub/AGENTS.md", "SUB-MEM");
        let file = runtime_path_from_native(&at(&dir, "proj/sub/file.ts"));
        let st = setup(
            &dir,
            std::slice::from_ref(&file),
            Arc::new(TestFileStateCache::default()),
        );

        // first turn injects
        assert!(st
            .provider
            .reminder(&ctx(&dir, "proj"))
            .await
            .unwrap()
            .contains("SUB-MEM"));
        st.buffer.lock().unwrap().push(file); // touched again next turn
                                              // already loaded → nothing
        assert_eq!(st.provider.reminder(&ctx(&dir, "proj")).await, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn skips_a_file_already_present_in_file_state() {
        // Already read/loaded elsewhere.
        let dir = temp_dir();
        write(&dir, "proj/sub/AGENTS.md", "SUB-MEM");
        let file = runtime_path_from_native(&at(&dir, "proj/sub/file.ts"));
        let mem_path = runtime_path_from_native(&at(&dir, "proj/sub/AGENTS.md"));
        let file_state = Arc::new(TestFileStateCache::default());
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64;
        file_state.record(&mem_path, now_ms, RecordOptions::default()); // pretend the agent already read it
        let st = setup(&dir, &[file], file_state);

        assert_eq!(st.provider.reminder(&ctx(&dir, "proj")).await, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn returns_none_when_the_trigger_buffer_is_empty() {
        let dir = temp_dir();
        let st = setup(&dir, &[], Arc::new(TestFileStateCache::default()));
        assert_eq!(st.provider.reminder(&ctx(&dir, "proj")).await, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn joins_multiple_memory_blocks_with_a_blank_line() {
        let dir = temp_dir();
        write(&dir, "proj/sub/AGENTS.md", "SUB-MEM");
        write(&dir, "proj/sub/deep/.klaus/rules/r.md", "DEEP-RULE");
        let file = runtime_path_from_native(&at(&dir, "proj/sub/deep/file.ts"));
        let st = setup(&dir, &[file], Arc::new(TestFileStateCache::default()));
        let out = st
            .provider
            .reminder(&ctx(&dir, "proj"))
            .await
            .unwrap_or_default();
        assert!(out.contains("SUB-MEM"));
        assert!(out.contains("DEEP-RULE"));
        assert!(out.contains("\n\n"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
