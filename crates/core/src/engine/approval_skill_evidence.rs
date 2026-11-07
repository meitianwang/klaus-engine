// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified: invoked skill receipts use the runtime's native skill and turn callbacks.

//! Bounded invocation receipts for actual skill files in user-owned roots.

use std::collections::BTreeSet;
use std::path::Path;

const MAX_INVOKED_SKILLS: usize = 16;
const MAX_INVOKED_SKILL_PATH_BYTES: usize = 512;
const MAX_INVOKED_SKILL_PATHS_BYTES: usize = 2_048;

fn canonical_invoked_skill_path(
    skill_resource: &str,
    klaus_home: &Path,
    user_home: Option<&Path>,
) -> Option<String> {
    let skill_path = Path::new(skill_resource).canonicalize().ok()?;
    let mut roots = vec![klaus_home.join("skills")];
    if let Some(user_home) = user_home {
        roots.push(user_home.join(".agents").join("skills"));
    }
    if !roots.iter().any(|root| {
        root.canonicalize()
            .is_ok_and(|trusted_root| skill_path.starts_with(trusted_root))
    }) {
        return None;
    }
    let path = skill_path.to_str()?.to_owned();
    if path.len() > MAX_INVOKED_SKILL_PATH_BYTES || !skill_path.is_file() {
        return None;
    }
    Some(path)
}

fn record_bounded(paths: &mut BTreeSet<String>, path: String) {
    if paths.contains(&path)
        || paths.len() >= MAX_INVOKED_SKILLS
        || paths
            .iter()
            .map(String::len)
            .sum::<usize>()
            .saturating_add(path.len())
            > MAX_INVOKED_SKILL_PATHS_BYTES
    {
        return;
    }
    paths.insert(path);
}

/// Runtime-only receipts for one host turn, never a discovery or activation list.
#[derive(Debug, Default)]
pub(crate) struct InvokedSkillEvidence {
    turn_id: Option<String>,
    paths: BTreeSet<String>,
}

impl InvokedSkillEvidence {
    /// `skill_resource` comes from the host-resolved skill, never the tool arguments.
    pub(crate) fn record(
        &mut self,
        turn_id: &str,
        skill_resource: &str,
        klaus_home: &Path,
        user_home: Option<&Path>,
    ) {
        let Some(path) = canonical_invoked_skill_path(skill_resource, klaus_home, user_home) else {
            return;
        };
        if turn_id.is_empty() {
            return;
        }
        if self.turn_id.as_deref() != Some(turn_id) {
            self.turn_id = Some(turn_id.to_owned());
            self.paths.clear();
        }
        record_bounded(&mut self.paths, path);
    }

    pub(crate) fn paths_for_turn(&self, turn_id: &str) -> Vec<String> {
        if self.turn_id.as_deref() != Some(turn_id) {
            return Vec::new();
        }
        self.paths.iter().cloned().collect()
    }
}

/// Local receipts take admission priority over the selected root turn's receipts.
pub(crate) fn merge_invoked_skill_paths(local: &[String], root: &[String]) -> Vec<String> {
    let mut paths = BTreeSet::new();
    for path in local.iter().chain(root) {
        record_bounded(&mut paths, path.clone());
    }
    paths.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    struct SkillFiles {
        home: PathBuf,
    }

    impl SkillFiles {
        fn new() -> Self {
            let home =
                std::env::temp_dir().join(format!("invoked-skills-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(home.join(".klaus/skills")).unwrap();
            std::fs::create_dir_all(home.join(".agents/skills")).unwrap();
            Self { home }
        }

        fn file(&self, path: &str) -> PathBuf {
            let path = self.home.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, "skill instructions").unwrap();
            path
        }

        fn record(&self, evidence: &mut InvokedSkillEvidence, turn: &str, path: &Path) {
            evidence.record(
                turn,
                path.to_str().unwrap(),
                &self.home.join(".klaus"),
                Some(&self.home),
            );
        }
    }

    impl Drop for SkillFiles {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.home).unwrap();
        }
    }

    #[test]
    fn only_existing_invoked_files_in_owned_roots_are_recorded() {
        let files = SkillFiles::new();
        let local = files.file(".klaus/skills/local/SKILL.md");
        let shared = files.file(".agents/skills/shared/SKILL.md");
        let outside = files.file("project/skills/outside/SKILL.md");
        let mut evidence = InvokedSkillEvidence::default();
        assert!(evidence.paths_for_turn("turn").is_empty());
        files.record(&mut evidence, "turn", &outside);
        files.record(
            &mut evidence,
            "turn",
            &files.home.join(".klaus/skills/missing.md"),
        );
        files.record(&mut evidence, "turn", &files.home.join(".klaus/skills"));
        assert!(evidence.paths_for_turn("turn").is_empty());
        files.record(&mut evidence, "turn", &local);
        files.record(&mut evidence, "turn", &shared);
        let mut expected =
            [local, shared].map(|path| path.canonicalize().unwrap().to_str().unwrap().to_owned());
        expected.sort();
        assert_eq!(evidence.paths_for_turn("turn"), expected);
    }

    #[test]
    fn receipts_deduplicate_and_never_cross_host_turns() {
        let files = SkillFiles::new();
        let first = files.file(".klaus/skills/first/SKILL.md");
        let second = files.file(".klaus/skills/second/SKILL.md");
        let mut evidence = InvokedSkillEvidence::default();
        files.record(&mut evidence, "first-turn", &first);
        files.record(&mut evidence, "first-turn", &first);
        assert_eq!(evidence.paths_for_turn("first-turn").len(), 1);
        assert!(evidence.paths_for_turn("second-turn").is_empty());
        files.record(&mut evidence, "", &second);
        assert_eq!(evidence.paths_for_turn("first-turn").len(), 1);
        files.record(&mut evidence, "second-turn", &second);
        assert!(evidence.paths_for_turn("first-turn").is_empty());
        assert_eq!(evidence.paths_for_turn("second-turn").len(), 1);
    }

    #[test]
    fn receipts_enforce_both_count_and_aggregate_path_limits() {
        let files = SkillFiles::new();
        let mut evidence = InvokedSkillEvidence::default();
        for index in 0..24 {
            let path = files.file(&format!(".klaus/skills/{index:02}/SKILL.md"));
            files.record(&mut evidence, "count", &path);
        }
        let short_paths = (0..24).fold(BTreeSet::new(), |mut paths, index| {
            record_bounded(&mut paths, format!("skill-{index:02}"));
            paths
        });
        assert_eq!(short_paths.len(), MAX_INVOKED_SKILLS);
        assert!(evidence.paths_for_turn("count").len() <= MAX_INVOKED_SKILLS);
        for index in 0..16 {
            let name = format!("{index:02}{}", "p".repeat(220));
            let path = files.file(&format!(".klaus/skills/{name}/SKILL.md"));
            files.record(&mut evidence, "bytes", &path);
        }
        let paths = evidence.paths_for_turn("bytes");
        assert!(paths.len() < MAX_INVOKED_SKILLS);
        assert!(paths.iter().map(String::len).sum::<usize>() <= MAX_INVOKED_SKILL_PATHS_BYTES);
    }

    #[test]
    fn paths_over_the_individual_byte_limit_are_not_receipts() {
        let files = SkillFiles::new();
        let path = files.file(&format!(
            ".klaus/skills/{}/{}/{}/SKILL.md",
            "a".repeat(200),
            "b".repeat(200),
            "c".repeat(100)
        ));
        assert!(path.to_str().unwrap().len() > MAX_INVOKED_SKILL_PATH_BYTES);
        let mut evidence = InvokedSkillEvidence::default();
        files.record(&mut evidence, "turn", &path);
        assert!(evidence.paths_for_turn("turn").is_empty());
    }

    #[test]
    fn local_receipts_take_union_priority_before_sorted_rendering() {
        let local = (0..16)
            .map(|index| format!("local-{index:02}"))
            .collect::<Vec<_>>();
        let root = vec!["root-extra".to_owned(), local[0].clone()];
        assert_eq!(merge_invoked_skill_paths(&local, &root), local);
        assert_eq!(
            merge_invoked_skill_paths(&["z".to_owned()], &["a".to_owned(), "z".to_owned()]),
            ["a", "z"]
        );
    }

    #[cfg(unix)]
    #[test]
    fn canonical_target_ownership_rejects_escaped_symlinks() {
        use std::os::unix::fs::symlink;
        let files = SkillFiles::new();
        let outside = files.file("outside/SKILL.md");
        let escaped = files.home.join(".klaus/skills/escaped.md");
        symlink(outside, &escaped).unwrap();
        let owned = files.file(".agents/skills/owned/SKILL.md");
        let alias = files.home.join("outside/owned-link.md");
        symlink(&owned, &alias).unwrap();
        let mut evidence = InvokedSkillEvidence::default();
        files.record(&mut evidence, "turn", &escaped);
        assert!(evidence.paths_for_turn("turn").is_empty());
        files.record(&mut evidence, "turn", &alias);
        assert_eq!(
            evidence.paths_for_turn("turn"),
            [owned.canonicalize().unwrap().to_str().unwrap()]
        );
    }
}
