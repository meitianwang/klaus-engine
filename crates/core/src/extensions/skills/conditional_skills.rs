//! Conditional-skill activation.
//!
//! A skill with a non-empty `paths` set is CONDITIONAL: it is hidden from the
//! model's skill listing — and not invocable — until a file matching one of its
//! `paths` globs is touched. The engine surfaces skills to both the `skill` tool
//! and the listing reminder through a single source, so the gate is applied
//! there: wrap the discovered skills with [`ConditionalSkillGate::filter`], and
//! call [`ConditionalSkillGate::activate_for_paths`] from the file tools
//! (Read/Edit/Write/…) as paths are touched. The gate is cwd-STATELESS — `cwd` is
//! passed per `activate_for_paths` call, so a session that switches cwd
//! (worktree) always matches against its live directory. Matching uses
//! gitignore-style globs.

use crate::extensions::skills::loader::Skill;
use crate::fs::node_path::{is_absolute_runtime, relative_runtime, resolve_runtime};
use crate::runtime_values::{RuntimePath, RuntimeString};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;

/// A skill is conditional iff it declares a non-empty `paths` set (already
/// `/**`-normalized).
pub fn is_conditional_skill(skill: &Skill) -> bool {
    matches!(&skill.paths, Some(p) if !p.is_empty())
}

/// POSIX `path.relative(from, to)` for absolute, resolved paths: build `../`
/// segments for the part of `from` past the common prefix, then append the
/// remainder of `to`.
/// Per-path normalization: relativize an absolute path against `cwd`; skip
/// (→ `""`) an empty / escaping (`..`) / still-absolute result, or the cwd
/// itself (which relativizes to `""`). A file outside cwd cannot match a
/// cwd-relative glob, and the matcher requires a relative path, so those are
/// dropped.
fn to_relative(p: &RuntimePath, cwd: &RuntimePath) -> RuntimePath {
    let r = if is_absolute_runtime(p) {
        let resolved_cwd = resolve_runtime(cwd, std::slice::from_ref(cwd))
            .expect("the live skill cwd resolves exactly");
        let resolved_path = resolve_runtime(cwd, std::slice::from_ref(p))
            .expect("a touched skill path resolves exactly");
        relative_runtime(cwd, &resolved_cwd, &resolved_path)
            .expect("resolved skill paths relativize exactly")
    } else {
        p.clone()
    };
    if r.as_runtime_string().is_empty()
        || r.as_runtime_string().starts_with("..")
        || is_absolute_runtime(&r)
    {
        return RuntimePath::from("");
    }
    r
}

/// Build a gitignore-style matcher for a skill's `paths`. Case-insensitive
/// (matching the glob library's default). Any pattern that fails to compile is
/// simply skipped — a malformed pattern must never break the file operation that
/// triggered activation.
fn build_matcher(patterns: &[RuntimeString]) -> Gitignore {
    let mut builder = GitignoreBuilder::new("");
    let _ = builder.case_insensitive(false);
    for p in patterns {
        let encoded = matcher_string(p, true);
        let _ = builder.add_line(None, &encoded);
    }
    builder.build().unwrap_or_else(|_| Gitignore::empty())
}

/// Encode every literal UTF-16 code unit as a distinct private-plane scalar,
/// while preserving gitignore syntax and separators. This keeps paths with an
/// unpaired surrogate distinct instead of collapsing them through UTF-8.
fn matcher_string(value: &RuntimeString, pattern: bool) -> String {
    let mut output = String::with_capacity(value.len_utf16());
    for point in value.code_points() {
        let syntax = pattern
            && matches!(
                point,
                0x0021 | 0x002a | 0x003f | 0x005b | 0x005d | 0x005c | 0x002d | 0x005e
            );
        let separator = matches!(point, 0x002f | 0x005c);
        if syntax || separator {
            output.push(char::from_u32(point).expect("ASCII matcher syntax"));
        } else if let Some(scalar) = char::from_u32(point) {
            for lowercase in scalar.to_lowercase() {
                let mut encoded = [0; 2];
                for unit in lowercase.encode_utf16(&mut encoded).iter().copied() {
                    output.push(
                        char::from_u32(0x0f0000 + u32::from(unit))
                            .expect("one private scalar exists per UTF-16 code unit"),
                    );
                }
            }
        } else {
            output.push(
                char::from_u32(0x0f0000 + point)
                    .expect("one private scalar exists per UTF-16 code unit"),
            );
        }
    }
    output
}

/// A conditional-skill activation gate. Activation state (the set of promoted
/// conditional skills) is owned by the gate via interior mutability, so a single
/// gate can be shared; `cwd` is supplied per call.
pub struct ConditionalSkillGate {
    activated: Mutex<HashSet<RuntimeString>>,
}

impl ConditionalSkillGate {
    /// Create a fresh gate with no activations.
    pub fn new() -> Self {
        ConditionalSkillGate {
            activated: Mutex::new(HashSet::new()),
        }
    }

    /// Whether a skill is currently visible (unconditional, or an activated
    /// conditional).
    pub fn is_active(&self, skill: &Skill) -> bool {
        !is_conditional_skill(skill) || self.activated.lock().unwrap().contains(&skill.name)
    }

    /// Skills visible right now: all unconditional skills, plus any activated
    /// conditional ones.
    pub fn filter(&self, skills: &[Skill]) -> Vec<Skill> {
        skills
            .iter()
            .filter(|s| self.is_active(s))
            .cloned()
            .collect()
    }

    /// Start a new catalog generation, retaining only unchanged activations.
    /// The previous gate remains owned by turns and children already in flight.
    pub fn for_catalog(&self, previous: &[Skill], current: &[Skill]) -> Self {
        let activated = self
            .activated
            .lock()
            .unwrap()
            .iter()
            .filter(|name| {
                current
                    .iter()
                    .any(|skill| &skill.name == *name && previous.contains(skill))
            })
            .cloned()
            .collect();
        Self {
            activated: Mutex::new(activated),
        }
    }

    /// Copy activations at the publication boundary, including file touches that
    /// occurred after the new catalog was staged but before its turn began.
    pub fn inherit_unchanged(&self, prior: &Self, previous: &[Skill], current: &[Skill]) {
        let next = prior.for_catalog(previous, current);
        *self.activated.lock().unwrap() = next.activated.into_inner().unwrap();
    }

    /// Forget all activations (e.g. on a fresh session).
    pub fn reset(&self) {
        self.activated.lock().unwrap().clear();
    }

    /// Activate every conditional skill whose `paths` match one of
    /// `touched_paths` (absolute or `cwd`-relative), matched relative to `cwd`.
    /// Returns the names newly activated this call (already-active ones are not
    /// repeated). Activation is STICKY — once activated a skill stays visible.
    pub fn activate_for_paths(
        &self,
        skills: &[Skill],
        touched_paths: &[RuntimePath],
        cwd: &RuntimePath,
    ) -> Vec<RuntimeString> {
        if touched_paths.is_empty() {
            return Vec::new();
        }
        let rels: Vec<RuntimePath> = touched_paths
            .iter()
            .map(|p| to_relative(p, cwd))
            .filter(|r| !r.as_runtime_string().is_empty())
            .collect();
        if rels.is_empty() {
            return Vec::new();
        }
        let mut newly: Vec<RuntimeString> = Vec::new();
        let mut activated = self.activated.lock().unwrap();
        for skill in skills {
            if !is_conditional_skill(skill) || activated.contains(&skill.name) {
                continue;
            }
            let paths = match &skill.paths {
                Some(p) => p,
                None => continue,
            };
            let matcher = build_matcher(paths);
            if rels.iter().any(|r| {
                matcher
                    .matched_path_or_any_parents(
                        Path::new(&matcher_string(r.as_runtime_string(), false)),
                        false,
                    )
                    .is_ignore()
            }) {
                activated.insert(skill.name.clone());
                newly.push(skill.name.clone());
            }
        }
        newly
    }
}

impl Default for ConditionalSkillGate {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a conditional-skill gate. Activation state is owned by the gate; cwd
/// is supplied per call.
pub fn create_conditional_skill_gate() -> ConditionalSkillGate {
    ConditionalSkillGate::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(name: &str, paths: Option<Vec<RuntimeString>>) -> Skill {
        Skill {
            name: RuntimeString::from(name),
            description: RuntimeString::from("d"),
            short_description: None,
            when_to_use: None,
            argument_hint: None,
            allowed_tools: None,
            model: None,
            effort: None,
            ultra_mode: None,
            context: None,
            agent: None,
            arguments: None,
            user_invocable: Some(true),
            disable_model_invocation: Some(false),
            paths,
            shell: None,
            plugin_namespace: None,
            body: RuntimeString::default(),
            path: RuntimeString::default(),
        }
    }

    fn names(skills: &[Skill]) -> Vec<RuntimeString> {
        skills.iter().map(|s| s.name.clone()).collect()
    }

    fn activate(
        gate: &ConditionalSkillGate,
        skills: &[Skill],
        paths: &[&str],
        cwd: &str,
    ) -> Vec<RuntimeString> {
        let paths = paths
            .iter()
            .map(|path| RuntimePath::from(*path))
            .collect::<Vec<_>>();
        gate.activate_for_paths(skills, &paths, &RuntimePath::from(cwd))
    }

    #[test]
    fn catalog_publication_copies_late_activations_without_mutating_prior_snapshots() {
        let prior = ConditionalSkillGate::new();
        let original = mk("draft", Some(vec!["**/*.rs".into()]));
        let old = vec![original.clone()];
        let next = prior.for_catalog(&old, &old);
        assert!(next.filter(&old).is_empty());
        activate(&prior, &old, &["src/main.rs"], "/workspace");
        next.inherit_unchanged(&prior, &old, &old);
        assert_eq!(
            names(&next.filter(&old)),
            vec![RuntimeString::from("draft")]
        );
        let changed = vec![Skill {
            body: "Updated instructions".into(),
            ..original
        }];
        let replacement = prior.for_catalog(&old, &changed);
        assert!(replacement.filter(&changed).is_empty());
        assert_eq!(
            names(&prior.filter(&old)),
            vec![RuntimeString::from("draft")]
        );
        assert!(prior.for_catalog(&old, &[]).filter(&[]).is_empty());
    }

    #[test]
    fn is_conditional_only_with_nonempty_paths() {
        assert!(!is_conditional_skill(&mk("a", None)));
        assert!(!is_conditional_skill(&mk("a", Some(vec![]))));
        assert!(is_conditional_skill(&mk(
            "a",
            Some(vec![RuntimeString::from("src")])
        )));
    }

    #[test]
    fn filter_hides_conditional_until_activated() {
        let gate = create_conditional_skill_gate();
        let skills = vec![
            mk("u", None),
            mk("c", Some(vec![RuntimeString::from("src")])),
        ];
        assert_eq!(names(&gate.filter(&skills)), vec![RuntimeString::from("u")]);

        let newly = activate(&gate, &skills, &["/repo/src/main.rs"], "/repo");
        assert_eq!(newly, vec![RuntimeString::from("c")]);
        assert_eq!(
            names(&gate.filter(&skills)),
            vec![RuntimeString::from("u"), RuntimeString::from("c")]
        );
    }

    #[test]
    fn activation_is_sticky_and_not_repeated() {
        let gate = create_conditional_skill_gate();
        let skills = vec![mk("c", Some(vec![RuntimeString::from("src")]))];
        let n1 = activate(&gate, &skills, &["/repo/src/a.rs"], "/repo");
        assert_eq!(n1, vec![RuntimeString::from("c")]);
        let n2 = activate(&gate, &skills, &["/repo/src/b.rs"], "/repo");
        assert!(n2.is_empty());
        assert!(gate.is_active(&skills[0]));

        gate.reset();
        assert!(!gate.is_active(&skills[0]));
    }

    #[test]
    fn paths_outside_cwd_and_cwd_itself_do_not_match() {
        let gate = create_conditional_skill_gate();
        let skills = vec![mk("c", Some(vec![RuntimeString::from("src")]))];
        // Outside cwd → relativizes to `../...` → skipped.
        assert!(activate(&gate, &skills, &["/other/src/a.rs"], "/repo").is_empty());
        // The cwd itself → relativizes to `` → skipped.
        assert!(activate(&gate, &skills, &["/repo"], "/repo").is_empty());
        // A cwd-relative path under a matching glob activates.
        assert_eq!(
            activate(&gate, &skills, &["src/a.rs"], "/repo"),
            vec![RuntimeString::from("c")]
        );
    }

    #[test]
    fn brace_expanded_globs_match() {
        let gate = create_conditional_skill_gate();
        let skills = vec![mk(
            "c",
            Some(vec![
                RuntimeString::from("src/*.ts"),
                RuntimeString::from("src/*.tsx"),
            ]),
        )];
        assert_eq!(
            activate(&gate, &skills, &["/r/src/app.tsx"], "/r"),
            vec![RuntimeString::from("c")]
        );
    }

    #[test]
    fn paths_match_case_insensitively_like_ts_ignore_default() {
        let gate = create_conditional_skill_gate();
        let skills = vec![mk("c", Some(vec![RuntimeString::from("src/*.ts")]))];
        assert_eq!(
            activate(&gate, &skills, &["/r/SRC/APP.TS"], "/r"),
            vec![RuntimeString::from("c")]
        );
    }

    #[test]
    fn empty_touched_paths_returns_empty() {
        let gate = create_conditional_skill_gate();
        let skills = vec![mk("c", Some(vec![RuntimeString::from("src")]))];
        assert!(activate(&gate, &skills, &[], "/r").is_empty());
    }

    #[test]
    fn lone_surrogate_patterns_and_paths_match_without_scalar_projection() {
        let gate = create_conditional_skill_gate();
        let pattern = RuntimeString::from_utf16(vec![
            u16::from(b's'),
            u16::from(b'r'),
            u16::from(b'c'),
            u16::from(b'/'),
            0xd800,
        ]);
        let skills = vec![mk("exact", Some(vec![pattern.clone()]))];
        let touched = RuntimePath::from_runtime_string(pattern);

        assert_eq!(
            gate.activate_for_paths(&skills, &[touched], &RuntimePath::from("/repo")),
            vec![RuntimeString::from("exact")]
        );
    }
}
