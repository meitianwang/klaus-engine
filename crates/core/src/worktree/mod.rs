//! Git worktree lifecycle (slug validation, create-or-resume, change
//! counting, removal, canonical-root resolution).

#[allow(clippy::module_inception)]
pub mod worktree;

#[cfg(test)]
mod compatibility_tests {
    #[test]
    fn legacy_nested_module_remains_the_canonical_type_path() {
        assert!(std::any::type_name::<super::worktree::WorktreeSession>()
            .ends_with("::worktree::worktree::WorktreeSession"));
    }
}
