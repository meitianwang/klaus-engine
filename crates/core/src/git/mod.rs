//! Engine-internal git helpers (repo state for the model-facing context block).

#[allow(clippy::module_inception)]
pub mod git;

#[cfg(test)]
mod compatibility_tests {
    #[test]
    fn legacy_nested_module_remains_the_canonical_type_path() {
        assert!(std::any::type_name::<super::git::RunResult>().ends_with("::git::git::RunResult"));
    }
}
