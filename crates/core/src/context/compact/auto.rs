//! Automatic context maintenance before every normal model request.

pub fn should_compact_proactively(config: &crate::schemas::config::ResolvedConfig) -> bool {
    config.auto_compaction && !config.compaction_disabled
}
