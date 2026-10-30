use super::AsyncReviewConfig;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextMode {
    Independent,
    Legacy,
    ThreadOwned,
}

pub struct CompactionCheckpoint<'a, T> {
    pub item: &'a T,
    pub usable: bool,
    pub model_hash: Option<&'a str>,
    pub serialized_bytes: Result<usize, ()>,
    pub estimated_tokens: usize,
}

pub struct SelectedCompaction<T> {
    pub item: Option<T>,
    pub model_hash: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionError {
    RequiresSynchronousReview,
    Serialization,
    Oversized,
    Unusable,
}

pub fn select_parent_compaction<T: Clone>(
    mode: ContextMode,
    config: &AsyncReviewConfig,
    checkpoint: Option<CompactionCheckpoint<'_, T>>,
    legacy_model_hash: Option<&str>,
    classifier_model_hash: Option<&str>,
) -> Result<SelectedCompaction<T>, CompactionError> {
    if mode == ContextMode::Independent {
        return Ok(SelectedCompaction {
            item: None,
            model_hash: None,
        });
    }
    let model_hash = match mode {
        ContextMode::Legacy => legacy_model_hash,
        ContextMode::ThreadOwned => checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.model_hash),
        ContextMode::Independent => unreachable!(),
    };
    let compatible = model_hash
        .zip(classifier_model_hash)
        .is_some_and(|(parent, classifier)| !parent.is_empty() && parent == classifier);
    if mode == ContextMode::ThreadOwned
        && checkpoint.is_some()
        && (!config.reuse_parent_compaction || !compatible)
    {
        return Err(CompactionError::RequiresSynchronousReview);
    }
    let item = if config.reuse_parent_compaction {
        match checkpoint {
            Some(checkpoint) if !checkpoint.usable => {
                if mode != ContextMode::Legacy {
                    return Err(CompactionError::Unusable);
                }
                None
            }
            Some(checkpoint) => {
                let bytes = checkpoint
                    .serialized_bytes
                    .map_err(|_| CompactionError::Serialization)?;
                if bytes > 100_000_usize.saturating_mul(4)
                    || checkpoint.estimated_tokens > config.max_parent_compaction_tokens
                {
                    return Err(CompactionError::Oversized);
                }
                Some(checkpoint.item.clone())
            }
            None => None,
        }
    } else {
        None
    };
    Ok(SelectedCompaction {
        item: item.filter(|_| mode == ContextMode::ThreadOwned || compatible),
        model_hash: model_hash.map(str::to_owned),
    })
}
