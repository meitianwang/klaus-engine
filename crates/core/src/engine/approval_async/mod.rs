//! Predictive action classification, compatible score reuse and review admission.
//!
//! A reusable score supplies approval evidence. The caller still validates the
//! exact action and enforces the owning task's execution and permission ceilings.

mod action;
mod compaction;
mod config;
mod conversation;
mod observation;
mod policy;
mod progress;
mod routing;
mod sampler;

pub use action::{render_action, render_review_action, ActionRenderError};
pub use compaction::{
    select_parent_compaction, CompactionCheckpoint, CompactionError, ContextMode,
    SelectedCompaction,
};
pub use config::{
    AsyncClassifierMode, AsyncFeatureConfig, AsyncLocalConfig, AsyncModelConfig, AsyncReviewConfig,
    AsyncTranscriptConfig, AsyncTranscriptModelConfig, LocalReviewScope, TranscriptFormat,
    TranscriptOptions, TranscriptSource, CLASSIFICATION_OUTPUT_INSTRUCTIONS,
};
pub use conversation::{
    CompletedConversation, ConversationBackend, ConversationReservation, ConversationReuseKey,
    TranscriptCursor,
};
pub use observation::{
    observe_action, publish_classification, ClassificationPublication, ObservationDisposition,
    ObservationInput,
};
pub use policy::{ModelReviewPolicy, PolicyLoader, ReviewMode, ReviewScope, UnscoredAction};
pub use progress::{
    CachedScore, ClassifierScore, PendingScore, ScoreProgress, ToolObservation, ToolOrigin,
};
pub use routing::{
    cached_evidence, race_review, wait_for_cached_approval, CacheCheck, CachedApproval,
    ReviewFallbackReason, ReviewRace,
};
pub use sampler::{
    ClassifierAdmission, ClassifierError, ClassifierOutput, ClassifierSampler, SamplingEvent,
    SamplingStream, CLASSIFIER_MODEL, MAX_CLASSIFIER_OUTPUT_BYTES,
};

#[cfg(test)]
mod tests;
