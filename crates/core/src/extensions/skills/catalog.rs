//! Skill catalog — the "store" seam for installable skills.
//!
//! This module defines only the [`SkillCatalog`] trait and its data types. It is a
//! host-integration seam: the engine carries no store endpoint, credential, or
//! service-specific request/response format. A host that wants a skill store
//! injects a backend implementing [`SkillCatalog`] (see `BuiltinsOptions::skill_catalog`).
//!
//! A backend's `install` receives a [`RuntimeProcessContext`] so it can stage and
//! materialize an artifact; the shared local install path lives in
//! [`crate::extensions::skills::remote`].

use crate::extensions::skills::loader::Skill;
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::RuntimeString;
use crate::types::abort::AbortSignal;
use async_trait::async_trait;

/// A boxed, thread-safe error carrying a human-readable message.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A catalog entry as surfaced to the model / CLI.
#[derive(Debug, Clone, PartialEq)]
pub struct SkillCatalogEntry {
    pub slug: RuntimeString,
    pub name: RuntimeString,
    pub description: RuntimeString,
    /// `OFFICIAL` | `COMMUNITY`.
    pub source: RuntimeString,
    pub tags: Option<Vec<RuntimeString>>,
    pub downloads: Option<f64>,
    pub version: Option<RuntimeString>,
}

#[derive(Default)]
pub struct SkillSearchOptions {
    /// Restrict to these sources. One value uses the server filter; many filter
    /// client-side.
    pub sources: Option<Vec<RuntimeString>>,
    pub limit: Option<u32>,
    /// Filter out moderation-suspicious entries (default `true`).
    pub non_suspicious_only: Option<bool>,
    pub signal: Option<AbortSignal>,
}

#[derive(Default)]
pub struct SkillInstallFromCatalogOptions {
    pub version: Option<RuntimeString>,
    /// Skills root override (defaults to the user skills dir). For tests.
    pub dir: Option<String>,
    pub force: bool,
    pub signal: Option<AbortSignal>,
}

/// A skill catalog backend: search the store, look up one entry, and install a
/// skill by slug.
#[async_trait]
pub trait SkillCatalog: Send + Sync {
    async fn search(
        &self,
        query: &RuntimeString,
        opts: SkillSearchOptions,
    ) -> Result<Vec<SkillCatalogEntry>, BoxError>;
    /// Look up a single skill by exact slug (the detail endpoint); `None` when not found.
    async fn get(
        &self,
        slug: &RuntimeString,
        signal: Option<AbortSignal>,
    ) -> Result<Option<SkillCatalogEntry>, BoxError>;
    async fn install(
        &self,
        process: &RuntimeProcessContext,
        slug: &RuntimeString,
        opts: SkillInstallFromCatalogOptions,
    ) -> Result<Option<Skill>, BoxError>;
}
