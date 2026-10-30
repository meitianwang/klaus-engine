//! Observability: cost tracking, logging, OpenTelemetry.
//!
//! `logger` is the general leveled logger. It needs a `tracing` subscriber to
//! have anywhere to go, and the production app-server entrypoints do not install
//! one, so it is not a place to look for a failure after the fact.
//! [`provider_diagnostics`] exists for exactly that case: it writes a fixed
//! file and needs nothing but the config home.

pub mod cost_tracker;
pub mod logger;
pub mod otel;
pub mod prefix_fingerprint;
pub mod provider_diagnostics;
pub(crate) mod provider_timing;
