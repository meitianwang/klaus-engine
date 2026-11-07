// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for native service identities and cancellable engine setup.

//! An authenticated attempt against an installed native provisioning service.
//!
//! This client never installs or starts a service. An unavailable service leaves
//! account preparation to the existing elevated setup path.

pub mod protocol;

#[cfg(windows)]
mod client;
#[cfg(windows)]
pub use client::provision;

pub use protocol::{ProvisioningSettings, ProxyListeners};

/// Result of contacting the installed service before shared elevated setup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisioningOutcome {
    Provisioned,
    Unavailable,
}
