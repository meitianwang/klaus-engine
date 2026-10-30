// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for the native process-security launcher namespace.

//! Host-owned startup variables and the original-empty environment marker.

use super::{environment_transport, MxcCommand};
#[cfg(windows)]
use anyhow::Context;
use anyhow::Result;
use std::collections::HashMap;

pub(super) fn encode(command: &MxcCommand, env: &mut HashMap<String, String>) -> Result<()> {
    #[cfg(windows)]
    for key in ["SystemDrive", "LOCALAPPDATA"]
        .into_iter()
        .chain(command.default_child_env.then_some("SystemRoot"))
    {
        let value = std::env::var(key)
            .ok()
            .filter(|value| !value.is_empty())
            .with_context(|| format!("MXC requires nonempty Unicode host {key}"))?;
        env.retain(|name, _| !name.eq_ignore_ascii_case(key));
        env.insert(key.to_owned(), value);
    }
    environment_transport::encode(&serde_json::to_string(command)?, env)
}

#[cfg(any(windows, test))]
pub(super) fn decode(env: &mut HashMap<String, String>) -> Result<MxcCommand> {
    use anyhow::Context;
    let payload = environment_transport::decode_and_scrub(env)?;
    let command: MxcCommand =
        serde_json::from_str(&payload).context("invalid MXC launcher request")?;
    if command.default_child_env {
        env.clear();
    }
    Ok(command)
}
