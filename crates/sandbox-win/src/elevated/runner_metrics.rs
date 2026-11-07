// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for native request paths, helper packaging, and execution lifecycle.

pub(crate) fn record_command(result: Option<(i32, bool)>) {
    let (exit_code, timed_out) =
        result.map_or((None, None), |(code, timeout)| (Some(code), Some(timeout)));
    tracing::debug!(exit_code, timed_out, "Sandbox command completed");
}

pub(crate) fn record(stage: &str, outcome: &str) {
    tracing::debug!(stage, outcome, "Sandbox runner lifecycle");
}
