// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for packaged native helper dispatch.

#[cfg(windows)]
fn main() {
    klaus_sandbox_win::elevated::managed_deny_probe_main();
}
#[cfg(not(windows))]
fn main() {
    eprintln!("This helper requires Windows.");
    std::process::exit(1);
}
