// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified for packaged native helper dispatch.

#[cfg(windows)]
fn main() {
    if let Err(error) = klaus_sandbox_win::elevated::setup_helper_main() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

#[cfg(not(windows))]
fn main() {
    eprintln!("This helper requires Windows.");
    std::process::exit(1);
}
