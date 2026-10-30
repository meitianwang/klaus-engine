//! One aggregate integration target for the CLI crate.
//!
//! Every test file is mounted here with `#[path]` so the crate links once, and
//! `autotests = false` in `Cargo.toml` keeps a new file from quietly becoming its
//! own target that nobody added to this list.

#[path = "cli_surface.rs"]
mod cli_surface;

#[path = "end_to_end.rs"]
mod end_to_end;

#[path = "facade_coverage.rs"]
mod facade_coverage;

#[path = "r20261018_exact_facade_cli.rs"]
mod r20261018_exact_facade_cli;
