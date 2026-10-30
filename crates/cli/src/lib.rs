//! The Klaus engine's terminal client.
//!
//! One binary drives the engine through the engine's OWN declared method table
//! and nothing else. It is a protocol client, not an embedding: it never reaches
//! into the engine's Rust API to do something the declared surface cannot
//! express, and it has no way to post a raw op either. That is what keeps it
//! honest as a reference for every other client, what lets `--remote` work at
//! all, and what makes a missing capability show up as a missing method instead
//! of quietly working here and nowhere else.
//!
//! Layers, bottom up: [`error`] and [`text`] are the value/exit-code boundary,
//! [`tui`] holds the whole visual grammar (palette, layout, raw-mode shell),
//! [`output`] decides where bytes go, [`cli`] declares the command surface,
//! [`discovery`] does the host-side disk reading the engine refuses to do,
//! [`engine`] is the only door to the engine, [`session`] opens one over either
//! transport, [`render`] turns protocol events into human text, [`turn`] runs one
//! turn to completion, and [`commands`] maps each subcommand onto its methods.

pub mod cli;
pub mod commands;
pub mod discovery;
pub mod engine;
pub mod error;
pub mod model;
pub mod output;
pub mod render;
pub mod session;
pub mod text;
pub mod tui;
pub mod turn;
