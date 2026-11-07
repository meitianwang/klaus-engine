//! Integration suite for this crate.
//!
//! One target instead of one per file. Cargo turns every tests/*.rs into its
//! own target and each target links the whole engine core, so every file here
//! cost another link of a large debug binary. Mounting them with `#[path]`
//! keeps each file where it is and leaves one link. Same convention as
//! `crates/core/tests/all.rs`.

#[path = "runtime_media_capability_codecs.rs"]
mod runtime_media_capability_codecs;

#[path = "protocol_wire_trace.rs"]
mod protocol_wire_trace;

#[path = "runtime_plain_struct_codecs.rs"]
mod runtime_plain_struct_codecs;

#[path = "runtime_unit_enum_codecs.rs"]
mod runtime_unit_enum_codecs;

#[path = "wire_input_roundtrip_snapshots.rs"]
mod wire_input_roundtrip_snapshots;
