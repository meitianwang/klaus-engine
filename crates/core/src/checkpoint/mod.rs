//! Filesystem checkpointing via a shadow git repository: snapshot and restore of
//! the project working tree, isolated from the user's real repository.

pub mod checkpoint_service;
