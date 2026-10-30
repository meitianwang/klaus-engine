//! Shared native Windows token, capability, and filesystem primitives.
#[cfg(windows)]
pub mod acl;
pub mod cap;
pub mod path_normalization;
#[cfg(windows)]
pub mod token;
#[cfg(windows)]
pub mod token_user;
#[cfg(windows)]
pub mod winutil;
