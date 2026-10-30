//! Identifiers and negotiation values of the code-mode host connection.
//!
//! Every type here is validated on construction AND on decode, so a value that
//! exists cannot violate its own invariant: request ids correlate one operation
//! with one response, session ids name one logical session on a shared
//! connection, and the capability / version sets are duplicate-free.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;

use crate::runtime_values::{
    RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode, RuntimeEncoder,
    RuntimeJson, RuntimeString,
};

/// Project a validation failure into the codec's custom-error category, so a
/// rejected value reports the same reason whether it was constructed or decoded.
fn custom_error(error: impl fmt::Display) -> RuntimeCodecError {
    RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
        error.to_string(),
    )))
}

/// The widest correlation id that survives the connection unchanged.
///
/// Frames are exact JSON, whose numbers are IEEE-754 doubles, so an id beyond
/// this bound could not be read back as the value that was written. Ids come
/// from a monotonic counter that starts at 1, so the bound is unreachable in
/// practice; it is enforced rather than assumed so a corrupted or hostile peer
/// cannot make two different ids collide.
pub const MAX_SAFE_CORRELATION_ID: i64 = (1_i64 << 53) - 1;

fn check_correlation_id(value: i64) -> Result<i64, RuntimeCodecError> {
    if value.unsigned_abs() > MAX_SAFE_CORRELATION_ID.unsigned_abs() {
        return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
            RuntimeString::from(format!(
                "correlation id {value} exceeds the exactly representable range \u{00b1}{MAX_SAFE_CORRELATION_ID}"
            )),
        )));
    }
    Ok(value)
}

/// Correlates one client operation request with the host's response.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestId(i64);

impl RequestId {
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl RuntimeEncode for RequestId {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        check_correlation_id(self.0)?.encode_runtime(out)
    }
}

impl RuntimeDecode for RequestId {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        i64::decode_runtime(value)
            .and_then(check_correlation_id)
            .map(Self)
    }
}

/// Correlates one host delegate request with the client's response.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DelegateRequestId(i64);

impl DelegateRequestId {
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Display for DelegateRequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl RuntimeEncode for DelegateRequestId {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        check_correlation_id(self.0)?.encode_runtime(out)
    }
}

impl RuntimeDecode for DelegateRequestId {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        i64::decode_runtime(value)
            .and_then(check_correlation_id)
            .map(Self)
    }
}

/// One negotiated revision of the host connection contract.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProtocolVersion(NonZeroU32);

impl ProtocolVersion {
    /// The first (and currently only) revision.
    pub const V1: Self = Self(NonZeroU32::MIN);

    pub const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.get())
    }
}

impl RuntimeEncode for ProtocolVersion {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0.get().encode_runtime(out)
    }
}

impl RuntimeDecode for ProtocolVersion {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let value = u32::decode_runtime(value)?;
        Self::new(value).ok_or_else(|| {
            RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
                "protocol version must not be 0",
            )))
        })
    }
}

/// A rejected identifier: the host contract has no meaning for a blank name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidIdentifier;

impl fmt::Display for InvalidIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("identifier must not be empty")
    }
}

impl std::error::Error for InvalidIdentifier {}

/// Whether every unit of the string is ECMAScript whitespace (or the string is
/// empty), which is what makes an identifier blank.
fn is_blank(value: &RuntimeString) -> bool {
    value.as_utf16().iter().all(|unit| {
        matches!(
            unit,
            0x0009 | 0x000a | 0x000b | 0x000c | 0x000d | 0x0020 | 0x00a0 | 0x1680 | 0x2000
                ..=0x200a | 0x2028 | 0x2029 | 0x202f | 0x205f | 0x3000 | 0xfeff
        )
    })
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct NonBlankString(RuntimeString);

impl NonBlankString {
    fn new(value: impl Into<RuntimeString>) -> Result<Self, InvalidIdentifier> {
        let value = value.into();
        if is_blank(&value) {
            Err(InvalidIdentifier)
        } else {
            Ok(Self(value))
        }
    }

    fn as_runtime_string(&self) -> &RuntimeString {
        &self.0
    }
}

impl RuntimeEncode for NonBlankString {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0.encode_runtime(out)
    }
}

impl RuntimeDecode for NonBlankString {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let value = RuntimeString::decode_runtime(value)?;
        Self::new(value).map_err(custom_error)
    }
}

/// A named protocol feature advertised during connection negotiation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Capability(NonBlankString);

impl Capability {
    pub fn new(value: impl Into<RuntimeString>) -> Result<Self, InvalidIdentifier> {
        NonBlankString::new(value).map(Self)
    }

    pub fn as_runtime_string(&self) -> &RuntimeString {
        self.0.as_runtime_string()
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", self.as_runtime_string())
    }
}

impl RuntimeEncode for Capability {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0.encode_runtime(out)
    }
}

impl RuntimeDecode for Capability {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        NonBlankString::decode_runtime(value).map(Self)
    }
}

/// Identifies one logical code-mode session on a connection.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SessionId(NonBlankString);

impl SessionId {
    pub fn new(value: impl Into<RuntimeString>) -> Result<Self, InvalidIdentifier> {
        NonBlankString::new(value).map(Self)
    }

    pub fn as_runtime_string(&self) -> &RuntimeString {
        self.0.as_runtime_string()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", self.as_runtime_string())
    }
}

impl RuntimeEncode for SessionId {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0.encode_runtime(out)
    }
}

impl RuntimeDecode for SessionId {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        NonBlankString::decode_runtime(value).map(Self)
    }
}

/// A duplicate-free set of capabilities, ordered so the wire form is canonical.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapabilitySet(BTreeSet<Capability>);

impl CapabilitySet {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn try_new(
        capabilities: impl IntoIterator<Item = Capability>,
    ) -> Result<Self, DuplicateCapability> {
        let mut unique = BTreeSet::new();
        for capability in capabilities {
            if !unique.insert(capability.clone()) {
                return Err(DuplicateCapability { capability });
            }
        }
        Ok(Self(unique))
    }

    pub fn contains(&self, capability: &Capability) -> bool {
        self.0.contains(capability)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Capability> {
        self.0.iter()
    }
}

impl RuntimeEncode for CapabilitySet {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .encode_runtime(out)
    }
}

impl RuntimeDecode for CapabilitySet {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let capabilities = Vec::<Capability>::decode_runtime(value)?;
        Self::try_new(capabilities).map_err(custom_error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DuplicateCapability {
    capability: Capability,
}

impl fmt::Display for DuplicateCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "duplicate capability {}", self.capability)
    }
}

impl std::error::Error for DuplicateCapability {}

/// The non-empty, duplicate-free version set a client offers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupportedProtocolVersions(BTreeSet<ProtocolVersion>);

impl SupportedProtocolVersions {
    pub fn try_new(
        versions: impl IntoIterator<Item = ProtocolVersion>,
    ) -> Result<Self, InvalidSupportedProtocolVersions> {
        let mut unique = BTreeSet::new();
        for version in versions {
            if !unique.insert(version) {
                return Err(InvalidSupportedProtocolVersions::Duplicate(version));
            }
        }
        if unique.is_empty() {
            return Err(InvalidSupportedProtocolVersions::Empty);
        }
        Ok(Self(unique))
    }

    pub fn contains(&self, version: ProtocolVersion) -> bool {
        self.0.contains(&version)
    }

    pub fn iter(&self) -> impl Iterator<Item = ProtocolVersion> + '_ {
        self.0.iter().copied()
    }
}

impl RuntimeEncode for SupportedProtocolVersions {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0
            .iter()
            .copied()
            .collect::<Vec<_>>()
            .encode_runtime(out)
    }
}

impl RuntimeDecode for SupportedProtocolVersions {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let versions = Vec::<ProtocolVersion>::decode_runtime(value)?;
        Self::try_new(versions).map_err(custom_error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidSupportedProtocolVersions {
    Empty,
    Duplicate(ProtocolVersion),
}

impl fmt::Display for InvalidSupportedProtocolVersions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("at least one protocol version is required"),
            Self::Duplicate(version) => {
                write!(formatter, "duplicate protocol version {version}")
            }
        }
    }
}

impl std::error::Error for InvalidSupportedProtocolVersions {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_codec;

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    #[test]
    fn identifiers_reject_blank_values_including_exotic_whitespace() {
        assert_eq!(SessionId::new(rs("")), Err(InvalidIdentifier));
        assert_eq!(SessionId::new(rs("  \t\n")), Err(InvalidIdentifier));
        assert_eq!(
            SessionId::new(RuntimeString::from_utf16(vec![0x3000])),
            Err(InvalidIdentifier)
        );
        assert!(SessionId::new(rs("thread-1")).is_ok());
        // A lone surrogate is not whitespace, so it is a usable identifier and
        // keeps its exact units.
        let lone = SessionId::new(RuntimeString::from_utf16(vec![0xd800])).expect("non-blank");
        assert_eq!(lone.as_runtime_string().as_utf16(), &[0xd800]);
    }

    #[test]
    fn identifier_decode_enforces_the_same_invariant_as_construction() {
        let blank = runtime_codec::encode_value(&rs(" ")).expect("encodes");
        let error = SessionId::decode_runtime(&blank).expect_err("blank is rejected");
        assert!(
            format!("{error}").contains("identifier must not be empty"),
            "{error}"
        );
    }

    #[test]
    fn protocol_version_zero_is_not_a_version() {
        assert!(ProtocolVersion::new(0).is_none());
        assert_eq!(ProtocolVersion::V1.get(), 1);
        let zero = runtime_codec::encode_value(&0_u32).expect("encodes");
        assert!(ProtocolVersion::decode_runtime(&zero).is_err());
    }

    #[test]
    fn capability_and_version_sets_reject_duplicates_and_canonicalize_order() {
        let read = Capability::new(rs("read")).expect("capability");
        let write = Capability::new(rs("write")).expect("capability");
        assert!(CapabilitySet::try_new([read.clone(), read.clone()]).is_err());
        let set = CapabilitySet::try_new([write.clone(), read.clone()]).expect("set");
        assert_eq!(
            set.iter().collect::<Vec<_>>(),
            vec![&read, &write],
            "the wire order is sorted, not insertion order"
        );
        assert!(set.contains(&read));

        assert_eq!(
            SupportedProtocolVersions::try_new([]),
            Err(InvalidSupportedProtocolVersions::Empty)
        );
        assert_eq!(
            SupportedProtocolVersions::try_new([ProtocolVersion::V1, ProtocolVersion::V1]),
            Err(InvalidSupportedProtocolVersions::Duplicate(
                ProtocolVersion::V1
            ))
        );
        let versions = SupportedProtocolVersions::try_new([ProtocolVersion::V1]).expect("versions");
        assert!(versions.contains(ProtocolVersion::V1));
    }

    #[test]
    fn duplicate_capabilities_are_rejected_on_decode_too() {
        let duplicated =
            runtime_codec::encode_value(&vec![rs("read"), rs("read")]).expect("encodes");
        let error = CapabilitySet::decode_runtime(&duplicated).expect_err("duplicate is rejected");
        assert!(
            format!("{error}").contains("duplicate capability"),
            "{error}"
        );
    }

    #[test]
    fn correlation_ids_round_trip_across_their_exact_range() {
        for value in [-MAX_SAFE_CORRELATION_ID, -1, 0, 7, MAX_SAFE_CORRELATION_ID] {
            let encoded = runtime_codec::encode_value(&RequestId::new(value)).expect("encodes");
            assert_eq!(
                RequestId::decode_runtime(&encoded).expect("decodes"),
                RequestId::new(value)
            );
            let encoded =
                runtime_codec::encode_value(&DelegateRequestId::new(value)).expect("encodes");
            assert_eq!(
                DelegateRequestId::decode_runtime(&encoded).expect("decodes"),
                DelegateRequestId::new(value)
            );
        }
    }

    #[test]
    fn a_correlation_id_beyond_exact_representation_is_refused_at_both_ends() {
        let too_wide = MAX_SAFE_CORRELATION_ID + 1;
        let error = runtime_codec::encode_value(&RequestId::new(too_wide))
            .expect_err("encoding refuses an inexact id");
        assert!(
            format!("{error}").contains("exceeds the exactly representable range"),
            "{error}"
        );
        // The same bound applies to a value arriving from a peer.
        let inexact = runtime_codec::encode_value(&(too_wide as f64)).expect("encodes as a number");
        assert!(RequestId::decode_runtime(&inexact).is_err());
        assert!(DelegateRequestId::decode_runtime(&inexact).is_err());
    }
}
