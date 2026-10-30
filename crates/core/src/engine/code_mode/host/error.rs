//! Why a host refused a connection before any session existed.

use crate::engine::code_mode::host::types::{Capability, SupportedProtocolVersions};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimeString,
};

/// Explains a rejected negotiation. A client that receives one of these MUST
/// NOT retry the same hello: the host has already reported everything it can
/// accept.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HandshakeRejectReason {
    /// No offered version overlaps what the host speaks; the host's own set is
    /// included so the client can report an actionable diagnostic.
    NoCompatibleVersion {
        supported_versions: SupportedProtocolVersions,
    },
    /// The client demanded a capability the host does not implement.
    MissingRequiredCapability { capability: Capability },
    /// The hello itself was malformed (for example a duplicate capability).
    InvalidHello { message: RuntimeString },
}

impl RuntimeEncode for HandshakeRejectReason {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        match self {
            Self::NoCompatibleVersion { supported_versions } => {
                object.field("supportedVersions", supported_versions)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "noCompatibleVersion",
                    &object.finish(),
                )
            }
            Self::MissingRequiredCapability { capability } => {
                object.field("capability", capability)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "missingRequiredCapability",
                    &object.finish(),
                )
            }
            Self::InvalidHello { message } => {
                object.field("message", message)?;
                runtime_codec::encode_internal(out, "type", &[], "invalidHello", &object.finish())
            }
        }
    }
}

impl RuntimeDecode for HandshakeRejectReason {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "type",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("noCompatibleVersion", &[]),
                runtime_codec::RuntimeEnumVariant::new("missingRequiredCapability", &[]),
                runtime_codec::RuntimeEnumVariant::new("invalidHello", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::NoCompatibleVersion {
                    supported_versions: object.required("supportedVersions", &[])?,
                }),
                1 => Ok(Self::MissingRequiredCapability {
                    capability: object.required("capability", &[])?,
                }),
                _ => Ok(Self::InvalidHello {
                    message: object.required("message", &[])?,
                }),
            },
        )
    }
}
