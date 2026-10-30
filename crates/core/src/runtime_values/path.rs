use crate::runtime_process::ProcessAtom;

use super::{
    RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeString,
};

/// Exact JavaScript-string path identity before a named host boundary.
///
/// This carrier deliberately exposes only its exact runtime-string identity.
/// It cannot be borrowed as a Rust filesystem path or converted to a native
/// process atom without selecting the explicit process-atom boundary.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimePath(RuntimeString);

impl RuntimePath {
    /// Wrap one exact JavaScript string as a logical path without projection.
    pub fn from_runtime_string(value: RuntimeString) -> Self {
        Self(value)
    }

    /// Borrow the authoritative logical path identity.
    pub fn as_runtime_string(&self) -> &RuntimeString {
        &self.0
    }

    /// Recover the authoritative logical path identity without projection.
    pub fn into_runtime_string(self) -> RuntimeString {
        self.0
    }

    /// Select the process-atom terminal without host-projecting the path.
    pub fn into_runtime_process_atom(self) -> ProcessAtom {
        ProcessAtom::Runtime(self.0)
    }
}

impl From<RuntimeString> for RuntimePath {
    fn from(value: RuntimeString) -> Self {
        Self::from_runtime_string(value)
    }
}

impl From<&str> for RuntimePath {
    fn from(value: &str) -> Self {
        Self::from_runtime_string(RuntimeString::from(value))
    }
}

impl From<String> for RuntimePath {
    fn from(value: String) -> Self {
        Self::from_runtime_string(RuntimeString::from(value))
    }
}

impl RuntimeEncode for RuntimePath {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0.encode_runtime(out)
    }
}

impl RuntimeDecode for RuntimePath {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        RuntimeString::decode_runtime(value).map(Self)
    }
}

/// Closed path-ingress policy selected separately for each filesystem result.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Node20FsPathIngress {
    OriginalLogicalOperand,
    ProjectedLogicalOperandNodeUtf8Echo,
    UnixNativeBytesToV8String,
    WindowsWideViaLibuvUtf8ToV8String,
    NodeBufferPathBytes,
}
