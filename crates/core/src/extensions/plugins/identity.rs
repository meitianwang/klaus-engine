//! Stable plugin identity.
//!
//! A plugin package is more than a directory
//! name: it is owned by a marketplace/source namespace. `PluginId` captures that
//! package identity as `plugin@marketplace` so installs, enablement, cache
//! paths, and data paths can stay unambiguous.

use std::fmt;
use std::str::FromStr;

use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimeString,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PluginId {
    pub plugin_name: RuntimeString,
    pub marketplace_name: RuntimeString,
}

impl PluginId {
    pub fn new(
        plugin_name: impl Into<RuntimeString>,
        marketplace_name: impl Into<RuntimeString>,
    ) -> Result<Self, PluginIdParseError> {
        let plugin_name = plugin_name.into();
        let marketplace_name = marketplace_name.into();
        validate_id_part("plugin name", &plugin_name)?;
        validate_id_part("marketplace name", &marketplace_name)?;
        Ok(Self {
            plugin_name,
            marketplace_name,
        })
    }

    pub fn parse(value: &str) -> Result<Self, PluginIdParseError> {
        Self::parse_runtime(&RuntimeString::from(value))
    }

    pub fn parse_runtime(value: &RuntimeString) -> Result<Self, PluginIdParseError> {
        let Some(separator) = value
            .as_utf16()
            .iter()
            .rposition(|unit| *unit == u16::from(b'@'))
        else {
            return Err(PluginIdParseError::new(
                "plugin id must be formatted as plugin@marketplace",
            ));
        };
        Self::new(
            value.slice_utf16(..separator),
            value.slice_utf16(separator + 1..),
        )
    }

    pub fn as_key(&self) -> RuntimeString {
        self.plugin_name
            .concat(&RuntimeString::from("@"))
            .concat(&self.marketplace_name)
    }
}

impl RuntimeEncode for PluginId {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("pluginName", &self.plugin_name)?;
        object.field("marketplaceName", &self.marketplace_name)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PluginId {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                plugin_name: object.required("pluginName", &[])?,
                marketplace_name: object.required("marketplaceName", &[])?,
            })
        })
    }
}

impl fmt::Display for PluginId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_key().try_to_utf8() {
            Ok(key) => f.write_str(&key),
            Err(_) => write!(f, "{:?}", self.as_key()),
        }
    }
}

impl FromStr for PluginId {
    type Err = PluginIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginIdParseError {
    message: String,
}

impl PluginIdParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for PluginIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PluginIdParseError {}

fn validate_id_part(kind: &str, value: &RuntimeString) -> Result<(), PluginIdParseError> {
    if value.is_empty() {
        return Err(PluginIdParseError::new(format!("{kind} must not be empty")));
    }
    if !value.as_utf16().iter().copied().all(|unit| {
        (u16::from(b'0')..=u16::from(b'9')).contains(&unit)
            || (u16::from(b'A')..=u16::from(b'Z')).contains(&unit)
            || (u16::from(b'a')..=u16::from(b'z')).contains(&unit)
            || unit == u16::from(b'_')
            || unit == u16::from(b'-')
    }) {
        return Err(PluginIdParseError::new(format!(
            "{kind} may only contain ASCII letters, digits, '_' or '-'"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_marketplace_qualified_plugin_id() {
        let id = PluginId::parse("browser@bundled").unwrap();
        assert_eq!(id.plugin_name, "browser");
        assert_eq!(id.marketplace_name, "bundled");
        assert_eq!(id.as_key(), "browser@bundled");
        assert_eq!(id.to_string(), "browser@bundled");
    }

    #[test]
    fn rejects_ambiguous_or_path_like_plugin_ids() {
        for bad in [
            "",
            "browser",
            "@marketplace",
            "browser@",
            "browser@market/place",
            "../browser@marketplace",
            "browser@market.place",
            "浏览器@marketplace",
            "scope@browser@marketplace",
        ] {
            assert!(PluginId::parse(bad).is_err(), "{bad}");
        }
    }
}
