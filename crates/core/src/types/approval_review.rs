//! Approval review lifecycle and the exact action approved by a reviewer.

use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

macro_rules! review_enum {
    ($name:ident { $($variant:ident => $wire:literal $(| $alias:literal)*),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name { $($variant),+ }
        impl RuntimeEncode for $name {
            fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
                runtime_codec::encode_external_unit(out, match self { $(Self::$variant => $wire),+ })
            }
        }
        impl RuntimeDecode for $name {
            fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                $(if value.as_str().is_some_and(|value| value == $wire $(|| value == $alias)*) { return Ok(Self::$variant); })+
                Err(RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant))
            }
        }
    };
}

review_enum!(ApprovalReviewStatus {
    InProgress => "in_progress", Approved => "approved", Denied => "denied",
    TimedOut => "timed_out", Aborted => "aborted"
});
review_enum!(ApprovalReviewRiskLevel { Low => "low", Medium => "medium", High => "high", Critical => "critical" });
review_enum!(ApprovalReviewUserAuthorization { Unknown => "unknown", Low => "low", Medium => "medium", High => "high" });
review_enum!(ApprovalReviewDecisionSource { Agent => "agent" });
review_enum!(ApprovalReviewCommandSource { Shell => "shell", UnifiedExec => "unified_exec" });
review_enum!(ApprovalReviewNetworkProtocol { Http => "http", Https => "https" | "https_connect" | "http-connect", Socks5Tcp => "socks5_tcp", Socks5Udp => "socks5_udp" });

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalReviewReason {
    Policy,
    FreshRequired,
    MissingScore,
    StaleScore,
    InvalidScore,
    IncompatibleCompaction,
    ElevatedRisk,
    ScoringFailure,
    AuthorizationChanged,
    Unknown,
}
impl RuntimeEncode for ApprovalReviewReason {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Policy => "policy",
                Self::FreshRequired => "fresh_required",
                Self::MissingScore => "missing_score",
                Self::StaleScore => "stale_score",
                Self::InvalidScore => "invalid_score",
                Self::IncompatibleCompaction => "incompatible_compaction",
                Self::ElevatedRisk => "elevated_risk",
                Self::ScoringFailure => "scoring_failure",
                Self::AuthorizationChanged => "authorization_changed",
                Self::Unknown => "unknown",
            },
        )
    }
}
impl RuntimeDecode for ApprovalReviewReason {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let text = value
            .as_str()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::ExpectedString))?;
        Ok(if text == "policy" {
            Self::Policy
        } else if text == "fresh_required" {
            Self::FreshRequired
        } else if text == "missing_score" {
            Self::MissingScore
        } else if text == "stale_score" {
            Self::StaleScore
        } else if text == "invalid_score" {
            Self::InvalidScore
        } else if text == "incompatible_compaction" {
            Self::IncompatibleCompaction
        } else if text == "elevated_risk" {
            Self::ElevatedRisk
        } else if text == "scoring_failure" {
            Self::ScoringFailure
        } else if text == "authorization_changed" {
            Self::AuthorizationChanged
        } else {
            Self::Unknown
        })
    }
}

/// Captured model labels used internally for attribution; never serialized to clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalReviewModelContext {
    pub model_slug: RuntimeString,
    pub reasoning_effort: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ApprovalReviewAction {
    Command {
        source: ApprovalReviewCommandSource,
        command: RuntimeString,
        cwd: RuntimeString,
    },
    Execve {
        source: ApprovalReviewCommandSource,
        program: RuntimeString,
        argv: Vec<RuntimeString>,
        cwd: RuntimeString,
    },
    WriteStdin {
        approval_id: RuntimeString,
        process_id: RuntimeString,
        stdin: RuntimeString,
        cwd: RuntimeString,
    },
    ApplyPatch {
        cwd: RuntimeString,
        files: Vec<RuntimeString>,
    },
    NetworkAccess {
        target: RuntimeString,
        host: RuntimeString,
        protocol: ApprovalReviewNetworkProtocol,
        port: u16,
    },
    McpToolCall {
        server: RuntimeString,
        tool_name: RuntimeString,
        connector_id: Option<RuntimeString>,
        connector_name: Option<RuntimeString>,
        tool_title: Option<RuntimeString>,
    },
    RequestPermissions {
        reason: Option<RuntimeString>,
        permissions: ApprovalReviewPermissionProfile,
    },
}

impl RuntimeEncode for ApprovalReviewAction {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        match self {
            Self::Command {
                source,
                command,
                cwd,
            } => {
                object.field("type", &RuntimeString::from("command"))?;
                object.field("source", source)?;
                object.field("command", command)?;
                object.field("cwd", cwd)?;
            }
            Self::Execve {
                source,
                program,
                argv,
                cwd,
            } => {
                object.field("type", &RuntimeString::from("execve"))?;
                object.field("source", source)?;
                object.field("program", program)?;
                object.field("argv", argv)?;
                object.field("cwd", cwd)?;
            }
            Self::WriteStdin {
                approval_id,
                process_id,
                stdin,
                cwd,
            } => {
                object.field("type", &RuntimeString::from("write_stdin"))?;
                object.field("approvalId", approval_id)?;
                object.field("processId", process_id)?;
                object.field("stdin", stdin)?;
                object.field("cwd", cwd)?;
            }
            Self::ApplyPatch { cwd, files } => {
                object.field("type", &RuntimeString::from("apply_patch"))?;
                object.field("cwd", cwd)?;
                object.field("files", files)?;
            }
            Self::NetworkAccess {
                target,
                host,
                protocol,
                port,
            } => {
                object.field("type", &RuntimeString::from("network_access"))?;
                object.field("target", target)?;
                object.field("host", host)?;
                object.field("protocol", protocol)?;
                object.field("port", port)?;
            }
            Self::McpToolCall {
                server,
                tool_name,
                connector_id,
                connector_name,
                tool_title,
            } => {
                object.field("type", &RuntimeString::from("mcp_tool_call"))?;
                object.field("server", server)?;
                object.field("toolName", tool_name)?;
                object.field("connectorId", connector_id)?;
                object.field("connectorName", connector_name)?;
                object.field("toolTitle", tool_title)?;
            }
            Self::RequestPermissions {
                reason,
                permissions,
            } => {
                object.field("type", &RuntimeString::from("request_permissions"))?;
                object.field("reason", reason)?;
                object.field("permissions", permissions)?;
            }
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ApprovalReviewAction {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let kind: RuntimeString = object.required("type", &[])?;
            if kind == "command" {
                Ok(Self::Command {
                    source: object.required("source", &[])?,
                    command: object.required("command", &[])?,
                    cwd: object.required("cwd", &[])?,
                })
            } else if kind == "execve" {
                Ok(Self::Execve {
                    source: object.required("source", &[])?,
                    program: object.required("program", &[])?,
                    argv: object.required("argv", &[])?,
                    cwd: object.required("cwd", &[])?,
                })
            } else if kind == "write_stdin" {
                Ok(Self::WriteStdin {
                    approval_id: object.required("approvalId", &[])?,
                    process_id: object.required("processId", &[])?,
                    stdin: object.required("stdin", &[])?,
                    cwd: object.required("cwd", &[])?,
                })
            } else if kind == "apply_patch" {
                Ok(Self::ApplyPatch {
                    cwd: object.required("cwd", &[])?,
                    files: object.required("files", &[])?,
                })
            } else if kind == "network_access" {
                Ok(Self::NetworkAccess {
                    target: object.required("target", &[])?,
                    host: object.required("host", &[])?,
                    protocol: object.required("protocol", &[])?,
                    port: object.required("port", &[])?,
                })
            } else if kind == "mcp_tool_call" {
                Ok(Self::McpToolCall {
                    server: object.required("server", &[])?,
                    tool_name: object.required("toolName", &[])?,
                    connector_id: object.optional("connectorId", &[])?,
                    connector_name: object.optional("connectorName", &[])?,
                    tool_title: object.optional("toolTitle", &[])?,
                })
            } else if kind == "request_permissions" {
                Ok(Self::RequestPermissions {
                    reason: object.optional("reason", &[])?,
                    permissions: object.required("permissions", &[])?,
                })
            } else {
                Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                ))
            }
        })
    }
}

/// The reviewed permission overlay. Filesystem entries retain their structured path policy.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ApprovalReviewPermissionProfile {
    pub network: Option<ApprovalReviewNetworkPermissions>,
    pub file_system: Option<RuntimeJson>,
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ApprovalReviewNetworkPermissions {
    pub enabled: Option<bool>,
}
impl RuntimeEncode for ApprovalReviewNetworkPermissions {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("enabled", &self.enabled)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}
impl RuntimeDecode for ApprovalReviewNetworkPermissions {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                enabled: object.optional("enabled", &[])?,
            })
        })
    }
}
impl RuntimeEncode for ApprovalReviewPermissionProfile {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("network", &self.network)?;
        object.field("fileSystem", &self.file_system)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}
impl RuntimeDecode for ApprovalReviewPermissionProfile {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                network: object.optional("network", &[])?,
                file_system: object.optional("fileSystem", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalReviewEvent {
    pub model_context: Option<ApprovalReviewModelContext>,
    pub review_reason: Option<ApprovalReviewReason>,
    pub id: RuntimeString,
    pub target_item_id: Option<RuntimeString>,
    pub plugin_id: Option<RuntimeString>,
    pub script_path: Option<RuntimeString>,
    pub turn_id: RuntimeString,
    pub started_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub status: ApprovalReviewStatus,
    pub risk_level: Option<ApprovalReviewRiskLevel>,
    pub user_authorization: Option<ApprovalReviewUserAuthorization>,
    pub rationale: Option<RuntimeString>,
    pub decision_source: Option<ApprovalReviewDecisionSource>,
    pub action: ApprovalReviewAction,
}

impl RuntimeEncode for ApprovalReviewEvent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.review_reason {
            object.field("reviewReason", value)?;
        }
        object.field("id", &self.id)?;
        if let Some(value) = &self.target_item_id {
            object.field("targetItemId", value)?;
        }
        if let Some(value) = &self.plugin_id {
            object.field("pluginId", value)?;
        }
        if let Some(value) = &self.script_path {
            object.field("scriptPath", value)?;
        }
        object.field("turnId", &self.turn_id)?;
        object.field("startedAtMs", &self.started_at_ms)?;
        if let Some(value) = &self.completed_at_ms {
            object.field("completedAtMs", value)?;
        }
        object.field("status", &self.status)?;
        if let Some(value) = &self.risk_level {
            object.field("riskLevel", value)?;
        }
        if let Some(value) = &self.user_authorization {
            object.field("userAuthorization", value)?;
        }
        if let Some(value) = &self.rationale {
            object.field("rationale", value)?;
        }
        if let Some(value) = &self.decision_source {
            object.field("decisionSource", value)?;
        }
        object.field("action", &self.action)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ApprovalReviewEvent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                model_context: None,
                review_reason: object.optional("reviewReason", &[])?,
                id: object.required("id", &[])?,
                target_item_id: object.optional("targetItemId", &[])?,
                plugin_id: object.optional("pluginId", &[])?,
                script_path: object.optional("scriptPath", &[])?,
                turn_id: object.defaulted("turnId", &[], Default::default)?,
                started_at_ms: object.defaulted("startedAtMs", &[], Default::default)?,
                completed_at_ms: object.optional("completedAtMs", &[])?,
                status: object.required("status", &[])?,
                risk_level: object.optional("riskLevel", &[])?,
                user_authorization: object.optional("userAuthorization", &[])?,
                rationale: object.optional("rationale", &[])?,
                decision_source: object.optional("decisionSource", &[])?,
                action: object.required("action", &[])?,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;

    fn parse(value: &str) -> RuntimeJson {
        runtime_json::parse(&value.into()).unwrap()
    }

    #[test]
    fn approval_review_event_preserves_action_strings_and_omits_internal_model_labels() {
        let value = parse(
            r#"{"id":"review-1","turnId":"turn-1","startedAtMs":1,"completedAtMs":2,"status":"denied","riskLevel":"critical","userAuthorization":"unknown","rationale":"\ud800","decisionSource":"agent","reviewReason":"policy","action":{"type":"command","source":"unified_exec","command":"printf '\udfff'","cwd":"/tmp"}}"#,
        );
        let mut review = ApprovalReviewEvent::decode_runtime(&value).unwrap();
        review.model_context = Some(ApprovalReviewModelContext {
            model_slug: "private-model".into(),
            reasoning_effort: Some("low".into()),
        });
        assert!(runtime_codec::encode_value(&review)
            .unwrap()
            .semantic_eq_unordered(&value));
        let envelope = crate::types::events::EngineEvent::ApprovalReview { review };
        let encoded = runtime_codec::encode_value(&envelope).unwrap();
        assert_eq!(
            encoded.get("type"),
            Some(&RuntimeJson::from("approval_review"))
        );
        assert!(encoded
            .get("review")
            .is_some_and(|review| review.semantic_eq_unordered(&value)));
        assert!(encoded.get("modelContext").is_none());
        assert!(matches!(
            crate::types::events::EngineEvent::decode_runtime(&encoded).unwrap(),
            crate::types::events::EngineEvent::ApprovalReview { .. }
        ));
    }

    #[test]
    fn approval_review_retains_every_supported_action_shape() {
        for source in [
            r#"{"type":"command","source":"shell","command":"pwd","cwd":"/tmp"}"#,
            r#"{"type":"execve","source":"unified_exec","program":"/bin/pwd","argv":["pwd"],"cwd":"/tmp"}"#,
            r#"{"type":"write_stdin","approvalId":"approval-1","processId":"42","stdin":"yes\n","cwd":"/tmp"}"#,
            r#"{"type":"apply_patch","cwd":"/tmp","files":["/tmp/a"]}"#,
            r#"{"type":"network_access","target":"https://example.test","host":"example.test","protocol":"https","port":443}"#,
            r#"{"type":"mcp_tool_call","server":"documents","toolName":"read","connectorId":null,"connectorName":"Documents","toolTitle":null}"#,
            r#"{"type":"request_permissions","reason":"read a file","permissions":{"network":{"enabled":false},"fileSystem":{"entries":[],"globScanMaxDepth":null}}}"#,
        ] {
            let value = parse(source);
            let action = ApprovalReviewAction::decode_runtime(&value).unwrap();
            assert_eq!(runtime_codec::encode_value(&action).unwrap(), value);
        }
        assert!(ApprovalReviewAction::decode_runtime(&parse(r#"{"type":"other"}"#)).is_err());
        assert!(ApprovalReviewStatus::decode_runtime(&parse(r#""failed""#)).is_err());
        assert_eq!(
            ApprovalReviewNetworkProtocol::decode_runtime(&parse(r#""https_connect""#)).unwrap(),
            ApprovalReviewNetworkProtocol::Https
        );
        assert_eq!(
            ApprovalReviewReason::decode_runtime(&parse(r#""future_reason""#)).unwrap(),
            ApprovalReviewReason::Unknown
        );
    }
}
