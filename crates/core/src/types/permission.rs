//! Permission model: modes, rules, results, updates, and the approval seam.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectEncoder, RuntimePath, RuntimeString,
};
use crate::types::abort::AbortSignal;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

/// Whether operations that exceed the captured policy may request approval.
#[derive(Debug, Clone, Copy, Default, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApprovalPolicy {
    #[serde(rename = "untrusted")]
    UnlessTrusted,
    #[serde(alias = "on-failure")]
    #[default]
    OnRequest,
    Granular(GranularApprovalConfig),
    Never,
}

/// Independent approval flows. A disabled flow is rejected before any reviewer or prompt.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct GranularApprovalConfig {
    pub sandbox_approval: bool,
    pub rules: bool,
    #[serde(default)]
    pub skill_approval: bool,
    #[serde(default)]
    pub request_permissions: bool,
    pub mcp_elicitations: bool,
}

impl GranularApprovalConfig {
    pub const fn allows_sandbox_approval(self) -> bool {
        self.sandbox_approval
    }
    pub const fn allows_rules_approval(self) -> bool {
        self.rules
    }
    pub const fn allows_skill_approval(self) -> bool {
        self.skill_approval
    }
    pub const fn allows_request_permissions(self) -> bool {
        self.request_permissions
    }
    pub const fn allows_mcp_elicitations(self) -> bool {
        self.mcp_elicitations
    }
}

/// The host supplies the actual prompt cause; model arguments cannot select this category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalPromptKind {
    Sandbox,
    Rules,
    RequestPermissions,
    McpElicitation,
    Network,
    Other,
}

impl ApprovalPolicy {
    pub const fn routes_to_automatic_review(self) -> bool {
        matches!(self, Self::OnRequest | Self::Granular(_))
    }

    pub const fn allows_prompt(self, kind: ApprovalPromptKind) -> bool {
        match self {
            Self::Never => false,
            Self::Granular(config) => match kind {
                ApprovalPromptKind::Sandbox => config.allows_sandbox_approval(),
                ApprovalPromptKind::Rules => config.allows_rules_approval(),
                ApprovalPromptKind::RequestPermissions => config.allows_request_permissions(),
                ApprovalPromptKind::McpElicitation => config.allows_mcp_elicitations(),
                ApprovalPromptKind::Network | ApprovalPromptKind::Other => true,
            },
            Self::OnRequest | Self::UnlessTrusted => true,
        }
    }
}

impl RuntimeEncode for GranularApprovalConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("sandbox_approval", &self.sandbox_approval)?;
        object.field("rules", &self.rules)?;
        object.field("skill_approval", &self.skill_approval)?;
        object.field("request_permissions", &self.request_permissions)?;
        object.field("mcp_elicitations", &self.mcp_elicitations)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for GranularApprovalConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                sandbox_approval: object.required("sandbox_approval", &[])?,
                rules: object.required("rules", &[])?,
                skill_approval: object.defaulted("skill_approval", &[], || false)?,
                request_permissions: object.defaulted("request_permissions", &[], || false)?,
                mcp_elicitations: object.required("mcp_elicitations", &[])?,
            })
        })
    }
}

impl RuntimeEncode for ApprovalPolicy {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::UnlessTrusted => runtime_codec::encode_external_unit(out, "untrusted"),
            Self::OnRequest => runtime_codec::encode_external_unit(out, "on-request"),
            Self::Granular(config) => {
                runtime_codec::encode_external_payload(out, "granular", config)
            }
            Self::Never => runtime_codec::encode_external_unit(out, "never"),
        }
    }
}

impl RuntimeDecode for ApprovalPolicy {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_external(
            value,
            &[
                RuntimeEnumVariant::new("untrusted", &[]),
                RuntimeEnumVariant::new("on-request", &["on-failure"]),
                RuntimeEnumVariant::new("granular", &[]),
                RuntimeEnumVariant::new("never", &[]),
            ],
            |variant| match variant {
                RuntimeExternalVariant::Unit { variant: 0 } => Ok(Self::UnlessTrusted),
                RuntimeExternalVariant::Unit { variant: 1 } => Ok(Self::OnRequest),
                RuntimeExternalVariant::Payload { variant: 2, value } => {
                    GranularApprovalConfig::decode_runtime(value).map(Self::Granular)
                }
                RuntimeExternalVariant::Unit { variant: 3 } => Ok(Self::Never),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::InvalidEnumShape,
                )),
            },
        )
    }
}

/// Filesystem confinement, independent of approval routing and backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

/// Who resolves operations that require approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApprovalsReviewer {
    User,
    #[serde(
        rename = "auto_review",
        alias = "guardian_subagent",
        alias = "automatic"
    )]
    Automatic,
}

macro_rules! policy_enum_codec {
    ($name:ident { $($variant:ident => $wire:literal),+ $(,)? }) => {
        impl RuntimeEncode for $name {
            fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
                runtime_codec::encode_external_unit(out, match self { $(Self::$variant => $wire),+ })
            }
        }
        impl RuntimeDecode for $name {
            fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                decode_provider_unit_enum(value, &[$(RuntimeEnumVariant::new($wire, &[])),+], &[$(Self::$variant),+])
            }
        }
    };
}
policy_enum_codec!(SandboxMode { ReadOnly => "read-only", WorkspaceWrite => "workspace-write", DangerFullAccess => "danger-full-access" });
impl RuntimeEncode for ApprovalsReviewer {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::User => "user",
                Self::Automatic => "auto_review",
            },
        )
    }
}

impl RuntimeDecode for ApprovalsReviewer {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("user", &[]),
                RuntimeEnumVariant::new("auto_review", &["guardian_subagent", "automatic"]),
            ],
            &[Self::User, Self::Automatic],
        )
    }
}

impl SandboxMode {
    /// A resumed worker may only retain the stricter of its captured ceilings.
    pub(crate) fn intersect(self, other: Self) -> Self {
        match (self, other) {
            (Self::ReadOnly, _) | (_, Self::ReadOnly) => Self::ReadOnly,
            (Self::WorkspaceWrite, _) | (_, Self::WorkspaceWrite) => Self::WorkspaceWrite,
            _ => Self::DangerFullAccess,
        }
    }
}

/// Historical permission presets accepted only by migration decoders.
/// Active sessions use filesystem, approval and reviewer policies instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExternalPermissionMode {
    Default,
    Plan,
    AcceptEdits,
    Auto,
    BypassPermissions,
    DontAsk,
}

impl RuntimeEncode for ExternalPermissionMode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Default => "default",
            Self::Plan => "plan",
            Self::AcceptEdits => "acceptEdits",
            Self::Auto => "auto",
            Self::BypassPermissions => "bypassPermissions",
            Self::DontAsk => "dontAsk",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ExternalPermissionMode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("default", &[]),
                RuntimeEnumVariant::new("plan", &[]),
                RuntimeEnumVariant::new("acceptEdits", &[]),
                RuntimeEnumVariant::new("auto", &[]),
                RuntimeEnumVariant::new("bypassPermissions", &[]),
                RuntimeEnumVariant::new("dontAsk", &[]),
            ],
            &[
                Self::Default,
                Self::Plan,
                Self::AcceptEdits,
                Self::Auto,
                Self::BypassPermissions,
                Self::DontAsk,
            ],
        )
    }
}

/// Historical permission tags retained for decoding and diagnostic reasons.
/// Tool execution never derives authority from these values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PermissionMode {
    Default,
    Plan,
    AcceptEdits,
    Auto,
    BypassPermissions,
    DontAsk,
    Bubble,
}

impl RuntimeEncode for PermissionMode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Default => "default",
            Self::Plan => "plan",
            Self::AcceptEdits => "acceptEdits",
            Self::Auto => "auto",
            Self::BypassPermissions => "bypassPermissions",
            Self::DontAsk => "dontAsk",
            Self::Bubble => "bubble",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for PermissionMode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("default", &[]),
                RuntimeEnumVariant::new("plan", &[]),
                RuntimeEnumVariant::new("acceptEdits", &[]),
                RuntimeEnumVariant::new("auto", &[]),
                RuntimeEnumVariant::new("bypassPermissions", &[]),
                RuntimeEnumVariant::new("dontAsk", &[]),
                RuntimeEnumVariant::new("bubble", &[]),
            ],
            &[
                Self::Default,
                Self::Plan,
                Self::AcceptEdits,
                Self::Auto,
                Self::BypassPermissions,
                Self::DontAsk,
                Self::Bubble,
            ],
        )
    }
}

impl From<ExternalPermissionMode> for PermissionMode {
    fn from(mode: ExternalPermissionMode) -> Self {
        match mode {
            ExternalPermissionMode::Default => PermissionMode::Default,
            ExternalPermissionMode::Plan => PermissionMode::Plan,
            ExternalPermissionMode::AcceptEdits => PermissionMode::AcceptEdits,
            ExternalPermissionMode::Auto => PermissionMode::Auto,
            ExternalPermissionMode::BypassPermissions => PermissionMode::BypassPermissions,
            ExternalPermissionMode::DontAsk => PermissionMode::DontAsk,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionBehavior {
    Allow,
    Deny,
    Ask,
}

impl RuntimeEncode for PermissionBehavior {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
            Self::Ask => "ask",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for PermissionBehavior {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("allow", &[]),
                RuntimeEnumVariant::new("deny", &[]),
                RuntimeEnumVariant::new("ask", &[]),
            ],
            &[Self::Allow, Self::Deny, Self::Ask],
        )
    }
}

/// A prefix-matching rule against a tool (and optionally a command prefix).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionRule {
    pub tool_name: RuntimeString,
    /// For bash/exec: a leading command token or prefix, e.g. "git status".
    pub argument_prefix: Option<RuntimeString>,
    /// For MCP wildcard rules (`mcp__server`, `mcp__server__*`,
    /// `mcp__server__pfx_*`): match any tool whose name starts with this prefix
    /// instead of an exact `tool_name`.
    pub tool_name_prefix: Option<RuntimeString>,
}

impl RuntimeEncode for PermissionRule {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("toolName", &self.tool_name)?;
        if let Some(value) = &self.argument_prefix {
            object.field("argumentPrefix", value)?;
        }
        if let Some(value) = &self.tool_name_prefix {
            object.field("toolNamePrefix", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PermissionRule {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                tool_name: object.required("toolName", &[])?,
                argument_prefix: object.optional("argumentPrefix", &[])?,
                tool_name_prefix: object.optional("toolNamePrefix", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionReason {
    Rule {
        rule: PermissionRule,
    },
    Mode {
        mode: PermissionMode,
    },
    PrefixList {
        matched: RuntimeString,
    },
    Hook {
        hook_name: RuntimeString,
    },
    ToolSelfCheck,
    UserPrompt,
    /// A bypass-immune safety gate fired (sensitive-file edit / catastrophic
    /// removal).
    SafetyCheck,
    Default,
}

impl RuntimeEncode for PermissionReason {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::Rule { rule } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("rule", rule)?;
                ("rule", fields.finish())
            }
            Self::Mode { mode } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("mode", mode)?;
                ("mode", fields.finish())
            }
            Self::PrefixList { matched } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("matched", matched)?;
                ("prefixList", fields.finish())
            }
            Self::Hook { hook_name } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("hookName", hook_name)?;
                ("hook", fields.finish())
            }
            Self::ToolSelfCheck => ("toolSelfCheck", RuntimeObject::new()),
            Self::UserPrompt => ("userPrompt", RuntimeObject::new()),
            Self::SafetyCheck => ("safetyCheck", RuntimeObject::new()),
            Self::Default => ("default", RuntimeObject::new()),
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields)
    }
}

impl RuntimeDecode for PermissionReason {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        const VARIANTS: &[RuntimeEnumVariant<'_>] = &[
            RuntimeEnumVariant::new("rule", &[]),
            RuntimeEnumVariant::new("mode", &[]),
            RuntimeEnumVariant::new("prefixList", &[]),
            RuntimeEnumVariant::new("hook", &[]),
            RuntimeEnumVariant::new("toolSelfCheck", &[]),
            RuntimeEnumVariant::new("userPrompt", &[]),
            RuntimeEnumVariant::new("safetyCheck", &[]),
            RuntimeEnumVariant::new("default", &[]),
        ];
        runtime_codec::decode_internal(value, false, "type", &[], VARIANTS, |variant, object| {
            Ok(match variant {
                0 => Self::Rule {
                    rule: object.required("rule", &[])?,
                },
                1 => Self::Mode {
                    mode: object.required("mode", &[])?,
                },
                2 => Self::PrefixList {
                    matched: object.required("matched", &[])?,
                },
                3 => Self::Hook {
                    hook_name: object.required("hookName", &[])?,
                },
                4 => Self::ToolSelfCheck,
                5 => Self::UserPrompt,
                6 => Self::SafetyCheck,
                7 => Self::Default,
                _ => unreachable!("variant index is closed by VARIANTS"),
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PermissionResult {
    Allow {
        updated_input: Option<RuntimeJson>,
        reason: PermissionReason,
    },
    Ask {
        message: RuntimeString,
        updated_input: Option<RuntimeJson>,
        reason: PermissionReason,
        suggestions: Option<Vec<PermissionUpdate>>,
    },
    Deny {
        message: RuntimeString,
        reason: PermissionReason,
    },
    /// Defer to the general permission engine (tool has no opinion).
    Passthrough { reason: Option<PermissionReason> },
}

impl RuntimeEncode for PermissionResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::Allow {
                updated_input,
                reason,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                if let Some(value) = updated_input {
                    fields.field("updatedInput", value)?;
                }
                fields.field("reason", reason)?;
                ("allow", fields.finish())
            }
            Self::Ask {
                message,
                updated_input,
                reason,
                suggestions,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("message", message)?;
                if let Some(value) = updated_input {
                    fields.field("updatedInput", value)?;
                }
                fields.field("reason", reason)?;
                if let Some(value) = suggestions {
                    fields.field("suggestions", value)?;
                }
                ("ask", fields.finish())
            }
            Self::Deny { message, reason } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("message", message)?;
                fields.field("reason", reason)?;
                ("deny", fields.finish())
            }
            Self::Passthrough { reason } => {
                let mut fields = RuntimeObjectEncoder::new();
                if let Some(value) = reason {
                    fields.field("reason", value)?;
                }
                ("passthrough", fields.finish())
            }
        };
        runtime_codec::encode_internal(out, "behavior", &[], tag, &fields)
    }
}

impl RuntimeDecode for PermissionResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        const VARIANTS: &[RuntimeEnumVariant<'_>] = &[
            RuntimeEnumVariant::new("allow", &[]),
            RuntimeEnumVariant::new("ask", &[]),
            RuntimeEnumVariant::new("deny", &[]),
            RuntimeEnumVariant::new("passthrough", &[]),
        ];
        runtime_codec::decode_internal(
            value,
            false,
            "behavior",
            &[],
            VARIANTS,
            |variant, object| {
                Ok(match variant {
                    0 => Self::Allow {
                        updated_input: object.optional("updatedInput", &[])?,
                        reason: object.required("reason", &[])?,
                    },
                    1 => Self::Ask {
                        message: object.required("message", &[])?,
                        updated_input: object.optional("updatedInput", &[])?,
                        reason: object.required("reason", &[])?,
                        suggestions: object.optional("suggestions", &[])?,
                    },
                    2 => Self::Deny {
                        message: object.required("message", &[])?,
                        reason: object.required("reason", &[])?,
                    },
                    3 => Self::Passthrough {
                        reason: object.optional("reason", &[])?,
                    },
                    _ => unreachable!("variant index is closed by VARIANTS"),
                })
            },
        )
    }
}

/// Where a permission update lands. The
/// engine OWNS `session` (in-memory, this session). Rule updates with `*Settings`
/// destinations join the separate durable layer immediately and emit a
/// `permission_update` event. The app-server facade persists that event in its
/// engine-owned store and confirms the grant; core itself performs no I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PermissionDestination {
    Session,
    LocalSettings,
    ProjectSettings,
    UserSettings,
}

impl RuntimeEncode for PermissionDestination {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Session => "session",
            Self::LocalSettings => "localSettings",
            Self::ProjectSettings => "projectSettings",
            Self::UserSettings => "userSettings",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for PermissionDestination {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("session", &[]),
                RuntimeEnumVariant::new("localSettings", &[]),
                RuntimeEnumVariant::new("projectSettings", &[]),
                RuntimeEnumVariant::new("userSettings", &[]),
            ],
            &[
                Self::Session,
                Self::LocalSettings,
                Self::ProjectSettings,
                Self::UserSettings,
            ],
        )
    }
}

/// A permission change to apply. Scoped to the two
/// variants the approval flow produces (v1): `addRules` (the "always allow this
/// command/tool" grant) and `setMode` ("accept edits for this session").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionUpdate {
    AddRules {
        rules: Vec<PermissionRule>,
        behavior: PermissionBehavior,
        destination: PermissionDestination,
    },
    SetMode {
        mode: ExternalPermissionMode,
        destination: PermissionDestination,
    },
}

impl RuntimeEncode for PermissionUpdate {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::AddRules {
                rules,
                behavior,
                destination,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("rules", rules)?;
                fields.field("behavior", behavior)?;
                fields.field("destination", destination)?;
                ("addRules", fields.finish())
            }
            Self::SetMode { mode, destination } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("mode", mode)?;
                fields.field("destination", destination)?;
                ("setMode", fields.finish())
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields)
    }
}

impl RuntimeDecode for PermissionUpdate {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        const VARIANTS: &[RuntimeEnumVariant<'_>] = &[
            RuntimeEnumVariant::new("addRules", &[]),
            RuntimeEnumVariant::new("setMode", &[]),
        ];
        runtime_codec::decode_internal(value, false, "type", &[], VARIANTS, |variant, object| {
            Ok(match variant {
                0 => Self::AddRules {
                    rules: object.required("rules", &[])?,
                    behavior: object.required("behavior", &[])?,
                    destination: object.required("destination", &[])?,
                },
                1 => Self::SetMode {
                    mode: object.required("mode", &[])?,
                    destination: object.required("destination", &[])?,
                },
                _ => unreachable!("variant index is closed by VARIANTS"),
            })
        })
    }
}

/// Per-turn permission view threaded into tool context.
///
/// The rule arrays are LIVE references to the Session's mutable
/// session-scope state, so an approval that adds a session rule mid-turn is
/// visible to the next tool call. That live-sharing is realized with shared
/// handles when the Session is built; the contract shape is owned here.
#[derive(Debug, Clone)]
pub struct PermissionContext {
    pub allow_rules: Vec<PermissionRule>,
    pub deny_rules: Vec<PermissionRule>,
    pub ask_rules: Vec<PermissionRule>,
    pub additional_writable_roots: Vec<RuntimePath>,
    /// Core-owned roots whose exact, symlink-safe descendants may bypass the
    /// sensitive-path prompt for internal maintenance forks. Host-supplied
    /// writable roots must never be copied here.
    pub internal_writable_roots: Vec<RuntimePath>,
    /// Handle-backed root injected only into restricted memory-maintenance
    /// forks. File tools must use it instead of reopening an admitted pathname.
    pub pinned_memory_root: Option<Arc<crate::memory::pinned_root::PinnedMemoryRoot>>,
    /// Headless contexts that cannot show UI auto-deny instead of hanging.
    pub should_avoid_prompts: bool,
}

/// The kind of thing an approval request concerns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalKind {
    Tool,
    Exec,
    Patch,
    Network,
}

impl RuntimeEncode for ApprovalKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Tool => "tool",
            Self::Exec => "exec",
            Self::Patch => "patch",
            Self::Network => "network",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ApprovalKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("tool", &[]),
                RuntimeEnumVariant::new("exec", &[]),
                RuntimeEnumVariant::new("patch", &[]),
                RuntimeEnumVariant::new("network", &[]),
            ],
            &[Self::Tool, Self::Exec, Self::Patch, Self::Network],
        )
    }
}

/// Registry-owned action identity captured before an approval starts.
/// These values are never decoded from model-supplied tool arguments.
#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalActionMetadata {
    pub registered_tool_name: RuntimeString,
    pub source: ApprovalActionSource,
    pub call_source: ApprovalActionCallSource,
    pub originating_item_id: Option<RuntimeString>,
    pub input_kind: ApprovalActionInputKind,
    pub paths: Vec<RuntimeString>,
    pub path_kind: Option<ApprovalActionPathKind>,
    pub cwd: Option<RuntimeString>,
    pub terminal_launch: Option<ApprovalTerminalMetadata>,
    pub network_trigger: Option<RuntimeJson>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ApprovalActionSource {
    Builtin,
    Mcp(Box<ApprovalMcpMetadata>),
    External,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ApprovalActionCallSource {
    #[default]
    Direct,
    CodeMode {
        originating_tool_call_id: RuntimeString,
        runtime_tool_call_id: RuntimeString,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalActionInputKind {
    Function,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalActionPathKind {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalTerminalMetadata {
    pub process_id: RuntimeString,
    pub cwd: RuntimeString,
    pub tty: bool,
}

/// Optional attribution supplied by the owner of an MCP registration.
/// Descriptions and annotations remain untrusted evidence, not instructions.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ApprovalMcpRegistration {
    /// Native prepared connection receipt, absent for external registrations.
    pub native_registration: Option<crate::mcp::approval_provenance::PreparedMcpRegistration>,
    pub connector_id: Option<RuntimeString>,
    pub connector_name: Option<RuntimeString>,
    pub connector_description: Option<RuntimeString>,
    pub connected_account_email: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalMcpMetadata {
    /// Captured by the native connection owner, never by a tool descriptor.
    pub native_registration: Option<crate::mcp::approval_provenance::PreparedMcpRegistration>,
    pub server: RuntimeString,
    pub tool_name: RuntimeString,
    pub connector_id: Option<RuntimeString>,
    pub connector_name: Option<RuntimeString>,
    pub connector_description: Option<RuntimeString>,
    pub connected_account_email: Option<RuntimeString>,
    pub tool_title: Option<RuntimeString>,
    pub tool_description: Option<RuntimeString>,
    pub annotations: Option<RuntimeJson>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalRequest {
    pub id: RuntimeString,
    pub kind: ApprovalKind,
    pub tool_name: RuntimeString,
    pub input: RuntimeJson,
    pub message: RuntimeString,
    pub suggestions: Option<Vec<PermissionUpdate>>,
}

impl RuntimeEncode for ApprovalRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("kind", &self.kind)?;
        object.field("toolName", &self.tool_name)?;
        object.field("input", &self.input)?;
        object.field("message", &self.message)?;
        if let Some(value) = &self.suggestions {
            object.field("suggestions", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ApprovalRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                kind: object.required("kind", &[])?,
                tool_name: object.required("toolName", &[])?,
                input: object.required("input", &[])?,
                message: object.required("message", &[])?,
                suggestions: object.optional("suggestions", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ApprovalResponse {
    Allow {
        updated_input: Option<RuntimeJson>,
        /// Permission updates the user chose to apply.
        /// Typically a subset of the request's `suggestions` (e.g. the "always
        /// allow" rule) or a `setMode`. A `session` destination mutates the live
        /// session rules; a `*Settings` destination also emits a
        /// `permission_update` event for the engine facade to persist.
        updated_permissions: Option<Vec<PermissionUpdate>>,
    },
    Deny {
        message: Option<RuntimeString>,
    },
}

impl RuntimeEncode for ApprovalResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::Allow {
                updated_input,
                updated_permissions,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                if let Some(value) = updated_input {
                    fields.field("updatedInput", value)?;
                }
                if let Some(value) = updated_permissions {
                    fields.field("updatedPermissions", value)?;
                }
                ("allow", fields.finish())
            }
            Self::Deny { message } => {
                let mut fields = RuntimeObjectEncoder::new();
                if let Some(value) = message {
                    fields.field("message", value)?;
                }
                ("deny", fields.finish())
            }
        };
        runtime_codec::encode_internal(out, "decision", &[], tag, &fields)
    }
}

impl RuntimeDecode for ApprovalResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        const VARIANTS: &[RuntimeEnumVariant<'_>] = &[
            RuntimeEnumVariant::new("allow", &[]),
            RuntimeEnumVariant::new("deny", &[]),
        ];
        runtime_codec::decode_internal(
            value,
            false,
            "decision",
            &[],
            VARIANTS,
            |variant, object| {
                Ok(match variant {
                    0 => Self::Allow {
                        updated_input: object.optional("updatedInput", &[])?,
                        updated_permissions: object.optional("updatedPermissions", &[])?,
                    },
                    1 => Self::Deny {
                        message: object.optional("message", &[])?,
                    },
                    _ => unreachable!("variant index is closed by VARIANTS"),
                })
            },
        )
    }
}

/// Optional pre-prompt reviewer decision for high-risk approval requests.
#[derive(Debug, Clone, PartialEq)]
pub enum ApprovalReviewResponse {
    Allow,
    /// The reviewer has granted this exact request; no human prompt is needed.
    Approved {
        message: Option<RuntimeString>,
    },
    /// The reviewer requires an explicit user decision.
    AskUser {
        message: Option<RuntimeString>,
    },
    Deny {
        message: Option<RuntimeString>,
    },
    Unavailable {
        message: Option<RuntimeString>,
    },
    Error {
        message: Option<RuntimeString>,
    },
}

impl RuntimeEncode for ApprovalReviewResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, message) = match self {
            Self::Allow => ("allow", None),
            Self::Approved { message } => ("approved", message.as_ref()),
            Self::AskUser { message } => ("askUser", message.as_ref()),
            Self::Deny { message } => ("deny", message.as_ref()),
            Self::Unavailable { message } => ("unavailable", message.as_ref()),
            Self::Error { message } => ("error", message.as_ref()),
        };
        let mut fields = RuntimeObjectEncoder::new();
        if let Some(value) = message {
            fields.field("message", value)?;
        }
        runtime_codec::encode_internal(out, "decision", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ApprovalReviewResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        const VARIANTS: &[RuntimeEnumVariant<'_>] = &[
            RuntimeEnumVariant::new("allow", &[]),
            RuntimeEnumVariant::new("deny", &[]),
            RuntimeEnumVariant::new("unavailable", &[]),
            RuntimeEnumVariant::new("error", &[]),
            RuntimeEnumVariant::new("approved", &[]),
            RuntimeEnumVariant::new("askUser", &[]),
        ];
        runtime_codec::decode_internal(
            value,
            false,
            "decision",
            &[],
            VARIANTS,
            |variant, object| {
                Ok(match variant {
                    0 => Self::Allow,
                    1 => Self::Deny {
                        message: object.optional("message", &[])?,
                    },
                    2 => Self::Unavailable {
                        message: object.optional("message", &[])?,
                    },
                    3 => Self::Error {
                        message: object.optional("message", &[])?,
                    },
                    4 => Self::Approved {
                        message: object.optional("message", &[])?,
                    },
                    5 => Self::AskUser {
                        message: object.optional("message", &[])?,
                    },
                    _ => unreachable!("variant index is closed by VARIANTS"),
                })
            },
        )
    }
}

/// Captured before provider projection; owned exclusively by the engine review lifecycle.
#[derive(Clone)]
pub struct ApprovalReviewContext {
    pub(crate) accounting: Option<Arc<crate::engine::session::InferenceBundle>>,
    pub turn_id: RuntimeString,
    pub(crate) agent_id: Option<RuntimeString>,
    pub request: crate::types::provider::ModelRequest,
    pub canonical_system: Vec<crate::types::provider::SystemPromptPart>,
    pub history: Vec<crate::schemas::message::Message>,
    pub config: crate::schemas::config::ResolvedConfig,
    pub provider: Option<Arc<dyn crate::types::provider::ModelProvider>>,
    pub emit: Arc<dyn Fn(crate::types::events::EngineEvent) + Send + Sync>,
}

/// Host-supplied callback that reviews an approval request before the user/host
/// approval handler is prompted. Any non-allow result fails closed.
#[async_trait]
pub trait ApprovalReviewer: Send + Sync {
    fn observe_context(&self, _context: ApprovalReviewContext) {}
    fn assistant_started(&self, _id: &RuntimeString) {}
    fn observe_action(&self, _request: ApprovalRequest) {}
    fn observe_action_with_metadata(
        &self,
        request: ApprovalRequest,
        _metadata: ApprovalActionMetadata,
    ) {
        self.observe_action(request);
    }
    fn set_action_metadata(&self, _id: &RuntimeString, _metadata: ApprovalActionMetadata) {}
    fn finish_action(&self, _id: &RuntimeString) {}
    fn ignore_allow_prefix_rules(&self) -> bool {
        false
    }
    fn requires_review(&self) -> bool {
        false
    }
    fn is_automatic(&self) -> bool {
        false
    }
    async fn review(&self, req: ApprovalRequest, signal: AbortSignal) -> ApprovalReviewResponse;
    async fn review_for_user(
        &self,
        req: ApprovalRequest,
        signal: AbortSignal,
    ) -> ApprovalReviewResponse {
        self.review(req, signal).await
    }
}

/// Host-supplied callback that resolves an approval request (or times out → mode
/// baseline).
#[async_trait]
pub trait ApprovalHandler: Send + Sync {
    async fn approve(&self, req: ApprovalRequest, signal: AbortSignal) -> ApprovalResponse;
}

#[cfg(test)]
mod approval_policy_tests {
    use super::*;
    use crate::runtime_values::runtime_json;

    fn parse(text: &str) -> RuntimeJson {
        runtime_json::parse(&text.into()).unwrap()
    }

    #[test]
    fn approval_reviewer_uses_one_canonical_tag_and_preserves_migration_aliases() {
        for tag in ["auto_review", "guardian_subagent", "automatic"] {
            let text = format!("\"{tag}\"");
            let reviewer: ApprovalsReviewer = runtime_codec::decode_value(&parse(&text)).unwrap();
            assert_eq!(reviewer, ApprovalsReviewer::Automatic);
            assert_eq!(
                serde_json::from_str::<ApprovalsReviewer>(&text).unwrap(),
                reviewer
            );
            assert_eq!(
                runtime_codec::encode_value(&reviewer).unwrap(),
                parse(r#""auto_review""#)
            );
            assert_eq!(
                serde_json::to_value(reviewer).unwrap(),
                serde_json::json!("auto_review")
            );
        }
        assert_eq!(
            runtime_codec::decode_value::<ApprovalsReviewer>(&parse(r#""user""#)).unwrap(),
            ApprovalsReviewer::User
        );
        for text in [r#""invalid""#, "null", r#"{"auto_review":null}"#] {
            assert!(runtime_codec::decode_value::<ApprovalsReviewer>(&parse(text)).is_err());
        }
        for text in [r#""invalid""#, "null"] {
            assert!(serde_json::from_str::<ApprovalsReviewer>(text).is_err());
        }
    }

    #[test]
    fn granular_policy_round_trips_the_typed_external_shape_and_missing_defaults() {
        let value = parse(
            r#"{"granular":{"sandbox_approval":false,"rules":true,"mcp_elicitations":true,"unknown":17}}"#,
        );
        let policy: ApprovalPolicy = runtime_codec::decode_value(&value).unwrap();
        let ApprovalPolicy::Granular(config) = policy else {
            panic!("A granular policy must retain its category flags");
        };
        assert_eq!(
            config,
            GranularApprovalConfig {
                sandbox_approval: false,
                rules: true,
                skill_approval: false,
                request_permissions: false,
                mcp_elicitations: true
            }
        );
        assert_eq!(
            serde_json::from_str::<ApprovalPolicy>(
                &runtime_json::stringify(&value).try_to_utf8().unwrap()
            )
            .unwrap(),
            policy
        );
        let encoded = runtime_codec::encode_value(&policy).unwrap();
        assert_eq!(
            encoded,
            parse(
                r#"{"granular":{"sandbox_approval":false,"rules":true,"skill_approval":false,"request_permissions":false,"mcp_elicitations":true}}"#
            )
        );
        let config: crate::schemas::config::EngineConfig = runtime_codec::decode_value(&parse(r#"{"approval_policy":{"granular":{"sandbox_approval":false,"rules":true,"mcp_elicitations":true}}}"#)).unwrap();
        assert_eq!(config.approval_policy, Some(policy));
    }

    #[test]
    fn granular_policy_rejects_missing_required_flags_and_invalid_payload_types() {
        for text in [
            r#""granular""#,
            r#"{"granular":null}"#,
            r#"{"granular":{"rules":true,"mcp_elicitations":true}}"#,
            r#"{"granular":{"sandbox_approval":true,"rules":"true","mcp_elicitations":true}}"#,
            r#"{"granular":{"sandbox_approval":true,"rules":true,"mcp_elicitations":true,"skill_approval":null}}"#,
            r#"{"granular":{"sandbox_approval":true,"rules":true,"mcp_elicitations":true,"request_permissions":0}}"#,
        ] {
            assert!(
                runtime_codec::decode_value::<ApprovalPolicy>(&parse(text)).is_err(),
                "{text}"
            );
            assert!(
                serde_json::from_str::<ApprovalPolicy>(text).is_err(),
                "{text}"
            );
        }
        let alias: ApprovalPolicy = runtime_codec::decode_value(&parse(r#""on-failure""#)).unwrap();
        assert_eq!(alias, ApprovalPolicy::OnRequest);
        assert_eq!(
            runtime_codec::encode_value(&alias).unwrap(),
            parse(r#""on-request""#)
        );
        assert_eq!(
            runtime_codec::decode_value::<ApprovalPolicy>(&parse(r#""untrusted""#)).unwrap(),
            ApprovalPolicy::UnlessTrusted
        );
    }

    #[test]
    fn granular_categories_are_independent_and_preserve_network_and_ordinary_tools() {
        let policy = ApprovalPolicy::Granular(GranularApprovalConfig {
            sandbox_approval: false,
            rules: true,
            skill_approval: false,
            request_permissions: true,
            mcp_elicitations: false,
        });
        assert!(!policy.allows_prompt(ApprovalPromptKind::Sandbox));
        assert!(policy.allows_prompt(ApprovalPromptKind::Rules));
        assert!(policy.allows_prompt(ApprovalPromptKind::RequestPermissions));
        assert!(!policy.allows_prompt(ApprovalPromptKind::McpElicitation));
        assert!(policy.allows_prompt(ApprovalPromptKind::Network));
        assert!(policy.allows_prompt(ApprovalPromptKind::Other));
        assert!(policy.routes_to_automatic_review());
        assert!(!ApprovalPolicy::UnlessTrusted.routes_to_automatic_review());
        assert!(!ApprovalPolicy::Never.allows_prompt(ApprovalPromptKind::Network));
    }
}
