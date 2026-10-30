//! Typed approval review request, notification, and schema payloads.

use super::runtime_wire::RawJson;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionApproveApprovalReviewDenialParams {
    pub session_id: String,
    #[schemars(with = "ApprovalReviewEventSchema")]
    pub event: RawJson,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApprovalReviewDenialsResult {
    #[schemars(with = "Vec<ApprovalReviewEventSchema>")]
    pub reviews: Vec<RawJson>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalReviewStatusSchema {
    InProgress,
    Approved,
    Denied,
    TimedOut,
    Aborted,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ProductApprovalReviewStatus {
    InProgress,
    Approved,
    Denied,
    TimedOut,
    Aborted,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalReviewRiskLevelSchema {
    Low,
    Medium,
    High,
    Critical,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalReviewUserAuthorizationSchema {
    Unknown,
    Low,
    Medium,
    High,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalReviewCommandSourceSchema {
    Shell,
    UnifiedExec,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ProductApprovalReviewCommandSource {
    Shell,
    UnifiedExec,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalReviewNetworkProtocolSchema {
    Http,
    #[serde(alias = "https_connect", alias = "http-connect")]
    Https,
    Socks5Tcp,
    Socks5Udp,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ProductApprovalReviewNetworkProtocol {
    Http,
    Https,
    Socks5Tcp,
    Socks5Udp,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalReviewReasonSchema {
    Policy,
    FreshRequired,
    MissingScore,
    StaleScore,
    InvalidScore,
    IncompatibleCompaction,
    ElevatedRisk,
    ScoringFailure,
    AuthorizationChanged,
    #[serde(other)]
    Unknown,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalReviewDecisionSourceSchema {
    Agent,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalReviewPermissionProfileSchema {
    pub network: Option<ApprovalReviewNetworkPermissionsSchema>,
    pub file_system: Option<ApprovalReviewFileSystemPermissionsSchema>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApprovalReviewNetworkPermissionsSchema {
    pub enabled: Option<bool>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalReviewFileSystemPermissionsSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entries: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub glob_scan_max_depth: Option<std::num::NonZeroUsize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApprovalReviewActionSchema {
    #[serde(rename_all = "camelCase")]
    Command {
        source: ApprovalReviewCommandSourceSchema,
        command: String,
        cwd: String,
    },
    #[serde(rename_all = "camelCase")]
    Execve {
        source: ApprovalReviewCommandSourceSchema,
        program: String,
        argv: Vec<String>,
        cwd: String,
    },
    #[serde(rename_all = "camelCase")]
    WriteStdin {
        approval_id: String,
        process_id: String,
        stdin: String,
        cwd: String,
    },
    #[serde(rename_all = "camelCase")]
    ApplyPatch { cwd: String, files: Vec<String> },
    #[serde(rename_all = "camelCase")]
    NetworkAccess {
        target: String,
        host: String,
        protocol: ApprovalReviewNetworkProtocolSchema,
        port: u16,
    },
    #[serde(rename_all = "camelCase")]
    McpToolCall {
        server: String,
        tool_name: String,
        connector_id: Option<String>,
        connector_name: Option<String>,
        tool_title: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    RequestPermissions {
        reason: Option<String>,
        permissions: ApprovalReviewPermissionProfileSchema,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ProductApprovalReviewActionSchema {
    #[serde(rename_all = "camelCase")]
    Command {
        source: ProductApprovalReviewCommandSource,
        command: String,
        cwd: String,
    },
    #[serde(rename_all = "camelCase")]
    Execve {
        source: ProductApprovalReviewCommandSource,
        program: String,
        argv: Vec<String>,
        cwd: String,
    },
    #[serde(rename_all = "camelCase")]
    WriteStdin {
        approval_id: String,
        process_id: String,
        stdin: String,
        cwd: String,
    },
    #[serde(rename_all = "camelCase")]
    ApplyPatch { cwd: String, files: Vec<String> },
    #[serde(rename_all = "camelCase")]
    NetworkAccess {
        target: String,
        host: String,
        protocol: ProductApprovalReviewNetworkProtocol,
        port: u16,
    },
    #[serde(rename_all = "camelCase")]
    McpToolCall {
        server: String,
        tool_name: String,
        connector_id: Option<String>,
        connector_name: Option<String>,
        tool_title: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    RequestPermissions {
        reason: Option<String>,
        permissions: ApprovalReviewPermissionProfileSchema,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalReviewEventSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_reason: Option<ApprovalReviewReasonSchema>,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script_path: Option<String>,
    #[serde(default)]
    pub turn_id: String,
    #[serde(default)]
    pub started_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<i64>,
    pub status: ApprovalReviewStatusSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk_level: Option<ApprovalReviewRiskLevelSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_authorization: Option<ApprovalReviewUserAuthorizationSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_source: Option<ApprovalReviewDecisionSourceSchema>,
    pub action: ApprovalReviewActionSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProductApprovalReview {
    pub status: ProductApprovalReviewStatus,
    pub risk_level: Option<ApprovalReviewRiskLevelSchema>,
    pub user_authorization: Option<ApprovalReviewUserAuthorizationSchema>,
    pub rationale: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalReviewStartedNotification {
    pub session_id: String,
    pub turn_id: String,
    pub started_at_ms: i64,
    pub review_id: String,
    pub target_item_id: Option<String>,
    #[schemars(with = "ProductApprovalReview")]
    pub review: RawJson,
    #[schemars(with = "ProductApprovalReviewActionSchema")]
    pub action: RawJson,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalReviewCompletedNotification {
    pub session_id: String,
    pub turn_id: String,
    pub started_at_ms: i64,
    pub completed_at_ms: i64,
    pub review_id: String,
    pub target_item_id: Option<String>,
    pub decision_source: ApprovalReviewDecisionSourceSchema,
    #[schemars(with = "ProductApprovalReview")]
    pub review: RawJson,
    #[schemars(with = "ProductApprovalReviewActionSchema")]
    pub action: RawJson,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalReviewStrictRequiredNotification {
    pub session_id: String,
    pub turn_id: String,
    pub started_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalReviewWarningNotification {
    pub session_id: String,
    #[schemars(with = "String")]
    pub message: RawJson,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
}
