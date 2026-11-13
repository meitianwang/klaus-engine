//! JSON-RPC facade over the engine's Op/Event protocol.
//!
//! See [`service::run_app_server`] for the connection loop, [`methods`] for the
//! declared request surface and [`wire`] for the params/result types.
//! [`schema_bundle`] exports the API contract as a JSON Schema bundle so
//! out-of-repo clients can generate types matching the exact engine version
//! (`klaus-agent-stdio generate-json-schema --out DIR`).

pub mod antigravity;
mod approval_review_wire;
pub mod ark;
pub mod bailian;
mod claude_usage;
pub mod client;
pub mod codex_catalog;
mod codex_dictation;
mod conversation_catalog;
pub mod cursor;
mod external_import;
pub mod kimi;
pub mod kiro;
mod maintenance;
pub mod methods;
pub mod minimax;
mod model_selection;
pub mod model_sources;
pub(crate) mod native_web_fetch;
pub(crate) mod native_web_search;
pub(crate) mod provider_operations;
pub mod qoder;
mod queue_store;
mod queue_wire;
pub mod qwenwork;
mod request_serialization;
pub mod runtime_wire;
pub mod service;
mod subscription_speech;
mod thread_read;
pub mod trae;
pub mod windows_sandbox;
pub mod wire;
pub mod workbuddy;
pub mod zhipu;

#[cfg(test)]
mod catalog_test_support;
#[cfg(test)]
mod tests;

pub use client::{AppServerClient, ClientError, Notification, Notifications, ReverseRequest};
pub use methods::{Method, ALL_METHODS};
pub use runtime_wire::{RawJson, RuntimeValue};
pub use service::{run_app_server, AppServerOptions};

use schemars::schema_for;
use serde_json::Value;

/// The full app-server API contract, keyed by JSON-RPC method / notification
/// name. Raw inner-protocol payloads (`op`, `event`, `init`, ...) are typed as
/// `true` (any) — the inner wire schema stays owned by the protocol crate.
///
/// The `requests` section is derived from [`methods::ALL_METHODS`] rather than
/// written out here, so a method cannot exist in one and be missing from the
/// other.
pub fn schema_bundle() -> Value {
    serde_json::json!({
        "protocolVersion": wire::APP_SERVER_PROTOCOL_VERSION,
        "requests": methods::request_schemas(),
        "serverRequests": {
            "approval/request": {
                "params": schema_for!(wire::ApprovalRequestParams),
                "result": schema_for!(wire::ApprovalRequestResult),
            },
            "elicitation/request": {
                "params": schema_for!(wire::ElicitationRequestParams),
                "result": schema_for!(wire::ElicitationRequestResult),
            },
        },
        "notifications": {
            "thread/queue/changed": {
                "params": schema_for!(wire::ThreadQueueChangedNotification),
            },
            "engine/windowsSandboxSetupCompleted": {
                "params": schema_for!(wire::WindowsSandboxSetupCompletedNotification),
            },
            "model/catalogChanged": {
                "params": schema_for!(wire::ModelCatalogChangedNotification),
            },
            "engine/dictationEvent": {"params": {"type":"object", "required":["operationId","seq","type"], "properties":{"operationId":{"type":"string"},"seq":{"type":"integer"},"type":{"enum":["partial","final","error"]},"text":{"type":"string"},"code":{"type":"string"}}}},
            "session/realtimeEvent": {"params": {"type":"object", "required":["sessionId","requestId","event"], "properties":{"sessionId":{"type":"string"},"requestId":{"type":"string"},"event":{"type":"object"}}}},
            "session/event": {
                "params": schema_for!(wire::SessionEventNotification),
            },
            "session/rawEvent": {
                "params": schema_for!(wire::SessionEventNotification),
                "requiresCapability": "productEvents",
            },
            "question/requested": {
                "params": schema_for!(wire::QuestionRequestedNotification),
                "requiresCapability": "productEvents",
            },
            "question/resolved": {
                "params": schema_for!(wire::QuestionResolvedNotification),
                "requiresCapability": "productEvents",
            },
            "taskList/changed": {
                "params": schema_for!(wire::TaskListChangedNotification),
                "requiresCapability": "productEvents",
            },
            "backgroundProcess/changed": {
                "params": schema_for!(wire::BackgroundProcessChangedNotification),
                "requiresCapability": "productEvents",
            },
            "engine/event": {
                "params": true,
            },
            "goal.continuation": {
                "params": schema_for!(wire::GoalContinuationNotification),
            },
            "turn/completed": {
                "params": schema_for!(wire::TurnCompletedNotification),
            },
            // Broadcast when the endpoint unloads an idle session: its engine
            // runtime and child processes are gone, and the next open resumes
            // from stored history.
            "session/closed": {
                "params": schema_for!(wire::SessionClosedNotification),
            },
            // Broadcast on session lifecycle transitions even to unsubscribed
            // clients so list surfaces stay current.
            "session/statusChanged": {
                "params": schema_for!(wire::SessionStatusChangedNotification),
            },
            // Broadcast the instant a delete commits: clients treat it as the
            // authoritative removal instead of deducing it from their reply.
            "thread/deleted": {
                "params": schema_for!(wire::ThreadDeletedNotification),
            },
            "thread/started": {
                "params": schema_for!(wire::ThreadStartedNotification),
                "requiresCapability": "productEvents",
            },
            "turn/started": {
                "params": schema_for!(wire::TurnLifecycleNotification),
                "requiresCapability": "productEvents",
            },
            "turn/failed": {
                "params": schema_for!(wire::TurnLifecycleNotification),
                "requiresCapability": "productEvents",
            },
            "autoApprovalReview/strictReviewRequired": {
                "params": schema_for!(wire::ApprovalReviewStrictRequiredNotification),
                "requiresCapability": "productEvents",
            },
            "approvalReview/warning": {
                "params": schema_for!(wire::ApprovalReviewWarningNotification),
                "requiresCapability": "productEvents",
            },
            "item/autoApprovalReview/started": {
                "params": schema_for!(wire::ApprovalReviewStartedNotification),
                "requiresCapability": "productEvents",
            },
            "item/autoApprovalReview/completed": {
                "params": schema_for!(wire::ApprovalReviewCompletedNotification),
                "requiresCapability": "productEvents",
            },
            "item/started": {
                "params": schema_for!(wire::ItemNotification),
                "requiresCapability": "productEvents",
            },
            "item/updated": {
                "params": schema_for!(wire::ItemNotification),
                "requiresCapability": "productEvents",
            },
            "item/completed": {
                "params": schema_for!(wire::ItemNotification),
                "requiresCapability": "productEvents",
            },
            "item/agentMessage/delta": {
                "params": schema_for!(wire::DeltaNotification),
                "requiresCapability": "productEvents",
            },
            "item/reasoning/delta": {
                "params": schema_for!(wire::DeltaNotification),
                "requiresCapability": "productEvents",
            },
        },
    })
}
