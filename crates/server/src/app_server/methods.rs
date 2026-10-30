//! The request surface, declared once.
//!
//! Every method a client may call is one row of [`facade_methods!`], and the row
//! is what binds the method name to its params type and its result type. A
//! caller writes `request::<methods::SessionHistory>(params)` and the compiler
//! picks both types; it cannot pass another method's params, and it cannot
//! decode the reply as another method's result.
//!
//! This exists because the surface was previously described in three places that
//! had to agree by hand: the dispatch in [`super::service`], the schema bundle in
//! [`super::schema_bundle`], and every caller's own idea of what came back. The
//! third one is what actually rotted — a host with an escape hatch never has to
//! name a method at all, so nothing ever told it the method existed. The table is
//! now the single declaration: the bundle is derived from it, the coverage test
//! walks it, and a new method that is not added here has no way to be called.
//!
//! Declaration order is the bundle's key order (`serde_json` runs with
//! `preserve_order`), so rows are grouped the way the surface reads, not
//! alphabetically.

use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::runtime_wire::RawJson;
use super::wire;

/// One request method, with its params and result bound to its name.
pub trait Method {
    /// The JSON-RPC method name.
    const NAME: &'static str;
    /// What the caller sends. Bound in both directions on purpose: the server
    /// decodes exactly what the client encodes, so the two cannot drift.
    type Params: Serialize + DeserializeOwned + JsonSchema;
    /// What the caller gets back.
    type Result: FacadeResult;
}

/// A result a client can decode and the bundle can describe.
///
/// Two shapes exist because two shapes are real: methods that answer with a
/// declared struct, and methods that hand back the inner protocol's own reply
/// envelope verbatim. The latter are typed `RawJson` here and `true` in the
/// bundle, because the inner wire schema belongs to the protocol crate and is
/// not restated in this one.
pub trait FacadeResult: DeserializeOwned {
    /// The JSON Schema the bundle publishes for this result.
    fn schema() -> Value;
}

/// Endpoint-owned scalar results retain their ordinary serde domain.
impl FacadeResult for Value {
    fn schema() -> Value {
        Value::Bool(true)
    }
}

/// Opaque replies retain their content and original scalar control tokens.
impl FacadeResult for RawJson {
    fn schema() -> Value {
        Value::Bool(true)
    }
}

/// The result of a method whose answer is that it finished.
///
/// `JsonSchema` is derived, not hand-waved into an empty object: every type this
/// module hands a client has to be describable in the bundle, and an empty schema
/// would say "anything" about a reply that is always `{}`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
pub struct Empty {}

/// The JSON Schema of `T`, in the exact form the bundle publishes.
///
/// This is what `schemars::schema_for!` expands to, spelled generically so the
/// table can name it as a function pointer instead of repeating a macro call per
/// row.
pub fn schema_of<T: JsonSchema>() -> Value {
    serde_json::to_value(schemars::gen::SchemaGenerator::default().into_root_schema_for::<T>())
        .expect("a schemars root schema always serializes")
}

macro_rules! typed_results {
    ($($ty:ty),* $(,)?) => {
        $(
            impl FacadeResult for $ty {
                fn schema() -> Value {
                    schema_of::<$ty>()
                }
            }
        )*
    };
}

typed_results! {
    Empty,
    wire::InitializeResult,
    wire::PlanningResult,
    wire::SessionGoalResult,
    wire::SessionReadGoalObjectiveResult,
    wire::SessionStartResult,
    wire::SessionPrepareResult,
    wire::SessionTaskIdentityResult,
    wire::SessionBeginModelUpdateResult,
    wire::SessionFailModelUpdateResult,
    wire::SessionReconfigureModelResult,
    wire::SessionModelRuntimeResult,
    wire::TurnStartResult,
    wire::TurnSubmitResult,
    wire::ThreadQueueListResult,
    wire::ThreadQueueClearResult,
    wire::ThreadQueueItemResult,
    wire::ThreadQueueDeleteResult,
    wire::ThreadQueueMutationResult,
    wire::ThreadQueueStartResult,
    wire::SubmissionAck,
    wire::SessionSubscriptionResult,
    wire::SessionFollowResult,
    wire::ApprovalReviewDenialsResult,
    wire::EngineBuiltinSkillsResult,
    wire::EngineBuiltinAgentsResult,
    wire::ThreadReadResult,
    wire::EngineImportExternalSessionResult,
    wire::EngineReadSessionSummaryResult,
    wire::EngineProfileActivityResult,
    wire::PermissionRulesResult,
    wire::ConfigResult,
    wire::WindowsSandboxReadinessResult,
    wire::WindowsSandboxSetupStartResult,
    wire::HooksResult,
    wire::ModelCredentialResult,
    wire::ModelCredentialReadResult,
    wire::ModelSelectionResolution,
    wire::SessionBundledDiagramMigrationResult,
    wire::SessionPredictionResult,
    wire::SubscriptionTranscriptResult,
    wire::EngineCompleteTextResult,
    wire::SubscriptionCancelTranscriptionResult,
    wire::RealtimeCapabilitiesResult,
    wire::RealtimeStartResult,
    wire::SessionPredictionContextResult,
}

/// One row of the table, reachable at runtime.
///
/// The compile-time binding is [`Method`]; this is for the callers that need to
/// walk the whole surface without knowing it — the schema bundle and the test
/// that asserts dispatch covers every declared method.
pub struct MethodSpec {
    pub name: &'static str,
    /// The marker type's own name, for a caller that has to point at the SOURCE
    /// rather than at the wire: a client names `SessionHistory`, not
    /// `"session/history"`, so a test asserting a client reaches every declared
    /// method has nothing else to look for.
    pub marker: &'static str,
    pub params_schema: fn() -> Value,
    pub result_schema: fn() -> Value,
}

macro_rules! facade_methods {
    ($(
        $(#[$meta:meta])*
        $marker:ident => $name:literal ($params:ty) -> $result:ty;
    )*) => {
        $(
            $(#[$meta])*
            #[derive(Debug, Clone, Copy)]
            pub struct $marker;

            impl Method for $marker {
                const NAME: &'static str = $name;
                type Params = $params;
                type Result = $result;
            }
        )*

        /// Every request method, in the order the bundle publishes them.
        pub const ALL_METHODS: &[MethodSpec] = &[
            $(MethodSpec {
                name: <$marker as Method>::NAME,
                marker: stringify!($marker),
                params_schema: schema_of::<$params>,
                result_schema: <$result as FacadeResult>::schema,
            },)*
        ];
    };
}

facade_methods! {
    /// Handshake. Declares the client's capabilities and gets the engine's.
    Initialize => "initialize" (wire::InitializeParams) -> wire::InitializeResult;
    /// Bounded plain-text inference through the same source resolver as sessions.
    EngineCompleteText => "engine/completeText" (wire::EngineCompleteTextParams) -> wire::EngineCompleteTextResult;
    EngineCancelTextCompletion => "engine/cancelTextCompletion" (wire::SubscriptionCancelTranscriptionParams) -> wire::SubscriptionCancelTranscriptionResult;

    /// Session lifecycle.
    SessionStart => "session/start" (wire::SessionStartParams) -> wire::SessionStartResult;
    /// Wait for core restoration and publication without serializing its history.
    SessionPrepare => "session/prepare" (wire::SessionPrepareParams) -> wire::SessionPrepareResult;
    TurnStart => "turn/start" (wire::TurnStartParams) -> wire::TurnStartResult;
    /// Atomically start an idle session or steer its current foreground turn.
    TurnSubmit => "turn/submit" (wire::TurnSubmitParams) -> wire::TurnSubmitResult;
    ThreadQueueClear => "thread/queue/clear" (wire::ThreadQueueClearParams) -> wire::ThreadQueueClearResult;
    ThreadQueueList => "thread/queue/list" (wire::ThreadQueueListParams) -> wire::ThreadQueueListResult;
    ThreadQueueAdd => "thread/queue/add" (wire::ThreadQueueAddParams) -> wire::ThreadQueueItemResult;
    ThreadQueueUpdate => "thread/queue/update" (wire::ThreadQueueUpdateParams) -> wire::ThreadQueueItemResult;
    ThreadQueueDelete => "thread/queue/delete" (wire::ThreadQueueDeleteParams) -> wire::ThreadQueueDeleteResult;
    ThreadQueueReorder => "thread/queue/reorder" (wire::ThreadQueueReorderParams) -> wire::ThreadQueueMutationResult;
    ThreadQueueStart => "thread/queue/start" (wire::ThreadQueueStartParams) -> wire::ThreadQueueStartResult;
    ThreadQueuePause => "thread/queue/pause" (wire::ThreadQueuePauseParams) -> wire::ThreadQueueMutationResult;
    ThreadQueueResume => "thread/queue/resume" (wire::ThreadQueueResumeParams) -> wire::ThreadQueueMutationResult;
    ThreadQueueImport => "thread/queue/import" (wire::ThreadQueueImportParams) -> wire::ThreadQueueMutationResult;
    ThreadQueueDiscard => "thread/queue/discard" (wire::ThreadQueueDiscardParams) -> wire::ThreadQueueMutationResult;
    /// Atomically hand pending main input to one continuation after a completed turn.
    SessionResumePending => "session/resumePending" (wire::SessionResumePendingParams) -> wire::TurnStartResult;
    /// Explicit foreground continuation of preserved queue input after stop or failure.
    SessionResumeQueued => "session/resumeQueued" (wire::SessionResumeQueuedParams) -> wire::TurnStartResult;
    TurnInterrupt => "turn/interrupt" (wire::TurnInterruptParams) -> wire::SubmissionAck;
    SessionClose => "session/close" (wire::SessionCloseParams) -> Empty;

    /// Standalone dictation accepts live PCM and publishes transcript snapshots.
    EngineDictationStart => "engine/dictationStart" (wire::DictationStartParams) -> Value;
    EngineDictationAppendAudio => "engine/dictationAppendAudio" (wire::DictationAudioParams) -> Value;
    EngineDictationEnd => "engine/dictationEnd" (wire::DictationEndParams) -> Value;

    /// Realtime media negotiation and control are owned by this facade.
    SessionRealtimeCapabilities => "session/realtimeCapabilities" (wire::RealtimeCapabilitiesParams) -> wire::RealtimeCapabilitiesResult;
    SessionRealtimeStart => "session/realtimeStart" (wire::RealtimeStartParams) -> wire::RealtimeStartResult;
    SessionRealtimeStop => "session/realtimeStop" (wire::RealtimeSessionParams) -> Empty;
    SessionRealtimeInterrupt => "session/realtimeInterrupt" (wire::RealtimeSessionParams) -> Empty;
    SessionRealtimeAppendText => "session/realtimeAppendText" (wire::RealtimeAppendTextParams) -> Empty;
    SessionRealtimeAppendSpeech => "session/realtimeAppendSpeech" (wire::RealtimeAppendSpeechParams) -> Empty;
    SessionRealtimeAppendAudio => "session/realtimeAppendAudio" (wire::RealtimeAppendAudioParams) -> Empty;
    SessionRealtimeHandoff => "session/realtimeHandoff" (wire::RealtimeHandoffParams) -> Empty;

    /// Per-session event-stream subscription gate.
    SessionSubscribe => "session/subscribe" (wire::SessionSubscriptionParams) -> wire::SessionSubscriptionResult;
    SessionUnsubscribe => "session/unsubscribe" (wire::SessionSubscriptionParams) -> wire::SessionSubscriptionResult;
    /// Atomically join history to the revisioned live stream. A retained cursor
    /// replays events; a stale cursor returns a fresh snapshot.
    SessionFollow => "session/follow" (wire::SessionFollowParams) -> wire::SessionFollowResult;

    /// Direct-result queries. The reply is the inner envelope minus
    /// `submissionId`, so the caller decodes it with the protocol crate's own
    /// codec rather than a restated copy of the model.
    SessionHistory => "session/history" (wire::SessionRequestParams) -> RawJson;
    SessionListApprovalReviewDenials => "session/listApprovalReviewDenials" (wire::SessionRequestParams) -> wire::ApprovalReviewDenialsResult;
    SessionApproveApprovalReviewDenial => "session/approveApprovalReviewDenial" (wire::SessionApproveApprovalReviewDenialParams) -> Empty;
    /// The files this session changed, folded per file with the line counts its
    /// edits reported. Deliberately separate from anything version-control: it
    /// answers in a plain directory, keeps what a mid-session commit would erase,
    /// and reports nothing the session did not do itself.
    SessionFileChanges => "session/fileChanges" (wire::SessionRequestParams) -> RawJson;
    SessionTakePending => "session/takePending" (wire::SessionRequestParams) -> RawJson;
    SessionQueueSnapshot => "session/queueSnapshot" (wire::SessionRequestParams) -> RawJson;
    SessionMemoryMaintenance => "session/memoryMaintenance" (wire::SessionRequestParams) -> RawJson;
    SessionSetupLifecycle => "session/setupLifecycle" (wire::SessionRequestParams) -> RawJson;
    SessionAgentMemorySnapshotDecisions => "session/agentMemorySnapshotDecisions" (wire::SessionRequestParams) -> RawJson;
    SessionMcpRuntime => "session/mcpRuntime" (wire::SessionRequestParams) -> RawJson;
    SessionRuntimeCapabilities => "session/runtimeCapabilities" (wire::SessionRequestParams) -> RawJson;
    SessionExtensionRuntime => "session/extensionRuntime" (wire::SessionRequestParams) -> RawJson;
    SessionPluginRuntime => "session/pluginRuntime" (wire::SessionRequestParams) -> RawJson;
    SessionInstalledPlugins => "session/installedPlugins" (wire::SessionRequestParams) -> RawJson;
    SessionMigrateBundledDiagram => "session/migrateBundledDiagram" (wire::SessionBundledDiagramMigrationParams) -> wire::SessionBundledDiagramMigrationResult;
    SessionPredictionContext => "session/predictionContext" (wire::SessionRequestParams) -> wire::SessionPredictionContextResult;
    SessionPredictMessage => "session/predictMessage" (wire::SessionPredictMessageParams) -> wire::SessionPredictionResult;
    SessionCancelPrediction => "session/cancelPrediction" (wire::SessionCancelPredictionParams) -> wire::SubmissionAck;
    SessionGenerateTitle => "session/generateTitle" (wire::SessionGenerateTitleParams) -> RawJson;
    SessionTaskList => "session/taskList" (wire::SessionTaskListParams) -> RawJson;
    /// Read a concrete persisted task identity, including archived plan entries.
    SessionTaskIdentity => "session/taskIdentity" (wire::SessionTaskIdentityParams) -> wire::SessionTaskIdentityResult;
    SessionShutdown => "session/shutdown" (wire::SessionRequestParams) -> RawJson;
    /// Read persisted and live turns without activating or resuming their session.
    EngineReadThread => "engine/readThread" (wire::EngineReadThreadParams) -> wire::ThreadReadResult;
    /// Full message/TurnIndex envelope for a saved conversation; no provider or writer.
    EngineReadThreadHistory => "engine/readThreadHistory" (wire::EngineReadThreadHistoryParams) -> RawJson;
    /// One turn's messages, paged by message cursor; no provider or writer.
    EngineReadThreadItems => "engine/readThreadItems" (wire::EngineReadThreadItemsParams) -> RawJson;
    /// Full child message/TurnIndex envelope through a strictly read-only store hook.
    EngineReadSubagentHistory => "engine/readSubagentHistory" (wire::EngineReadSubagentHistoryParams) -> RawJson;
    /// Import normalized external text through the engine's atomic store, without a model.
    EngineImportExternalSession => "engine/importExternalSession" (wire::EngineImportExternalSessionParams) -> wire::EngineImportExternalSessionResult;
    EngineListSessions => "engine/listSessions" (wire::EngineListSessionsParams) -> RawJson;
    /// Metadata-only lookup including archived sessions, without transcript hydration.
    EngineReadSessionSummary => "engine/readSessionSummary" (wire::EngineReadSessionSummaryParams) -> wire::EngineReadSessionSummaryResult;
    /// Read durable usage and activity totals without activating saved sessions.
    EngineProfileActivity => "engine/profileActivity" (wire::EngineProfileActivityParams) -> wire::EngineProfileActivityResult;
    EngineShutdown => "engine/shutdown" (wire::EngineShutdownParams) -> RawJson;
    /// Kick cold-store compression without waiting for filesystem maintenance.
    EngineCompressRollouts => "engine/compressRollouts" (wire::EngineCompressRolloutsParams) -> Empty;
    /// Preview or explicitly apply an auditable stored-history migration.
    EngineMigrateRollouts => "engine/migrateRollouts" (wire::EngineMigrateRolloutsParams) -> RawJson;

    /// Session-less binary introspection: the built-in skill roster is compiled
    /// into the engine, so a host can list it before (or without) any session.
    EngineBuiltinSkills => "engine/builtinSkills" (wire::EngineBuiltinSkillsParams) -> wire::EngineBuiltinSkillsResult;

    /// Session-less binary introspection: the built-in agent roster (with each
    /// role's declared model tier and effort) is compiled into the engine, so a
    /// host lists it instead of hand-copying the roles and drifting from them.
    EngineBuiltinAgents => "engine/builtinAgents" (wire::EngineBuiltinAgentsParams) -> wire::EngineBuiltinAgentsResult;

    /// The durable permission rules the engine owns: read and edited without a
    /// session, because they are endpoint state and outlive every session.
    EnginePermissionRules => "engine/permissionRules" (wire::PermissionRulesParams) -> wire::PermissionRulesResult;
    EngineAddPermissionRules => "engine/addPermissionRules" (wire::PermissionRulesEditParams) -> wire::PermissionRulesResult;
    EngineRemovePermissionRule => "engine/removePermissionRule" (wire::PermissionRuleRemoveParams) -> wire::PermissionRulesResult;

    /// The engine's user config: written ONLY here — a host that edits the file
    /// itself puts the settings page and the running engine on two different
    /// sets of bytes.
    EngineWindowsSandboxReadiness => "engine/windowsSandboxReadiness" (Option<wire::WindowsSandboxReadinessParams>) -> wire::WindowsSandboxReadinessResult;
    EngineStartWindowsSandboxSetup => "engine/startWindowsSandboxSetup" (wire::WindowsSandboxSetupStartParams) -> wire::WindowsSandboxSetupStartResult;

    ConfigRead => "config/read" (wire::ConfigReadParams) -> wire::ConfigResult;
    ConfigValueWrite => "config/value/write" (wire::ConfigValueWriteParams) -> wire::ConfigResult;
    ConfigBatchWrite => "config/batchWrite" (wire::ConfigBatchWriteParams) -> wire::ConfigResult;

    /// The hooks file, same ownership argument: the engine's dispatch roster IS
    /// the file's schema, so the engine validates the write.
    HooksRead => "hooks/read" (wire::HooksReadParams) -> wire::HooksResult;
    HooksWrite => "hooks/write" (wire::HooksWriteParams) -> wire::HooksResult;

    /// BYO model-provider API keys. The definitions live in the config file's
    /// `[modelProviders]`, but the secret never does.
    ModelCredentialWrite => "model/credential/write" (wire::ModelCredentialWriteParams) -> wire::ModelCredentialResult;
    ModelCredentialDelete => "model/credential/delete" (wire::ModelCredentialDeleteParams) -> wire::ModelCredentialResult;
    ModelCredentialRead => "model/credential/read" (wire::ModelCredentialReadParams) -> wire::ModelCredentialReadResult;

    /// Resolve a source-scoped picker choice through the same rules used when a
    /// session acquires its provider, without requiring a model credential.
    ModelSelectionResolve => "model/selection/resolve" (wire::ModelSelectionResolveParams) -> wire::ModelSelectionResolution;

    /// The Qoder subscription. Endpoint state like the config and the rule store:
    /// a settings page signs in with no session bound, and the credential outlives
    /// every session.
    ///
    /// These exist because the engine owns this subscription end to end — the
    /// signer, the credential and the wire translator. A host that carried its own
    /// copy could only keep it in step by hand, and only that host could offer the
    /// subscription at all.
    QoderStatus => "qoder/status" (wire::QoderParams) -> Value;
    QoderLoginStart => "qoder/loginStart" (wire::QoderParams) -> Value;
    QoderLoginPoll => "qoder/loginPoll" (wire::QoderPollParams) -> Value;
    QoderLogout => "qoder/logout" (wire::QoderParams) -> Value;
    QoderImportLocal => "qoder/importLocal" (wire::QoderParams) -> Value;
    /// Store a profile the host read out of a store the engine cannot open.
    QoderAdoptCredential => "qoder/adoptCredential" (wire::QoderAdoptParams) -> Value;
    QoderModels => "qoder/models" (wire::QoderModelsParams) -> Value;
    /// The credit quota, read with the stored token so the token itself never
    /// crosses the wire.
    QoderUsage => "qoder/usage" (wire::QoderParams) -> Value;
    QoderTranscribeAudio => "qoder/transcribeAudio" (wire::SubscriptionTranscribeParams) -> wire::SubscriptionTranscriptResult;
    QoderCancelTranscription => "qoder/cancelTranscription" (wire::SubscriptionCancelTranscriptionParams) -> wire::SubscriptionCancelTranscriptionResult;
    QoderGenerateImage => "qoder/generateImage" (wire::QoderImageParams) -> Value;
    QoderSetThinkingLevel => "qoder/setThinkingLevel" (wire::QoderThinkingParams) -> Value;

    /// The QwenWork subscription uses the same endpoint-owned device-flow and
    /// catalog shapes as Qoder, with its own credential and upstream scene.
    QwenWorkStatus => "qwenwork/status" (wire::QoderParams) -> Value;
    QwenWorkLoginStart => "qwenwork/loginStart" (wire::QoderParams) -> Value;
    QwenWorkLoginPoll => "qwenwork/loginPoll" (wire::QoderPollParams) -> Value;
    QwenWorkLogout => "qwenwork/logout" (wire::QoderParams) -> Value;
    QwenWorkImportLocal => "qwenwork/importLocal" (wire::QoderParams) -> Value;
    QwenWorkAdoptCredential => "qwenwork/adoptCredential" (wire::QoderAdoptParams) -> Value;
    QwenWorkModels => "qwenwork/models" (wire::QoderModelsParams) -> Value;
    /// Spend the stored refresh token. The host calls this only after its own
    /// reread of the official stores failed: refreshing invalidates whichever
    /// copy the official client holds, so it is the last resort, not the first.
    QwenWorkRefreshToken => "qwenwork/refreshToken" (wire::QoderParams) -> Value;
    QwenWorkUsage => "qwenwork/usage" (wire::QoderParams) -> Value;
    QwenWorkCapabilities => "qwenwork/capabilities" (wire::QwenWorkCapabilitiesParams) -> Value;
    QwenWorkGenerateImage => "qwenwork/generateImage" (wire::QwenWorkImageParams) -> Value;
    QwenWorkGenerateVideo => "qwenwork/generateVideo" (wire::QwenWorkVideoParams) -> Value;
    QwenWorkGenerateMusic => "qwenwork/generateMusic" (wire::QwenWorkMusicParams) -> Value;
    QwenWorkSearch => "qwenwork/search" (wire::QwenWorkSearchParams) -> Value;
    QwenWorkCancelOperation => "qwenwork/cancelOperation" (wire::QwenWorkCancelOperationParams) -> Value;
    QwenWorkTranscribeAudio => "qwenwork/transcribeAudio" (wire::SubscriptionTranscribeParams) -> Value;
    QwenWorkCancelTranscription => "qwenwork/cancelTranscription" (wire::SubscriptionCancelTranscriptionParams) -> Value;
    QwenWorkSetThinkingLevel => "qwenwork/setThinkingLevel" (wire::QoderThinkingParams) -> Value;

    /// The QoderWork subscription. The engine owns the device flow, the request
    /// signing and the model catalog; the host opens the browser page and reads
    /// the login this process cannot decrypt. Its OAuth client, deep link and
    /// credential store are its own, so a QoderWork login is a distinct session
    /// from any other subscription on the same machine. There is deliberately no
    /// `refreshToken` method: this gateway exposes none, so a dead login is
    /// recovered by rereading the official store instead.
    QoderWorkStatus => "qoderwork/status" (wire::QoderParams) -> Value;
    QoderWorkLoginStart => "qoderwork/loginStart" (wire::QoderParams) -> Value;
    QoderWorkLoginPoll => "qoderwork/loginPoll" (wire::QoderPollParams) -> Value;
    QoderWorkLogout => "qoderwork/logout" (wire::QoderParams) -> Value;
    QoderWorkImportLocal => "qoderwork/importLocal" (wire::QoderParams) -> Value;
    QoderWorkAdoptCredential => "qoderwork/adoptCredential" (wire::QoderAdoptParams) -> Value;
    QoderWorkModels => "qoderwork/models" (wire::QoderModelsParams) -> Value;
    QoderWorkUsage => "qoderwork/usage" (wire::QoderParams) -> Value;
    QoderWorkTranscribeAudio => "qoderwork/transcribeAudio" (wire::SubscriptionTranscribeParams) -> wire::SubscriptionTranscriptResult;
    QoderWorkCancelTranscription => "qoderwork/cancelTranscription" (wire::SubscriptionCancelTranscriptionParams) -> wire::SubscriptionCancelTranscriptionResult;
    QoderWorkGenerateImage => "qoderwork/generateImage" (wire::QoderImageParams) -> Value;
    QoderWorkSetThinkingLevel => "qoderwork/setThinkingLevel" (wire::QoderThinkingParams) -> Value;

    /// The WorkBuddy subscription. Same endpoint-owned login and catalog shape as
    /// QwenWork, but the gateway signs nothing, so there is no `setThinkingLevel`
    /// side channel: the engine talks OpenAI chat completions to it directly and
    /// declares the effort ladder itself from the cloud catalog.
    ///
    /// `importLocal` always answers "unavailable" — WorkBuddy's only credential
    /// store is encrypted under a key compiled into its own binary, so the host
    /// decrypts it and hands the profile over through `adoptCredential`. The method
    /// exists so a host's import path is uniform across subscriptions and the
    /// reason is reported rather than guessed at.
    /// Cursor browser grants and imported snapshots are engine-owned.
    CursorStatus => "cursor/status" (wire::QoderParams) -> Value;
    CursorLoginStart => "cursor/loginStart" (wire::CursorLoginStartParams) -> Value;
    CursorLoginPoll => "cursor/loginPoll" (wire::CursorLoginFlowParams) -> Value;
    CursorLoginCancel => "cursor/loginCancel" (wire::CursorLoginFlowParams) -> Value;
    CursorAdoptCredential => "cursor/adoptCredential" (wire::CursorAdoptParams) -> Value;
    CursorLogout => "cursor/logout" (wire::CursorLogoutParams) -> Value;
    CursorModels => "cursor/models" (wire::CursorModelsParams) -> Value;
    CursorUsage => "cursor/usage" (wire::QoderParams) -> Value;
    CursorGenerateImage => "cursor/generateImage" (wire::CursorImageParams) -> Value;
    CursorGenerateAudio => "cursor/generateAudio" (wire::CursorAudioParams) -> Value;
    CursorGenerateVideo => "cursor/generateVideo" (wire::CursorVideoParams) -> Value;
    CursorSearch => "cursor/search" (wire::CursorSearchParams) -> Value;
    CursorCancelOperation => "cursor/cancelOperation" (wire::CursorCancelOperationParams) -> Value;

    WorkBuddyStatus => "workbuddy/status" (wire::QoderParams) -> Value;
    WorkBuddyLoginStart => "workbuddy/loginStart" (wire::WorkBuddyLoginStartParams) -> Value;
    WorkBuddyLoginPoll => "workbuddy/loginPoll" (wire::QoderPollParams) -> Value;
    WorkBuddyLogout => "workbuddy/logout" (wire::QoderParams) -> Value;
    WorkBuddyImportLocal => "workbuddy/importLocal" (wire::QoderParams) -> Value;
    WorkBuddyAdoptCredential => "workbuddy/adoptCredential" (wire::QoderAdoptParams) -> Value;
    WorkBuddyModels => "workbuddy/models" (wire::QoderModelsParams) -> Value;
    /// Spend the stored refresh token. The host calls this only after its own
    /// reread of the official store failed, because refreshing rotates the token
    /// the WorkBuddy client itself holds.
    WorkBuddyRefreshToken => "workbuddy/refreshToken" (wire::QoderParams) -> Value;
    WorkBuddyUsage => "workbuddy/usage" (wire::QoderParams) -> Value;
    WorkBuddyCapabilities => "workbuddy/capabilities" (wire::WorkBuddyOperationParams) -> Value;
    WorkBuddyGenerateImage => "workbuddy/generateImage" (wire::WorkBuddyImageParams) -> Value;
    WorkBuddyGenerateVideo => "workbuddy/generateVideo" (wire::WorkBuddyVideoParams) -> Value;
    WorkBuddySynthesizeSpeech => "workbuddy/synthesizeSpeech" (wire::WorkBuddySpeechParams) -> Value;
    WorkBuddyTranscribeAudio => "workbuddy/transcribeAudio" (wire::WorkBuddyTranscribeParams) -> Value;
    WorkBuddySearch => "workbuddy/search" (wire::WorkBuddySearchParams) -> Value;
    WorkBuddyFetchWebPage => "workbuddy/fetchWebPage" (wire::WorkBuddyFetchParams) -> Value;
    WorkBuddyCancelOperation => "workbuddy/cancelOperation" (wire::WorkBuddyCancelParams) -> Value;

    /// The Kimi membership. Same endpoint-owned login and catalog shape as
    /// WorkBuddy — the coding gateway takes a plain bearer or API key and speaks
    /// the ordinary Anthropic wire, so there is no signer, no bridge and no
    /// `setThinkingLevel` side channel.
    ///
    /// Two credentials reach the one gateway and the store keeps whichever the
    /// user last supplied: a console API key through `setApiKey` (the route Kimi
    /// documents for third parties), or a device-flow login through `loginStart`
    /// and `loginPoll`. `importLocal` always answers "unavailable" — Kimi's CLI
    /// keeps its tokens under its own profile and the web session is a browser
    /// cookie, neither of which is a store this engine can read.
    KimiStatus => "kimi/status" (wire::QoderParams) -> Value;
    /// Store a console API key, verifying it against the account route first.
    KimiSetApiKey => "kimi/setApiKey" (wire::KimiApiKeyParams) -> Value;
    KimiLoginStart => "kimi/loginStart" (wire::QoderParams) -> Value;
    KimiLoginPoll => "kimi/loginPoll" (wire::QoderPollParams) -> Value;
    KimiLogout => "kimi/logout" (wire::QoderParams) -> Value;
    KimiImportLocal => "kimi/importLocal" (wire::QoderParams) -> Value;
    KimiModels => "kimi/models" (wire::QoderModelsParams) -> Value;
    /// The membership allowance, read with the stored credential so the secret
    /// never crosses the wire. Answered as rate windows, because Kimi reports
    /// ratios against a pool rather than a count of credits.
    KimiUsage => "kimi/usage" (wire::QoderParams) -> Value;

    /// The Aliyun Bailian Token Plan subscription: a monthly Credits plan whose
    /// coding gateway serves the ordinary Anthropic wire, so like Kimi there is no
    /// signer, no bridge and no thinking-level side channel.
    ///
    /// There is no `loginStart`/`loginPoll` pair here, and that is a fact about the
    /// service rather than an omission: Token Plan authenticates with a plan API
    /// key minted in the Bailian console and offers no OAuth or device flow for
    /// third-party clients. `setApiKey` is therefore the only credential door.
    ///
    /// `usage` answers with the connection and no allowance figures. The gateway
    /// exposes no quota route to a plan key (measured: every candidate path 404s),
    /// and the published TokenPlan OpenAPI wants an Aliyun AccessKey signed with
    /// ACS3-HMAC-SHA256 whose shapes are seat-based. Reporting signed-in with no
    /// numbers lets the host show the console link instead of drawing a bar out of
    /// an invented total.
    BailianStatus => "bailian/status" (wire::QoderParams) -> Value;
    /// Store a plan key, verifying it against the catalog route first.
    BailianSetApiKey => "bailian/setApiKey" (wire::BailianApiKeyParams) -> Value;
    BailianLogout => "bailian/logout" (wire::QoderParams) -> Value;
    BailianModels => "bailian/models" (wire::QoderModelsParams) -> Value;
    BailianUsage => "bailian/usage" (wire::QoderParams) -> Value;
    /// Open the console login: binds a loopback callback listener and answers
    /// with the authorize URL the host opens in a browser. The credential comes
    /// back on the user's clock, so the host polls `consoleStatus` rather than
    /// holding this call open.
    BailianConsoleLogin => "bailian/consoleLogin" (wire::QoderParams) -> Value;
    /// Whether a console session is stored, and which site it belongs to.
    BailianConsoleStatus => "bailian/consoleStatus" (wire::QoderParams) -> Value;
    /// Drop the console session only; the plan key stays.
    BailianConsoleLogout => "bailian/consoleLogout" (wire::QoderParams) -> Value;

    /// MiniMax M Plan: a subscription that spends one shared allowance across text,
    /// image, audio and video, served on the ordinary Anthropic wire with a pasted
    /// subscription key (`sk-cp-…`).
    ///
    /// There is no `loginStart`/`loginPoll` pair, for the same reason Bailian has
    /// none: the plan authenticates only with a console-minted key and offers no
    /// OAuth or device flow to third-party clients, so `setApiKey` is the only
    /// credential door. It takes a `region` because the China and global sites both
    /// mint the same key shape and do not share keys; with no region named the
    /// engine detects the site rather than defaulting it.
    ///
    /// Unlike Bailian, `usage` answers with real allowance figures: the plan exposes
    /// `/v1/token_plan/remains` to the subscription key, so this reports two rate
    /// windows (a five-hour one and a weekly one) rather than a bare connection.
    /// There is no video method here even though Explore and Build tiers serve the
    /// H3 video model: that API is the service's own async task route, which the
    /// host runs directly with the stored key, so it never crosses this facade.
    MinimaxStatus => "minimax/status" (wire::QoderParams) -> Value;
    /// Store a subscription key, detecting its site and verifying it first.
    MinimaxSetApiKey => "minimax/setApiKey" (wire::MinimaxApiKeyParams) -> Value;
    MinimaxLogout => "minimax/logout" (wire::QoderParams) -> Value;
    MinimaxModels => "minimax/models" (wire::QoderModelsParams) -> Value;
    MinimaxUsage => "minimax/usage" (wire::QoderParams) -> Value;
    /// Hand the stored subscription credential to the host for the plan's media
    /// routes, which the host runs itself rather than the engine.
    ///
    /// Explore and Build tiers serve the H3 video model, and video generation is the
    /// service's own async task protocol — submit, poll, then download a URL. That is
    /// not something the provider layer speaks, and re-implementing the host's
    /// existing task polling, container validation, size caps and cancellation for it
    /// would duplicate machinery that already works. So the host calls the video
    /// endpoint directly and needs the key; this is that hand-off, on the same
    /// precedent as `model/credential/read`.
    ///
    /// The credential goes to the HOST only. It must never reach a renderer or a
    /// tool result, and the host keeps it out of both.
    ///
    /// Answers `{ "ok": false }` when signed out rather than an error, so a
    /// conversation on this source without a key reads as "not configured" instead
    /// of as a failed call.
    MinimaxMediaCredential => "minimax/mediaCredential" (wire::QoderParams) -> Value;

    /// The Bailian Token Plan's speech route runs on the plan gateway with the plan
    /// key, so the host — which runs that protocol — needs the stored credential, on
    /// the same precedent as `minimax/mediaCredential`.
    ///
    /// The credential goes to the HOST only. It must never reach a renderer or a
    /// tool result, and the host keeps it out of both.
    ///
    /// Answers `{ "ok": false }` when signed out rather than an error, so a
    /// conversation on this source without a key reads as "not configured" instead
    /// of as a failed call.
    BailianMediaCredential => "bailian/mediaCredential" (wire::QoderParams) -> Value;

    /// The Zhipu GLM Coding Plan subscription: a monthly credit-metered plan whose
    /// coding gateway serves the ordinary Anthropic wire, so like Bailian there is
    /// no signer, no bridge and no thinking-level side channel.
    ///
    /// There is no `loginStart`/`loginPoll` pair here, and that is a fact about the
    /// service rather than an omission: Coding Plan authenticates with a plan API
    /// key minted on the plan page and offers no OAuth or device flow for
    /// third-party clients. `setApiKey` is therefore the only credential door.
    ///
    /// `usage` answers with the connection and no allowance figures. The gateway
    /// exposes no quota route to a plan key (measured: every candidate path 404s),
    /// and the plan's credit totals are published per tier on its web console
    /// rather than per account on an endpoint. Reporting signed-in with no numbers
    /// lets the host show the console link instead of drawing a bar out of an
    /// invented total.
    ZhipuStatus => "zhipu/status" (wire::QoderParams) -> Value;
    /// Store a plan key, verifying it against the catalog route first.
    ZhipuSetApiKey => "zhipu/setApiKey" (wire::ZhipuApiKeyParams) -> Value;
    ZhipuLogout => "zhipu/logout" (wire::QoderParams) -> Value;
    ZhipuModels => "zhipu/models" (wire::QoderModelsParams) -> Value;
    ZhipuUsage => "zhipu/usage" (wire::QoderParams) -> Value;

    /// The Volcengine Ark Coding Plan subscription: a monthly plan whose gateway
    /// serves the ordinary Anthropic wire, so like Bailian and MiniMax there is no
    /// signer and no local bridge.
    ///
    /// There is no `loginStart`/`loginPoll` pair here, and that is a fact about the
    /// service rather than an omission: Coding Plan authenticates with a plan API
    /// key minted on the plan page and offers no OAuth or device flow for
    /// third-party clients. `setApiKey` is therefore the only credential door.
    ///
    /// `usage` answers with the plan's own remaining-allowance windows when the
    /// gateway publishes them to the plan key, in the same rate-window shape the
    /// Codex and Kimi gauges draw; when it does not, it answers signed-in with no
    /// numbers and the host renders the console link instead.
    ArkStatus => "ark/status" (wire::QoderParams) -> Value;
    /// Store a plan key, verifying it against the catalog route first.
    ArkSetApiKey => "ark/setApiKey" (wire::ArkApiKeyParams) -> Value;
    ArkLogout => "ark/logout" (wire::QoderParams) -> Value;
    ArkModels => "ark/models" (wire::QoderModelsParams) -> Value;
    ArkUsage => "ark/usage" (wire::QoderParams) -> Value;

    /// The console session that unlocks the plan's allowance figures. The plan key
    /// authenticates inference only; the allowance lives behind a control-plane
    /// interface that refuses the key, and the service publishes one supported way
    /// for a third-party client to reach it — the browser authorize flow its own
    /// CLI walks. `consoleLogin` binds a loopback listener and answers the authorize
    /// URL; `consoleStatus` is what the host polls while the browser round trip runs
    /// on the user's clock; `consoleLogout` drops the session without touching the
    /// plan key.
    ArkConsoleLogin => "ark/consoleLogin" (wire::QoderParams) -> Value;
    ArkConsoleStatus => "ark/consoleStatus" (wire::QoderParams) -> Value;
    ArkConsoleLogout => "ark/consoleLogout" (wire::QoderParams) -> Value;

    /// The ChatGPT/Codex subscription's model catalog, for the same reason: which
    /// models the plan serves and how wide each one's context is is only knowable
    /// from the service, and the engine already needs the answer to declare a
    /// session's capability set. A host that fetched its own copy would be a
    /// second reader of the same truth, drifting from the one the sessions use.
    CodexModels => "codex/models" (wire::CodexModelsParams) -> Value;
    /// Transcribe audio with the selected subscription's own credentials.
    CodexTranscribeAudio => "codex/transcribeAudio" (wire::SubscriptionTranscribeParams) -> wire::SubscriptionTranscriptResult;
    CodexCancelTranscription => "codex/cancelTranscription" (wire::SubscriptionCancelTranscriptionParams) -> wire::SubscriptionCancelTranscriptionResult;
    ClaudeTranscribeAudio => "claude/transcribeAudio" (wire::SubscriptionTranscribeParams) -> wire::SubscriptionTranscriptResult;
    ClaudeCancelTranscription => "claude/cancelTranscription" (wire::SubscriptionCancelTranscriptionParams) -> wire::SubscriptionCancelTranscriptionResult;

    /// Account, models and native capabilities supplied by the direct cloud source.
    KiroStatus => "kiro/status" (wire::QoderParams) -> Value;
    KiroLoginStart => "kiro/loginStart" (wire::QoderParams) -> Value;
    KiroLoginPoll => "kiro/loginPoll" (wire::TraeLoginFlowParams) -> Value;
    KiroLoginCancel => "kiro/loginCancel" (wire::TraeLoginFlowParams) -> Value;
    KiroAdoptCredential => "kiro/adoptCredential" (wire::KiroAdoptParams) -> Value;
    KiroLogout => "kiro/logout" (wire::QoderParams) -> Value;
    KiroModels => "kiro/models" (wire::QoderModelsParams) -> Value;
    KiroUsage => "kiro/usage" (wire::QoderParams) -> Value;
    KiroCapabilities => "kiro/capabilities" (wire::QoderParams) -> Value;
    KiroSearch => "kiro/search" (wire::TraeSearchParams) -> Value;
    KiroCancelOperation => "kiro/cancelOperation" (wire::WorkBuddyCancelParams) -> Value;
    TraeStatus => "trae/status" (wire::QoderParams) -> Value;
    TraeLoginStart => "trae/loginStart" (wire::TraeLoginStartParams) -> Value;
    TraeLoginPoll => "trae/loginPoll" (wire::TraeLoginFlowParams) -> Value;
    TraeLoginCancel => "trae/loginCancel" (wire::TraeLoginFlowParams) -> Value;
    TraeAdoptCredential => "trae/adoptCredential" (wire::TraeAdoptParams) -> Value;
    TraeLogout => "trae/logout" (wire::QoderParams) -> Value;
    TraeModels => "trae/models" (wire::QoderModelsParams) -> Value;
    TraeUsage => "trae/usage" (wire::QoderParams) -> Value;
    TraeCapabilities => "trae/capabilities" (wire::QoderParams) -> Value;
    TraeSearch => "trae/search" (wire::TraeSearchParams) -> Value;
    TraeGenerateImage => "trae/generateImage" (wire::TraeImageParams) -> Value;
    TraeGenerateVideo => "trae/generateVideo" (wire::TraeVideoParams) -> Value;
    TraeTranscribeAudio => "trae/transcribeAudio" (wire::SubscriptionTranscribeParams) -> Value;
    TraeCancelOperation => "trae/cancelOperation" (wire::WorkBuddyCancelParams) -> Value;

    AntigravityStatus => "antigravity/status" (wire::QoderParams) -> Value;
    AntigravityLoginStart => "antigravity/loginStart" (wire::AntigravityLoginStartParams) -> Value;
    AntigravityLoginPoll => "antigravity/loginPoll" (wire::AntigravityLoginFlowParams) -> Value;
    AntigravityLoginCancel => "antigravity/loginCancel" (wire::AntigravityLoginFlowParams) -> Value;
    AntigravityAdoptCredential => "antigravity/adoptCredential" (wire::AntigravityAdoptParams) -> Value;
    AntigravityLogout => "antigravity/logout" (wire::QoderParams) -> Value;
    AntigravityModels => "antigravity/models" (wire::QoderModelsParams) -> Value;
    AntigravityUsage => "antigravity/usage" (wire::QoderParams) -> Value;
    AntigravityCapabilities => "antigravity/capabilities" (wire::QoderParams) -> Value;
    AntigravityGenerateImage => "antigravity/generateImage" (wire::AntigravityImageParams) -> Value;
    AntigravitySearch => "antigravity/search" (wire::AntigravitySearchParams) -> Value;
    AntigravityTranscribeAudio => "antigravity/transcribeAudio" (wire::AntigravityTranscribeParams) -> Value;
    AntigravityCancelOperation => "antigravity/cancelOperation" (wire::WorkBuddyCancelParams) -> Value;

    /// The Claude subscription lineup, for the third time the same reason: which
    /// models the subscription serves and what effort ladder each one honours is
    /// knowledge this engine already holds to declare a session's capability set.
    /// A host that kept its own table was a second declaration of one fact, and it
    /// drifted — the table stopped at `max` while sessions accepted `xhigh`, so a
    /// level the engine would have taken could not be picked.
    SubscriptionModels => "subscription/models" (wire::SubscriptionModelsParams) -> Value;

    /// Read subscription usage through the inference credential store.
    ClaudeUsage => "claude/usage" (wire::QoderParams) -> Value;

    /// Side tasks.
    SessionCreateSideTask => "session/createSideTask" (wire::SessionCreateSideTaskParams) -> RawJson;
    SessionStartSideTask => "session/startSideTask" (wire::SessionStartSideTaskParams) -> wire::SessionStartResult;
    SessionForkLive => "session/forkLive" (wire::SessionForkLiveParams) -> wire::SessionStartResult;
    /// Replace one live session's complete inference selection without rebinding it.
    SessionBeginModelUpdate => "session/beginModelUpdate" (wire::SessionBeginModelUpdateParams) -> wire::SessionBeginModelUpdateResult;
    SessionFailModelUpdate => "session/failModelUpdate" (wire::SessionFailModelUpdateParams) -> wire::SessionFailModelUpdateResult;
    SessionReconfigureModel => "session/reconfigureModel" (wire::SessionReconfigureModelParams) -> wire::SessionReconfigureModelResult;
    SessionModelRuntime => "session/modelRuntime" (wire::SessionModelRuntimeParams) -> wire::SessionModelRuntimeResult;

    /// Remaining no-arg direct-result queries.
    SessionCost => "session/cost" (wire::SessionRequestParams) -> RawJson;
    SessionContextSnapshot => "session/contextSnapshot" (wire::SessionRequestParams) -> RawJson;
    SessionSubagents => "session/subagents" (wire::SessionRequestParams) -> RawJson;
    SessionSubagentLifecycle => "session/subagentLifecycle" (wire::SessionRequestParams) -> RawJson;
    SessionBackgroundProcesses => "session/backgroundProcesses" (wire::SessionRequestParams) -> RawJson;
    SessionBackgroundProcessOutput => "session/backgroundProcessOutput" (wire::SessionBackgroundProcessOutputParams) -> RawJson;
    SessionStopBackgroundProcess => "session/stopBackgroundProcess" (wire::SessionBackgroundProcessStopParams) -> RawJson;
    SessionQueued => "session/queued" (wire::SessionRequestParams) -> RawJson;
    SessionCollaborationMode => "session/collaborationMode" (wire::SessionPlanningAddressParams) -> wire::PlanningResult;
    SessionSetCollaborationMode => "session/setCollaborationMode" (wire::SessionCollaborationModeParams) -> wire::PlanningResult;
    SessionPlans => "session/plans" (wire::SessionPlanningAddressParams) -> wire::PlanningResult;
    SessionReadPlanDocument => "session/readPlanDocument" (wire::SessionReadPlanDocumentParams) -> wire::PlanningResult;
    SessionSavePlanDocument => "session/savePlanDocument" (wire::SessionSavePlanDocumentParams) -> wire::PlanningResult;
    SessionAcceptPlan => "session/acceptPlan" (wire::SessionAcceptPlanParams) -> wire::PlanningResult;
    SessionPlanFeedback => "session/planFeedback" (wire::SessionPlanFeedbackParams) -> wire::PlanningResult;
    SessionCancelPlanImplementation => "session/cancelPlanImplementation" (wire::SessionPlanDocumentParams) -> wire::PlanningResult;
    SessionSettlePlanImplementation => "session/settlePlanImplementation" (wire::SessionSettlePlanImplementationParams) -> wire::PlanningResult;
    SessionAnswerUserInput => "session/answerUserInput" (wire::SessionAnswerUserInputParams) -> wire::PlanningResult;
    SessionGoal => "session/goal" (wire::SessionGoalParams) -> wire::SessionGoalResult;
    SessionReadGoalObjective => "session/readGoalObjective" (wire::SessionReadGoalObjectiveParams) -> wire::SessionReadGoalObjectiveResult;
    SessionClearGoal => "session/clearGoal" (wire::SessionGoalParams) -> wire::SessionGoalResult;

    /// Parameterized direct-result queries.
    SessionSetGoal => "session/setGoal" (wire::SessionSetGoalParams) -> wire::SessionGoalResult;
    SessionSubagentTranscript => "session/subagentTranscript" (wire::SessionSubagentTranscriptParams) -> RawJson;
    SessionSendSubagentMessage => "session/sendSubagentMessage" (wire::SessionSendSubagentMessageParams) -> RawJson;
    SessionMcpAuthorizationStatus => "session/mcpAuthorizationStatus" (wire::SessionMcpServerParams) -> RawJson;
    SessionAgentMemorySnapshotDecision => "session/agentMemorySnapshotDecision" (wire::SessionRequestIdParams) -> RawJson;

    /// MCP request/response calls. Long enough to be worth correlating, short
    /// enough to answer once, so they reply directly instead of acking.
    SessionCallMcpTool => "session/callMcpTool" (wire::SessionMcpToolCallParams) -> RawJson;
    SessionReadMcpResource => "session/readMcpResource" (wire::SessionMcpResourceParams) -> RawJson;
    SessionListMcpPrompts => "session/listMcpPrompts" (wire::SessionRequestParams) -> RawJson;
    SessionGetMcpPrompt => "session/getMcpPrompt" (wire::SessionMcpPromptParams) -> RawJson;

    /// Queue mutations. Each answers with the authoritative post-operation
    /// snapshot, which is the whole reason they are requests and not acks: an
    /// ack would leave the caller guessing what the queue now looks like.
    SessionEditQueued => "session/editQueued" (wire::SessionQueueEditParams) -> RawJson;
    SessionRemoveQueued => "session/removeQueued" (wire::SessionQueueRemoveParams) -> RawJson;
    SessionReorderQueued => "session/reorderQueued" (wire::SessionQueueReorderParams) -> RawJson;

    /// Ack-reply methods: write/long-running ops whose reply envelopes stream on
    /// `session/rawEvent` (same contract as `turn/start`); the JSON-RPC result is
    /// the ack.
    SessionApprove => "session/approve" (wire::SessionRequestResolutionParams) -> wire::SubmissionAck;
    SessionResolveElicitation => "session/resolveElicitation" (wire::SessionRequestResolutionParams) -> wire::SubmissionAck;
    /// Optional questions do not park the turn; answers target their original turn.
    SessionAnswerQuestion => "session/answerQuestion" (wire::SessionAnswerQuestionParams) -> RawJson;
    SessionGetPendingQuestions => "session/getPendingQuestions" (wire::SessionPendingQuestionsParams) -> RawJson;
    SessionResolveMcpAuthorization => "session/resolveMcpAuthorization" (wire::SessionRequestResolutionParams) -> wire::SubmissionAck;
    SessionResolveMcpElicitation => "session/resolveMcpElicitation" (wire::SessionRequestResolutionParams) -> wire::SubmissionAck;
    SessionCancelRequest => "session/cancelRequest" (wire::SessionRequestIdParams) -> wire::SubmissionAck;
    SessionRevokeMcpAuthorization => "session/revokeMcpAuthorization" (wire::SessionMcpServerParams) -> wire::SubmissionAck;
    SessionReconnectMcpServer => "session/reconnectMcpServer" (wire::SessionMcpServerParams) -> wire::SubmissionAck;
    SessionRemoveMcpServer => "session/removeMcpServer" (wire::SessionMcpServerParams) -> wire::SubmissionAck;
    SessionUpsertMcpServer => "session/upsertMcpServer" (wire::SessionMcpServerUpsertParams) -> wire::SubmissionAck;
    SessionReloadMcpServers => "session/reloadMcpServers" (wire::SessionMcpServersReloadParams) -> wire::SubmissionAck;
    SessionSetInstalledPluginEnabled => "session/setInstalledPluginEnabled" (wire::SessionPluginEnabledParams) -> wire::SubmissionAck;
    SessionUninstallPlugin => "session/uninstallPlugin" (wire::SessionPluginUninstallParams) -> wire::SubmissionAck;
    SessionInstallPlugin => "session/installPlugin" (wire::SessionPluginInstallParams) -> wire::SubmissionAck;
    SessionPluginCatalog => "session/pluginCatalog" (wire::SessionPluginCatalogParams) -> wire::SubmissionAck;
    SessionResolveDynamicToolCall => "session/resolveDynamicToolCall" (wire::SessionDynamicToolCallParams) -> wire::SubmissionAck;
    SessionRewind => "session/rewind" (wire::SessionRewindParams) -> wire::SubmissionAck;
    /// What a `session/rewind` would do to the working tree, having done nothing.
    /// A getter, not an ack: the caller needs the answer before it can ask the
    /// user to confirm a destructive file restore.
    SessionRewindPreview => "session/rewindPreview" (wire::SessionRewindPreviewParams) -> RawJson;
    SessionSteer => "session/steer" (wire::SessionSteerParams) -> wire::SubmissionAck;
    /// Persist one host-rendered message as a completed turn without running a
    /// model, so parked conversations still read through ordinary history.
    SessionAppendDisplayMessage => "session/appendDisplayMessage" (wire::SessionAppendDisplayMessageParams) -> wire::SubmissionAck;
    SessionSetSkills => "session/setSkills" (wire::SessionSetSkillsParams) -> wire::SubmissionAck;
    SessionSetModel => "session/setModel" (wire::SessionSetModelParams) -> wire::SubmissionAck;
    SessionSetPermissionMode => "session/setPermissionMode" (wire::SessionPermissionModeParams) -> wire::SubmissionAck;
    SessionReloadPermissionRules => "session/reloadPermissionRules" (wire::PermissionRulesReloadParams) -> wire::PermissionRulesResult;
    SessionSetRuntimePolicy => "session/setRuntimePolicy" (wire::SessionSetRuntimePolicyParams) -> wire::SubmissionAck;
    SessionSetRuntimeModel => "session/setRuntimeModel" (wire::SessionSetRuntimeModelParams) -> wire::SubmissionAck;
    SessionUpdateConfig => "session/updateConfig" (wire::SessionConfigUpdateParams) -> wire::SubmissionAck;
    SessionCompactStart => "session/compactStart" (wire::SessionCompactParams) -> wire::SubmissionAck;
    /// Self-review of the working tree: a streaming turn, so it acks like one.
    SessionReviewStart => "session/reviewStart" (wire::SessionReviewParams) -> wire::SubmissionAck;
    SessionSideQuestion => "session/sideQuestion" (wire::SessionSideQuestionParams) -> wire::SubmissionAck;
    SessionDreamNow => "session/dreamNow" (wire::SessionRequestParams) -> wire::SubmissionAck;
    SessionRunSetup => "session/runSetup" (wire::SessionRunSetupParams) -> wire::SubmissionAck;
    SessionResolveAgentMemorySnapshotDecision => "session/resolveAgentMemorySnapshotDecision" (wire::SessionMemorySnapshotDecisionParams) -> wire::SubmissionAck;
    SessionSettleAgentMemorySnapshotUnavailable => "session/settleAgentMemorySnapshotUnavailable" (wire::SessionMemorySnapshotUnavailableParams) -> wire::SubmissionAck;
    SessionResume => "session/resumeSession" (wire::SessionThreadParams) -> wire::SubmissionAck;
    SessionFork => "session/forkSession" (wire::SessionThreadParams) -> wire::SubmissionAck;
    SessionArchive => "session/archiveSession" (wire::SessionThreadParams) -> wire::SubmissionAck;
    SessionUnarchive => "session/unarchiveSession" (wire::SessionThreadParams) -> wire::SubmissionAck;
    SessionDelete => "session/deleteSession" (wire::SessionThreadParams) -> wire::SubmissionAck;
    SessionUpdateTitle => "session/updateTitle" (wire::SessionUpdateTitleParams) -> wire::SubmissionAck;
    SessionRenameThread => "session/renameThread" (wire::SessionRenameThreadParams) -> wire::SubmissionAck;
    SessionUpdateThreadHostMetadata => "session/updateThreadHostMetadata" (wire::SessionUpdateThreadHostMetadataParams) -> wire::SubmissionAck;
}

/// The `requests` section of the schema bundle, derived from the table.
pub fn request_schemas() -> Value {
    let mut requests = Map::new();
    for spec in ALL_METHODS {
        requests.insert(
            spec.name.to_string(),
            serde_json::json!({
                "params": (spec.params_schema)(),
                "result": (spec.result_schema)(),
            }),
        );
    }
    Value::Object(requests)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_method_name_is_declared_once() {
        let mut seen = std::collections::BTreeSet::new();
        for spec in ALL_METHODS {
            assert!(seen.insert(spec.name), "duplicate method: {}", spec.name);
        }
    }

    #[test]
    fn pass_through_results_publish_any() {
        assert_eq!(<Value as FacadeResult>::schema(), Value::Bool(true));
    }

    #[test]
    fn typed_results_publish_an_object_schema() {
        let schema = <wire::SubmissionAck as FacadeResult>::schema();
        assert_eq!(schema["type"], serde_json::json!("object"));
        // Including the finished-is-the-answer result: a reply that carries no
        // fields is still an object, and the bundle says so.
        let empty = <Empty as FacadeResult>::schema();
        assert_eq!(empty["type"], serde_json::json!("object"));
    }
}
