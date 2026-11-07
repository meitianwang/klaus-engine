//! Does `klaus` reach every method the engine declares — and only through the
//! one door?
//!
//! Two properties, and they decay in opposite directions.
//!
//! COVERAGE decays by addition: a method added to [`ALL_METHODS`] leaves every
//! existing test green while the CLI silently stops being a complete client of
//! this engine. The table below is checked against `ALL_METHODS` for set
//! equality, so a new method fails this test until someone says how `klaus`
//! reaches it or why it deliberately does not.
//!
//! SINGULARITY decays by shortcut: the reason those 8 methods sat unused for so
//! long is that the previous host never had to name a method at all — it built
//! the engine's inner op itself and pushed it through a raw submit. So this file
//! also asserts the shortcut is gone: the CLI names no `OpKind`, holds no
//! `ProtocolClient`, and touches the facade client in exactly two files. It
//! still depends on the protocol crate, and must — the events and direct results
//! it decodes are that crate's model. Reading the wire is not the same
//! permission as writing it.
//!
//! What this cannot catch — a call sitting in unreachable code — is what
//! `end_to_end` and `cli_surface` are for.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use klaus_agent_server::app_server::methods::ALL_METHODS;

/// How `klaus` reaches one declared method.
enum Reach {
    /// A command names the method's marker type under `src/`, which is the only
    /// way to call it: the marker is what binds the params and result types, so
    /// naming it means the compiler checked both ends.
    Called,
    /// Deliberately not called, for the reason given.
    Withheld(&'static str),
}

/// Every declared method, and how the CLI reaches it.
///
/// Keyed on the marker type's own name rather than the wire name, because the
/// marker is what a caller writes. `MethodSpec::marker` exists for exactly this.
const COVERAGE: &[(&str, Reach)] = &[
    ("EngineReadThreadFileChanges", Reach::Withheld("The CLI does not present saved file-change cards")),
    ("KiroStatus", Reach::Withheld("The CLI has no account-management command")),
    ("KiroLoginStart", Reach::Withheld("Browser authorization uses the named account facade")),
    ("KiroLoginPoll", Reach::Withheld("Browser authorization uses the named account facade")),
    ("KiroLoginCancel", Reach::Withheld("Browser authorization uses the named account facade")),
    ("KiroAdoptCredential", Reach::Withheld("The desktop imports the signed-in local account")),
    ("KiroLogout", Reach::Withheld("The CLI has no account-management command")),
    ("KiroModels", Reach::Withheld("The CLI accepts concrete source-qualified model IDs")),
    ("KiroUsage", Reach::Withheld("The CLI has no account quota gauge")),
    ("KiroCapabilities", Reach::Withheld("The CLI does not display source capability settings")),
    ("KiroSearch", Reach::Withheld("The search tool calls the selected subscription backend")),
    ("KiroCancelOperation", Reach::Withheld("The CLI does not start standalone source operations")),
    ("TraeStatus", Reach::Withheld("The CLI has no account-management command")),
    ("TraeLoginStart", Reach::Withheld("Browser authorization uses the named account facade")),
    ("TraeLoginPoll", Reach::Withheld("Browser authorization uses the named account facade")),
    ("TraeLoginCancel", Reach::Withheld("Browser authorization uses the named account facade")),
    ("TraeAdoptCredential", Reach::Withheld("The desktop imports the signed-in local account")),
    ("TraeLogout", Reach::Withheld("The CLI has no account-management command")),
    ("TraeModels", Reach::Withheld("The CLI accepts concrete source-qualified model IDs")),
    ("TraeUsage", Reach::Withheld("The CLI has no account quota gauge")),
    ("TraeCapabilities", Reach::Withheld("The CLI does not display source capability settings")),
    ("TraeSearch", Reach::Withheld("The search tool calls the selected subscription backend")),
    ("TraeGenerateImage", Reach::Withheld("Image generation uses the named host method")),
    ("TraeGenerateVideo", Reach::Withheld("Video generation uses the named host method")),
    ("TraeTranscribeAudio", Reach::Withheld("The CLI does not record standalone dictation audio")),
    ("TraeCancelOperation", Reach::Withheld("The CLI does not start standalone source operations")),
    ("AntigravityStatus", Reach::Withheld("The CLI has no account-management command")),
    ("AntigravityLoginStart", Reach::Withheld("Browser authorization is managed through the named account facade")),
    ("AntigravityLoginPoll", Reach::Withheld("Browser authorization is managed through the named account facade")),
    ("AntigravityLoginCancel", Reach::Withheld("Browser authorization is managed through the named account facade")),
    ("AntigravityAdoptCredential", Reach::Withheld("The desktop imports the signed-in account")),
    ("AntigravityLogout", Reach::Withheld("The CLI has no account-management command")),
    ("AntigravityModels", Reach::Withheld("The CLI accepts concrete source-qualified model ids")),
    ("AntigravityUsage", Reach::Withheld("The CLI draws no account quota gauge")),
    ("AntigravityCapabilities", Reach::Withheld("The CLI does not display native media settings")),
    ("AntigravityGenerateImage", Reach::Withheld("Image generation is available through the named host method")),
    ("AntigravitySearch", Reach::Withheld("The search tool calls the engine-owned source backend")),
    ("AntigravityTranscribeAudio", Reach::Withheld("The CLI does not record standalone dictation audio")),
    ("AntigravityCancelOperation", Reach::Withheld("The CLI does not start standalone source operations")),
    ("QoderTranscribeAudio", Reach::Withheld("The CLI does not record standalone dictation audio")),
    ("QoderCancelTranscription", Reach::Withheld("The CLI does not start standalone dictation operations")),
    ("QoderWorkTranscribeAudio", Reach::Withheld("The CLI does not record standalone dictation audio")),
    ("QoderWorkCancelTranscription", Reach::Withheld("The CLI does not start standalone dictation operations")),
    ("CursorStatus", Reach::Withheld("The CLI has no Cursor account-management command")),
    ("CursorLoginStart", Reach::Withheld("The CLI has no account browser-login command")),
    ("CursorLoginPoll", Reach::Withheld("The CLI does not poll account login flows")),
    ("CursorLoginCancel", Reach::Withheld("The CLI does not own account browser-login flows")),
    ("CursorAdoptCredential", Reach::Withheld("The CLI does not decrypt or import application credentials")),
    ("CursorLogout", Reach::Withheld("The CLI has no Cursor account-management command")),
    ("CursorModels", Reach::Withheld("The CLI accepts concrete model selections and does not list account catalogs")),
    ("CursorUsage", Reach::Withheld("The CLI draws no account quota gauge")),
    ("CursorGenerateImage", Reach::Withheld("The CLI has no standalone image-generation command")),
    ("CursorGenerateAudio", Reach::Withheld("The CLI has no standalone speech-generation command")),
    ("CursorGenerateVideo", Reach::Withheld("The CLI has no standalone video-generation command")),
    ("CursorSearch", Reach::Withheld("The configured search tool calls the engine-owned native backend directly")),
    ("CursorCancelOperation", Reach::Withheld("The CLI does not start standalone account operations")),
    ("WorkBuddyCapabilities", Reach::Withheld("The CLI does not display standalone media settings")),
    ("WorkBuddyGenerateImage", Reach::Withheld("Media generation is exposed by the named facade for host tools")),
    ("WorkBuddyGenerateVideo", Reach::Withheld("Media generation is exposed by the named facade for host tools")),
    ("WorkBuddySynthesizeSpeech", Reach::Withheld("The CLI has no standalone speech-generation command")),
    ("WorkBuddyTranscribeAudio", Reach::Withheld("The CLI does not own audio capture")),
    ("WorkBuddySearch", Reach::Withheld("The configured search tool calls the engine-owned backend directly")),
    ("WorkBuddyFetchWebPage", Reach::Withheld("The CLI uses its ordinary web fetch tool")),
    ("WorkBuddyCancelOperation", Reach::Withheld("The CLI does not start standalone account operations")),
    // ---- Connection and session lifetime -----------------------------------
    ("Initialize", Reach::Called),
    ("EngineCompleteText", Reach::Withheld("Interactive hosts use bounded standalone text inference for dictation refinement and correction learning.")),
    ("EngineCancelTextCompletion", Reach::Withheld("Interactive hosts own cancellation of their standalone text inference.")),
    ("EngineMigrateRollouts", Reach::Called),
    ("EngineCompressRollouts", Reach::Withheld("Cold compression is an endpoint background worker; hosts may kick the same worker through the named maintenance facade without changing its cold threshold.")),
    ("SessionStart", Reach::Called),
    ("SessionPrepare", Reach::Withheld("The desktop prepares a retained endpoint before turn admission; the CLI binds resume or fork through its lifecycle commands before its first session operation.")),
    ("TurnStart", Reach::Called),
    ("TurnSubmit", Reach::Withheld("The foreground CLI owns its serial turn loop; multi-client hosts use atomic submission to arbitrate idle starts and steering.")),
    ("ThreadQueueClear", Reach::Withheld("Interactive hosts atomically clear durable next-turn input while retaining identity receipts.")),
    ("ThreadQueueList", Reach::Withheld("Interactive hosts own durable next-turn queue presentation; the CLI owns a foreground turn loop.")),
    ("ThreadQueueAdd", Reach::Withheld("Interactive hosts enqueue future turns through the durable facade.")),
    ("ThreadQueueUpdate", Reach::Withheld("Interactive hosts edit durable next-turn input.")),
    ("ThreadQueueDelete", Reach::Withheld("Interactive hosts remove durable next-turn input.")),
    ("ThreadQueueReorder", Reach::Withheld("Interactive hosts order durable next-turn input.")),
    ("ThreadQueueStart", Reach::Withheld("Interactive hosts explicitly dispatch durable next-turn input.")),
    ("ThreadQueuePause", Reach::Withheld("Interactive hosts pause future turn dispatch.")),
    ("ThreadQueueResume", Reach::Withheld("Interactive hosts resume paused future turns.")),
    ("ThreadQueueImport", Reach::Withheld("Host migrations transfer existing queues and receipts to engine persistence.")),
    ("ThreadQueueDiscard", Reach::Withheld("Hosts discard ephemeral-thread queue payloads and receipts when ending their lifetime.")),
    ("SessionResumePending", Reach::Withheld("The interactive host owns post-turn pending input continuation; the CLI submits its own foreground turns.")),
    ("SessionResumeQueued", Reach::Withheld("The interactive host explicitly continues preserved queue input; the CLI submits foreground turns and manages its queue separately.")),
    ("TurnInterrupt", Reach::Called),
    ("SessionClose", Reach::Called),
    // The event stream is opt-OUT: a session's notifications flow to this
    // connection unless it says otherwise, so subscribing would ask for what it
    // already has. Unsubscribing is worse than redundant — the CLI follows one
    // session for as long as it holds it, and muting its own stream mid-turn
    // would drop the replies it is parked on.
    (
        "SessionSubscribe",
        Reach::Withheld("notifications are opt-out, so this connection is already subscribed"),
    ),
    (
        "SessionUnsubscribe",
        Reach::Withheld(
            "the CLI follows the one session it holds; muting it would drop its replies",
        ),
    ),
    (
        "SessionFollow",
        Reach::Withheld(
            "the desktop remote follower joins history to a resumable live stream; the foreground CLI already owns its session stream",
        ),
    ),
    ("SessionRealtimeCapabilities", Reach::Withheld("The desktop owns negotiated realtime media and its sideband control lifecycle.")),
    ("SessionRealtimeStart", Reach::Withheld("The desktop owns negotiated realtime media and its sideband control lifecycle.")),
    ("SessionRealtimeStop", Reach::Withheld("The desktop owns negotiated realtime media and its sideband control lifecycle.")),
    ("SessionRealtimeInterrupt", Reach::Withheld("The desktop owns negotiated realtime media and its sideband control lifecycle.")),
    ("SessionRealtimeAppendText", Reach::Withheld("The desktop owns negotiated realtime media and its sideband control lifecycle.")),
    ("SessionRealtimeAppendAudio", Reach::Withheld("The desktop owns negotiated realtime media and its sideband control lifecycle.")),
    ("SessionRealtimeAppendSpeech", Reach::Withheld("The desktop owns negotiated realtime media and its sideband control lifecycle.")),
    ("SessionRealtimeHandoff", Reach::Withheld("The desktop owns negotiated realtime media and its sideband control lifecycle.")),
    // ---- Reading the session -----------------------------------------------
    ("SessionHistory", Reach::Called),
    ("SessionListApprovalReviewDenials", Reach::Withheld("The interactive desktop owns the retained denial chooser; the CLI reads review lifecycle events through its bound session history.")),
    ("SessionApproveApprovalReviewDenial", Reach::Withheld("The interactive desktop submits a denial selected by the user; the CLI does not maintain that selection window.")),
    ("SessionFileChanges", Reach::Called),
    ("SessionTakePending", Reach::Called),
    ("SessionQueueSnapshot", Reach::Called),
    ("SessionMemoryMaintenance", Reach::Called),
    ("SessionSetupLifecycle", Reach::Called),
    ("SessionAgentMemorySnapshotDecisions", Reach::Called),
    ("SessionMcpRuntime", Reach::Called),
    ("SessionRuntimeCapabilities", Reach::Called),
    ("SessionExtensionRuntime", Reach::Called),
    ("SessionPluginRuntime", Reach::Called),
    ("SessionInstalledPlugins", Reach::Called),
    ("SessionGenerateTitle", Reach::Called),
    ("SessionPredictionContext", Reach::Withheld("The desktop composer queries live inference and terminal turn eligibility.")),
    ("SessionPredictMessage", Reach::Withheld("The desktop composer owns ephemeral next-message suggestions.")),
    ("SessionCancelPrediction", Reach::Withheld("The desktop composer owns cancellation of its ephemeral suggestions.")),
    ("SessionTaskList", Reach::Called),
    ("SessionTaskIdentity", Reach::Withheld("Interactive hosts validate a concrete pending collaboration step; the CLI exposes tasks through its agent tools.")),
    ("SessionShutdown", Reach::Called),
    ("EngineListSessions", Reach::Called),
    ("EngineDictationStart", Reach::Withheld("The desktop owns live microphone capture for standalone dictation.")),
    ("EngineDictationAppendAudio", Reach::Withheld("The desktop owns live microphone capture for standalone dictation.")),
    ("EngineDictationEnd", Reach::Withheld("The desktop owns live microphone capture for standalone dictation.")),
    ("EngineReadSessionSummary", Reach::Withheld("the remote desktop catalog refreshes individual rows; the CLI lists metadata through engine/listSessions")),
    ("EngineProfileActivity", Reach::Withheld("the desktop profile displays lifetime activity; the CLI has no personal profile page")),
    ("EngineReadThread", Reach::Withheld("cross-thread paginated inspection is a desktop tool; CLI history keeps the bound-session full transcript contract")),
    ("EngineReadThreadHistory", Reach::Withheld("provider-free saved history is used by desktop recovery; CLI history reads its bound session")),
    ("EngineReadThreadItems", Reach::Withheld("single-turn message paging serves the desktop transcript's bounded first paint; CLI history reads its bound session whole")),
    ("EngineReadSubagentHistory", Reach::Withheld("desktop recovery reads saved child history without a parent binding; CLI child history uses its bound session")),
    ("EngineImportExternalSession", Reach::Withheld("external ecosystem normalization belongs to the importing desktop host; the CLI has no import-source workflow")),
    // Ends the whole endpoint, which under `--remote` is someone else's. The
    // CLI releases only what it opened: `session/shutdown` when it assembled the
    // engine itself, `session/close` when it did not.
    (
        "EngineShutdown",
        Reach::Withheld(
            "stopping the endpoint would kill a shared engine because one client exited",
        ),
    ),
    ("EngineBuiltinSkills", Reach::Called),
    (
        "EngineBuiltinAgents",
        Reach::Withheld(
            "the interactive host lists the built-in agent roster for its agents page; the CLI inspects a session's resolved agents through its snapshot",
        ),
    ),
    // ---- Endpoint state: rules, config, hooks, credentials -----------------
    ("EnginePermissionRules", Reach::Called),
    ("EngineAddPermissionRules", Reach::Called),
    ("EngineRemovePermissionRule", Reach::Called),
    (
        "SessionReloadPermissionRules",
        Reach::Withheld("the desktop refreshes open sessions after settings edits; CLI rule edits use a separate endpoint"),
    ),
    ("EngineWindowsSandboxReadiness", Reach::Withheld("native Windows preparation is managed by the desktop settings surface")),
    ("EngineStartWindowsSandboxSetup", Reach::Withheld("native Windows preparation is managed by the desktop settings surface")),
    ("ConfigRead", Reach::Called),
    ("ConfigValueWrite", Reach::Called),
    ("ConfigBatchWrite", Reach::Called),
    ("HooksRead", Reach::Called),
    ("HooksWrite", Reach::Called),
    ("ModelCredentialWrite", Reach::Called),
    ("ModelCredentialDelete", Reach::Called),
    ("ModelCredentialRead", Reach::Called),
    (
        "ModelSelectionResolve",
        Reach::Withheld("the desktop persists source-scoped picker choices; CLI selections are resolved by the same rules during session/start"),
    ),
    // ---- Subagents, cost, diagnostics --------------------------------------
    ("SessionCreateSideTask", Reach::Called),
    (
        "SessionStartSideTask",
        Reach::Withheld(
            "the desktop creates an ephemeral side endpoint atomically; CLI binds its existing endpoint through session/createSideTask",
        ),
    ),
    (
        "SessionForkLive",
        Reach::Withheld(
            "the desktop promotes an already-live ephemeral side task; CLI forks persisted threads",
        ),
    ),
    (
        "SessionBeginModelUpdate",
        Reach::Withheld("the desktop fences live model publication; CLI selects its provider at session startup and does not publish live bundles"),
    ),
    (
        "SessionFailModelUpdate",
        Reach::Withheld("the desktop reports host-side preparation failures to its exact capture fence; CLI does not prepare live model publications"),
    ),
    (
        "SessionReconfigureModel",
        Reach::Withheld("the desktop replaces live account/model/advisor bundles; CLI selects its provider at session startup with the advisor off"),
    ),
    (
        "SessionModelRuntime",
        Reach::Withheld("the desktop uses this non-secret revision and advisor status snapshot for live model compare-and-swap"),
    ),
    ("SessionCost", Reach::Called),
    ("SessionContextSnapshot", Reach::Called),
    ("SessionSubagents", Reach::Called),
    ("SessionSubagentLifecycle", Reach::Called),
    (
        "SessionBackgroundProcesses",
        Reach::Withheld("the desktop Monitor owns background-process presentation"),
    ),
    (
        "SessionBackgroundProcessOutput",
        Reach::Withheld("the desktop process workspace owns independent output cursors"),
    ),
    (
        "SessionStopBackgroundProcess",
        Reach::Withheld("the desktop process workspace owns direct process controls"),
    ),
    ("SessionQueued", Reach::Called),
    ("SessionCollaborationMode", Reach::Withheld("The desktop owns the collaborative planning document and structured question workflow.")),
    ("SessionSetCollaborationMode", Reach::Called),
    ("SessionPlans", Reach::Withheld("The desktop owns the collaborative planning document and structured question workflow.")),
    ("SessionReadPlanDocument", Reach::Withheld("The desktop owns the collaborative planning document and structured question workflow.")),
    ("SessionSavePlanDocument", Reach::Withheld("The desktop owns the collaborative planning document and structured question workflow.")),
    ("SessionAcceptPlan", Reach::Withheld("The desktop owns the collaborative planning document and structured question workflow.")),
    ("SessionPlanFeedback", Reach::Withheld("The desktop owns the collaborative planning document and structured question workflow.")),
    ("SessionCancelPlanImplementation", Reach::Withheld("The desktop owns the collaborative planning document and structured question workflow.")),
    ("SessionSettlePlanImplementation", Reach::Withheld("The desktop owns the collaborative planning document and structured question workflow.")),
    ("SessionAnswerUserInput", Reach::Withheld("The desktop owns the collaborative planning document and structured question workflow.")),
    ("SessionGoal", Reach::Called),
    ("SessionReadGoalObjective", Reach::Withheld("The desktop objective editor and resume preview own full materialized objective reads.")),
    ("SessionClearGoal", Reach::Called),
    ("SessionSetGoal", Reach::Called),
    ("SessionSubagentTranscript", Reach::Called),
    (
        "SessionSendSubagentMessage",
        Reach::Withheld("the desktop child-conversation composer owns explicit user messages and user-cancelled task recovery"),
    ),
    ("SessionMcpAuthorizationStatus", Reach::Called),
    ("SessionAgentMemorySnapshotDecision", Reach::Called),
    // ---- MCP ---------------------------------------------------------------
    ("SessionCallMcpTool", Reach::Called),
    ("SessionReadMcpResource", Reach::Called),
    ("SessionListMcpPrompts", Reach::Called),
    ("SessionGetMcpPrompt", Reach::Called),
    // ---- Queue -------------------------------------------------------------
    ("SessionEditQueued", Reach::Called),
    ("SessionRemoveQueued", Reach::Called),
    ("SessionReorderQueued", Reach::Called),
    // ---- Answering the engine mid-turn -------------------------------------
    // Approvals and elicitations do not arrive as events; the facade turns them
    // into requests BACK to this process, so exactly one consumer owns each
    // answer. Answering one produces the very op these two methods submit — the
    // turn loop replies to `approval/request` and `elicitation/request` instead,
    // and calling forward would answer the same question twice.
    (
        "SessionApprove",
        Reach::Withheld(
            "answered as the reverse request `approval/request`, which submits this op",
        ),
    ),
    (
        "SessionResolveElicitation",
        Reach::Withheld(
            "answered as the reverse request `elicitation/request`, which submits this op",
        ),
    ),
    ("SessionAnswerQuestion", Reach::Withheld("optional questions keep running in the CLI; the desktop answers them through the named facade method")),
    ("SessionGetPendingQuestions", Reach::Withheld("optional question reconnection is a desktop interaction; CLI displays the durable question text")),
    // An MCP server's own asks are NOT reverse requests — they ride the event
    // stream — so these two are called forward, and are the reason the pair
    // above is a deliberate exemption rather than a category.
    ("SessionResolveMcpAuthorization", Reach::Called),
    ("SessionResolveMcpElicitation", Reach::Called),
    ("SessionCancelRequest", Reach::Called),
    ("SessionRevokeMcpAuthorization", Reach::Called),
    ("SessionReconnectMcpServer", Reach::Called),
    ("SessionRemoveMcpServer", Reach::Called),
    ("SessionUpsertMcpServer", Reach::Called),
    ("SessionReloadMcpServers", Reach::Called),
    // ---- Plugins -----------------------------------------------------------
    ("SessionSetInstalledPluginEnabled", Reach::Called),
    ("SessionUninstallPlugin", Reach::Called),
    ("SessionInstallPlugin", Reach::Called),
    ("SessionPluginCatalog", Reach::Called),
    (
        "SessionMigrateBundledDiagram",
        Reach::Withheld("the desktop host owns the retired bundled Diagram mirror"),
    ),
    // The one method a CLI has no business answering: it exists so a host that
    // owns the tool registry can run a tool the engine asked it to run. `klaus`
    // owns no such registry — it hands the engine the tools the engine already
    // has — so a reply from here could only be a fabricated result.
    (
        "SessionResolveDynamicToolCall",
        Reach::Withheld(
            "the CLI hosts no dynamic tool registry, so it has nothing to resolve with",
        ),
    ),
    // ---- Steering and session configuration --------------------------------
    ("SessionRewind", Reach::Called),
    // `rewind --dry-run`: the file half is irreversible, so the CLI can ask what
    // it would take out before taking anything out.
    ("SessionRewindPreview", Reach::Called),
    ("SessionSteer", Reach::Called),
    ("SessionSetSkills", Reach::Withheld("host discovery publishes skill directory changes; the CLI does not own a discovery watcher")),
    ("SessionSetModel", Reach::Called),
    ("SessionSetPermissionMode", Reach::Withheld("Deprecated presets are translated to canonical runtime policy or collaboration requests at CLI input boundaries.")),
    ("SessionSetRuntimePolicy", Reach::Called),
    // Active-turn model replacement remains a host-only immediate operation.
    ("SessionSetRuntimeModel", Reach::Withheld("immediate op called by the desktop shell, not by the CLI")),
    ("SessionUpdateConfig", Reach::Called),
    ("SessionCompactStart", Reach::Called),
    ("SessionReviewStart", Reach::Called),
    ("SessionSideQuestion", Reach::Called),
    // ---- Memory ------------------------------------------------------------
    ("SessionDreamNow", Reach::Called),
    ("SessionRunSetup", Reach::Called),
    ("SessionResolveAgentMemorySnapshotDecision", Reach::Called),
    ("SessionSettleAgentMemorySnapshotUnavailable", Reach::Called),
    // ---- The Qoder subscription --------------------------------------------
    // Every one of these is withheld, and for ONE reason with two halves.
    //
    // The facade's `qoder/*` methods exist for a host in a DIFFERENT process (the
    // desktop client), which cannot read the credential or run the signer itself.
    // `klaus` links this engine, so for the parts it needs it calls the module
    // directly — `klaus login qoder` / `logout qoder` / `auth status` drive
    // `qoder::{start, poll, logout, auth_status}` in process, exactly as the
    // Claude and Codex logins call `model_sources`. Going out through the facade
    // to reach code already linked in would be the long way round.
    //
    // The rest are surfaces this CLI does not have at all, and inventing one to
    // satisfy a table would be worse than saying so.
    (
        "QoderStatus",
        Reach::Withheld("`klaus auth status` reads the same record in process"),
    ),
    (
        "QoderLoginStart",
        Reach::Withheld("`klaus login qoder` drives `qoder::start` in process"),
    ),
    (
        "QoderLoginPoll",
        Reach::Withheld("`klaus login qoder` drives `qoder::poll` in process"),
    ),
    (
        "QoderLogout",
        Reach::Withheld("`klaus logout qoder` drives `qoder::logout` in process"),
    ),
    (
        "QoderImportLocal",
        Reach::Withheld(
            "adopting another Qoder client's login is a GUI convenience; `klaus login qoder` signs in itself",
        ),
    ),
    (
        "QoderAdoptCredential",
        Reach::Withheld(
            "only a host that can open the Qoder IDE's SQLite store has a profile to hand over",
        ),
    ),
    (
        "QoderModels",
        Reach::Withheld("`klaus` lists no provider catalogs; `--model` takes an id"),
    ),
    (
        "QoderUsage",
        Reach::Withheld("`klaus` draws no quota gauge, so it has nothing to fill"),
    ),
    (
        "QoderGenerateImage",
        Reach::Withheld("the CLI hosts no dynamic tool registry, so it generates no images"),
    ),
    (
        "QoderSetThinkingLevel",
        Reach::Withheld(
            "`klaus` has no thinking-tier surface to push; the level comes from the engine's own config",
        ),
    ),
    // ---- The QwenWork subscription -----------------------------------------
    (
        "QwenWorkStatus",
        Reach::Withheld("the CLI has no QwenWork account-management surface"),
    ),
    (
        "QwenWorkLoginStart",
        Reach::Withheld("the CLI has no QwenWork device-login command"),
    ),
    (
        "QwenWorkLoginPoll",
        Reach::Withheld("the CLI has no QwenWork device-login command"),
    ),
    (
        "QwenWorkLogout",
        Reach::Withheld("the CLI has no QwenWork account-management surface"),
    ),
    (
        "QwenWorkImportLocal",
        Reach::Withheld("the CLI has no QwenWork account-management surface"),
    ),
    (
        "QwenWorkAdoptCredential",
        Reach::Withheld("the CLI host has no desktop safeStorage capability"),
    ),
    (
        "QwenWorkModels",
        Reach::Withheld("`klaus` lists no provider catalogs; `--model` takes an id"),
    ),
    (
        "QwenWorkUsage",
        Reach::Withheld("`klaus` draws no quota gauge, so it has nothing to fill"),
    ),
    (
        "QwenWorkTranscribeAudio",
        Reach::Withheld("the CLI has no standalone subscription transcription command"),
    ),
    (
        "QwenWorkCancelTranscription",
        Reach::Withheld("the CLI has no standalone subscription transcription command"),
    ),
    (
        "QwenWorkCapabilities",
        Reach::Withheld("the CLI has no independent subscription capability command"),
    ),
    (
        "QwenWorkGenerateVideo",
        Reach::Withheld("the CLI has no independent subscription capability command"),
    ),
    (
        "QwenWorkGenerateMusic",
        Reach::Withheld("the CLI has no independent subscription capability command"),
    ),
    (
        "QwenWorkSearch",
        Reach::Withheld("the CLI has no independent subscription capability command"),
    ),
    (
        "QwenWorkCancelOperation",
        Reach::Withheld("the CLI has no independent subscription capability command"),
    ),
    (
        "QwenWorkGenerateImage",
        Reach::Withheld("the CLI hosts no dynamic tool registry, so it generates no images"),
    ),
    (
        "QwenWorkRefreshToken",
        Reach::Withheld(
            "the CLI has no QwenWork token-refresh command; the desktop host drives refresh over the facade",
        ),
    ),
    (
        "QwenWorkSetThinkingLevel",
        Reach::Withheld(
            "`klaus` has no thinking-tier surface to push; the level comes from the engine's own config",
        ),
    ),
    // ---- The QoderWork subscription ----------------------------------------
    (
        "QoderWorkStatus",
        Reach::Withheld("the CLI has no QoderWork account-management surface"),
    ),
    (
        "QoderWorkLoginStart",
        Reach::Withheld("the CLI has no QoderWork device-login command"),
    ),
    (
        "QoderWorkLoginPoll",
        Reach::Withheld("the CLI has no QoderWork device-login command"),
    ),
    (
        "QoderWorkLogout",
        Reach::Withheld("the CLI has no QoderWork account-management surface"),
    ),
    (
        "QoderWorkImportLocal",
        Reach::Withheld("the CLI has no QoderWork account-management surface"),
    ),
    (
        "QoderWorkAdoptCredential",
        Reach::Withheld("the CLI host has no desktop safeStorage capability"),
    ),
    (
        "QoderWorkModels",
        Reach::Withheld("`klaus` lists no provider catalogs; `--model` takes an id"),
    ),
    (
        "QoderWorkUsage",
        Reach::Withheld("`klaus` draws no quota gauge, so it has nothing to fill"),
    ),
    (
        "QoderWorkGenerateImage",
        Reach::Withheld("the CLI hosts no dynamic tool registry, so it generates no images"),
    ),
    (
        "QoderWorkSetThinkingLevel",
        Reach::Withheld(
            "`klaus` has no thinking-tier surface to push; the level comes from the engine's own config",
        ),
    ),
    // ---- The WorkBuddy subscription ----------------------------------------
    (
        "WorkBuddyStatus",
        Reach::Withheld("the CLI has no WorkBuddy account-management surface"),
    ),
    (
        "WorkBuddyLoginStart",
        Reach::Withheld("the CLI has no WorkBuddy device-login command"),
    ),
    (
        "WorkBuddyLoginPoll",
        Reach::Withheld("the CLI has no WorkBuddy device-login command"),
    ),
    (
        "WorkBuddyLogout",
        Reach::Withheld("the CLI has no WorkBuddy account-management surface"),
    ),
    (
        "WorkBuddyImportLocal",
        Reach::Withheld("the CLI has no WorkBuddy account-management surface"),
    ),
    (
        "WorkBuddyAdoptCredential",
        Reach::Withheld(
            "the CLI host cannot open WorkBuddy's store: its key is compiled into that client's own binary",
        ),
    ),
    (
        "WorkBuddyModels",
        Reach::Withheld("`klaus` lists no provider catalogs; `--model` takes an id"),
    ),
    (
        "WorkBuddyRefreshToken",
        Reach::Withheld(
            "the CLI has no WorkBuddy token-refresh command; the desktop host drives refresh over the facade",
        ),
    ),
    (
        "WorkBuddyUsage",
        Reach::Withheld("`klaus` draws no quota gauge, so it has nothing to fill"),
    ),
    // ---- The Kimi membership ------------------------------------------------
    // `klaus login` covers Claude, Codex and Qoder only. Kimi's two credentials
    // are both driven from the desktop settings pane: a console API key is pasted
    // there, and the device flow has the host open the browser and poll.
    (
        "KimiStatus",
        Reach::Withheld("the CLI has no Kimi account-management surface"),
    ),
    (
        "KimiSetApiKey",
        Reach::Withheld("the CLI has no Kimi credential command; a key is set from the desktop settings pane"),
    ),
    (
        "KimiLoginStart",
        Reach::Withheld("the CLI has no Kimi device-login command"),
    ),
    (
        "KimiLoginPoll",
        Reach::Withheld("the CLI has no Kimi device-login command"),
    ),
    (
        "KimiLogout",
        Reach::Withheld("the CLI has no Kimi account-management surface"),
    ),
    (
        "KimiImportLocal",
        Reach::Withheld(
            "the CLI has no Kimi account-management surface, and the method answers 'unavailable' by design: Kimi leaves no store this engine can read",
        ),
    ),
    (
        "KimiModels",
        Reach::Withheld("`klaus` lists no provider catalogs; `--model` takes an id"),
    ),
    (
        "KimiUsage",
        Reach::Withheld("`klaus` draws no quota gauge, so it has nothing to fill"),
    ),
    // ---- The Bailian Token Plan subscription --------------------------------
    // Token Plan's console session login is a desktop-owned loopback flow: the
    // browser round trip and the stored session have no CLI equivalent, so the
    // three console methods stay withheld like the rest of this surface.
    (
        "BailianStatus",
        Reach::Withheld("the CLI has no Bailian account-management surface"),
    ),
    (
        "BailianSetApiKey",
        Reach::Withheld("the CLI has no Bailian credential command; a key is set from the desktop settings pane"),
    ),
    (
        "BailianLogout",
        Reach::Withheld("the CLI has no Bailian account-management surface"),
    ),
    (
        "BailianModels",
        Reach::Withheld("`klaus` lists no provider catalogs; `--model` takes an id"),
    ),
    (
        "BailianUsage",
        Reach::Withheld("`klaus` draws no quota gauge; the allowance reads need a console session the CLI does not hold"),
    ),
    (
        "BailianConsoleLogin",
        Reach::Withheld("the console session login is a desktop loopback flow; the CLI has no browser handoff"),
    ),
    (
        "BailianConsoleStatus",
        Reach::Withheld("the CLI surfaces no Bailian console session state"),
    ),
    (
        "BailianConsoleLogout",
        Reach::Withheld("the CLI has no Bailian account-management surface"),
    ),
    (
        "BailianMediaCredential",
        Reach::Withheld("the CLI has no media route; the desktop host runs the plan's speech API and is the only caller"),
    ),
    // ---- The MiniMax M Plan subscription --------------------------------------
    // M Plan likewise has no login flow to reach: the plan takes a console
    // subscription key and offers no OAuth or device flow to third-party clients.
    (
        "MinimaxStatus",
        Reach::Withheld("the CLI has no MiniMax account-management surface"),
    ),
    (
        "MinimaxSetApiKey",
        Reach::Withheld("the CLI has no MiniMax credential command; a key is set from the desktop settings pane"),
    ),
    (
        "MinimaxLogout",
        Reach::Withheld("the CLI has no MiniMax account-management surface"),
    ),
    (
        "MinimaxModels",
        Reach::Withheld("`klaus` lists no provider catalogs; `--model` takes an id"),
    ),
    (
        "MinimaxUsage",
        Reach::Withheld("`klaus` draws no quota gauge, so it has nothing to fill"),
    ),
    (
        "MinimaxMediaCredential",
        Reach::Withheld("the CLI has no media route; the desktop host runs the plan's video API and is the only caller"),
    ),
    // ---- The Zhipu Coding Plan subscription ---------------------------------
    // Same shape as Bailian: a plan key and no login flow to reach, so there is no
    // device-flow entry here either.
    (
        "ZhipuStatus",
        Reach::Withheld("the CLI has no Zhipu account-management surface"),
    ),
    (
        "ZhipuSetApiKey",
        Reach::Withheld("the CLI has no Zhipu credential command; a key is set from the desktop settings pane"),
    ),
    (
        "ZhipuLogout",
        Reach::Withheld("the CLI has no Zhipu account-management surface"),
    ),
    (
        "ZhipuModels",
        Reach::Withheld("`klaus` lists no provider catalogs; `--model` takes an id"),
    ),
    (
        "ZhipuUsage",
        Reach::Withheld("`klaus` draws no quota gauge, and the gateway exposes no quota route to a plan key anyway"),
    ),
    // ---- The Ark Coding Plan subscription ------------------------------------
    // Same shape as the other plan sources: a plan key and no login flow to reach,
    // so there is no device-flow entry here either.
    (
        "ArkStatus",
        Reach::Withheld("the CLI has no Ark account-management surface"),
    ),
    (
        "ArkSetApiKey",
        Reach::Withheld("the CLI has no Ark credential command; a key is set from the desktop settings pane"),
    ),
    (
        "ArkLogout",
        Reach::Withheld("the CLI has no Ark account-management surface"),
    ),
    (
        "ArkModels",
        Reach::Withheld("`klaus` lists no provider catalogs; `--model` takes an id"),
    ),
    (
        "ArkUsage",
        Reach::Withheld("`klaus` draws no quota gauge without a console session, and the gateway exposes no quota route to a plan key anyway"),
    ),
    // The console-session trio beside the plan key: a loopback browser login that
    // stores the console session, the read the card polls while the browser round
    // trip runs, and its logout. None of them is a credential or catalog read the
    // CLI performs; the CLI has no console to log into.
    (
        "ArkConsoleLogin",
        Reach::Withheld("the console login is a browser loopback flow the desktop card drives; the CLI has no console session to open"),
    ),
    (
        "ArkConsoleStatus",
        Reach::Withheld("the stored console session is a desktop-card fact; the CLI reads no allowance"),
    ),
    (
        "ArkConsoleLogout",
        Reach::Withheld("the console session is cleared from the desktop card; the CLI holds none"),
    ),
    // The Codex catalog is withheld for the same reason as `QoderModels`, plus the
    // in-process one: `klaus` links this engine, so the resolver reads the catalog
    // module directly while binding a session's capability set. A `--model` value
    // is an id the user already knows.
    (
        "CodexModels",
        Reach::Withheld("`klaus` lists no provider catalogs; `--model` takes an id"),
    ),
    // The Claude lineup, withheld on the same two grounds. It is the strongest
    // case for the in-process half: the lineup is this engine's own static
    // knowledge, so `klaus` resolves `--model subscription:opus` through the same
    // module the facade answers from, with no round trip to make.
    (
        "SubscriptionModels",
        Reach::Withheld("`klaus` lists no provider catalogs; `--model` takes an id"),
    ),
    (
        "ClaudeUsage",
        Reach::Withheld("`klaus` draws no subscription quota gauge; account usage is an endpoint settings query"),
    ),
    (
        "CodexTranscribeAudio",
        Reach::Withheld("audio capture belongs to the desktop; the CLI submits text"),
    ),
    (
        "CodexCancelTranscription",
        Reach::Withheld("the CLI owns no subscription audio capture operation"),
    ),
    (
        "ClaudeTranscribeAudio",
        Reach::Withheld("audio capture belongs to the desktop; the CLI submits text"),
    ),
    (
        "ClaudeCancelTranscription",
        Reach::Withheld("the CLI owns no subscription audio capture operation"),
    ),
    // ---- Persisted threads -------------------------------------------------
    ("SessionAppendDisplayMessage", Reach::Withheld("The CLI submits canonical user messages and does not persist host-rendered display envelopes")),
    ("SessionResume", Reach::Called),
    ("SessionFork", Reach::Called),
    ("SessionArchive", Reach::Called),
    ("SessionUnarchive", Reach::Called),
    ("SessionDelete", Reach::Called),
    ("SessionUpdateTitle", Reach::Called),
    ("SessionRenameThread", Reach::Called),
    ("SessionUpdateThreadHostMetadata", Reach::Called),
];

/// The table accounts for the engine's whole request surface, and nothing else.
///
/// Both directions matter and they catch different mistakes. A method the engine
/// declares and the table omits is the silent coverage loss this file exists to
/// prevent. A method the table lists and the engine no longer declares is a
/// stale row that would keep asserting a call the compiler already deleted.
#[test]
fn the_table_accounts_for_every_declared_method() {
    let declared: BTreeSet<&str> = ALL_METHODS.iter().map(|spec| spec.marker).collect();
    let tabled: BTreeSet<&str> = COVERAGE.iter().map(|(marker, _)| *marker).collect();

    let unaccounted: Vec<&&str> = declared.difference(&tabled).collect();
    assert!(
        unaccounted.is_empty(),
        "the engine declares methods this CLI's coverage table does not mention, so nobody has \
         said whether `klaus` can reach them: {unaccounted:?}"
    );

    let stale: Vec<&&str> = tabled.difference(&declared).collect();
    assert!(
        stale.is_empty(),
        "the coverage table lists methods the engine no longer declares: {stale:?}"
    );
}

/// No method is listed twice, which would let one row hide a false one.
#[test]
fn the_table_lists_each_method_once() {
    let mut seen = BTreeSet::new();
    for (marker, _) in COVERAGE {
        assert!(seen.insert(*marker), "{marker} is listed twice");
    }
}

/// Every method the table calls `Called` is named in the CLI's own sources.
///
/// Read off the sources rather than exercised: "does any command path reach this
/// method" is a property of the whole crate, and running one command per method
/// would answer a much narrower question much more slowly.
#[test]
fn every_declared_method_is_called_by_the_cli() {
    let sources = all_text(&cli_sources());
    let mut wrong = Vec::new();
    for (marker, reach) in COVERAGE {
        let named = mentions(&sources, marker);
        match reach {
            Reach::Called => {
                if !named {
                    wrong.push(format!("{marker}: named nowhere under src/"));
                }
            }
            Reach::Withheld(reason) => {
                assert!(!reason.is_empty(), "{marker} is withheld without a reason");
                // An exemption that stopped being true is worse than no note at
                // all: a comment claiming the CLI cannot do what it now does.
                if named {
                    wrong.push(format!(
                        "{marker}: withheld as {reason:?}, but src/ calls it after all"
                    ));
                }
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "the coverage table does not match the code:\n  {}",
        wrong.join("\n  ")
    );
}

/// The declared method table is the CLI's only way to drive the engine.
///
/// These names are the previous escape hatch, spelled out so it cannot come
/// back: `OpKind` is the engine's inner op enum and `ProtocolClient` is the
/// thing that submits one. A host holding either can reach engine behaviour
/// without naming a method, which is precisely how methods came to exist that no
/// client had ever called.
#[test]
fn the_cli_never_submits_a_raw_op() {
    let sources = cli_sources();
    let mut found = Vec::new();
    for (path, text) in &sources {
        for needle in ["OpKind", "ProtocolClient", "ProtocolClientOptions"] {
            if text.contains(needle) {
                found.push(format!("{path} names `{needle}`"));
            }
        }
    }
    assert!(
        found.is_empty(),
        "the CLI has a second way into the engine, which is the one thing this surface may not \
         have — every request goes through the declared method table or it is not a request:\n  {}",
        found.join("\n  ")
    );
}

/// The facade client itself is held in one place, not reached for per command.
///
/// `AppServerClient::request::<M>` is already the only send it offers, so this is
/// not about what a caller could smuggle through — it is about there being one
/// place that decides how a call is made. `engine.rs` wraps it into the two
/// clients everything else uses; `session.rs` dials it and runs the handshake.
/// A command file that named it directly would be deciding again, alone.
#[test]
fn only_the_client_layer_holds_the_facade() {
    let sources = cli_sources();
    let holders: BTreeSet<&str> = sources
        .iter()
        .filter(|(_, text)| text.contains("AppServerClient"))
        .map(|(path, _)| path.as_str())
        .map(|path| path.trim_start_matches("src/"))
        .collect();
    let expected: BTreeSet<&str> = ["engine.rs", "session.rs"].into_iter().collect();
    assert_eq!(
        holders, expected,
        "the facade client must stay behind `EngineClient`/`EndpointClient`; if a new file has a \
         real reason to hold it, that reason belongs here too"
    );
}

/// Every `.rs` under the crate's `src/`, as (path relative to the crate, text).
fn cli_sources() -> Vec<(String, String)> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    collect(&root.join("src"), &root, &mut out);
    assert!(
        out.len() > 10,
        "the CLI sources did not load; these tests would pass vacuously"
    );
    out.sort();
    out
}

fn collect(dir: &Path, root: &Path, out: &mut Vec<(String, String)>) {
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("reading {dir:?}: {e}"));
    for entry in entries {
        let path = entry.expect("a readable directory entry").path();
        if path.is_dir() {
            collect(&path, root, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let text = std::fs::read_to_string(&path).expect("a readable source file");
            // Separators normalized so the names these tests compare against are the
            // same on every host. Without this, a Windows run yields `src\engine.rs`
            // and every `src/`-prefixed comparison below quietly stops matching.
            let name = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            out.push((name, text));
        }
    }
}

fn all_text(sources: &[(String, String)]) -> String {
    let mut out = String::new();
    for (_, text) in sources {
        out.push_str(text);
        out.push('\n');
    }
    assert!(
        out.len() > 10_000,
        "the CLI sources are too small to be the real ones"
    );
    out
}

/// `SessionGoal` must not match `SessionGoalThing`, and `SessionFork` must not
/// be found inside `SessionForkOptions`, so the name has to END here.
fn mentions(sources: &str, needle: &str) -> bool {
    sources.match_indices(needle).any(|(at, _)| {
        sources[at + needle.len()..]
            .chars()
            .next()
            .is_none_or(|next| !next.is_alphanumeric() && next != '_')
    })
}
