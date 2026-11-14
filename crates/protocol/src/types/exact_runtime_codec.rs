use super::*;

struct ExactSubAgentTranscript(SubAgentTranscript);

impl RuntimeEncode for ExactSubAgentTranscript {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("messages", &self.0.messages)?;
        if let Some(index) = &self.0.turn_index {
            object.field("turnIndex", index)?;
        }
        if let Some(meta) = &self.0.meta {
            object.field("meta", meta)?;
        }
        if let Some(surfaced_tools) = &self.0.surfaced_tools {
            object.field("surfacedTools", surfaced_tools)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

/// Protocol list responses historically emit an explicit `nextCursor: null`
/// at the end of pagination even though the core runtime DTO omits absent
/// optionals. Keep that framing contract without weakening the exact carriers.
struct ExactThreadPage(ThreadPage);

impl RuntimeEncode for ExactThreadPage {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("items", &self.0.items)?;
        object.field("nextCursor", &self.0.next_cursor)?;
        if let Some(value) = &self.0.membership_fingerprint {
            object.field("membershipFingerprint", value)?;
        }
        if let Some(value) = &self.0.metadata_fingerprint {
            object.field("metadataFingerprint", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ExactThreadPage {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self(ThreadPage {
                items: object.required("items", &[])?,
                next_cursor: object.optional("nextCursor", &[])?,
                membership_fingerprint: object.optional("membershipFingerprint", &[])?,
                metadata_fingerprint: object.optional("metadataFingerprint", &[])?,
            }))
        })
    }
}

impl RuntimeDecode for ExactSubAgentTranscript {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self(SubAgentTranscript {
                messages: object.required("messages", &[])?,
                meta: object.optional("meta", &[])?,
                surfaced_tools: object.optional("surfacedTools", &[])?,
                turn_index: object.optional("turnIndex", &[])?,
            }))
        })
    }
}

macro_rules! encode_struct_field {
    ($object:ident, $value:ident, $wire:literal, required) => {
        $object.field($wire, $value)?;
    };
    ($object:ident, $value:ident, $wire:literal, nullable) => {
        $object.field($wire, $value)?;
    };
    ($object:ident, $value:ident, $wire:literal, legacy_optional) => {
        let _ = $value;
    };
    ($object:ident, $value:ident, $wire:literal, optional) => {
        if let Some(value) = $value {
            $object.field($wire, value)?;
        }
    };
    ($object:ident, $value:ident, $wire:literal, optional_terminal_disposition) => {
        if let Some(value) = $value {
            $object.field($wire, value)?;
        }
    };
    ($object:ident, $value:ident, $wire:literal, goal_budget_patch) => {
        if let Some(value) = $value {
            $object.field($wire, value)?;
        }
    };
    ($object:ident, $value:ident, $wire:literal, nullable_patch) => {
        if let Some(value) = $value {
            $object.field($wire, value)?;
        }
    };
    ($object:ident, $value:ident, $wire:literal, goal_integer) => {
        $object.field($wire, &validate_goal_wire_integer($wire, *$value)?)?;
    };
    ($object:ident, $value:ident, $wire:literal, optional_goal_integer) => {
        if let Some(value) = $value {
            $object.field($wire, &validate_goal_wire_integer($wire, *value)?)?;
        }
    };
    ($object:ident, $value:ident, $wire:literal, goal_revision) => {
        if let Some(value) = $value {
            if !(0..=9_007_199_254_740_991).contains(value) {
                return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                    "goalRevision must be a nonnegative safe integer".into(),
                )));
            }
            $object.field($wire, value)?;
        }
    };
    ($object:ident, $value:ident, $wire:literal, history_error_details) => {
        if let Some(value) = $value {
            let details = value
                .iter()
                .filter_map(|(id, value)| {
                    klaus_agent_core::engine::session::sanitize_turn_error_details(value)
                        .map(|value| (id.clone(), value))
                })
                .collect::<BTreeMap<_, _>>();
            if !details.is_empty() {
                $object.field($wire, &details)?;
            }
        }
    };
    ($object:ident, $value:ident, $wire:literal, default_empty) => {
        if !$value.is_empty() {
            $object.field($wire, $value)?;
        }
    };
    ($object:ident, $value:ident, $wire:literal, default_false) => {
        if *$value {
            $object.field($wire, $value)?;
        }
    };
    ($object:ident, $value:ident, $wire:literal, default_true) => {
        if !*$value {
            $object.field($wire, $value)?;
        }
    };
    ($object:ident, $value:ident, $wire:literal, transcript_optional) => {
        if let Some(value) = $value {
            $object.field($wire, &ExactSubAgentTranscript(value.clone()))?;
        }
    };
    ($object:ident, $value:ident, $wire:literal, thread_page_optional) => {
        if let Some(value) = $value {
            $object.field($wire, &ExactThreadPage(value.clone()))?;
        }
    };
}

macro_rules! decode_struct_field {
    ($object:ident, $wire:literal, required) => {
        $object.required($wire, &[])?
    };
    ($object:ident, $wire:literal, nullable) => {
        $object.required($wire, &[])?
    };
    ($object:ident, $wire:literal, legacy_optional) => {
        $object.optional($wire, &[])?
    };
    ($object:ident, $wire:literal, optional) => {
        $object.optional($wire, &[])?
    };
    ($object:ident, $wire:literal, optional_terminal_disposition) => {
        match $object.optional_nullable($wire, &[])? {
            Some(None) => {
                return Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::ExpectedString,
                ));
            }
            value => value.flatten(),
        }
    };
    ($object:ident, $wire:literal, goal_budget_patch) => {
        decode_maximum_goal_budget($object.optional_nullable($wire, &[])?)?
    };
    ($object:ident, $wire:literal, nullable_patch) => {
        $object.optional_nullable($wire, &[])?
    };
    ($object:ident, $wire:literal, goal_integer) => {
        validate_goal_wire_integer($wire, $object.required($wire, &[])?)?
    };
    ($object:ident, $wire:literal, optional_goal_integer) => {
        $object
            .optional($wire, &[])?
            .map(|value| validate_goal_wire_integer($wire, value))
            .transpose()?
    };
    ($object:ident, $wire:literal, goal_revision) => {
        decode_goal_revision($object.optional_nullable($wire, &[])?)?
    };
    ($object:ident, $wire:literal, history_error_details) => {
        $object
            .optional::<BTreeMap<RuntimeString, RuntimeJson>>($wire, &[])?
            .map(|values| {
                values
                    .into_iter()
                    .filter_map(|(id, value)| {
                        klaus_agent_core::engine::session::sanitize_turn_error_details(&value)
                            .map(|value| (id, value))
                    })
                    .collect::<BTreeMap<_, _>>()
            })
            .filter(|values| !values.is_empty())
    };
    ($object:ident, $wire:literal, default_empty) => {
        $object.defaulted($wire, &[], Default::default)?
    };
    ($object:ident, $wire:literal, default_false) => {
        $object.defaulted($wire, &[], || false)?
    };
    ($object:ident, $wire:literal, default_true) => {
        $object.defaulted($wire, &[], || true)?
    };
    ($object:ident, $wire:literal, transcript_optional) => {
        $object
            .optional::<ExactSubAgentTranscript>($wire, &[])?
            .map(|value| value.0)
    };
    ($object:ident, $wire:literal, thread_page_optional) => {
        $object
            .optional::<ExactThreadPage>($wire, &[])?
            .map(|value| value.0)
    };
}

macro_rules! impl_runtime_struct {
    ($type:ty, deny_unknown: $deny_unknown:expr, {
        $( $field:ident => $wire:literal : $kind:ident ),* $(,)?
    }) => {
        impl RuntimeEncode for $type {
            fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
                let Self { $( $field ),* } = self;
                let mut object = RuntimeObjectEncoder::new();
                $( encode_struct_field!(object, $field, $wire, $kind); )*
                out.emit(RuntimeJson::Object(object.finish()))
            }
        }

        impl RuntimeDecode for $type {
            fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                runtime_codec::decode_object(value, $deny_unknown, |object| {
                    Ok(Self {
                        $( $field: decode_struct_field!(object, $wire, $kind) ),*
                    })
                })
            }
        }
    };
}

impl_runtime_struct!(RunOpOptions, deny_unknown: false, {
    ultra_mode => "ultraMode": optional,
    model => "model": optional,
    effort => "effort": optional,
    permission_mode => "permissionMode": optional,
    output_schema => "outputSchema": optional,
    system_prompt_append => "systemPromptAppend": optional,
    display_metadata => "displayMetadata": optional,
    meta_input => "metaInput": optional,
});

impl_runtime_struct!(Op, deny_unknown: false, {
    id => "id": required,
    op => "op": required,
});

impl_runtime_struct!(ElicitRequestWire, deny_unknown: false, {
    prompt => "prompt": required,
    questions => "questions": optional,
    attachments_supported => "attachmentsSupported": optional,
    metadata => "metadata": optional,
    options => "options": optional,
    multi_select => "multiSelect": optional,
});

impl_runtime_struct!(Elicitation, deny_unknown: false, {
    request_id => "requestId": required,
    request => "request": required,
});

impl_runtime_struct!(McpElicitationResolvedWire, deny_unknown: false, {
    request_id => "requestId": required,
    action => "action": required,
});

impl_runtime_struct!(McpAuthorizationRequestWire, deny_unknown: false, {
    request_id => "requestId": required,
    server_name => "serverName": required,
    authorization_url => "authorizationUrl": required,
    redirect_uri => "redirectUri": required,
});

impl_runtime_struct!(McpAuthorizationResolvedWire, deny_unknown: false, {
    request_id => "requestId": required,
    accepted => "accepted": required,
});

impl_runtime_struct!(McpAuthorizationStatusWire, deny_unknown: false, {
    server_name => "serverName": required,
    auth_status => "authStatus": required,
});

impl_runtime_struct!(McpAuthorizationRevokedWire, deny_unknown: false, {
    server_name => "serverName": required,
    auth_status => "authStatus": required,
    revoked => "revoked": required,
});

impl_runtime_struct!(McpRuntimeResult, deny_unknown: false, {
    runtime => "runtime": required,
    activation_scope => "activationScope": required,
});

impl_runtime_struct!(McpToolCallResultWire, deny_unknown: false, {
    server_name => "serverName": required,
    content => "content": required,
    structured_content => "structuredContent": optional,
    is_error => "isError": optional,
});

impl_runtime_struct!(McpResourceReadResultWire, deny_unknown: false, {
    server_name => "serverName": required,
    contents => "contents": required,
});

impl_runtime_struct!(McpServerPromptsWire, deny_unknown: false, {
    server_name => "serverName": required,
    prompts => "prompts": required,
});

impl_runtime_struct!(McpPromptsResultWire, deny_unknown: false, {
    servers => "servers": required,
});

impl_runtime_struct!(McpPromptResultWire, deny_unknown: false, {
    server_name => "serverName": required,
    name => "name": required,
    content => "content": required,
});

impl_runtime_struct!(BackgroundAgentSummary, deny_unknown: false, {
    id => "id": required,
    description => "description": required,
    status => "status": required,
    teammate => "teammate": required,
    idle => "idle": required,
    transcript => "transcript": required,
    output_file => "outputFile": required,
});

impl_runtime_struct!(RunResult, deny_unknown: false, {
    stop_reason => "stopReason": required,
    usage => "usage": nullable,
    final_text => "finalText": required,
    final_message_id => "finalMessageId": optional,
    structured_output => "structuredOutput": optional,
});

impl_runtime_struct!(BoundSession, deny_unknown: false, {
    thread_id => "threadId": required,
    forked_from_id => "forkedFromId": optional,
});

impl_runtime_struct!(ArchiveSessionResult, deny_unknown: false, {
    thread_id => "threadId": required,
    archived => "archived": required,
});

impl_runtime_struct!(DeleteSessionResult, deny_unknown: false, {
    thread_id => "threadId": required,
    deleted => "deleted": required,
});

impl_runtime_struct!(UpdateTitleResult, deny_unknown: false, {
    thread_id => "threadId": required,
    title => "title": required,
});

impl_runtime_struct!(RenameThreadResult, deny_unknown: false, {
    thread_id => "threadId": required,
    renamed => "renamed": required,
});

impl_runtime_struct!(ProtocolThreadGoal, deny_unknown: false, {
    thread_id => "threadId": required,
    goal_id => "goalId": required,
    objective => "objective": required,
    status => "status": required,
    token_budget => "tokenBudget": optional_goal_integer,
    tokens_used => "tokensUsed": goal_integer,
    time_used_seconds => "timeUsedSeconds": goal_integer,
    created_at_ms => "createdAtMs": goal_integer,
    updated_at_ms => "updatedAtMs": goal_integer,
});

impl_runtime_struct!(UpdateThreadHostMetadataResult, deny_unknown: false, {
    thread_id => "threadId": required,
    updated => "updated": required,
});

impl_runtime_struct!(CompactSelection, deny_unknown: true, {
    message_id => "messageId": required,
    direction => "direction": required,
});

impl_runtime_struct!(CompactSessionResult, deny_unknown: false, {
    compacted => "compacted": required,
    freed_tokens => "freedTokens": required,
    reason => "reason": optional,
    diagnostics => "diagnostics": default_empty,
    before_tokens => "beforeTokens": required,
    after_tokens => "afterTokens": required,
    before_messages => "beforeMessages": required,
    after_messages => "afterMessages": required,
    boundary => "boundary": optional,
});

impl_runtime_struct!(CompactProgress, deny_unknown: false, {
    id => "id": required,
    phase => "phase": required,
    trigger => "trigger": required,
    mode => "mode": required,
    result => "result": optional,
    error => "error": optional,
});

impl_runtime_struct!(ProtocolCommandHook, deny_unknown: true, {
    event => "event": required,
    matcher => "matcher": optional,
    command => "command": required,
    timeout_ms => "timeoutMs": optional,
    is_async => "isAsync": default_false,
});

impl_runtime_struct!(ProtocolPluginCatalogRoot, deny_unknown: true, {
    marketplace => "marketplace": required,
    dir => "dir": required,
});

impl_runtime_struct!(ProtocolPluginContributions, deny_unknown: false, {
    skills => "skills": default_empty,
    agents => "agents": default_empty,
    commands => "commands": default_empty,
    output_styles => "outputStyles": default_empty,
    hooks => "hooks": default_empty,
    mcp_servers => "mcpServers": default_empty,
    lsp_servers => "lspServers": default_empty,
});

impl_runtime_struct!(ProtocolPluginCatalogEntry, deny_unknown: false, {
    id => "id": required,
    name => "name": required,
    marketplace => "marketplace": required,
    version => "version": optional,
    description => "description": optional,
    installed => "installed": required,
    enabled => "enabled": required,
    contributions => "contributions": required,
});

impl_runtime_struct!(ProtocolPluginCatalog, deny_unknown: false, {
    entries => "entries": default_empty,
});

impl_runtime_struct!(ProtocolDynamicToolFunctionSpec, deny_unknown: false, {
    name => "name": required,
    description => "description": required,
    input_schema => "inputSchema": required,
    defer_loading => "deferLoading": default_false,
    read_only => "readOnly": default_false,
});

impl_runtime_struct!(ProtocolDynamicToolNamespaceSpec, deny_unknown: false, {
    name => "name": required,
    description => "description": required,
    tools => "tools": required,
});

impl_runtime_struct!(ProtocolDynamicToolCall, deny_unknown: false, {
    call_id => "callId": required,
    thread_id => "threadId": required,
    turn_id => "turnId": required,
    namespace => "namespace": optional,
    tool => "tool": required,
    arguments => "arguments": required,
});

impl_runtime_struct!(ProtocolDynamicToolResponse, deny_unknown: false, {
    content_items => "contentItems": required,
    success => "success": required,
});

impl_runtime_struct!(ProtocolDynamicToolResolved, deny_unknown: false, {
    call_id => "callId": required,
});

impl_runtime_struct!(ProtocolGoalSubmission, deny_unknown: true, {
    message_uuid => "messageUuid": required,
    display_uuid => "displayUuid": required,
    text => "text": required,
});

impl_runtime_struct!(ProtocolSessionConfigUpdate, deny_unknown: true, {
    subagent_model_catalog => "subagentModelCatalog": optional,
    subagent_model_options => "subagentModelOptions": optional,
    ultra_mode => "ultraMode": optional,
    goal => "goal": optional,
    maximum_goal_token_budget => "maximumGoalTokenBudget": goal_budget_patch,
    model => "model": optional,
    permission_mode => "permissionMode": optional,
    model_verbosity => "modelVerbosity": nullable_patch,
    reasoning_summary => "reasoningSummary": optional,
    web_search_access => "webSearchAccess": optional,
    approval_review => "approvalReview": nullable_patch,
});

impl_runtime_struct!(ProtocolSessionConfigUpdateResult, deny_unknown: false, {
    subagent_model_catalog => "subagentModelCatalog": optional,
    subagent_model_options => "subagentModelOptions": optional,
    ultra_mode => "ultraMode": optional,
    goal => "goal": optional,
    maximum_goal_token_budget => "maximumGoalTokenBudget": goal_budget_patch,
    model => "model": optional,
    permission_mode => "permissionMode": legacy_optional,
    model_verbosity => "modelVerbosity": nullable_patch,
    reasoning_summary => "reasoningSummary": optional,
    web_search_access => "webSearchAccess": optional,
    approval_review => "approvalReview": nullable_patch,
});

impl_runtime_struct!(ProtocolHostCapabilitiesInit, deny_unknown: true, {
    mcp_servers => "mcpServers": default_empty,
    runtime_capability_roots => "runtimeCapabilityRoots": default_empty,
    selected_capability_roots => "selectedCapabilityRoots": optional,
});

impl_runtime_struct!(ProtocolRuntimeCapabilityRootInit, deny_unknown: false, {
    id => "id": required,
    enabled => "enabled": optional,
    browser => "browser": optional,
});

impl_runtime_struct!(ProtocolBrowserCapabilityInit, deny_unknown: false, {
    mcp_server => "mcpServer": optional,
    host_reference => "hostReference": optional,
});

impl_runtime_struct!(GeminiCodeAssistInit, deny_unknown: false, {
    project => "project": optional,
    endpoint => "endpoint": optional,
    session_id => "sessionId": optional,
});

impl RuntimeEncode for ProviderOAuthInit {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.client_id {
            object.field("clientId", value)?;
        }
        if let Some(value) = &self.authorize_url {
            object.field("authorizeUrl", value)?;
        }
        if let Some(value) = &self.token_url {
            object.field("tokenUrl", value)?;
        }
        if let Some(value) = &self.redirect_uri {
            object.field("redirectUri", value)?;
        }
        if let Some(value) = &self.scopes {
            object.field("scopes", value)?;
        }
        if let Some(value) = &self.extra_authorize_params {
            object.field("extraAuthorizeParams", &RuntimeStringPairs(value.clone()))?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ProviderOAuthInit {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                client_id: object.optional("clientId", &[])?,
                authorize_url: object.optional("authorizeUrl", &[])?,
                token_url: object.optional("tokenUrl", &[])?,
                redirect_uri: object.optional("redirectUri", &[])?,
                scopes: object.optional("scopes", &[])?,
                extra_authorize_params: object
                    .optional::<RuntimeStringPairs>("extraAuthorizeParams", &[])?
                    .map(|pairs| pairs.0),
            })
        })
    }
}

impl_runtime_struct!(ProviderRotationAccountInit, deny_unknown: false, {
    id => "id": required,
    credential_id => "credentialId": required,
    access_token => "accessToken": required,
    refresh_token => "refreshToken": optional,
    expires_at => "expiresAt": optional,
});

impl_runtime_struct!(ProviderCredentialRotationInit, deny_unknown: false, {
    file_path => "filePath": required,
    accounts => "accounts": required,
});

impl_runtime_struct!(InstalledPluginMutationResult, deny_unknown: false, {
    persisted => "persisted": required,
    applied_to_current_session => "appliedToCurrentSession": required,
    activation_scope => "activationScope": required,
    restart_required => "restartRequired": required,
    plugin => "plugin": required,
    installed_plugins => "installedPlugins": required,
    runtime => "runtime": required,
});

impl_runtime_struct!(PredictionResult, deny_unknown: false, {
    suggestion => "suggestion": required,
});

impl_runtime_struct!(GenerateTitleOpResult, deny_unknown: false, {
    thread_id => "threadId": required,
    title => "title": required,
});

impl_runtime_struct!(ProtocolSessionTurnFileChanges, deny_unknown: false, {
    turn_id => "turnId": optional,
    files => "files": required,
});

impl_runtime_struct!(PendingResumeResult, deny_unknown: false, {
    started => "started": required,
    reason => "reason": optional,
});

impl_runtime_struct!(ProtocolSessionFileChange, deny_unknown: false, {
    path => "path": required,
    turn_diff => "turnDiff": optional,
    kind => "kind": required,
    added => "added": optional,
    removed => "removed": optional,
});

impl_runtime_struct!(ShutdownCompletion, deny_unknown: false, {
    ok => "ok": required,
    flush_completed => "flushCompleted": required,
    cleanup_completed => "cleanupCompleted": required,
    timed_out => "timedOut": required,
    error => "error": optional,
});

impl_runtime_struct!(AgentMemorySnapshotDecisionQueryResult, deny_unknown: false, {
    request_id => "requestId": required,
    audit => "audit": optional,
});

impl RuntimeEncode for ProtocolInterruptOrigin {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let name = match self {
            Self::UserStop => "user-stop",
            Self::System => "system",
            Self::TargetedTurn => "targeted-turn",
        };
        RuntimeString::from(name).encode_runtime(out)
    }
}

impl RuntimeDecode for ProtocolInterruptOrigin {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let name = RuntimeString::decode_runtime(value)?;
        match name.try_to_utf8().as_deref() {
            Ok("user-stop") => Ok(Self::UserStop),
            Ok("system") => Ok(Self::System),
            Ok("targeted-turn") => Ok(Self::TargetedTurn),
            _ => Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                "interrupt origin must be user-stop, system, or targeted-turn".into(),
            ))),
        }
    }
}

impl_runtime_struct!(ProtocolTurnInterruptResult, deny_unknown: false, {
    accepted => "accepted": required,
    interrupted_turn_id => "interruptedTurnId": optional,
    goal_pause_error => "goalPauseError": optional,
});

impl RuntimeEncode for ProtocolTerminalNotificationDisposition {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        RuntimeString::from(match self {
            Self::Inherit => "inherit",
            Self::Suppress => "suppress",
        })
        .encode_runtime(out)
    }
}

impl RuntimeDecode for ProtocolTerminalNotificationDisposition {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match RuntimeString::decode_runtime(value)?
            .try_to_utf8()
            .as_deref()
        {
            Ok("inherit") => Ok(Self::Inherit),
            Ok("suppress") => Ok(Self::Suppress),
            _ => Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                "terminal notification disposition must be inherit or suppress".into(),
            ))),
        }
    }
}

impl ProtocolGoalContinuation {
    fn validate(&self) -> Result<(), RuntimeCodecError> {
        if self.decision_id.is_empty()
            || self.prior_submission_id.is_empty()
            || self.goal_id.as_ref().is_some_and(RuntimeString::is_empty)
            || self
                .successor_submission_id
                .as_ref()
                .is_some_and(RuntimeString::is_empty)
            || self.pending == self.continued.is_some()
            || self.pending == self.prior_completion_notification_disposition.is_some()
            || self.continued == Some(true)
                && self.prior_completion_notification_disposition
                    != Some(ProtocolTerminalNotificationDisposition::Suppress)
        {
            return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                "continuation disposition requires identities, continued and prior presentation exactly when resolved, and suppression when admitted".into())));
        }
        Ok(())
    }
}

impl RuntimeEncode for ProtocolGoalContinuation {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.validate()?;
        let mut object = RuntimeObjectEncoder::new();
        object.field("decisionId", &self.decision_id)?;
        object.field("priorSubmissionId", &self.prior_submission_id)?;
        if let Some(value) = &self.goal_id {
            object.field("goalId", value)?;
        }
        if let Some(value) = &self.successor_submission_id {
            object.field("successorSubmissionId", value)?;
        }
        object.field("pending", &self.pending)?;
        if let Some(value) = self.continued {
            object.field("continued", &value)?;
        }
        if let Some(value) = self.prior_completion_notification_disposition {
            object.field("priorCompletionNotificationDisposition", &value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}
impl RuntimeDecode for ProtocolGoalContinuation {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        if value.get("continued").is_some_and(RuntimeJson::is_null) {
            return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::ExpectedBool));
        }
        if value
            .get("priorCompletionNotificationDisposition")
            .is_some_and(RuntimeJson::is_null)
        {
            return Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::ExpectedString,
            ));
        }
        let result = runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                decision_id: object.required("decisionId", &[])?,
                prior_submission_id: object.required("priorSubmissionId", &[])?,
                goal_id: object.optional("goalId", &[])?,
                successor_submission_id: object.optional("successorSubmissionId", &[])?,
                pending: object.required("pending", &[])?,
                continued: object.optional("continued", &[])?,
                prior_completion_notification_disposition: object
                    .optional("priorCompletionNotificationDisposition", &[])?,
            })
        })?;
        result.validate()?;
        Ok(result)
    }
}

impl_runtime_struct!(ProtocolEvent, deny_unknown: false, {
    planning => "planning": optional,
    document => "document": optional,
    implementation_input => "implementationInput": optional,
    feedback_input => "feedbackInput": optional,
    implementation_id => "implementationId": optional,
    accepted => "accepted": optional,
    turn_interrupt => "turnInterrupt": optional,
    goal_continuation_decision => "goalContinuationDecision": optional,
    terminal_notification_disposition => "terminalNotificationDisposition": optional_terminal_disposition,
    submission_id => "submissionId": required,
    initialized => "initialized": optional,
    event => "event": optional,
    goal_continuation => "goalContinuation": optional,
    input_origin => "inputOrigin": optional,
    goal_id => "goalId": optional,
    goal_pursuit_eligible => "goalPursuitEligible": optional,
    goal_pursuit_eligibility_notice => "goalPursuitEligibilityNotice": optional,
    goal_submission => "goalSubmission": optional,
    turn_goal_metadata => "turnGoalMetadata": optional,
    elicitation => "elicitation": optional,
    mcp_elicitation => "mcpElicitation": optional,
    mcp_elicitation_resolved => "mcpElicitationResolved": optional,
    mcp_authorization => "mcpAuthorization": optional,
    mcp_authorization_resolved => "mcpAuthorizationResolved": optional,
    mcp_authorization_status => "mcpAuthorizationStatus": optional,
    mcp_authorization_revoked => "mcpAuthorizationRevoked": optional,
    mcp_runtime => "mcpRuntime": optional,
    mcp_server_status => "mcpServerStatus": optional,
    mcp_tool_call => "mcpToolCall": optional,
    mcp_resource_read => "mcpResourceRead": optional,
    mcp_prompts => "mcpPrompts": optional,
    mcp_prompt => "mcpPrompt": optional,
    runtime_capabilities => "runtimeCapabilities": optional,
    extension_runtime => "extensionRuntime": optional,
    installed_plugins => "installedPlugins": optional,
    plugin_runtime => "pluginRuntime": optional,
    plugin_mutation => "pluginMutation": optional,
    plugin_catalog => "pluginCatalog": optional,
    dynamic_tool_call => "dynamicToolCall": optional,
    dynamic_tool_resolved => "dynamicToolResolved": optional,
    session_config => "sessionConfig": optional,
    runtime_policy_updated => "runtimePolicyUpdated": optional,
    skills_updated => "skillsUpdated": optional,
    rewound => "rewound": optional,
    rewind_preview => "rewindPreview": optional,
    result => "result": optional,
    steered => "steered": optional,
    pending_resume => "pendingResume": optional,
    history => "history": optional,
    approval_reviews => "approvalReviews": optional,
    approval_review_denials => "reviews": optional,
    approval_review_denial_approved => "approvalReviewDenialApproved": optional,
    history_turn_ids => "historyTurnIds": optional,
    turn_final_message_ids => "turnFinalMessageIds": optional,
    turn_started_at_ms => "turnStartedAtMs": optional,
    turn_finished_at_ms => "turnFinishedAtMs": optional,
    turn_statuses => "turnStatuses": optional,
    turn_durations => "turnDurations": optional,
    turn_errors => "turnErrors": optional,
    turn_error_kinds => "turnErrorKinds": optional,
    turn_error_details => "turnErrorDetails": history_error_details,
    turn_cancelled_tools => "turnCancelledTools": optional,
    file_changes => "fileChanges": optional,
    cost => "cost": optional,
    context => "context": optional,
    subagents => "subagents": optional,
    subagent_lifecycle => "subagentLifecycle": optional,
    subagent_message => "subagentMessage": optional,
    subagent_event => "subagentEvent": optional,
    agent_notification => "agentNotification": optional,
    agent_notifications => "agentNotifications": optional,
    background_processes => "backgroundProcesses": optional,
    background_process_output => "backgroundProcessOutput": optional,
    background_process_stop => "backgroundProcessStop": optional,
    background_process_event => "backgroundProcessEvent": optional,
    memory_maintenance_event => "memoryMaintenanceEvent": optional,
    extension_failure => "extensionFailure": optional,
    memory_maintenance => "memoryMaintenance": optional,
    subagent_transcript => "subagentTranscript": transcript_optional,
    queued => "queued": optional,
    pending => "pending": optional,
    queue_snapshot => "queueSnapshot": optional,
    queue_mutation => "queueMutation": optional,
    setup => "setup": optional,
    setup_lifecycle => "setupLifecycle": optional,
    task_list => "taskList": optional,
    task_list_change => "taskListChange": optional,
    agent_memory_snapshot_decision => "agentMemorySnapshotDecision": optional,
    agent_memory_snapshot_decisions => "agentMemorySnapshotDecisions": optional,
    agent_memory_snapshot_decision_query => "agentMemorySnapshotDecisionQuery": optional,
    compact => "compact": optional,
    compact_progress => "compactProgress": optional,
    side_question => "sideQuestion": optional,
    review => "review": optional,
    dream => "dream": optional,
    dream_completed => "dreamCompleted": optional,
    generated_title => "generatedTitle": optional,
    prediction => "prediction": optional,
    prediction_context => "predictionContext": optional,
    sessions => "sessions": thread_page_optional,
    session => "session": optional,
    archived => "archived": optional,
    unarchived => "unarchived": optional,
    deleted => "deleted": optional,
    renamed => "renamed": optional,
    host_metadata_updated => "hostMetadataUpdated": optional,
    title => "title": optional,
    shutdown_completed => "shutdownCompleted": optional,
    thread_goal => "threadGoal": optional,
    goal_completions => "goalCompletions": optional,
    goal_cleared => "goalCleared": optional,
    goal_revision => "goalRevision": goal_revision,
    goal_admission => "goalAdmission": optional,
    error => "error": optional,
    error_detail => "errorDetail": optional,
});

impl_runtime_struct!(ProtocolSessionInit, deny_unknown: true, {
    collaboration_mode => "collaborationMode": optional,
    thread_id => "threadId": optional,
    cwd => "cwd": optional,
    config => "config": optional,
    permission_mode => "permissionMode": optional,
    pre_plan_permission_mode => "prePlanPermissionMode": optional,
    network_allowed => "networkAllowed": optional,
    provider => "provider": optional,
    auth => "auth": optional,
    trusted_windows_sandbox_context => "trustedWindowsSandboxContext": optional,
    mcp_oauth_redirect_uri => "mcpOauthRedirectUri": optional,
    host_capabilities => "hostCapabilities": optional,
    agent_memory_snapshot_policy => "agentMemorySnapshotPolicy": optional,
    base_instructions => "baseInstructions": optional,
    developer_instructions => "developerInstructions": optional,
    dynamic_tools => "dynamicTools": default_empty,
    lsp_servers => "lspServers": default_empty,
    shell_env => "shellEnv": optional,
    agents => "agents": default_empty,
    agent_definition_name => "agentDefinitionName": optional,
    skills => "skills": default_empty,
    output_styles => "outputStyles": default_empty,
    selected_output_style => "selectedOutputStyle": optional,
    command_hooks => "commandHooks": default_empty,
    pricing => "pricing": optional,
    capabilities => "capabilities": optional,
    capabilities_by_model => "capabilitiesByModel": optional,
    model_tier_targets => "modelTierTargets": optional,
    advisor_target => "advisorTarget": optional,
    orchestrator_target => "orchestratorTarget": optional,
    orchestration_mode => "orchestrationMode": optional,
    orchestrator_fallback_to_main => "orchestratorFallbackToMain": optional,
    feature_targets => "featureTargets": optional,
    model_source => "modelSource": optional,
});

impl_runtime_struct!(ProtocolModelCapabilitiesEntry, deny_unknown: true, {
    model => "model": required,
    capabilities => "capabilities": required,
});

impl_runtime_struct!(ModelCapabilitiesInit, deny_unknown: true, {
    native_deferred_tools => "nativeDeferredTools": optional,
    tools => "tools": optional,
    forced_tool_choice_with_thinking => "forcedToolChoiceWithThinking": optional,
    effort => "effort": optional,
    output_verbosity => "outputVerbosity": optional,
    auto_review_model_override => "autoReviewModelOverride": optional,
    requires_sandboxed_review => "requiresSandboxedReview": default_false,
    node_repl_auto_review_required => "nodeReplAutoReviewRequired": default_false,
    guardian => "guardian": optional,
    guardian_v2 => "guardianV2": optional,
    model_messages => "modelMessages": optional,
    comp_hash => "compHash": optional,
    supports_reasoning_summary_parameter => "supportsReasoningSummaryParameter": optional,
    default_reasoning_summary => "defaultReasoningSummary": optional,
    supported_reasoning_efforts => "supportedReasoningEfforts": default_empty,
    default_reasoning_effort => "defaultReasoningEffort": optional,
    multi_agent_reasoning_effort => "multiAgentReasoningEffort": optional,
    effective_context_window_percent => "effectiveContextWindowPercent": optional,
    auto_compact_token_limit => "autoCompactTokenLimit": optional,
    used_fallback_model_metadata => "usedFallbackModelMetadata": default_false,
    model_specialty => "modelSpecialty": optional,
    supports_image_detail_original => "supportsImageDetailOriginal": default_false,
    thinking => "thinking": optional,
    structured_output => "structuredOutput": default_false,
    web_search => "webSearch": default_false,
    prompt_cache_control => "promptCacheControl": default_false,
    native_pdf => "nativePdf": default_false,
    vision => "vision": default_false,
    one_million_context => "oneMillionContext": default_false,
    task_budgets => "taskBudgets": default_false,
    fast_mode => "fastMode": default_false,
    context_window => "contextWindow": optional,
    max_output_tokens => "maxOutputTokens": optional,
    image_limits => "imageLimits": optional,
    max_media_blocks => "maxMediaBlocks": optional,
    media_input_mime_types => "mediaInputMimeTypes": default_empty,
    reasoning_dialect => "reasoningDialect": optional,
    gemini_thinking => "geminiThinking": optional,
});

impl_runtime_struct!(EffortCapabilityInit, deny_unknown: true, {
    levels => "levels": default_empty,
    default => "default": optional,
});

impl_runtime_struct!(ThinkingCapabilityInit, deny_unknown: true, {
    adaptive => "adaptive": default_false,
    interleaved => "interleaved": default_false,
    can_disable_thinking => "canDisableThinking": default_false,
});

impl_runtime_struct!(GeminiThinkingInit, deny_unknown: true, {
    mode => "mode": required,
    min => "min": optional,
    max => "max": optional,
    can_disable => "canDisable": default_false,
});

impl_runtime_struct!(ProtocolTrustedWindowsSandboxContext, deny_unknown: true, {
    route_sid => "routeSid": required,
    http_addr => "httpAddr": required,
    socks_addr => "socksAddr": optional,
    broker_generation => "brokerGeneration": required,
    policy_digest => "policyDigest": required,
});

impl RuntimeEncode for ProtocolModelPricing {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("model", &self.model)?;
        object.flatten(&self.pricing)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ProtocolModelPricing {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let model = object.required("model", &[])?;
            let pricing =
                RuntimeDecode::decode_runtime(&RuntimeJson::Object(object.take_remaining()))?;
            Ok(Self { model, pricing })
        })
    }
}

#[derive(Debug)]
struct RuntimeStringMap(BTreeMap<RuntimeString, RuntimeString>);

impl RuntimeEncode for RuntimeStringMap {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = klaus_agent_core::runtime_values::RuntimeObject::new();
        for (key, value) in &self.0 {
            object.insert(key.clone(), RuntimeJson::String(value.clone()));
        }
        out.emit(RuntimeJson::Object(object))
    }
}

impl RuntimeDecode for RuntimeStringMap {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let RuntimeJson::Object(object) = value else {
            return Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::ExpectedObject,
            ));
        };
        let mut decoded = BTreeMap::new();
        for (key, value) in object.entries() {
            let RuntimeJson::String(value) = value else {
                return Err(
                    RuntimeCodecError::new(RuntimeCodecErrorKind::ExpectedString).prepend(key),
                );
            };
            decoded.insert(key, value.clone());
        }
        Ok(Self(decoded))
    }
}

#[derive(Debug)]
struct RuntimeStringPairs(Vec<(RuntimeString, RuntimeString)>);

impl RuntimeEncode for RuntimeStringPairs {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        out.emit(RuntimeJson::Array(
            self.0
                .iter()
                .map(|(key, value)| {
                    RuntimeJson::Array(vec![
                        RuntimeJson::String(key.clone()),
                        RuntimeJson::String(value.clone()),
                    ])
                })
                .collect(),
        ))
    }
}

impl RuntimeDecode for RuntimeStringPairs {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let RuntimeJson::Array(values) = value else {
            return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::ExpectedArray));
        };
        let mut decoded = Vec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            let RuntimeJson::Array(pair) = value else {
                return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::ExpectedArray)
                    .prepend(index.to_string()));
            };
            if pair.len() != 2 {
                return Err(
                    RuntimeCodecError::new(RuntimeCodecErrorKind::InvalidEnumShape)
                        .prepend(index.to_string()),
                );
            }
            let key = RuntimeString::decode_runtime(&pair[0])
                .map_err(|error| error.prepend("0").prepend(index.to_string()))?;
            let value = RuntimeString::decode_runtime(&pair[1])
                .map_err(|error| error.prepend("1").prepend(index.to_string()))?;
            decoded.push((key, value));
        }
        Ok(Self(decoded))
    }
}

impl RuntimeEncode for ElicitResponseWire {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::Answers {
                answers,
                attachments,
            } => {
                fields.field("answers", answers)?;
                if let Some(attachments) = attachments {
                    fields.field("attachments", attachments)?;
                }
                "answers"
            }
            Self::Accept { values } => {
                fields.field("values", values)?;
                "accept"
            }
            Self::Decline { reason } => {
                if let Some(reason) = reason {
                    fields.field("reason", reason)?;
                }
                "decline"
            }
        };
        runtime_codec::encode_internal(out, "kind", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ElicitResponseWire {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "kind",
            &[],
            &[
                RuntimeEnumVariant::new("accept", &[]),
                RuntimeEnumVariant::new("decline", &[]),
                RuntimeEnumVariant::new("answers", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Accept {
                    values: object.required("values", &[])?,
                }),
                1 => Ok(Self::Decline {
                    reason: object.optional("reason", &[])?,
                }),
                2 => Ok(Self::Answers {
                    answers: object.required("answers", &[])?,
                    attachments: object.optional("attachments", &[])?,
                }),
                _ => unreachable!(),
            },
        )
    }
}

impl RuntimeEncode for McpAuthorizationResponseWire {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::Code { code, state } => {
                fields.field("code", code)?;
                if let Some(state) = state {
                    fields.field("state", state)?;
                }
                "code"
            }
            Self::Decline { reason } => {
                if let Some(reason) = reason {
                    fields.field("reason", reason)?;
                }
                "decline"
            }
        };
        runtime_codec::encode_internal(out, "kind", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for McpAuthorizationResponseWire {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "kind",
            &[],
            &[
                RuntimeEnumVariant::new("code", &[]),
                RuntimeEnumVariant::new("decline", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Code {
                    code: object.required("code", &[])?,
                    state: object.optional("state", &[])?,
                }),
                1 => Ok(Self::Decline {
                    reason: object.optional("reason", &[])?,
                }),
                _ => unreachable!(),
            },
        )
    }
}

impl RuntimeEncode for ProtocolDynamicToolCallOutputContentItem {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::InputText { text } => {
                fields.field("text", text)?;
                "inputText"
            }
            Self::InputImage { image_url } => {
                fields.field("imageUrl", image_url)?;
                "inputImage"
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ProtocolDynamicToolCallOutputContentItem {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("inputText", &[]),
                RuntimeEnumVariant::new("inputImage", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::InputText {
                    text: object.required("text", &[])?,
                }),
                1 => Ok(Self::InputImage {
                    image_url: object.required("imageUrl", &[])?,
                }),
                _ => unreachable!(),
            },
        )
    }
}

impl RuntimeEncode for ProtocolSelectedCapabilityRootInit {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::Host { id } => {
                fields.field("id", id)?;
                "host"
            }
            Self::Plugin { id } => {
                fields.field("id", id)?;
                "plugin"
            }
            Self::Environment {
                environment_id,
                root,
            } => {
                fields.field("environmentId", environment_id)?;
                fields.field("root", root)?;
                "environment"
            }
        };
        runtime_codec::encode_internal(out, "kind", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ProtocolSelectedCapabilityRootInit {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "kind",
            &[],
            &[
                RuntimeEnumVariant::new("host", &[]),
                RuntimeEnumVariant::new("plugin", &[]),
                RuntimeEnumVariant::new("environment", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Host {
                    id: object.required("id", &[])?,
                }),
                1 => Ok(Self::Plugin {
                    id: object.required("id", &[])?,
                }),
                2 => Ok(Self::Environment {
                    environment_id: object.required("environmentId", &[])?,
                    root: object.required("root", &[])?,
                }),
                _ => unreachable!(),
            },
        )
    }
}

impl RuntimeEncode for ProtocolMcpOAuthOption {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::Advertised(value) => value.encode_runtime(out),
            Self::Object { scopes } => {
                let mut object = RuntimeObjectEncoder::new();
                object.field("scopes", scopes)?;
                out.emit(RuntimeJson::Object(object.finish()))
            }
        }
    }
}

impl RuntimeDecode for ProtocolMcpOAuthOption {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::Bool(value) => Ok(Self::Advertised(*value)),
            RuntimeJson::Object(_) => runtime_codec::decode_object(value, false, |object| {
                Ok(Self::Object {
                    scopes: object.optional("scopes", &[])?,
                })
            }),
            _ => Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::NoMatchingUntaggedVariant,
            )),
        }
    }
}

impl RuntimeEncode for ProtocolDynamicToolSpec {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::Function(spec) => {
                let encoded = runtime_codec::encode_value(spec)?;
                let RuntimeJson::Object(fields) = &encoded else {
                    unreachable!("function spec codec is object-shaped")
                };
                runtime_codec::encode_internal(out, "type", &[], "function", fields)
            }
            Self::Namespace(spec) => {
                let encoded = runtime_codec::encode_value(spec)?;
                let RuntimeJson::Object(fields) = &encoded else {
                    unreachable!("namespace spec codec is object-shaped")
                };
                runtime_codec::encode_internal(out, "type", &[], "namespace", fields)
            }
        }
    }
}

impl RuntimeDecode for ProtocolDynamicToolSpec {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("function", &[]),
                RuntimeEnumVariant::new("namespace", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Function(ProtocolDynamicToolFunctionSpec {
                    name: object.required("name", &[])?,
                    description: object.required("description", &[])?,
                    input_schema: object.required("inputSchema", &[])?,
                    defer_loading: object.defaulted("deferLoading", &[], || false)?,
                    read_only: object.defaulted("readOnly", &[], || false)?,
                })),
                1 => Ok(Self::Namespace(ProtocolDynamicToolNamespaceSpec {
                    name: object.required("name", &[])?,
                    description: object.required("description", &[])?,
                    tools: object.required("tools", &[])?,
                })),
                _ => unreachable!(),
            },
        )
    }
}

impl RuntimeEncode for ProtocolDynamicToolNamespaceTool {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::Function(spec) => {
                let encoded = runtime_codec::encode_value(spec)?;
                let RuntimeJson::Object(fields) = &encoded else {
                    unreachable!("function spec codec is object-shaped")
                };
                runtime_codec::encode_internal(out, "type", &[], "function", fields)
            }
        }
    }
}

impl RuntimeDecode for ProtocolDynamicToolNamespaceTool {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[RuntimeEnumVariant::new("function", &[])],
            |variant, object| match variant {
                0 => Ok(Self::Function(ProtocolDynamicToolFunctionSpec {
                    name: object.required("name", &[])?,
                    description: object.required("description", &[])?,
                    input_schema: object.required("inputSchema", &[])?,
                    defer_loading: object.defaulted("deferLoading", &[], || false)?,
                    read_only: object.defaulted("readOnly", &[], || false)?,
                })),
                _ => unreachable!(),
            },
        )
    }
}

impl RuntimeEncode for ProtocolMcpServerInit {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::Stdio {
                name,
                command,
                args,
                env,
                always_load,
                disabled_tools,
            } => {
                fields.field("name", name)?;
                fields.field("command", command)?;
                if let Some(args) = args {
                    fields.field("args", args)?;
                }
                if let Some(env) = env {
                    fields.field("env", env)?;
                }
                if let Some(always_load) = always_load {
                    fields.field("alwaysLoad", always_load)?;
                }
                if let Some(disabled_tools) = disabled_tools {
                    fields.field("disabledTools", disabled_tools)?;
                }
                "stdio"
            }
            Self::Http {
                name,
                url,
                headers,
                oauth,
                always_load,
                disabled_tools,
            } => {
                fields.field("name", name)?;
                fields.field("url", url)?;
                if let Some(headers) = headers {
                    fields.field("headers", &RuntimeStringMap(headers.clone()))?;
                }
                if let Some(oauth) = oauth {
                    fields.field("oauth", oauth)?;
                }
                if let Some(always_load) = always_load {
                    fields.field("alwaysLoad", always_load)?;
                }
                if let Some(disabled_tools) = disabled_tools {
                    fields.field("disabledTools", disabled_tools)?;
                }
                "http"
            }
            Self::Sse {
                name,
                url,
                headers,
                oauth,
                always_load,
                disabled_tools,
            } => {
                fields.field("name", name)?;
                fields.field("url", url)?;
                if let Some(headers) = headers {
                    fields.field("headers", &RuntimeStringMap(headers.clone()))?;
                }
                if let Some(oauth) = oauth {
                    fields.field("oauth", oauth)?;
                }
                if let Some(always_load) = always_load {
                    fields.field("alwaysLoad", always_load)?;
                }
                if let Some(disabled_tools) = disabled_tools {
                    fields.field("disabledTools", disabled_tools)?;
                }
                "sse"
            }
        };
        runtime_codec::encode_internal(out, "transport", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ProtocolMcpServerInit {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "transport",
            &[],
            &[
                RuntimeEnumVariant::new("stdio", &[]),
                RuntimeEnumVariant::new("http", &[]),
                RuntimeEnumVariant::new("sse", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Stdio {
                    name: object.required("name", &[])?,
                    command: object.required("command", &[])?,
                    args: object.optional("args", &[])?,
                    env: object.optional::<RuntimeEnv>("env", &[])?,
                    always_load: object.optional("alwaysLoad", &[])?,
                    disabled_tools: object.optional("disabledTools", &[])?,
                }),
                1 => Ok(Self::Http {
                    name: object.required("name", &[])?,
                    url: object.required("url", &[])?,
                    headers: object
                        .optional::<RuntimeStringMap>("headers", &[])?
                        .map(|headers| headers.0),
                    oauth: object.optional("oauth", &[])?,
                    always_load: object.optional("alwaysLoad", &[])?,
                    disabled_tools: object.optional("disabledTools", &[])?,
                }),
                2 => Ok(Self::Sse {
                    name: object.required("name", &[])?,
                    url: object.required("url", &[])?,
                    headers: object
                        .optional::<RuntimeStringMap>("headers", &[])?
                        .map(|headers| headers.0),
                    oauth: object.optional("oauth", &[])?,
                    always_load: object.optional("alwaysLoad", &[])?,
                    disabled_tools: object.optional("disabledTools", &[])?,
                }),
                _ => unreachable!(),
            },
        )
    }
}

impl RuntimeEncode for OpenAiCompatibilityInit {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(role) = &self.instruction_role {
            object.field("instructionRole", role)?;
        }
        if let Some(enabled) = &self.use_model_default_effort {
            object.field("useModelDefaultEffort", enabled)?;
        }
        if let Some(extra_headers) = &self.extra_headers {
            object.field("extraHeaders", &RuntimeStringMap(extra_headers.clone()))?;
        }
        if let Some(request_identity) = &self.request_identity {
            object.field("requestIdentity", request_identity)?;
        }
        if let Some(responses_function_tools) = &self.responses_function_tools {
            object.field("responsesFunctionTools", responses_function_tools)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for OpenAiCompatibilityInit {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                instruction_role: object.optional("instructionRole", &[])?,
                use_model_default_effort: object.optional("useModelDefaultEffort", &[])?,
                extra_headers: object
                    .optional::<RuntimeStringMap>("extraHeaders", &[])?
                    .map(|headers| headers.0),
                request_identity: object.optional("requestIdentity", &[])?,
                responses_function_tools: object.optional("responsesFunctionTools", &[])?,
            })
        })
    }
}

impl_runtime_struct!(OpenAiRequestIdentityInit, deny_unknown: true, {
    session_id => "sessionId": required,
});

impl RuntimeEncode for ProviderInit {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::Anthropic {
                model,
                base_url,
                beta_header,
                http_headers,
                env_http_headers,
                api_key_scheme,
                metadata_user_id,
            } => {
                if let Some(model) = model {
                    fields.field("model", model)?;
                }
                if let Some(base_url) = base_url {
                    fields.field("baseUrl", base_url)?;
                }
                if let Some(beta_header) = beta_header {
                    fields.field("betaHeader", beta_header)?;
                }
                if let Some(http_headers) = http_headers {
                    fields.field("httpHeaders", &RuntimeStringMap(http_headers.clone()))?;
                }
                if let Some(env_http_headers) = env_http_headers {
                    fields.field(
                        "envHttpHeaders",
                        &RuntimeStringMap(env_http_headers.clone()),
                    )?;
                }
                if let Some(api_key_scheme) = api_key_scheme {
                    fields.field("apiKeyScheme", api_key_scheme)?;
                }
                if let Some(metadata_user_id) = metadata_user_id {
                    fields.field("metadataUserId", metadata_user_id)?;
                }
                "anthropic"
            }
            Self::Openai {
                model,
                api,
                base_url,
                compatibility,
            } => {
                if let Some(model) = model {
                    fields.field("model", model)?;
                }
                if let Some(api) = api {
                    fields.field("api", api)?;
                }
                if let Some(base_url) = base_url {
                    fields.field("baseUrl", base_url)?;
                }
                if let Some(compatibility) = compatibility {
                    fields.field("compatibility", compatibility)?;
                }
                "openai"
            }
            Self::Gemini {
                model,
                base_url,
                code_assist,
            } => {
                if let Some(model) = model {
                    fields.field("model", model)?;
                }
                if let Some(base_url) = base_url {
                    fields.field("baseUrl", base_url)?;
                }
                if let Some(code_assist) = code_assist {
                    fields.field("codeAssist", code_assist)?;
                }
                "gemini"
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ProviderInit {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("anthropic", &[]),
                RuntimeEnumVariant::new("openai", &[]),
                RuntimeEnumVariant::new("gemini", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Anthropic {
                    model: object.optional("model", &[])?,
                    base_url: object.optional("baseUrl", &[])?,
                    beta_header: object.optional("betaHeader", &[])?,
                    http_headers: object
                        .optional::<RuntimeStringMap>("httpHeaders", &[])?
                        .map(|headers| headers.0),
                    env_http_headers: object
                        .optional::<RuntimeStringMap>("envHttpHeaders", &[])?
                        .map(|headers| headers.0),
                    api_key_scheme: object.optional("apiKeyScheme", &[])?,
                    metadata_user_id: object.optional("metadataUserId", &[])?,
                }),
                1 => Ok(Self::Openai {
                    model: object.optional("model", &[])?,
                    api: object.optional("api", &[])?,
                    base_url: object.optional("baseUrl", &[])?,
                    compatibility: object.optional("compatibility", &[])?,
                }),
                2 => Ok(Self::Gemini {
                    model: object.optional("model", &[])?,
                    base_url: object.optional("baseUrl", &[])?,
                    code_assist: object.optional("codeAssist", &[])?,
                }),
                _ => unreachable!(),
            },
        )
    }
}

impl RuntimeEncode for ProviderAuthInit {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::Env => "env",
            Self::ApiKey { key } => {
                fields.field("key", key)?;
                "api_key"
            }
            Self::ApiKeyStored { credential_id } => {
                fields.field("credentialId", credential_id)?;
                "api_key_stored"
            }
            Self::StaticBearer { token } => {
                fields.field("token", token)?;
                "static_bearer"
            }
            Self::OAuthToken {
                access_token,
                refresh_token,
                expires_at,
                id_token,
                oauth,
                base_url,
                extra_headers,
                credential_id,
                replace_stored,
                rotation,
            } => {
                fields.field("accessToken", access_token)?;
                if let Some(value) = refresh_token {
                    fields.field("refreshToken", value)?;
                }
                if let Some(value) = expires_at {
                    fields.field("expiresAt", value)?;
                }
                if let Some(value) = id_token {
                    fields.field("idToken", value)?;
                }
                if let Some(value) = oauth {
                    fields.field("oauth", value)?;
                }
                if let Some(value) = base_url {
                    fields.field("baseUrl", value)?;
                }
                if let Some(value) = extra_headers {
                    fields.field("extraHeaders", &RuntimeStringMap(value.clone()))?;
                }
                if let Some(value) = credential_id {
                    fields.field("credentialId", value)?;
                }
                if let Some(value) = replace_stored {
                    fields.field("replaceStored", value)?;
                }
                if let Some(value) = rotation {
                    fields.field("rotation", value)?;
                }
                "oauth_token"
            }
            Self::OAuthStored {
                credential_id,
                oauth,
                base_url,
                extra_headers,
            } => {
                fields.field("credentialId", credential_id)?;
                if let Some(value) = oauth {
                    fields.field("oauth", value)?;
                }
                if let Some(value) = base_url {
                    fields.field("baseUrl", value)?;
                }
                if let Some(value) = extra_headers {
                    fields.field("extraHeaders", &RuntimeStringMap(value.clone()))?;
                }
                "oauth_stored"
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ProviderAuthInit {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("env", &[]),
                RuntimeEnumVariant::new("api_key", &[]),
                RuntimeEnumVariant::new("api_key_stored", &[]),
                RuntimeEnumVariant::new("static_bearer", &[]),
                RuntimeEnumVariant::new("oauth_token", &["o_auth_token"]),
                RuntimeEnumVariant::new("oauth_stored", &["o_auth_stored"]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Env),
                1 => Ok(Self::ApiKey {
                    key: object.required("key", &[])?,
                }),
                2 => Ok(Self::ApiKeyStored {
                    credential_id: object.required("credentialId", &[])?,
                }),
                3 => Ok(Self::StaticBearer {
                    token: object.required("token", &[])?,
                }),
                4 => Ok(Self::OAuthToken {
                    access_token: object.required("accessToken", &[])?,
                    refresh_token: object.optional("refreshToken", &[])?,
                    expires_at: object.optional("expiresAt", &[])?,
                    id_token: object.optional("idToken", &[])?,
                    oauth: object.optional("oauth", &[])?,
                    base_url: object.optional("baseUrl", &[])?,
                    extra_headers: object
                        .optional::<RuntimeStringMap>("extraHeaders", &[])?
                        .map(|headers| headers.0),
                    credential_id: object.optional("credentialId", &[])?,
                    replace_stored: object.optional("replaceStored", &[])?,
                    rotation: object.optional("rotation", &[])?,
                }),
                5 => Ok(Self::OAuthStored {
                    credential_id: object.required("credentialId", &[])?,
                    oauth: object.optional("oauth", &[])?,
                    base_url: object.optional("baseUrl", &[])?,
                    extra_headers: object
                        .optional::<RuntimeStringMap>("extraHeaders", &[])?
                        .map(|headers| headers.0),
                }),
                _ => unreachable!(),
            },
        )
    }
}

impl RuntimeEncode for OpKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::InitSession { init } => {
                fields.field("init", init)?;
                "init_session"
            }
            Self::Run { input, options } => {
                fields.field_value("input", wire_input::serialize(input)?);
                if let Some(options) = options {
                    fields.field("options", options)?;
                }
                "run"
            }
            Self::ResumePending {
                after_submission_id,
            } => {
                fields.field("afterSubmissionId", after_submission_id)?;
                "resume_pending"
            }
            Self::ResumeQueued => "resume_queued",
            Self::Interrupt {
                origin,
                expected_turn_id,
            } => {
                if let Some(origin) = origin {
                    fields.field("origin", origin)?;
                }
                if let Some(expected) = expected_turn_id {
                    fields.field("expectedTurnId", expected)?;
                }
                "interrupt"
            }
            Self::CancelRequest { request_id } => {
                fields.field("requestId", request_id)?;
                "cancel_request"
            }
            Self::Approve {
                request_id,
                response,
            } => {
                fields.field("requestId", request_id)?;
                fields.field("response", response)?;
                "approve"
            }
            Self::ResolveElicitation {
                request_id,
                response,
            } => {
                fields.field("requestId", request_id)?;
                fields.field("response", response)?;
                "resolve_elicitation"
            }
            Self::ResolveMcpElicitation {
                request_id,
                response,
            } => {
                fields.field("requestId", request_id)?;
                fields.field("response", response)?;
                "resolve_mcp_elicitation"
            }
            Self::ResolveMcpAuthorization {
                request_id,
                response,
            } => {
                fields.field("requestId", request_id)?;
                fields.field("response", response)?;
                "resolve_mcp_authorization"
            }
            Self::GetMcpAuthorizationStatus { server_name } => {
                fields.field("serverName", server_name)?;
                "get_mcp_authorization_status"
            }
            Self::RevokeMcpAuthorization { server_name } => {
                fields.field("serverName", server_name)?;
                "revoke_mcp_authorization"
            }
            Self::GetMcpRuntime { detail, cursor } => {
                if let Some(d) = detail {
                    fields.field("detail", d)?;
                }
                if let Some(c) = cursor {
                    fields.field("cursor", c)?;
                }
                "get_mcp_runtime"
            }
            Self::ReconnectMcpServer { server_name } => {
                fields.field("serverName", server_name)?;
                "reconnect_mcp_server"
            }
            Self::UpsertMcpServer { server } => {
                fields.field("server", server)?;
                "upsert_mcp_server"
            }
            Self::RemoveMcpServer { server_name } => {
                fields.field("serverName", server_name)?;
                "remove_mcp_server"
            }
            Self::ReloadMcpServers { servers } => {
                fields.field("servers", servers)?;
                "reload_mcp_servers"
            }
            Self::CallMcpTool {
                server_name,
                tool,
                arguments,
            } => {
                fields.field("serverName", server_name)?;
                fields.field("tool", tool)?;
                if let Some(arguments) = arguments {
                    fields.field("arguments", arguments)?;
                }
                "call_mcp_tool"
            }
            Self::ReadMcpResource { server_name, uri } => {
                fields.field("serverName", server_name)?;
                fields.field("uri", uri)?;
                "read_mcp_resource"
            }
            Self::ListApprovalReviewDenials => "list_approval_review_denials",
            Self::ApproveApprovalReviewDenial { event } => {
                fields.field("event", event)?;
                "approve_approval_review_denial"
            }
            Self::ListMcpPrompts => "list_mcp_prompts",
            Self::GetMcpPrompt {
                server_name,
                name,
                arguments,
            } => {
                fields.field("serverName", server_name)?;
                fields.field("name", name)?;
                if let Some(arguments) = arguments {
                    fields.field("arguments", arguments)?;
                }
                "get_mcp_prompt"
            }
            Self::GetRuntimeCapabilities => "get_runtime_capabilities",
            Self::GetExtensionRuntime => "get_extension_runtime",
            Self::ListInstalledPlugins => "list_installed_plugins",
            Self::GetPluginRuntime => "get_plugin_runtime",
            Self::SetInstalledPluginEnabled { name, enabled } => {
                fields.field("name", name)?;
                fields.field("enabled", enabled)?;
                "set_installed_plugin_enabled"
            }
            Self::UninstallPlugin { name, remove_files } => {
                fields.field("name", name)?;
                fields.field("removeFiles", remove_files)?;
                "uninstall_plugin"
            }
            Self::InstallPlugin {
                marketplace,
                source_dir,
                version,
                expected_installed_version,
            } => {
                fields.field("marketplace", marketplace)?;
                fields.field("sourceDir", source_dir)?;
                if let Some(version) = version {
                    fields.field("version", version)?;
                }
                if let Some(expected_installed_version) = expected_installed_version {
                    fields.field("expectedInstalledVersion", expected_installed_version)?;
                }
                "install_plugin"
            }
            Self::ListPluginCatalog { roots } => {
                fields.field("roots", roots)?;
                "list_plugin_catalog"
            }
            Self::ResolveDynamicToolCall { call_id, response } => {
                fields.field("callId", call_id)?;
                fields.field("response", response)?;
                "resolve_dynamic_tool_call"
            }
            Self::Rewind { num_turns, scope } => {
                fields.field("numTurns", num_turns)?;
                if let Some(scope) = scope {
                    fields.field("scope", scope)?;
                }
                "rewind"
            }
            Self::RewindPreview { num_turns } => {
                fields.field("numTurns", num_turns)?;
                "rewind_preview"
            }
            Self::Steer {
                input,
                priority,
                delivery,
                display_metadata,
                expected_turn_id,
                queue_message_id,
            } => {
                fields.field_value("input", wire_input::serialize(input)?);
                if let Some(priority) = priority {
                    fields.field("priority", priority)?;
                }
                if let Some(delivery) = delivery {
                    fields.field("delivery", delivery)?;
                }
                if let Some(metadata) = display_metadata {
                    fields.field("displayMetadata", metadata)?;
                }
                if let Some(expected_turn_id) = expected_turn_id {
                    fields.field("expectedTurnId", expected_turn_id)?;
                }
                if let Some(queue_message_id) = queue_message_id {
                    fields.field("queueMessageId", queue_message_id)?;
                }
                "steer"
            }
            Self::AppendDisplayMessage {
                message_id,
                input,
                display_metadata,
            } => {
                fields.field("messageId", message_id)?;
                fields.field_value("input", wire_input::serialize(input)?);
                fields.field("displayMetadata", display_metadata)?;
                "append_display_message"
            }
            Self::SetModel { model } => {
                fields.field("model", model)?;
                "set_model"
            }
            Self::GetPlanning => "get_planning",
            Self::SetCollaborationMode {
                mode,
                expected_revision,
            } => {
                fields.field("mode", mode)?;
                if let Some(r) = expected_revision {
                    fields.field("expectedRevision", r)?;
                }
                "set_collaboration_mode"
            }
            Self::ReadPlanDocument { plan_id, revision } => {
                fields.field("planId", plan_id)?;
                fields.field("revision", revision)?;
                "read_plan_document"
            }
            Self::SavePlanDocument {
                plan_id,
                revision,
                content,
                expected_content,
            } => {
                fields.field("planId", plan_id)?;
                fields.field("revision", revision)?;
                fields.field("content", content)?;
                if let Some(content) = expected_content {
                    fields.field("expectedContent", content)?;
                }
                "save_plan_document"
            }
            Self::AcceptPlan {
                plan_id,
                revision,
                client_request_id,
                content,
            } => {
                fields.field("planId", plan_id)?;
                fields.field("revision", revision)?;
                fields.field("clientRequestId", client_request_id)?;
                if let Some(c) = content {
                    fields.field("content", c)?;
                }
                "accept_plan"
            }
            Self::PlanFeedback {
                plan_id,
                revision,
                client_request_id,
                feedback,
            } => {
                fields.field("planId", plan_id)?;
                fields.field("revision", revision)?;
                fields.field("clientRequestId", client_request_id)?;
                fields.field("feedback", feedback)?;
                "plan_feedback"
            }
            Self::CancelPlanImplementation { plan_id, revision } => {
                fields.field("planId", plan_id)?;
                fields.field("revision", revision)?;
                "cancel_plan_implementation"
            }
            Self::SettlePlanImplementation {
                plan_id,
                revision,
                client_request_id,
                outcome,
            } => {
                fields.field("planId", plan_id)?;
                fields.field("revision", revision)?;
                fields.field("clientRequestId", client_request_id)?;
                fields.field("outcome", outcome)?;
                "settle_plan_implementation"
            }
            Self::AnswerUserInput {
                request_id,
                turn_id,
                tool_call_id,
                answers,
                status,
            } => {
                fields.field("requestId", request_id)?;
                fields.field("turnId", turn_id)?;
                fields.field("toolCallId", tool_call_id)?;
                fields.field("answers", answers)?;
                fields.field("status", status)?;
                "answer_user_input"
            }
            Self::SetPermissionMode { mode } => {
                fields.field("mode", mode)?;
                "set_permission_mode"
            }
            Self::UpdateSessionConfig { update } => {
                fields.field("update", update)?;
                "update_session_config"
            }
            Self::GetHistory => "get_history",
            Self::GetOriginalHistory => "get_original_history",
            Self::GetFileChanges => "get_file_changes",
            Self::GetCost => "get_cost",
            Self::ContextSnapshot => "context_snapshot",
            Self::GetSubagents => "get_subagents",
            Self::GetSubagentLifecycle => "get_subagent_lifecycle",
            Self::SendSubagentMessage {
                agent_id,
                message,
                summary,
                user_initiated,
            } => {
                fields.field("agentId", agent_id)?;
                fields.field("message", message)?;
                if let Some(summary) = summary {
                    fields.field("summary", summary)?;
                }
                fields.field("userInitiated", user_initiated)?;
                "send_subagent_message"
            }
            Self::GetBackgroundProcesses => "get_background_processes",
            Self::ReadBackgroundProcessOutput {
                shell_id,
                after_offset,
                max_bytes,
            } => {
                fields.field("shellId", shell_id)?;
                if let Some(value) = after_offset {
                    fields.field("afterOffset", value)?;
                }
                if let Some(value) = max_bytes {
                    fields.field("maxBytes", value)?;
                }
                "read_background_process_output"
            }
            Self::StopBackgroundProcess { shell_id, signal } => {
                fields.field("shellId", shell_id)?;
                if let Some(value) = signal {
                    fields.field("signal", value)?;
                }
                "stop_background_process"
            }
            Self::LoadSubagentTranscript {
                thread_id,
                agent_id,
            } => {
                fields.field("threadId", thread_id)?;
                fields.field("agentId", agent_id)?;
                "load_subagent_transcript"
            }
            Self::GetQueued => "get_queued",
            Self::TakePending => "take_pending",
            Self::CompactSession {
                instructions,
                selection,
            } => {
                if let Some(instructions) = instructions {
                    fields.field("instructions", instructions)?;
                }
                if let Some(selection) = selection {
                    fields.field("selection", selection)?;
                }
                "compact_session"
            }
            Self::SideQuestion { question } => {
                fields.field_value("question", wire_input::serialize(question)?);
                "side_question"
            }
            Self::DreamNow => "dream_now",
            Self::PredictionContext => "prediction_context",
            Self::CancelPrediction { request_id } => {
                fields.field("requestId", request_id)?;
                "cancel_prediction"
            }
            Self::PredictMessage {
                prompt,
                source_turn_id,
                effort,
            } => {
                fields.field("prompt", prompt)?;
                fields.field("sourceTurnId", source_turn_id)?;
                if let Some(effort) = effort {
                    fields.field("effort", effort)?;
                }
                "predict_message"
            }
            Self::GenerateTitle {
                instructions,
                prompt,
                persist,
            } => {
                if let Some(instructions) = instructions {
                    fields.field("instructions", instructions)?;
                }
                if let Some(prompt) = prompt {
                    fields.field("prompt", prompt)?;
                }
                if let Some(persist) = persist {
                    fields.field("persist", persist)?;
                }
                "generate_title"
            }
            Self::ListSessions { options } => {
                if let Some(options) = options {
                    fields.field("options", options)?;
                }
                "list_sessions"
            }
            Self::ResumeSession { thread_id } => {
                fields.field("threadId", thread_id)?;
                "resume_session"
            }
            Self::ForkSession {
                thread_id,
                new_thread_id,
                before_message_uuid,
                after_message_uuid,
                last_turn_id,
                before_turn_id,
                inherit_goal,
            } => {
                fields.field("threadId", thread_id)?;
                if let Some(inherit_goal) = inherit_goal {
                    fields.field("inheritGoal", inherit_goal)?;
                }
                if let Some(new_thread_id) = new_thread_id {
                    fields.field("newThreadId", new_thread_id)?;
                }
                if let Some(before_message_uuid) = before_message_uuid {
                    fields.field("beforeMessageUuid", before_message_uuid)?;
                }
                if let Some(after_message_uuid) = after_message_uuid {
                    fields.field("afterMessageUuid", after_message_uuid)?;
                }
                if let Some(last_turn_id) = last_turn_id {
                    fields.field("lastTurnId", last_turn_id)?;
                }
                if let Some(before_turn_id) = before_turn_id {
                    fields.field("beforeTurnId", before_turn_id)?;
                }
                "fork_session"
            }
            Self::ArchiveSession { thread_id } => {
                fields.field("threadId", thread_id)?;
                "archive_session"
            }
            Self::DeleteSession { thread_id } => {
                fields.field("threadId", thread_id)?;
                "delete_session"
            }
            Self::UpdateTitle { thread_id, title } => {
                fields.field("threadId", thread_id)?;
                fields.field("title", title)?;
                "update_title"
            }
            Self::GetMemoryMaintenance => "get_memory_maintenance",
            Self::GetGoal => "get_goal",
            Self::SetGoal {
                owned_resources,
                client_request_id,
                submit,
                display_text,
                objective,
                status,
                token_budget,
                expected_goal_id,
                expected_goal_revision,
                origin,
            } => {
                if let Some(owned_resources) = owned_resources {
                    fields.field("ownedResources", owned_resources)?;
                }
                if let Some(client_request_id) = client_request_id {
                    fields.field("clientRequestId", client_request_id)?;
                }
                if let Some(submit) = submit {
                    fields.field("submit", submit)?;
                }
                if let Some(display_text) = display_text {
                    fields.field("displayText", display_text)?;
                }
                if let Some(expected_goal_id) = expected_goal_id {
                    fields.field("expectedGoalId", expected_goal_id)?;
                }
                if let Some(expected_goal_revision) = expected_goal_revision {
                    fields.field("expectedGoalRevision", expected_goal_revision)?;
                }
                if let Some(origin) = origin {
                    fields.field("origin", origin)?;
                }
                if let Some(objective) = objective {
                    fields.field("objective", objective)?;
                }
                if let Some(status) = status {
                    fields.field("status", status)?;
                }
                // `Some(None)` is a REMOVE and must reach the wire as an
                // explicit null; absent means "leave it alone".
                if let Some(token_budget) = token_budget {
                    match token_budget {
                        Some(value) => fields.field("tokenBudget", value)?,
                        None => fields.field("tokenBudget", &RuntimeJson::Null)?,
                    }
                }
                "set_goal"
            }
            Self::ClearGoal {
                expected_goal_id,
                expected_goal_revision,
                origin,
            } => {
                if let Some(expected_goal_id) = expected_goal_id {
                    fields.field("expectedGoalId", expected_goal_id)?;
                }
                if let Some(expected_goal_revision) = expected_goal_revision {
                    fields.field("expectedGoalRevision", expected_goal_revision)?;
                }
                if let Some(origin) = origin {
                    fields.field("origin", origin)?;
                }
                "clear_goal"
            }
            Self::GetQueueSnapshot => "get_queue_snapshot",
            Self::EditQueued {
                mutation_id,
                uuid,
                expected_revision,
                edit,
            } => {
                fields.field("mutationId", mutation_id)?;
                fields.field("uuid", uuid)?;
                fields.field("expectedRevision", expected_revision)?;
                fields.field("edit", edit)?;
                "edit_queued"
            }
            Self::RemoveQueued {
                mutation_id,
                uuid,
                expected_revision,
            } => {
                fields.field("mutationId", mutation_id)?;
                fields.field("uuid", uuid)?;
                fields.field("expectedRevision", expected_revision)?;
                "remove_queued"
            }
            Self::ReorderQueued {
                mutation_id,
                uuid,
                before_uuid,
                expected_revision,
            } => {
                fields.field("mutationId", mutation_id)?;
                fields.field("uuid", uuid)?;
                if let Some(before_uuid) = before_uuid {
                    fields.field("beforeUuid", before_uuid)?;
                }
                fields.field("expectedRevision", expected_revision)?;
                "reorder_queued"
            }
            Self::UnarchiveSession { thread_id } => {
                fields.field("threadId", thread_id)?;
                "unarchive_session"
            }
            Self::RunSetup { request } => {
                fields.field("request", request)?;
                "run_setup"
            }
            Self::GetSetupLifecycle => "get_setup_lifecycle",
            Self::GetTaskList { options } => {
                if let Some(options) = options {
                    fields.field("options", options)?;
                }
                "get_task_list"
            }
            Self::ResolveAgentMemorySnapshotDecision {
                request_id,
                decision,
            } => {
                fields.field("requestId", request_id)?;
                fields.field("decision", decision)?;
                "resolve_agent_memory_snapshot_decision"
            }
            Self::SettleAgentMemorySnapshotUnavailable {
                request_id,
                reason,
                error,
            } => {
                fields.field("requestId", request_id)?;
                fields.field("reason", reason)?;
                if let Some(error) = error {
                    fields.field("error", error)?;
                }
                "settle_agent_memory_snapshot_unavailable"
            }
            Self::GetAgentMemorySnapshotDecision { request_id } => {
                fields.field("requestId", request_id)?;
                "get_agent_memory_snapshot_decision"
            }
            Self::GetAgentMemorySnapshotDecisions => "get_agent_memory_snapshot_decisions",
            Self::CreateSideTask {
                thread_id,
                new_thread_id,
            } => {
                fields.field("threadId", thread_id)?;
                if let Some(new_thread_id) = new_thread_id {
                    fields.field("newThreadId", new_thread_id)?;
                }
                "create_side_task"
            }
            Self::Shutdown => "shutdown",
            Self::ReviewSession {
                target,
                user_facing_hint,
            } => {
                fields.field("target", target)?;
                if let Some(user_facing_hint) = user_facing_hint {
                    fields.field("userFacingHint", user_facing_hint)?;
                }
                "review_session"
            }
            Self::RenameThread {
                thread_id,
                new_thread_id,
            } => {
                fields.field("threadId", thread_id)?;
                fields.field("newThreadId", new_thread_id)?;
                "rename_thread"
            }
            Self::UpdateThreadHostMetadata {
                thread_id,
                host_metadata,
            } => {
                fields.field("threadId", thread_id)?;
                if let Some(value) = host_metadata {
                    fields.field("hostMetadata", value)?;
                }
                "update_thread_host_metadata"
            }
            Self::SetRuntimePolicy {
                permission_mode,
                pre_plan_permission_mode,
                approval_policy,
                sandbox_mode,
                approvals_reviewer,
                sandbox_workspace_write,
                network_allowed,
            } => {
                // Retired fields are accepted only by the decoder. New updates
                // contain canonical policy fields exclusively.
                let _ = (permission_mode, pre_plan_permission_mode);
                if let Some(value) = approval_policy {
                    fields.field("approvalPolicy", value)?;
                }
                if let Some(value) = sandbox_mode {
                    fields.field("sandboxMode", value)?;
                }
                if let Some(value) = approvals_reviewer {
                    fields.field("approvalsReviewer", value)?;
                }
                if let Some(value) = sandbox_workspace_write {
                    fields.field("sandboxWorkspaceWrite", value)?;
                }
                if let Some(value) = network_allowed {
                    fields.field("networkAllowed", value)?;
                }
                "set_runtime_policy"
            }
            Self::SetSkills { skills } => {
                fields.field("skills", skills)?;
                "set_skills"
            }
            Self::SetRuntimeModel { model } => {
                fields.field("model", model)?;
                "set_runtime_model"
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for OpKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        const VARIANTS: &[RuntimeEnumVariant<'_>] = &[
            RuntimeEnumVariant::new("init_session", &[]),
            RuntimeEnumVariant::new("run", &[]),
            RuntimeEnumVariant::new("interrupt", &[]),
            RuntimeEnumVariant::new("cancel_request", &[]),
            RuntimeEnumVariant::new("approve", &[]),
            RuntimeEnumVariant::new("resolve_elicitation", &[]),
            RuntimeEnumVariant::new("resolve_mcp_elicitation", &[]),
            RuntimeEnumVariant::new("resolve_mcp_authorization", &[]),
            RuntimeEnumVariant::new("get_mcp_authorization_status", &[]),
            RuntimeEnumVariant::new("revoke_mcp_authorization", &[]),
            RuntimeEnumVariant::new("get_mcp_runtime", &[]),
            RuntimeEnumVariant::new("reconnect_mcp_server", &[]),
            RuntimeEnumVariant::new("upsert_mcp_server", &[]),
            RuntimeEnumVariant::new("remove_mcp_server", &[]),
            RuntimeEnumVariant::new("reload_mcp_servers", &[]),
            RuntimeEnumVariant::new("get_runtime_capabilities", &[]),
            RuntimeEnumVariant::new("get_extension_runtime", &[]),
            RuntimeEnumVariant::new("list_installed_plugins", &[]),
            RuntimeEnumVariant::new("get_plugin_runtime", &[]),
            RuntimeEnumVariant::new("set_installed_plugin_enabled", &[]),
            RuntimeEnumVariant::new("uninstall_plugin", &[]),
            RuntimeEnumVariant::new("resolve_dynamic_tool_call", &[]),
            RuntimeEnumVariant::new("rewind", &[]),
            RuntimeEnumVariant::new("steer", &[]),
            RuntimeEnumVariant::new("set_model", &[]),
            RuntimeEnumVariant::new("set_permission_mode", &[]),
            RuntimeEnumVariant::new("update_session_config", &[]),
            RuntimeEnumVariant::new("get_history", &[]),
            RuntimeEnumVariant::new("get_cost", &[]),
            RuntimeEnumVariant::new("context_snapshot", &[]),
            RuntimeEnumVariant::new("get_subagents", &[]),
            RuntimeEnumVariant::new("get_subagent_lifecycle", &[]),
            RuntimeEnumVariant::new("load_subagent_transcript", &[]),
            RuntimeEnumVariant::new("get_queued", &[]),
            RuntimeEnumVariant::new("take_pending", &[]),
            RuntimeEnumVariant::new("compact_session", &[]),
            RuntimeEnumVariant::new("side_question", &[]),
            RuntimeEnumVariant::new("dream_now", &[]),
            RuntimeEnumVariant::new("generate_title", &[]),
            RuntimeEnumVariant::new("list_sessions", &[]),
            RuntimeEnumVariant::new("resume_session", &[]),
            RuntimeEnumVariant::new("fork_session", &[]),
            RuntimeEnumVariant::new("archive_session", &[]),
            RuntimeEnumVariant::new("delete_session", &[]),
            RuntimeEnumVariant::new("update_title", &[]),
            RuntimeEnumVariant::new("get_memory_maintenance", &[]),
            RuntimeEnumVariant::new("get_queue_snapshot", &[]),
            RuntimeEnumVariant::new("edit_queued", &[]),
            RuntimeEnumVariant::new("remove_queued", &[]),
            RuntimeEnumVariant::new("reorder_queued", &[]),
            RuntimeEnumVariant::new("unarchive_session", &[]),
            RuntimeEnumVariant::new("run_setup", &[]),
            RuntimeEnumVariant::new("get_setup_lifecycle", &[]),
            RuntimeEnumVariant::new("get_task_list", &[]),
            RuntimeEnumVariant::new("resolve_agent_memory_snapshot_decision", &[]),
            RuntimeEnumVariant::new("settle_agent_memory_snapshot_unavailable", &[]),
            RuntimeEnumVariant::new("get_agent_memory_snapshot_decision", &[]),
            RuntimeEnumVariant::new("get_agent_memory_snapshot_decisions", &[]),
            RuntimeEnumVariant::new("create_side_task", &[]),
            RuntimeEnumVariant::new("shutdown", &[]),
            RuntimeEnumVariant::new("review_session", &[]),
            RuntimeEnumVariant::new("rename_thread", &[]),
            RuntimeEnumVariant::new("update_thread_host_metadata", &[]),
            // Appended after the existing set on purpose: this decoder maps a
            // variant string to its INDEX in this array, so inserting mid-list
            // would renumber every following op. New ops go at the end.
            RuntimeEnumVariant::new("call_mcp_tool", &[]),
            RuntimeEnumVariant::new("read_mcp_resource", &[]),
            RuntimeEnumVariant::new("list_mcp_prompts", &[]),
            RuntimeEnumVariant::new("get_mcp_prompt", &[]),
            RuntimeEnumVariant::new("install_plugin", &[]),
            RuntimeEnumVariant::new("list_plugin_catalog", &[]),
            RuntimeEnumVariant::new("get_goal", &[]),
            RuntimeEnumVariant::new("set_goal", &[]),
            RuntimeEnumVariant::new("clear_goal", &[]),
            RuntimeEnumVariant::new("get_file_changes", &[]),
            RuntimeEnumVariant::new("rewind_preview", &[]),
            RuntimeEnumVariant::new("set_runtime_policy", &[]),
            // Appended at the end: adding mid-list would renumber every later op.
            RuntimeEnumVariant::new("set_runtime_model", &[]),
            RuntimeEnumVariant::new("get_background_processes", &[]),
            RuntimeEnumVariant::new("read_background_process_output", &[]),
            RuntimeEnumVariant::new("stop_background_process", &[]),
            RuntimeEnumVariant::new("resume_pending", &[]),
            RuntimeEnumVariant::new("get_original_history", &[]),
            RuntimeEnumVariant::new("resume_queued", &[]),
            RuntimeEnumVariant::new("send_subagent_message", &[]),
            RuntimeEnumVariant::new("set_skills", &[]),
            RuntimeEnumVariant::new("predict_message", &[]),
            RuntimeEnumVariant::new("prediction_context", &[]),
            RuntimeEnumVariant::new("cancel_prediction", &[]),
            RuntimeEnumVariant::new("get_planning", &[]),
            RuntimeEnumVariant::new("set_collaboration_mode", &[]),
            RuntimeEnumVariant::new("read_plan_document", &[]),
            RuntimeEnumVariant::new("save_plan_document", &[]),
            RuntimeEnumVariant::new("accept_plan", &[]),
            RuntimeEnumVariant::new("plan_feedback", &[]),
            RuntimeEnumVariant::new("cancel_plan_implementation", &[]),
            RuntimeEnumVariant::new("settle_plan_implementation", &[]),
            RuntimeEnumVariant::new("answer_user_input", &[]),
            RuntimeEnumVariant::new("list_approval_review_denials", &[]),
            RuntimeEnumVariant::new("approve_approval_review_denial", &[]),
            RuntimeEnumVariant::new("append_display_message", &[]),
        ];
        runtime_codec::decode_internal(value, false, "type", &[], VARIANTS, |variant, object| {
            match variant {
                0 => Ok(Self::InitSession {
                    init: object.required("init", &[])?,
                }),
                1 => {
                    let input: RuntimeJson = object.required("input", &[])?;
                    Ok(Self::Run {
                        input: wire_input::deserialize(&input)
                            .map_err(|error| error.prepend("input"))?,
                        options: object.optional("options", &[])?,
                    })
                }
                2 => {
                    let expected_turn_id: Option<RuntimeString> =
                        object.optional("expectedTurnId", &[])?;
                    if expected_turn_id
                        .as_ref()
                        .is_some_and(RuntimeString::is_empty)
                    {
                        return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                            "expectedTurnId must not be empty".into(),
                        ))
                        .prepend("expectedTurnId"));
                    }
                    Ok(Self::Interrupt {
                        origin: object.optional("origin", &[])?,
                        expected_turn_id,
                    })
                }
                3 => Ok(Self::CancelRequest {
                    request_id: object.required("requestId", &[])?,
                }),
                4 => Ok(Self::Approve {
                    request_id: object.required("requestId", &[])?,
                    response: object.required("response", &[])?,
                }),
                5 => Ok(Self::ResolveElicitation {
                    request_id: object.required("requestId", &[])?,
                    response: object.required("response", &[])?,
                }),
                6 => Ok(Self::ResolveMcpElicitation {
                    request_id: object.required("requestId", &[])?,
                    response: object.required("response", &[])?,
                }),
                7 => Ok(Self::ResolveMcpAuthorization {
                    request_id: object.required("requestId", &[])?,
                    response: object.required("response", &[])?,
                }),
                8 => Ok(Self::GetMcpAuthorizationStatus {
                    server_name: object.required("serverName", &[])?,
                }),
                9 => Ok(Self::RevokeMcpAuthorization {
                    server_name: object.required("serverName", &[])?,
                }),
                10 => Ok(Self::GetMcpRuntime {
                    detail: object.optional("detail", &[])?,
                    cursor: object.optional("cursor", &[])?,
                }),
                11 => Ok(Self::ReconnectMcpServer {
                    server_name: object.required("serverName", &[])?,
                }),
                12 => Ok(Self::UpsertMcpServer {
                    server: object.required("server", &[])?,
                }),
                13 => Ok(Self::RemoveMcpServer {
                    server_name: object.required("serverName", &[])?,
                }),
                14 => Ok(Self::ReloadMcpServers {
                    servers: object.required("servers", &[])?,
                }),
                15 => Ok(Self::GetRuntimeCapabilities),
                16 => Ok(Self::GetExtensionRuntime),
                17 => Ok(Self::ListInstalledPlugins),
                18 => Ok(Self::GetPluginRuntime),
                19 => Ok(Self::SetInstalledPluginEnabled {
                    name: object.required("name", &[])?,
                    enabled: object.required("enabled", &[])?,
                }),
                20 => Ok(Self::UninstallPlugin {
                    name: object.required("name", &[])?,
                    remove_files: object.defaulted("removeFiles", &[], || false)?,
                }),
                21 => Ok(Self::ResolveDynamicToolCall {
                    call_id: object.required("callId", &[])?,
                    response: object.required("response", &[])?,
                }),
                22 => {
                    let value: RuntimeJson = object.required("numTurns", &[])?;
                    let num_turns =
                        de_positive_int(&value).map_err(|error| error.prepend("numTurns"))?;
                    Ok(Self::Rewind {
                        num_turns,
                        scope: object.optional("scope", &[])?,
                    })
                }
                23 => {
                    let input: RuntimeJson = object.required("input", &[])?;
                    Ok(Self::Steer {
                        input: wire_input::deserialize(&input)
                            .map_err(|error| error.prepend("input"))?,
                        priority: object.optional("priority", &[])?,
                        delivery: object.optional("delivery", &[])?,
                        display_metadata: object.optional("displayMetadata", &[])?,
                        expected_turn_id: object.optional("expectedTurnId", &[])?,
                        queue_message_id: object.optional("queueMessageId", &[])?,
                    })
                }
                24 => Ok(Self::SetModel {
                    model: object.required("model", &[])?,
                }),
                25 => Ok(Self::SetPermissionMode {
                    mode: object.required("mode", &[])?,
                }),
                26 => Ok(Self::UpdateSessionConfig {
                    update: object.required("update", &[])?,
                }),
                27 => Ok(Self::GetHistory),
                28 => Ok(Self::GetCost),
                29 => Ok(Self::ContextSnapshot),
                30 => Ok(Self::GetSubagents),
                31 => Ok(Self::GetSubagentLifecycle),
                32 => Ok(Self::LoadSubagentTranscript {
                    thread_id: object.required("threadId", &[])?,
                    agent_id: object.required("agentId", &[])?,
                }),
                33 => Ok(Self::GetQueued),
                34 => Ok(Self::TakePending),
                35 => Ok(Self::CompactSession {
                    instructions: object.optional("instructions", &[])?,
                    selection: object.optional("selection", &[])?,
                }),
                36 => {
                    let question: RuntimeJson = object.required("question", &[])?;
                    Ok(Self::SideQuestion {
                        question: wire_input::deserialize(&question)
                            .map_err(|error| error.prepend("question"))?,
                    })
                }
                37 => Ok(Self::DreamNow),
                38 => Ok(Self::GenerateTitle {
                    instructions: object.optional("instructions", &[])?,
                    prompt: object.optional("prompt", &[])?,
                    persist: object.optional("persist", &[])?,
                }),
                39 => Ok(Self::ListSessions {
                    options: object.optional("options", &[])?,
                }),
                40 => Ok(Self::ResumeSession {
                    thread_id: object.required("threadId", &[])?,
                }),
                41 => Ok(Self::ForkSession {
                    thread_id: object.required("threadId", &[])?,
                    new_thread_id: object.optional("newThreadId", &[])?,
                    before_message_uuid: object.optional("beforeMessageUuid", &[])?,
                    after_message_uuid: object.optional("afterMessageUuid", &[])?,
                    last_turn_id: object.optional("lastTurnId", &[])?,
                    before_turn_id: object.optional("beforeTurnId", &[])?,
                    inherit_goal: object.optional("inheritGoal", &[])?,
                }),
                42 => Ok(Self::ArchiveSession {
                    thread_id: object.required("threadId", &[])?,
                }),
                43 => Ok(Self::DeleteSession {
                    thread_id: object.required("threadId", &[])?,
                }),
                44 => Ok(Self::UpdateTitle {
                    thread_id: object.required("threadId", &[])?,
                    title: object.required("title", &[])?,
                }),
                45 => Ok(Self::GetMemoryMaintenance),
                46 => Ok(Self::GetQueueSnapshot),
                47 => Ok(Self::EditQueued {
                    mutation_id: object.required("mutationId", &[])?,
                    uuid: object.required("uuid", &[])?,
                    expected_revision: object.required("expectedRevision", &[])?,
                    edit: object.required("edit", &[])?,
                }),
                48 => Ok(Self::RemoveQueued {
                    mutation_id: object.required("mutationId", &[])?,
                    uuid: object.required("uuid", &[])?,
                    expected_revision: object.required("expectedRevision", &[])?,
                }),
                49 => Ok(Self::ReorderQueued {
                    mutation_id: object.required("mutationId", &[])?,
                    uuid: object.required("uuid", &[])?,
                    before_uuid: object.optional("beforeUuid", &[])?,
                    expected_revision: object.required("expectedRevision", &[])?,
                }),
                50 => Ok(Self::UnarchiveSession {
                    thread_id: object.required("threadId", &[])?,
                }),
                51 => Ok(Self::RunSetup {
                    request: object.required("request", &[])?,
                }),
                52 => Ok(Self::GetSetupLifecycle),
                53 => Ok(Self::GetTaskList {
                    options: object.optional("options", &[])?,
                }),
                54 => Ok(Self::ResolveAgentMemorySnapshotDecision {
                    request_id: object.required("requestId", &[])?,
                    decision: object.required("decision", &[])?,
                }),
                55 => Ok(Self::SettleAgentMemorySnapshotUnavailable {
                    request_id: object.required("requestId", &[])?,
                    reason: object.required("reason", &[])?,
                    error: object.optional("error", &[])?,
                }),
                56 => Ok(Self::GetAgentMemorySnapshotDecision {
                    request_id: object.required("requestId", &[])?,
                }),
                57 => Ok(Self::GetAgentMemorySnapshotDecisions),
                58 => Ok(Self::CreateSideTask {
                    thread_id: object.required("threadId", &[])?,
                    new_thread_id: object.optional("newThreadId", &[])?,
                }),
                59 => Ok(Self::Shutdown),
                60 => Ok(Self::ReviewSession {
                    target: object.required("target", &[])?,
                    user_facing_hint: object.optional("userFacingHint", &[])?,
                }),
                61 => Ok(Self::RenameThread {
                    thread_id: object.required("threadId", &[])?,
                    new_thread_id: object.required("newThreadId", &[])?,
                }),
                62 => Ok(Self::UpdateThreadHostMetadata {
                    thread_id: object.required("threadId", &[])?,
                    host_metadata: object.optional("hostMetadata", &[])?,
                }),
                63 => Ok(Self::CallMcpTool {
                    server_name: object.required("serverName", &[])?,
                    tool: object.required("tool", &[])?,
                    arguments: object.optional("arguments", &[])?,
                }),
                64 => Ok(Self::ReadMcpResource {
                    server_name: object.required("serverName", &[])?,
                    uri: object.required("uri", &[])?,
                }),
                65 => Ok(Self::ListMcpPrompts),
                66 => Ok(Self::GetMcpPrompt {
                    server_name: object.required("serverName", &[])?,
                    name: object.required("name", &[])?,
                    arguments: object.optional("arguments", &[])?,
                }),
                67 => Ok(Self::InstallPlugin {
                    marketplace: object.required("marketplace", &[])?,
                    source_dir: object.required("sourceDir", &[])?,
                    version: object.optional("version", &[])?,
                    expected_installed_version: object.optional("expectedInstalledVersion", &[])?,
                }),
                68 => Ok(Self::ListPluginCatalog {
                    roots: object.defaulted("roots", &[], Vec::new)?,
                }),
                69 => Ok(Self::GetGoal),
                70 => {
                    let owned_resources: Option<
                        klaus_agent_core::goal::owned_resources::GoalOwnedResourcesInput,
                    > = object.optional("ownedResources", &[])?;
                    let objective: Option<RuntimeString> = object.optional("objective", &[])?;
                    let origin: Option<RuntimeString> = object.optional("origin", &[])?;
                    if owned_resources.is_some()
                        && (origin.as_ref().is_none_or(|origin| *origin != "user")
                            || objective.as_ref().is_none_or(|objective| {
                                objective
                                    .try_to_utf8()
                                    .map(|text| text.trim().is_empty())
                                    .unwrap_or(true)
                            }))
                    {
                        return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                            "ownedResources requires an explicit user objective".into(),
                        ))
                        .prepend("ownedResources"));
                    }
                    Ok(Self::SetGoal {
                        owned_resources,
                        client_request_id: object.optional("clientRequestId", &[])?,
                        submit: object.optional("submit", &[])?,
                        display_text: object.optional("displayText", &[])?,
                        objective,
                        status: object.optional("status", &[])?,
                        expected_goal_id: object.optional_nullable("expectedGoalId", &[])?,
                        expected_goal_revision: decode_goal_revision(
                            object.optional_nullable("expectedGoalRevision", &[])?,
                        )?,
                        origin,
                        token_budget: object.optional_nullable("tokenBudget", &[])?,
                    })
                }
                71 => Ok(Self::ClearGoal {
                    expected_goal_id: object.optional("expectedGoalId", &[])?,
                    expected_goal_revision: decode_goal_revision(
                        object.optional_nullable("expectedGoalRevision", &[])?,
                    )?,
                    origin: object.optional("origin", &[])?,
                }),
                72 => Ok(Self::GetFileChanges),
                73 => {
                    let value: RuntimeJson = object.required("numTurns", &[])?;
                    let num_turns =
                        de_positive_int(&value).map_err(|error| error.prepend("numTurns"))?;
                    Ok(Self::RewindPreview { num_turns })
                }
                74 => {
                    let update = Self::SetRuntimePolicy {
                        permission_mode: object.optional("permissionMode", &[])?,
                        pre_plan_permission_mode: object.optional("prePlanPermissionMode", &[])?,
                        approval_policy: object.optional("approvalPolicy", &[])?,
                        sandbox_mode: object.optional("sandboxMode", &[])?,
                        approvals_reviewer: object.optional("approvalsReviewer", &[])?,
                        sandbox_workspace_write: object.optional("sandboxWorkspaceWrite", &[])?,
                        network_allowed: object.optional("networkAllowed", &[])?,
                    };
                    if !object.take_remaining().is_empty() {
                        return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownField));
                    }
                    Ok(update)
                }
                75 => Ok(Self::SetRuntimeModel {
                    model: object.required("model", &[])?,
                }),
                76 => Ok(Self::GetBackgroundProcesses),
                77 => Ok(Self::ReadBackgroundProcessOutput {
                    shell_id: object.required("shellId", &[])?,
                    after_offset: object.optional("afterOffset", &[])?,
                    max_bytes: object.optional("maxBytes", &[])?,
                }),
                78 => Ok(Self::StopBackgroundProcess {
                    shell_id: object.required("shellId", &[])?,
                    signal: object.optional("signal", &[])?,
                }),
                80 => Ok(Self::GetOriginalHistory),
                79 => Ok(Self::ResumePending {
                    after_submission_id: object.required("afterSubmissionId", &[])?,
                }),
                81 => Ok(Self::ResumeQueued),
                82 => {
                    let message = Self::SendSubagentMessage {
                        agent_id: object.required("agentId", &[])?,
                        message: object.required("message", &[])?,
                        summary: object.optional("summary", &[])?,
                        user_initiated: object.defaulted("userInitiated", &[], || false)?,
                    };
                    if !object.take_remaining().is_empty() {
                        return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownField));
                    }
                    Ok(message)
                }
                83 => Ok(Self::SetSkills {
                    skills: object.required("skills", &[])?,
                }),
                84 => Ok(Self::PredictMessage {
                    prompt: object.required("prompt", &[])?,
                    source_turn_id: object.required("sourceTurnId", &[])?,
                    effort: object.optional("effort", &[])?,
                }),
                85 => Ok(Self::PredictionContext),
                86 => Ok(Self::CancelPrediction {
                    request_id: object.required("requestId", &[])?,
                }),
                87 => Ok(Self::GetPlanning),
                88 => Ok(Self::SetCollaborationMode {
                    mode: object.required("mode", &[])?,
                    expected_revision: object.optional("expectedRevision", &[])?,
                }),
                89 => Ok(Self::ReadPlanDocument {
                    plan_id: object.required("planId", &[])?,
                    revision: object.required("revision", &[])?,
                }),
                90 => Ok(Self::SavePlanDocument {
                    plan_id: object.required("planId", &[])?,
                    revision: object.required("revision", &[])?,
                    content: object.required("content", &[])?,
                    expected_content: object.optional("expectedContent", &[])?,
                }),
                91 => Ok(Self::AcceptPlan {
                    plan_id: object.required("planId", &[])?,
                    revision: object.required("revision", &[])?,
                    client_request_id: object.required("clientRequestId", &[])?,
                    content: object.optional("content", &[])?,
                }),
                92 => Ok(Self::PlanFeedback {
                    plan_id: object.required("planId", &[])?,
                    revision: object.required("revision", &[])?,
                    client_request_id: object.required("clientRequestId", &[])?,
                    feedback: object.required("feedback", &[])?,
                }),
                93 => Ok(Self::CancelPlanImplementation {
                    plan_id: object.required("planId", &[])?,
                    revision: object.required("revision", &[])?,
                }),
                94 => Ok(Self::SettlePlanImplementation {
                    plan_id: object.required("planId", &[])?,
                    revision: object.required("revision", &[])?,
                    client_request_id: object.required("clientRequestId", &[])?,
                    outcome: object.required("outcome", &[])?,
                }),
                95 => Ok(Self::AnswerUserInput {
                    request_id: object.required("requestId", &[])?,
                    turn_id: object.required("turnId", &[])?,
                    tool_call_id: object.required("toolCallId", &[])?,
                    answers: object.required("answers", &[])?,
                    status: object.required("status", &[])?,
                }),
                96 => Ok(Self::ListApprovalReviewDenials),
                97 => Ok(Self::ApproveApprovalReviewDenial {
                    event: object.required("event", &[])?,
                }),
                98 => {
                    let input: RuntimeJson = object.required("input", &[])?;
                    Ok(Self::AppendDisplayMessage {
                        message_id: object.required("messageId", &[])?,
                        input: wire_input::deserialize(&input)
                            .map_err(|error| error.prepend("input"))?,
                        display_metadata: object.required("displayMetadata", &[])?,
                    })
                }
                _ => unreachable!(),
            }
        })
    }
}

#[cfg(test)]
mod subagent_message_tests {
    use super::*;
    use klaus_agent_core::runtime_values::runtime_json;

    #[test]
    fn child_input_defaults_to_agent_origin_and_rejects_unknown_authority_fields() {
        let parse = |text: &str| runtime_json::parse(&RuntimeString::from(text)).unwrap();
        let ordinary = OpKind::decode_runtime(&parse(
            r#"{"type":"send_subagent_message","agentId":"child","message":"Continue"}"#,
        ))
        .unwrap();
        assert!(matches!(
            ordinary,
            OpKind::SendSubagentMessage {
                user_initiated: false,
                ..
            }
        ));
        let manual = OpKind::decode_runtime(&parse(
            r#"{"type":"send_subagent_message","agentId":"child","message":"Continue","userInitiated":true}"#,
        )).unwrap();
        assert!(matches!(
            manual,
            OpKind::SendSubagentMessage {
                user_initiated: true,
                ..
            }
        ));
        for text in [
            r#"{"type":"send_subagent_message","agentId":"child","message":"Continue","permissionMode":"bypassPermissions"}"#,
            r#"{"type":"send_subagent_message","agentId":"child","message":"Continue","userInitiated":null}"#,
            r#"{"type":"send_subagent_message","agentId":"child","message":"Continue","userInitiated":"true"}"#,
        ] {
            assert!(OpKind::decode_runtime(&parse(text)).is_err());
        }
    }

    #[test]
    fn child_input_acknowledgement_uses_its_own_exact_reply_field() {
        let reply = runtime_json::parse(&RuntimeString::from(
            r#"{"ok":true,"agentId":"child","resumed":true}"#,
        ))
        .unwrap();
        let event = ProtocolEvent {
            submission_id: "request-1".into(),
            subagent_message: Some(reply.clone()),
            ..Default::default()
        };
        let encoded = runtime_codec::encode_value(&event).unwrap();
        assert_eq!(encoded.get("subagentMessage"), Some(&reply));
        assert_eq!(
            ProtocolEvent::decode_runtime(&encoded)
                .unwrap()
                .subagent_message,
            Some(reply)
        );
    }
}

#[cfg(test)]
mod background_process_tests {
    use super::*;

    fn assert_wire(op: OpKind, expected: &str) {
        let encoded = runtime_codec::encode_value(&op).expect("op encodes");
        assert_eq!(
            klaus_agent_core::runtime_values::runtime_json::stringify(&encoded)
                .try_to_utf8()
                .expect("encoded op is scalar JSON"),
            expected
        );
        let decoded = runtime_codec::decode_value::<OpKind>(&encoded).expect("op decodes");
        assert_eq!(runtime_codec::encode_value(&decoded).unwrap(), encoded);
    }

    #[test]
    fn background_process_ops_have_exact_append_only_wire_shapes() {
        assert_wire(
            OpKind::GetBackgroundProcesses,
            r#"{"type":"get_background_processes"}"#,
        );
        assert_wire(
            OpKind::ReadBackgroundProcessOutput {
                shell_id: "shell-1".into(),
                after_offset: Some(12),
                max_bytes: Some(4096),
            },
            r#"{"type":"read_background_process_output","shellId":"shell-1","afterOffset":12,"maxBytes":4096}"#,
        );
        assert_wire(
            OpKind::StopBackgroundProcess {
                shell_id: "shell-1".into(),
                signal: Some("SIGTERM".into()),
            },
            r#"{"type":"stop_background_process","shellId":"shell-1","signal":"SIGTERM"}"#,
        );
    }

    #[test]
    fn prediction_ops_preserve_exact_wire_shapes() {
        assert_wire(
            OpKind::PredictionContext,
            r#"{"type":"prediction_context"}"#,
        );
        assert_wire(
            OpKind::CancelPrediction {
                request_id: "request-1".into(),
            },
            r#"{"type":"cancel_prediction","requestId":"request-1"}"#,
        );
        assert_wire(
            OpKind::PredictMessage {
                prompt: "dynamic prompt".into(),
                source_turn_id: "turn-1".into(),
                effort: None,
            },
            r#"{"type":"predict_message","prompt":"dynamic prompt","sourceTurnId":"turn-1"}"#,
        );
        assert_wire(
            OpKind::PredictMessage {
                prompt: "dynamic prompt".into(),
                source_turn_id: "turn-1".into(),
                effort: Some(klaus_agent_core::schemas::effort::EffortValue::Level(
                    klaus_agent_core::schemas::effort::EffortLevel::Low,
                )),
            },
            r#"{"type":"predict_message","prompt":"dynamic prompt","sourceTurnId":"turn-1","effort":"low"}"#,
        );
    }

    #[test]
    fn prediction_context_and_suggestion_preserve_explicit_null_fields() {
        let context = klaus_agent_core::engine::session::PredictionContext {
            turn_id: None,
            model: "main-model".into(),
            provider: "test-provider".into(),
            plan_mode: false,
            running: false,
        };
        let event = ProtocolEvent {
            submission_id: "context-request".into(),
            prediction_context: Some(context.clone()),
            prediction: Some(PredictionResult { suggestion: None }),
            ..Default::default()
        };
        let encoded = runtime_codec::encode_value(&event).unwrap();
        assert_eq!(
            encoded
                .get("predictionContext")
                .and_then(|value| value.get("turnId")),
            Some(&RuntimeJson::Null)
        );
        assert_eq!(
            encoded
                .get("prediction")
                .and_then(|value| value.get("suggestion")),
            Some(&RuntimeJson::Null)
        );
        let decoded = ProtocolEvent::decode_runtime(&encoded).unwrap();
        assert_eq!(decoded.prediction_context, Some(context));
        assert_eq!(decoded.prediction, event.prediction);
    }

    #[test]
    fn followup_ops_preserve_exact_optional_identity_and_resume_wire_shapes() {
        assert_wire(OpKind::ResumeQueued, r#"{"type":"resume_queued"}"#);
        assert_wire(
            OpKind::Steer {
                input: Input::Text("follow-up".into()),
                priority: None,
                delivery: None,
                display_metadata: None,
                expected_turn_id: Some("turn-1".into()),
                queue_message_id: Some("c9a02ab1-092b-4206-8b14-bbb7d37d5f20".into()),
            },
            r#"{"type":"steer","input":"follow-up","expectedTurnId":"turn-1","queueMessageId":"c9a02ab1-092b-4206-8b14-bbb7d37d5f20"}"#,
        );
        assert_wire(
            OpKind::Steer {
                input: Input::Text("expanded browser context".into()),
                priority: None,
                delivery: Some(klaus_agent_core::engine::message_queue::SteerDelivery::User),
                display_metadata: Some(
                    klaus_agent_core::runtime_values::runtime_json::parse(&RuntimeString::from(
                        r#"{"displayText":"Inspect Orders","browserTabs":[{"tabId":42}]}"#,
                    ))
                    .unwrap(),
                ),
                expected_turn_id: None,
                queue_message_id: None,
            },
            r#"{"type":"steer","input":"expanded browser context","delivery":"user","displayMetadata":{"displayText":"Inspect Orders","browserTabs":[{"tabId":42}]}}"#,
        );
        assert_wire(
            OpKind::Steer {
                input: Input::Text("legacy".into()),
                priority: None,
                delivery: None,
                display_metadata: None,
                expected_turn_id: None,
                queue_message_id: None,
            },
            r#"{"type":"steer","input":"legacy"}"#,
        );
    }
}

fn decode_goal_revision(value: Option<Option<i64>>) -> Result<Option<i64>, RuntimeCodecError> {
    match value {
        None => Ok(None),
        Some(Some(revision)) if (0..=9_007_199_254_740_991).contains(&revision) => {
            Ok(Some(revision))
        }
        _ => Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
            "expectedGoalRevision must be a nonnegative safe integer".into(),
        ))
        .prepend("expectedGoalRevision")),
    }
}

fn decode_maximum_goal_budget(
    value: Option<Option<i64>>,
) -> Result<Option<Option<i64>>, RuntimeCodecError> {
    if value
        .flatten()
        .is_some_and(|budget| !(1..=9_007_199_254_740_991).contains(&budget))
    {
        return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
            "maximumGoalTokenBudget must be a positive safe integer or null".into(),
        ))
        .prepend("maximumGoalTokenBudget"));
    }
    Ok(value)
}

fn validate_goal_wire_integer(field: &str, value: i64) -> Result<i64, RuntimeCodecError> {
    if !(-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&value) {
        return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
            format!("{field} ({value}) cannot be represented as an exact JSON integer").into(),
        ))
        .prepend(field));
    }
    Ok(value)
}

#[cfg(test)]
mod fast_capability_tests {
    use super::*;

    #[test]
    fn model_fast_capability_has_an_explicit_exact_wire_field() {
        let value = klaus_agent_core::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"fastMode":true,"vision":true}"#,
        ))
        .unwrap();
        let caps: ModelCapabilitiesInit = runtime_codec::decode_value(&value).unwrap();
        assert!(caps.fast_mode);
        assert!(caps.vision);
        let encoded = runtime_codec::encode_value(&caps).unwrap();
        assert_eq!(
            encoded.get("fastMode").and_then(RuntimeJson::as_bool),
            Some(true)
        );
        let default: ModelCapabilitiesInit = runtime_codec::decode_value(
            &klaus_agent_core::runtime_values::runtime_json::parse(&RuntimeString::from("{}"))
                .unwrap(),
        )
        .unwrap();
        assert!(!default.fast_mode);
    }
}

#[cfg(test)]
mod approval_review_update_tests {
    use super::*;
    use klaus_agent_core::runtime_values::runtime_json;

    #[test]
    fn approval_review_update_distinguishes_absence_reset_disabled_and_full_options() {
        for source in [
            r#"{}"#,
            r#"{"approvalReview":null}"#,
            r#"{"approvalReview":false}"#,
            r#"{"approvalReview":{"enabled":true,"review_threshold":0.7}}"#,
        ] {
            let value = runtime_json::parse(&RuntimeString::from(source)).unwrap();
            let update = ProtocolSessionConfigUpdate::decode_runtime(&value).unwrap();
            assert_eq!(runtime_codec::encode_value(&update).unwrap(), value);
        }
        let invalid = runtime_json::parse(&RuntimeString::from(
            r#"{"approvalReview":{"review_threshold":1.5}}"#,
        ))
        .unwrap();
        assert!(ProtocolSessionConfigUpdate::decode_runtime(&invalid).is_err());
    }
}

#[cfg(test)]
mod ultra_mode_codec_tests {
    use super::*;

    #[test]
    fn independent_mode_update_roundtrips_false_and_rejects_non_boolean() {
        let update: ProtocolSessionConfigUpdate =
            runtime_codec::decode_text(&RuntimeString::from(r#"{"ultraMode":false}"#)).unwrap();
        assert_eq!(update.ultra_mode, Some(false));
        let encoded = runtime_codec::encode_value(&update).unwrap();
        assert_eq!(encoded.get("ultraMode"), Some(&RuntimeJson::Bool(false)));
        assert!(
            runtime_codec::decode_text::<ProtocolSessionConfigUpdate>(&RuntimeString::from(
                r#"{"ultraMode":"false"}"#,
            ))
            .is_err()
        );
    }
}

#[cfg(test)]
mod op_kind_index_tests {
    use super::*;

    fn round_trip(op: &OpKind) -> OpKind {
        let encoded = runtime_codec::encode_value(op).expect("op encodes");
        OpKind::decode_runtime(&encoded).expect("op decodes")
    }

    /// The numeric op kind comes from the position in `VARIANTS`, so a variant
    /// inserted anywhere but the end silently renumbers every later op. Pin a
    /// few late entries to the index their own name encodes to.
    #[test]
    fn op_kind_indices_stay_aligned_with_the_variant_list() {
        assert!(matches!(
            round_trip(&OpKind::SetModel {
                model: RuntimeString::from("m")
            }),
            OpKind::SetModel { .. }
        ));
        assert!(matches!(
            round_trip(&OpKind::GetHistory),
            OpKind::GetHistory
        ));
        assert!(matches!(round_trip(&OpKind::DreamNow), OpKind::DreamNow));
        assert!(matches!(
            round_trip(&OpKind::AppendDisplayMessage {
                message_id: RuntimeString::from("m"),
                input: Input::Text(RuntimeString::from("t")),
                display_metadata: RuntimeJson::Null,
            }),
            OpKind::AppendDisplayMessage { .. }
        ));
    }
}
