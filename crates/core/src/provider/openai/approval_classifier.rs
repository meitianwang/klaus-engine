// Copyright 2026 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified: integrates Responses requests and trust configuration with local providers.

//! Native Responses adapter for isolated background classification.

use super::*;
use crate::provider::approval_classifier_transport::{
    ApprovalClassifierTransport, ClassifierAttempt, ClassifierAttribution, ClassifierAuthRecovery,
    ClassifierConnector, ClassifierEndpoint, ClassifierRequest, ClassifierStream, ClassifierTarget,
    ClassifierTransportEvent, RawClassifierStream, ResponsesClassifierTransport,
};
use crate::provider::auth::{
    AuthSource, CredentialChangeState, ManagedReviewCredentialIdentity,
    ManagedReviewCredentialRecovery,
};
use std::time::Duration;
use tokio::sync::{watch, OnceCell};

pub(super) fn create(
    mut provider: OpenAIProvider,
    attribution: ClassifierAttribution,
    target: Option<ClassifierTarget>,
) -> Arc<dyn ApprovalClassifierTransport> {
    if let Some(target) = &target {
        if let Ok(model) = target.model.try_to_utf8() {
            provider
                .capabilities_by_model
                .insert(model, target.metadata.clone());
        }
    }
    Arc::new(ResponsesClassifierTransport::new(
        Arc::new(OpenAIClassifierConnector {
            provider,
            raw_effort: target.and_then(|target| target.reasoning_effort),
            tls_config: OnceCell::new(),
            regular_client: OnceCell::new(),
            restricted_client: OnceCell::new(),
        }),
        attribution,
    ))
}

struct OpenAIClassifierConnector {
    provider: OpenAIProvider,
    raw_effort: Option<RuntimeString>,
    tls_config: OnceCell<Arc<rustls::ClientConfig>>,
    regular_client: OnceCell<reqwest::Client>,
    restricted_client: OnceCell<reqwest::Client>,
}

enum ClassifierRecoveryMode {
    Managed(ManagedReviewCredentialIdentity),
    External,
}

struct NativeClassifierAuthRecovery {
    auth: Arc<dyn AuthSource>,
    auth_changes: Option<watch::Receiver<CredentialChangeState>>,
    owner: Option<u64>,
    mode: ClassifierRecoveryMode,
    step: usize,
    exhausted: bool,
}

impl NativeClassifierAuthRecovery {
    fn owner_unchanged(&self) -> bool {
        self.auth_changes
            .as_ref()
            .map(|changes| changes.borrow().owner_generation)
            == self.owner
    }
}

#[async_trait::async_trait]
impl ClassifierAuthRecovery for NativeClassifierAuthRecovery {
    async fn next(&mut self) -> bool {
        if self.exhausted || !self.owner_unchanged() {
            self.exhausted = true;
            return false;
        }
        let step = self.step;
        self.step += 1;
        let recovered = match &self.mode {
            ClassifierRecoveryMode::Managed(identity) if step < 2 => matches!(
                self.auth
                    .recover_managed_review_unauthorized(step, identity)
                    .await,
                ManagedReviewCredentialRecovery::Refreshed
            ),
            ClassifierRecoveryMode::External if step == 0 => {
                self.auth.force_refresh().await == Some(true)
            }
            _ => false,
        } && self.owner_unchanged();
        self.exhausted = !recovered
            || match &self.mode {
                ClassifierRecoveryMode::Managed(_) => step >= 1,
                ClassifierRecoveryMode::External => true,
            };
        recovered
    }
}

impl OpenAIClassifierConnector {
    async fn client(&self, restricted: bool) -> Result<reqwest::Client, ProviderError> {
        if self.provider.classifier_requires_client_factory {
            return Err(classifier_build_error("A custom provider HTTP client requires a classifier client factory so proxy, certificate, cookie and redirect settings remain intact."));
        }
        let cell = if restricted {
            &self.restricted_client
        } else {
            &self.regular_client
        };
        let factory = self.provider.classifier_http_client_factory.clone();
        let tls_config = if factory.is_none() {
            Some(
                self.tls_config
                    .get_or_try_init(|| async {
                        tokio::task::spawn_blocking(default_tls_config)
                            .await
                            .map_err(|_| {
                                classifier_build_error("classifier TLS setup task failed")
                            })?
                    })
                    .await?
                    .clone(),
            )
        } else {
            None
        };
        cell.get_or_try_init(|| async move {
            tokio::task::spawn_blocking(move || match factory {
                Some(factory) => factory(restricted),
                None => default_client(restricted, tls_config.expect("default TLS config")),
            })
            .await
            .map_err(|_| classifier_build_error("classifier client setup task failed"))?
        })
        .await
        .cloned()
    }
}

#[async_trait::async_trait]
impl ClassifierConnector for OpenAIClassifierConnector {
    async fn auth_recovery(&self, signal: &AbortSignal) -> Option<Box<dyn ClassifierAuthRecovery>> {
        let auth = self.provider.auth_source.clone()?;
        let mut auth_changes = auth.credential_changes();
        let owner = auth_changes
            .as_mut()
            .map(|changes| changes.borrow_and_update().owner_generation);
        let credentials = auth.managed_review_credentials(signal).await.ok()?;
        if signal.is_cancelled()
            || auth_changes
                .as_ref()
                .map(|changes| changes.borrow().owner_generation)
                != owner
        {
            return None;
        }
        let mode = match credentials {
            Some(credentials) => {
                if credentials.identity.account_id.is_none()
                    || owner.is_some_and(|owner| owner != credentials.identity.owner_generation)
                {
                    return None;
                }
                ClassifierRecoveryMode::Managed(credentials.identity)
            }
            None => ClassifierRecoveryMode::External,
        };
        Some(Box::new(NativeClassifierAuthRecovery {
            auth,
            auth_changes,
            owner,
            mode,
            step: 0,
            exhausted: false,
        }))
    }
    async fn resolve(&self, signal: &AbortSignal) -> Result<ClassifierEndpoint, ProviderError> {
        let mut auth_changes = self
            .provider
            .auth_source
            .as_ref()
            .and_then(|auth| auth.credential_changes());
        let auth_state = auth_changes
            .as_mut()
            .map(|changes| *changes.borrow_and_update());
        let mut headers = self
            .provider
            .compatibility
            .as_ref()
            .map(|compatibility| compatibility.extra_headers().clone())
            .unwrap_or_default()
            .into_iter()
            .map(|(name, value)| (name.to_ascii_lowercase(), value))
            .collect::<HashMap<_, _>>();
        let url = self.provider.endpoint("/responses");
        let attempt =
            self.provider.interceptor.as_ref().map(|interceptor| {
                Arc::new(ClassifierAttempt::new(interceptor.clone(), url.clone()))
            });
        let intercepted = admit_attempt(
            self.provider.interceptor.as_ref(),
            &ProviderRequestInfo {
                provider_id: "openai",
                model: "gpt-5.6-luna",
                url: &url,
                attempt: 1,
            },
        )
        .map_err(|denied| {
            if let Some(attempt) = &attempt {
                attempt.settle(false, None, Some(&denied));
            }
            classifier_build_error(&denied)
        })?;
        for (name, value) in intercepted {
            headers.insert(name.to_ascii_lowercase(), value);
        }
        if let Some(auth) = &self.provider.auth_source {
            for (name, value) in auth.get_headers(signal).await? {
                headers.insert(name.to_ascii_lowercase(), value);
            }
        } else if let Some(key) = &self.provider.api_key {
            headers.insert("authorization".into(), format!("Bearer {key}"));
        }
        if auth_changes
            .as_ref()
            .is_some_and(|changes| changes.has_changed().unwrap_or(true))
        {
            let owner_changed = auth_changes
                .as_ref()
                .map(|changes| changes.borrow().owner_generation)
                != auth_state.map(|state| state.owner_generation);
            return Err(classifier_auth_error(owner_changed));
        }
        let restricted = headers
            .keys()
            .any(|name| name.eq_ignore_ascii_case("x-openai-account-routing-override"));
        let client = self.client(restricted).await?;
        // Upgrade requests always reject redirects, retaining the same client factory.
        let websocket_client = self.client(true).await?;
        Ok(ClassifierEndpoint {
            url,
            headers,
            client,
            websocket_client,
            auth_changes,
            auth_state,
            classifier_route: self.provider.is_codex_subscription_backend(),
            connect_timeout: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(300),
            attempt,
        })
    }

    fn prepare(
        &self,
        request: &ClassifierRequest,
        thread_id: &str,
        endpoint: &ClassifierEndpoint,
    ) -> Result<Value, ProviderError> {
        if !request.request.tools.is_empty()
            || request.request.web_search.is_some()
            || request.request.output_schema.is_some()
        {
            return Err(invalid_openai_tool_choice(
                "Classifier",
                "classifications cannot use tools or an output schema",
            ));
        }
        let capabilities = self.provider.capabilities_for(&request.request.model);
        let mut body = build_responses_params_with_tool_mode(
            &request.request,
            &capabilities,
            self.provider.reasoning_dialect,
            false,
        )?;
        let effort = self
            .raw_effort
            .as_ref()
            .map(|effort| vs(effort.clone()))
            .or_else(|| {
                body.get("reasoning")
                    .and_then(|reasoning| reasoning.get("effort"))
                    .cloned()
            });
        let instructions = body
            .get("instructions")
            .and_then(Value::as_str)
            .cloned()
            .unwrap_or_default();
        if let Some(object) = body.as_object_mut() {
            for key in [
                "max_output_tokens",
                "max_completion_tokens",
                "text",
                "tools",
                "instructions",
                "service_tier",
            ] {
                object.remove(key);
            }
        }
        let input = if let Some(input) = &request.native_input {
            input.clone()
        } else {
            let mut input = vec![vobj([
                ("type", vs("additional_tools")),
                ("role", vs("developer")),
                ("tools", Value::Array(Vec::new())),
            ])];
            if !instructions.is_empty() {
                input.push(vobj([
                    ("type", vs("message")),
                    ("role", vs("developer")),
                    (
                        "content",
                        Value::Array(vec![vobj([
                            ("type", vs("input_text")),
                            ("text", vs(instructions)),
                        ])]),
                    ),
                ]));
            }
            if let Some(items) = body.get("input").and_then(Value::as_array) {
                input.extend(items.iter().cloned());
            }
            input
        };
        body["input"] = Value::Array(input);
        body["tool_choice"] = vs("none");
        body["parallel_tool_calls"] = Value::Bool(false);
        let mut reasoning = vobj([("context", vs("all_turns"))]);
        if let Some(effort) = effort {
            reasoning["effort"] = effort;
        }
        body["reasoning"] = reasoning;
        body["store"] = Value::Bool(false);
        body["stream"] = Value::Bool(true);
        body["include"] = Value::Array(if request.retain_history {
            vec![vs("reasoning.encrypted_content")]
        } else {
            Vec::new()
        });
        if !endpoint.classifier_route {
            if let Some(tier) = &request.attribution.service_tier {
                body["service_tier"] = vs(tier.as_str());
            }
        }
        let attribution = &request.attribution;
        let mut turn_metadata = vobj([
            ("session_id", vs(attribution.session_id.as_str())),
            ("thread_id", vs(thread_id)),
            (
                "guardian_classifier_source_thread_id",
                vs(attribution.parent_thread_id.as_str()),
            ),
            ("turn_id", vs(attribution.turn_id.as_str())),
            ("parent_turn_id", vs(attribution.parent_turn_id.as_str())),
            ("thread_source", vs("guardian_classifier")),
            ("turn_trigger", vs("guardian_classifier")),
        ]);
        let mut metadata = Map::new();
        for (name, value) in [
            ("session_id", attribution.session_id.clone()),
            ("thread_id", thread_id.to_owned()),
            ("turn_id", attribution.turn_id.clone()),
            ("parent_turn_id", attribution.parent_turn_id.clone()),
            ("x-openai-subagent", "guardian".into()),
            ("x-codex-window-id", format!("{thread_id}:0")),
            (
                "ws_request_header_x_openai_internal_codex_responses_lite",
                "true".into(),
            ),
        ] {
            metadata.insert(name, vs(value));
        }
        if let Some(root) = &attribution.root_turn_id {
            metadata.insert("root_turn_id", vs(root.as_str()));
            turn_metadata["root_turn_id"] = vs(root.as_str());
        }
        metadata.insert(
            "x-codex-turn-metadata",
            vs(runtime_json::stringify(&turn_metadata)),
        );
        if endpoint.classifier_route {
            if let Some(parent_response_id) = &attribution.parent_response_id {
                metadata.insert("parent_response_id", vs(parent_response_id.as_str()));
            }
        }
        body["client_metadata"] = Value::Object(metadata);
        Ok(body)
    }

    fn map_stream(
        &self,
        request: ClassifierRequest,
        mut stream: RawClassifierStream,
    ) -> ClassifierStream {
        Box::pin(async_stream::stream! {
            let mut state = ResponsesState::default();
            let mut failed = false;
            while let Some(event) = stream.next().await {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        yield ClassifierTransportEvent::Model(ModelStreamEvent::Error { error });
                        failed = true;
                        break;
                    }
                };
                if update_responses_usage(&event, &mut state) {
                    yield ClassifierTransportEvent::Model(ModelStreamEvent::UsageSnapshot { usage: state.usage() });
                }
                if event.get("type").and_then(Value::as_str).is_some_and(|kind| kind == "error") {
                    if let Some(status) = event.get("status").and_then(Value::as_u64).and_then(|status| u16::try_from(status).ok()).filter(|status| *status >= 400) {
                        let headers: HashMap<String, String> = event.get("headers").and_then(Value::as_object).map(|headers| headers.entries().filter_map(|(name, value)| {
                            Some((name.try_to_utf8().ok()?.to_ascii_lowercase(), value.as_str()?.try_to_utf8().ok()?))
                        }).collect()).unwrap_or_default();
                        let text = runtime_json::stringify(&event).try_to_utf8().expect("serialized JSON is scalar UTF-8");
                        let raw = raw_http_error(status, &headers, &text);
                        let mut error = classify_error(&raw);
                        if raw.code.as_deref().is_some_and(|code| matches!(code, "previous_response_not_found" | "websocket_connection_limit_reached")) {
                            error.kind = ProviderErrorKind::Network;
                        }
                        yield ClassifierTransportEvent::Model(ModelStreamEvent::Error { error });
                        failed = true;
                        break;
                    }
                }
                if event.get("type").and_then(Value::as_str).is_some_and(|kind| kind == "response.output_item.done") {
                    if let Some(item) = event.get("item") {
                        yield ClassifierTransportEvent::OutputItem(item.clone());
                    }
                }
                let (events, terminate) = map_responses_event(&event, &mut state);
                for event in events {
                    yield ClassifierTransportEvent::Model(event);
                }
                if terminate {
                    failed = true;
                    break;
                }
            }
            if !failed && state.terminal_received {
                for event in finish_responses_state(&state, &request.request.model) {
                    yield ClassifierTransportEvent::Model(event);
                }
            } else if !failed {
                yield ClassifierTransportEvent::Model(ModelStreamEvent::Error {
                    error: incomplete_responses_error(Some(&RuntimeString::from("missing_terminal_event"))),
                });
            }
        })
    }
}

fn classifier_build_error(message: &str) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: message.into(),
        status: None,
        code: Some("classifier_transport_build".into()),
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        retry_after_ms: None,
        request_id: None,
    }
}

fn classifier_auth_error(owner_changed: bool) -> ProviderError {
    let mut error =
        classifier_build_error("authentication changed while resolving classifier routing");
    error.kind = if owner_changed {
        ProviderErrorKind::Auth
    } else {
        ProviderErrorKind::Network
    };
    error.code = Some(
        if owner_changed {
            "classifier_auth_changed"
        } else {
            "classifier_auth_revision_changed"
        }
        .into(),
    );
    error
}

fn default_client(
    restricted: bool,
    tls_config: Arc<rustls::ClientConfig>,
) -> Result<reqwest::Client, ProviderError> {
    let mut builder = reqwest::Client::builder()
        .http1_only()
        .use_preconfigured_tls(tls_config.as_ref().clone());
    if restricted {
        builder = builder.redirect(reqwest::redirect::Policy::none());
    }
    builder
        .build()
        .map_err(|_| classifier_build_error("could not build the classifier HTTP client"))
}

fn default_tls_config() -> Result<Arc<rustls::ClientConfig>, ProviderError> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    if !native.errors.is_empty() {
        tracing::warn!(
            native_root_error_count = native.errors.len(),
            "encountered errors while loading system root certificates"
        );
    }
    roots.add_parsable_certificates(native.certs);
    let ca = std::env::var("KLAUS_CA_CERTIFICATE")
        .ok()
        .filter(|path| !path.is_empty())
        .or_else(|| {
            std::env::var("SSL_CERT_FILE")
                .ok()
                .filter(|path| !path.is_empty())
        });
    if let Some(path) = ca {
        let bytes = std::fs::read(&path).map_err(|_| {
            classifier_build_error("could not read the configured CA certificate bundle")
        })?;
        for certificate in parse_ca_certificates(&bytes)? {
            roots
                .add(rustls::pki_types::CertificateDer::from(certificate))
                .map_err(|_| classifier_build_error("configured CA certificate is invalid"))?;
        }
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| classifier_build_error("could not configure classifier TLS protocols"))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(Arc::new(config))
}

fn parse_ca_certificates(bytes: &[u8]) -> Result<Vec<Vec<u8>>, ProviderError> {
    use base64::Engine;
    let text = String::from_utf8_lossy(bytes);
    let mut remaining = text.as_ref();
    let mut certificates = Vec::new();
    while let Some(start) = remaining.find("-----BEGIN ") {
        remaining = &remaining[start + "-----BEGIN ".len()..];
        let Some(label_end) = remaining.find("-----") else {
            return Err(classifier_build_error(
                "configured CA bundle has an incomplete PEM label",
            ));
        };
        let label = &remaining[..label_end];
        remaining = &remaining[label_end + 5..];
        let end_label = format!("-----END {label}-----");
        let Some(end) = remaining.find(&end_label) else {
            return Err(classifier_build_error(
                "configured CA bundle has an incomplete PEM section",
            ));
        };
        if matches!(label, "CERTIFICATE" | "TRUSTED CERTIFICATE" | "X509 CRL") {
            let encoded: Vec<_> = remaining[..end]
                .bytes()
                .filter(|byte| !byte.is_ascii_whitespace())
                .collect();
            let mut der = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| classifier_build_error("configured CA bundle has invalid PEM data"))?;
            if label == "TRUSTED CERTIFICATE" {
                let length = first_der_length(&der).ok_or_else(|| {
                    classifier_build_error(
                        "configured trusted certificate has an invalid DER length",
                    )
                })?;
                der.truncate(length);
            }
            if label != "X509 CRL" {
                certificates.push(der);
            }
        }
        remaining = &remaining[end + end_label.len()..];
    }
    if certificates.is_empty() {
        return Err(classifier_build_error(
            "configured CA bundle contains no certificates",
        ));
    }
    Ok(certificates)
}

fn first_der_length(der: &[u8]) -> Option<usize> {
    let length = *der.get(1)?;
    if length & 0x80 == 0 {
        return (2usize + usize::from(length) <= der.len()).then_some(2usize + usize::from(length));
    }
    let count = usize::from(length & 0x7f);
    if count == 0 {
        return None;
    }
    let end = 2usize.checked_add(count)?;
    let mut content = 0usize;
    for byte in der.get(2..end)? {
        content = content.checked_mul(256)?.checked_add(usize::from(*byte))?;
    }
    end.checked_add(content)
        .filter(|length| *length <= der.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::approval_classifier_transport::test_request;
    use crate::provider::auth::ManagedReviewCredentials;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    struct RecoveryFixtureAuth {
        managed: bool,
        outcome: ManagedReviewCredentialRecovery,
        steps: Mutex<Vec<usize>>,
        external_refreshes: AtomicUsize,
        account_available: AtomicBool,
        replace_during_recovery: AtomicBool,
        changes: watch::Sender<CredentialChangeState>,
    }

    impl RecoveryFixtureAuth {
        fn new(managed: bool, outcome: ManagedReviewCredentialRecovery) -> Arc<Self> {
            let (changes, _) = watch::channel(CredentialChangeState {
                revision: 0,
                owner_generation: 7,
            });
            Arc::new(Self {
                managed,
                outcome,
                steps: Mutex::new(Vec::new()),
                external_refreshes: AtomicUsize::new(0),
                account_available: AtomicBool::new(true),
                replace_during_recovery: AtomicBool::new(false),
                changes,
            })
        }
    }

    #[async_trait::async_trait]
    impl AuthSource for RecoveryFixtureAuth {
        async fn get_headers(
            &self,
            _signal: &AbortSignal,
        ) -> Result<crate::types::provider::AuthHeaders, ProviderError> {
            Ok(HashMap::new())
        }

        fn credential_changes(&self) -> Option<watch::Receiver<CredentialChangeState>> {
            Some(self.changes.subscribe())
        }

        async fn managed_review_credentials(
            &self,
            _signal: &AbortSignal,
        ) -> Result<Option<ManagedReviewCredentials>, ProviderError> {
            Ok(self.managed.then(|| ManagedReviewCredentials {
                identity: ManagedReviewCredentialIdentity {
                    chatgpt_user_id: Some("owner-user".into()),
                    account_id: self
                        .account_available
                        .load(Ordering::SeqCst)
                        .then(|| "owner-account".into()),
                    plan_type: None,
                    owner_generation: self.changes.borrow().owner_generation,
                },
                headers: HashMap::new(),
            }))
        }

        async fn recover_managed_review_unauthorized(
            &self,
            step: usize,
            expected: &ManagedReviewCredentialIdentity,
        ) -> ManagedReviewCredentialRecovery {
            assert_eq!(expected.owner_generation, 7);
            let mut steps = self.steps.lock().unwrap();
            assert!(steps.len() < 4, "fixture recovery log is bounded");
            steps.push(step);
            drop(steps);
            if self.replace_during_recovery.load(Ordering::SeqCst) {
                self.changes
                    .send_modify(|state| state.owner_generation += 1);
            }
            self.outcome.clone()
        }

        async fn force_refresh(&self) -> Option<bool> {
            self.external_refreshes.fetch_add(1, Ordering::SeqCst);
            Some(true)
        }
    }

    fn connector(raw_effort: Option<RuntimeString>) -> OpenAIClassifierConnector {
        OpenAIClassifierConnector {
            provider: OpenAIProvider::new(OpenAIProviderOptions::default()),
            raw_effort,
            tls_config: OnceCell::new(),
            regular_client: OnceCell::new(),
            restricted_client: OnceCell::new(),
        }
    }

    fn endpoint(classifier_route: bool) -> ClassifierEndpoint {
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        ClassifierEndpoint {
            url: "https://example.invalid/responses".into(),
            headers: HashMap::new(),
            client: client.clone(),
            websocket_client: client,
            auth_changes: None,
            auth_state: None,
            classifier_route,
            connect_timeout: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(300),
            attempt: None,
        }
    }

    #[tokio::test]
    async fn managed_classifier_recovery_retries_after_reload_without_change_then_refreshes_once() {
        let auth = RecoveryFixtureAuth::new(true, ManagedReviewCredentialRecovery::Refreshed);
        let mut connector = connector(None);
        connector.provider.auth_source = Some(auth.clone());
        let mut first = connector.auth_recovery(&AbortSignal::new()).await.unwrap();
        let mut second = connector.auth_recovery(&AbortSignal::new()).await.unwrap();
        assert!(first.next().await);
        assert!(first.next().await);
        assert!(!first.next().await);
        assert!(!first.next().await);
        assert!(second.next().await);
        assert_eq!(*auth.steps.lock().unwrap(), vec![0, 1, 0]);
        assert_eq!(auth.external_refreshes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn classifier_recovery_stops_before_or_after_owner_loss_and_after_any_recovery_error() {
        let unidentified =
            RecoveryFixtureAuth::new(true, ManagedReviewCredentialRecovery::Refreshed);
        unidentified
            .account_available
            .store(false, Ordering::SeqCst);
        let mut unidentified_connector = connector(None);
        unidentified_connector.provider.auth_source = Some(unidentified.clone());
        assert!(unidentified_connector
            .auth_recovery(&AbortSignal::new())
            .await
            .is_none());
        assert!(unidentified.steps.lock().unwrap().is_empty());
        assert_eq!(unidentified.external_refreshes.load(Ordering::SeqCst), 0);
        for during in [false, true] {
            let auth = RecoveryFixtureAuth::new(true, ManagedReviewCredentialRecovery::Refreshed);
            let mut connector = connector(None);
            connector.provider.auth_source = Some(auth.clone());
            let mut recovery = connector.auth_recovery(&AbortSignal::new()).await.unwrap();
            if during {
                auth.replace_during_recovery.store(true, Ordering::SeqCst);
            } else {
                auth.changes
                    .send_modify(|state| state.owner_generation += 1);
            }
            assert!(!recovery.next().await);
            assert!(!recovery.next().await);
            assert_eq!(auth.steps.lock().unwrap().len(), usize::from(during));
        }
        for failure in [
            ManagedReviewCredentialRecovery::TransientFailure("temporary failure".into()),
            ManagedReviewCredentialRecovery::PermanentFailure("permanent failure".into()),
            ManagedReviewCredentialRecovery::Unavailable,
        ] {
            let auth = RecoveryFixtureAuth::new(true, failure);
            let mut connector = connector(None);
            connector.provider.auth_source = Some(auth.clone());
            let mut recovery = connector.auth_recovery(&AbortSignal::new()).await.unwrap();
            assert!(!recovery.next().await);
            assert!(!recovery.next().await);
            assert_eq!(*auth.steps.lock().unwrap(), vec![0]);
        }
    }

    #[tokio::test]
    async fn external_classifier_recovery_refreshes_once_per_classification() {
        let auth = RecoveryFixtureAuth::new(false, ManagedReviewCredentialRecovery::Unavailable);
        let mut connector = connector(None);
        connector.provider.auth_source = Some(auth.clone());
        let mut first = connector.auth_recovery(&AbortSignal::new()).await.unwrap();
        let mut second = connector.auth_recovery(&AbortSignal::new()).await.unwrap();
        assert!(first.next().await);
        assert!(!first.next().await);
        assert!(second.next().await);
        assert!(!second.next().await);
        assert_eq!(auth.external_refreshes.load(Ordering::SeqCst), 2);
        assert!(auth.steps.lock().unwrap().is_empty());
    }

    #[test]
    fn native_request_keeps_item_ids_raw_effort_and_attribution_without_parent_output_controls() {
        let connector = connector(Some("minimal".into()));
        let mut request = test_request();
        request.request.model_verbosity = Some(crate::types::provider::OutputVerbosity::High);
        request.attribution.service_tier = Some("priority".into());
        let items = runtime_json::parse(&RuntimeString::from(r#"[
            {"type":"message","id":"instruction-id","role":"developer","content":[{"type":"input_text","text":"trusted policy"}]},
            {"type":"reasoning","id":"reasoning-id","summary":[],"encrypted_content":"opaque-original"},
            {"type":"message","id":"assistant-id","role":"assistant","content":[{"type":"output_text","text":"low"}]}
        ]"#)).unwrap();
        request.native_input = Some(items.as_array().unwrap().clone());
        let ordinary = connector
            .prepare(&request, "classifier-thread", &endpoint(false))
            .unwrap();
        assert_eq!(ordinary["input"], items);
        assert_eq!(ordinary["reasoning"]["effort"], vs("minimal"));
        assert_eq!(ordinary["reasoning"]["context"], vs("all_turns"));
        assert_eq!(ordinary["service_tier"], vs("priority"));
        assert!(ordinary.get("max_output_tokens").is_none());
        assert!(ordinary.get("text").is_none());
        assert!(ordinary.get("tools").is_none());
        assert!(ordinary.get("instructions").is_none());
        assert_eq!(
            ordinary["client_metadata"]["thread_id"],
            vs("classifier-thread")
        );
        assert_eq!(ordinary["client_metadata"]["root_turn_id"], vs("root-turn"));
        assert_eq!(
            ordinary["include"],
            Value::Array(vec![vs("reasoning.encrypted_content")])
        );
        let scoped = connector
            .prepare(&request, "classifier-thread", &endpoint(true))
            .unwrap();
        assert!(scoped.get("service_tier").is_none());
        assert_eq!(
            scoped["client_metadata"]["parent_response_id"],
            vs("parent-response")
        );
    }

    #[test]
    fn trusted_certificate_auxiliary_data_and_crl_sections_preserve_all_ca_certificates() {
        use base64::Engine;
        let original = parse_ca_certificates(include_bytes!(
            "../approval_classifier_transport/responses/fixtures/root.pem"
        ))
        .unwrap();
        let mut trusted = original[0].clone();
        trusted.extend_from_slice(&[0x30, 0x00]);
        let pem = format!("-----BEGIN TRUSTED CERTIFICATE-----\n{}\n-----END TRUSTED CERTIFICATE-----\n-----BEGIN X509 CRL-----\nMAA=\n-----END X509 CRL-----\n",
            base64::engine::general_purpose::STANDARD.encode(trusted));
        assert_eq!(parse_ca_certificates(pem.as_bytes()).unwrap(), original);
        let malformed = format!("{pem}-----BEGIN X509 CRL-----\n%%%\n-----END X509 CRL-----\n");
        assert!(parse_ca_certificates(malformed.as_bytes()).is_err());
        assert!(parse_ca_certificates(b"no certificate data").is_err());
    }
    #[test]
    fn an_unspecified_classifier_effort_stays_absent_on_the_wire() {
        let mut request = test_request();
        request.request.effort = None;
        let body = connector(None)
            .prepare(&request, "thread", &endpoint(false))
            .unwrap();
        assert!(body["reasoning"].get("effort").is_none());
        assert_eq!(body["reasoning"]["context"], vs("all_turns"));
    }
}
