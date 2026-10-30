//! Regression: server-selected realtime models stayed absent from capabilities.
//! The provider had no configured model and confirmed session metadata was ignored.
//! The observation remains process-local and belongs to one credential generation.
use super::*;

fn source() -> Provider {
    let base_url = url::Url::parse("https://voice.example.test/").unwrap();
    Provider {
        sideband_base_url: base_url.clone(),
        base_url,
        api_key: "synthetic-signed-envelope".into(),
        credential_identity: credential_identity(&["account-one", "generation-one"]),
        model: None,
        protocol: Protocol::Qoder,
        source_id: "subscription:qoder".into(),
        account_id: None,
        voice: Some("longanqian".into()),
        transcription_model: None,
        extra_headers: Vec::new(),
    }
}

fn updated(model: &str) -> Value {
    json!({"type":"session.updated","session":{"model":model,"input_audio_transcription":{"model":"fixture-asr-model"}}})
}

#[test]
fn confirmed_session_model_is_available_without_a_configured_model() {
    let cache = RealtimeSessions::default();
    let source = source();
    assert_eq!(cache.resolved_model(&source), None);
    assert_eq!(
        cache.observe_model(&source, &updated("  fixture-realtime-model  ")),
        Some("fixture-realtime-model".into())
    );
    assert_eq!(
        cache.resolved_model(&source),
        Some("fixture-realtime-model".into())
    );
    assert_eq!(source.model, None);
    assert_eq!(RealtimeSessions::default().resolved_model(&source), None);
}

#[test]
fn only_nonempty_bounded_confirmed_session_models_are_observed() {
    let cache = RealtimeSessions::default();
    let source = source();
    for event in [
        json!({"type":"session.created","session":{"model":"unconfirmed-model"}}),
        json!({"type":"session.updated","session":{"input_audio_transcription":{"model":"fixture-asr-model"}}}),
        json!({"type":"session.updated","session":{"model":42}}),
        updated(""),
        updated(" \t\n "),
        updated(&"a".repeat(MAX_MODEL_NAME + 1)),
    ] {
        assert_eq!(cache.observe_model(&source, &event), None, "{event}");
        assert_eq!(cache.resolved_model(&source), None, "{event}");
    }
    cache.observe_model(&source, &updated("confirmed-model"));
    cache.observe_model(&source, &updated("  "));
    assert_eq!(
        cache.resolved_model(&source),
        Some("confirmed-model".into())
    );
}

#[test]
fn explicit_models_keep_their_configured_semantics() {
    let cache = RealtimeSessions::default();
    let mut source = source();
    source.model = Some("configured-model-alias".into());
    cache.observe_model(&source, &updated("resolved-server-model"));
    assert_eq!(
        cache.resolved_model(&source),
        Some("configured-model-alias".into())
    );
}

#[test]
fn observations_are_scoped_to_source_credential_endpoints_and_protocol() {
    let cache = RealtimeSessions::default();
    let source = source();
    cache.observe_model(&source, &updated("confirmed-model"));
    let mut other_source = source.clone();
    other_source.source_id = "subscription:other".into();
    let mut other_credential = source.clone();
    other_credential.credential_identity = credential_identity(&["account-two", "generation-two"]);
    let mut other_endpoint = source.clone();
    other_endpoint.base_url = url::Url::parse("https://other.example.test/").unwrap();
    let mut other_sideband = source.clone();
    other_sideband.sideband_base_url = url::Url::parse("https://sideband.example.test/").unwrap();
    let mut other_protocol = source.clone();
    other_protocol.protocol = Protocol::Public;
    for other in [
        other_source,
        other_credential,
        other_endpoint,
        other_sideband,
        other_protocol,
    ] {
        assert_eq!(cache.resolved_model(&other), None);
    }
    assert_eq!(
        cache.resolved_model(&source),
        Some("confirmed-model".into())
    );
}

#[test]
fn request_signing_changes_do_not_invalidate_a_credential_observation() {
    let cache = RealtimeSessions::default();
    let mut source = source();
    cache.observe_model(&source, &updated("confirmed-model"));
    source.api_key = "another-synthetic-signed-envelope".into();
    source.extra_headers = vec![("Date".into(), "another-request-date".into())];
    assert_eq!(
        cache.resolved_model(&source),
        Some("confirmed-model".into())
    );
}

#[test]
fn qoder_identity_changes_with_account_or_token_generation() {
    let profile = json!({"uid":123,"security_oauth_token":"security-token","access_token":"access-token","display_name":"Account"});
    let identity = qoder_credential_identity("profile-one", &profile);
    let mut irrelevant_change = profile.clone();
    irrelevant_change["display_name"] = json!("Updated display name");
    assert_eq!(
        identity,
        qoder_credential_identity("profile-one", &irrelevant_change)
    );
    assert_ne!(identity, qoder_credential_identity("profile-two", &profile));
    for (field, value) in [
        ("uid", json!(456)),
        ("security_oauth_token", json!("new-security-token")),
        ("access_token", json!("new-access-token")),
    ] {
        let mut rotated = profile.clone();
        rotated[field] = value;
        assert_ne!(identity, qoder_credential_identity("profile-one", &rotated));
    }
    assert_ne!(
        credential_identity(&["a", "bc"]),
        credential_identity(&["ab", "c"])
    );
}

#[test]
fn observation_storage_is_bounded_and_retains_the_latest_confirmation() {
    let cache = RealtimeSessions::default();
    let first = source();
    for index in 0..MAX_MODEL_OBSERVATIONS {
        let mut source = first.clone();
        source.credential_identity = credential_identity(&[&index.to_string()]);
        cache.observe_model(&source, &updated("first-confirmation"));
        cache.observe_model(&source, &updated("latest-confirmation"));
        assert_eq!(
            cache.resolved_model(&source),
            Some("latest-confirmation".into())
        );
    }
    assert_eq!(
        cache.observed_models.lock().unwrap().len(),
        MAX_MODEL_OBSERVATIONS
    );
    let mut oldest = first.clone();
    oldest.credential_identity = credential_identity(&["0"]);
    cache.observe_model(&first, &updated("new-generation-model"));
    assert_eq!(
        cache.observed_models.lock().unwrap().len(),
        MAX_MODEL_OBSERVATIONS
    );
    assert_eq!(cache.resolved_model(&oldest), None);
    assert_eq!(
        cache.resolved_model(&first),
        Some("new-generation-model".into())
    );
}
