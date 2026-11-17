//! Content-free fingerprints at the final outgoing and raw incoming boundaries.

use klaus_agent_core::observability::provider_diagnostics;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const LOG_FILE: &str = "antigravity-cache.log";
const USAGE_FIELDS: [&str; 6] = [
    "promptTokenCount",
    "cachedContentTokenCount",
    "candidatesTokenCount",
    "thoughtsTokenCount",
    "totalTokenCount",
    "toolUsePromptTokenCount",
];

fn hash_bytes(domain: &[u8], bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update([0]);
    hash.update(bytes);
    format!("{:x}", hash.finalize())
}

fn hash_value(value: Option<&RuntimeJson>) -> String {
    let mut bytes = Vec::new();
    match value {
        Some(value) => {
            // The outgoing body uses this same writer, including signed parts,
            // media bytes, object order, and escaped UTF-16 code units.
            let _ = runtime_json::write_to(value, &mut bytes);
            hash_bytes(b"wire-json", &bytes)
        }
        None => hash_bytes(b"wire-json-missing", &[]),
    }
}

#[derive(Clone)]
pub(super) struct WireRequest {
    identity: Value,
}

fn request_fields(bytes: &[u8], generation: Option<&str>) -> Value {
    let mut fields = json!({
        "source": "antigravity",
        "fingerprintVersion": 1,
        "wireAttemptId": uuid::Uuid::new_v4().to_string(),
        "wireBodyHash": hash_bytes(b"wire-body", bytes),
        "wireBodyBytes": bytes.len(),
        "accountGenerationHash": generation.map(|id| hash_bytes(b"account-generation", id.as_bytes())),
        "sessionId": provider_diagnostics::trace_session_id(),
    });
    let payload = std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| runtime_json::parse(&RuntimeString::from(text)).ok());
    fields["wireParsed"] = json!(payload.is_some());
    if let Some(payload) = &payload {
        fields["model"] = json!(payload
            .get("model")
            .and_then(RuntimeJson::as_string)
            .and_then(|text| text.try_to_utf8().ok()));
        fields["upstreamRequestId"] = json!(payload
            .get("requestId")
            .and_then(RuntimeJson::as_string)
            .and_then(|text| text.try_to_utf8().ok()));
        fields["projectHash"] = json!(hash_value(payload.get("project")));
        if let Some(request) = payload.get("request") {
            fields["wireSessionIdHash"] = json!(hash_value(request.get("sessionId")));
            fields["wireSystemHash"] = json!(hash_value(request.get("systemInstruction")));
            fields["wireToolsHash"] = json!(hash_value(request.get("tools")));
            fields["wireGenerationConfigHash"] = json!(hash_value(request.get("generationConfig")));
            fields["wireContentsHash"] = json!(hash_value(request.get("contents")));
            fields["continuationTokenPresent"] = json!(request.get("continuationToken").is_some());
            if let Some(contents) = request.get("contents").and_then(RuntimeJson::as_array) {
                fields["wireMessageCount"] = json!(contents.len());
                fields["wireMessageHashes"] = json!(contents
                    .iter()
                    .map(|content| hash_value(Some(content)))
                    .collect::<Vec<_>>());
            }
        }
    }
    fields
}

impl WireRequest {
    /// Inspect exactly the bytes passed to the HTTP body after all projections.
    /// A parse failure only limits diagnostics; it never changes the request.
    pub(super) fn record(bytes: &[u8], generation: Option<&str>) -> Arc<Self> {
        let fields = request_fields(bytes, generation);
        let identity = json!({
            "source": "antigravity",
            "wireAttemptId": fields["wireAttemptId"],
            "upstreamRequestId": fields.get("upstreamRequestId"),
            "model": fields.get("model"),
            "sessionId": fields["sessionId"],
            "accountGenerationHash": fields["accountGenerationHash"],
            "projectHash": fields.get("projectHash"),
        });
        provider_diagnostics::record_named(LOG_FILE, "wire_request", fields);
        Arc::new(Self { identity })
    }
}

/// Cover attempts that never reach a readable response body.
pub(super) struct WireSend {
    request: Option<Arc<WireRequest>>,
    outcome: &'static str,
    status: Option<u16>,
}

impl WireSend {
    pub(super) fn new(request: Option<Arc<WireRequest>>) -> Self {
        Self {
            request,
            outcome: "dropped",
            status: None,
        }
    }

    pub(super) fn outcome(&mut self, outcome: &'static str, status: Option<u16>) {
        self.outcome = outcome;
        self.status = status;
    }
}

impl Drop for WireSend {
    fn drop(&mut self) {
        if let Some(request) = &self.request {
            let mut fields = request.identity.clone();
            fields["outcome"] = json!(self.outcome);
            fields["status"] = json!(self.status);
            provider_diagnostics::record_named(LOG_FILE, "wire_send_end", fields);
        }
    }
}

/// Capture raw field presence before numeric normalization loses that distinction.
#[derive(Clone)]
pub(super) struct RawUsage {
    fields: Value,
    usage_present: bool,
    cached_present: bool,
}

impl RawUsage {
    pub(super) fn of(response: &RuntimeJson) -> Self {
        let usage = response
            .get("usageMetadata")
            .or_else(|| response.get("usage_metadata"));
        if usage.is_none() {
            return Self {
                fields: Value::Null,
                usage_present: false,
                cached_present: false,
            };
        }
        let mut fields = json!({
            "usageMetadataPresent": usage.is_some(),
            "usageMetadataIsObject": usage.and_then(RuntimeJson::as_object).is_some(),
            "usageMetadataIsNull": usage.is_some_and(|value| matches!(value, RuntimeJson::Null)),
        });
        let mut cached_present = false;
        let mut values = serde_json::Map::new();
        for name in USAGE_FIELDS {
            let value = usage.and_then(|usage| usage.get(name));
            if name == "cachedContentTokenCount" {
                cached_present = value.is_some();
            }
            values.insert(
                name.to_string(),
                json!({
                    "present": value.is_some(),
                    "isNull": value.is_some_and(|value| matches!(value, RuntimeJson::Null)),
                    "value": value.and_then(RuntimeJson::as_u64),
                }),
            );
        }
        fields["fields"] = Value::Object(values);
        Self {
            fields,
            usage_present: usage.is_some(),
            cached_present,
        }
    }
}

/// Each HTTP response owns its summary, including retries and continuation legs.
/// Dropping a cancelled or interrupted stream also preserves the evidence so far.
pub(super) struct WireResponse {
    request: Option<Arc<WireRequest>>,
    frames: usize,
    usage_frames: usize,
    cached_frames: usize,
    last_usage: Option<Value>,
    outcome: &'static str,
}

impl WireResponse {
    pub(super) fn new(request: Option<Arc<WireRequest>>) -> Self {
        Self {
            request,
            frames: 0,
            usage_frames: 0,
            cached_frames: 0,
            last_usage: None,
            outcome: "dropped",
        }
    }

    pub(super) fn outcome(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }

    pub(super) fn observe(&mut self, usage: &RawUsage) {
        self.frames += 1;
        if usage.usage_present {
            self.usage_frames += 1;
            self.cached_frames += usize::from(usage.cached_present);
            self.last_usage = Some(usage.fields.clone());
            if let Some(request) = &self.request {
                let mut fields = request.identity.clone();
                fields["frameIndex"] = json!(self.frames);
                fields["rawUsage"] = usage.fields.clone();
                provider_diagnostics::record_named(LOG_FILE, "wire_usage", fields);
            }
        }
    }
}

impl Drop for WireResponse {
    fn drop(&mut self) {
        if let Some(request) = &self.request {
            let mut fields = request.identity.clone();
            fields["rawFrames"] = json!(self.frames);
            fields["usageMetadataFrames"] = json!(self.usage_frames);
            fields["cachedTokenCountPresentFrames"] = json!(self.cached_frames);
            fields["lastRawUsage"] = json!(self.last_usage);
            fields["outcome"] = json!(self.outcome);
            provider_diagnostics::record_named(LOG_FILE, "wire_response_end", fields);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(message: &str, signature: &str, media: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "requestId": "agent/fixture/step",
            "model": "gemini-fixture",
            "project": "private-project-fixture",
            "request": {
                "sessionId": "fixture-session",
                "systemInstruction": {"parts": [{"text": "private-system-fixture"}]},
                "tools": [{"functionDeclarations": [{"name": "fixture-tool"}]}],
                "contents": [
                    {"role": "user", "parts": [{"text": message}]},
                    {"role": "model", "parts": [{
                        "thought": true, "text": "private-thought-fixture",
                        "thoughtSignature": signature
                    }, {"inlineData": {"mimeType": "image/png", "data": media}}]}
                ]
            }
        }))
        .unwrap()
    }

    #[test]
    fn wire_hashes_cover_signed_and_media_bytes_without_recording_content() {
        let bytes = body("private-message-fixture", "signed-a", "media-a");
        let original = request_fields(&bytes, Some("private-generation-fixture"));
        let changed = request_fields(
            &body("private-message-fixture", "signed-b", "media-a"),
            Some("private-generation-fixture"),
        );
        let media = request_fields(
            &body("private-message-fixture", "signed-a", "media-b"),
            Some("private-generation-fixture"),
        );
        assert_eq!(
            original["wireMessageHashes"][0],
            changed["wireMessageHashes"][0]
        );
        assert_ne!(
            original["wireMessageHashes"][1],
            changed["wireMessageHashes"][1]
        );
        assert_ne!(
            original["wireMessageHashes"][1],
            media["wireMessageHashes"][1]
        );
        assert_ne!(original["wireContentsHash"], changed["wireContentsHash"]);
        assert_eq!(original["wireSystemHash"], changed["wireSystemHash"]);
        assert_eq!(original["wireBodyHash"], hash_bytes(b"wire-body", &bytes));
        let rotated = request_fields(&bytes, Some("different-generation-fixture"));
        assert_ne!(
            original["accountGenerationHash"],
            rotated["accountGenerationHash"]
        );
        assert_eq!(original["wireMessageHashes"], rotated["wireMessageHashes"]);
        let diagnostic = original.to_string();
        for private in [
            "private-generation-fixture",
            "private-project-fixture",
            "private-message-fixture",
            "private-system-fixture",
            "private-thought-fixture",
            "signed-a",
            "media-a",
        ] {
            assert!(!diagnostic.contains(private), "{private}");
        }
    }

    #[test]
    fn raw_usage_distinguishes_omitted_zero_null_and_positive_cache_counts() {
        for (usage, present, is_null, value) in [
            (json!({"promptTokenCount": 20}), false, false, Value::Null),
            (
                json!({"promptTokenCount": 20, "cachedContentTokenCount": 0}),
                true,
                false,
                json!(0),
            ),
            (
                json!({"promptTokenCount": 20, "cachedContentTokenCount": null}),
                true,
                true,
                Value::Null,
            ),
            (
                json!({"promptTokenCount": 20, "cachedContentTokenCount": 12}),
                true,
                false,
                json!(12),
            ),
        ] {
            let payload = runtime_json::parse(&RuntimeString::from(
                json!({"usageMetadata": usage}).to_string(),
            ))
            .unwrap();
            let raw = RawUsage::of(&payload);
            assert!(raw.usage_present);
            assert_eq!(raw.cached_present, present);
            let cached = &raw.fields["fields"]["cachedContentTokenCount"];
            assert_eq!(cached["present"], present);
            assert_eq!(cached["isNull"], is_null);
            assert_eq!(cached["value"], value);
        }
        let empty = runtime_json::parse(&RuntimeString::from("{}")).unwrap();
        assert!(!RawUsage::of(&empty).usage_present);
    }

    #[test]
    fn hashes_preserve_escaped_utf16_and_missing_is_not_null() {
        let bytes = br#"{"model":"fixture","request":{"contents":[{"role":"user","parts":[{"text":"\ud800"}]}]}}"#;
        let fields = request_fields(bytes, None);
        assert_eq!(fields["wireParsed"], true);
        assert_eq!(fields["wireMessageCount"], 1);
        let null = runtime_json::parse(&RuntimeString::from("null")).unwrap();
        assert_ne!(hash_value(None), hash_value(Some(&null)));
    }
}
