//! Per-request prompt-prefix fingerprints.
//!
//! Prompt caching is a longest-prefix match over the bytes a request sends, so a
//! dropped cache hit can only be diagnosed by knowing WHICH region of the prefix
//! changed. The usage ledger reports how much was read from cache but never what
//! was sent, which leaves a low hit rate unattributable: a tool appearing
//! mid-array, a volatile reminder landing inside the system block, a rewritten
//! history entry and a model switch stripping prior thinking blocks all look
//! identical in the ledger.
//!
//! This module hashes the three cached regions of a request separately — tools,
//! system, and the conversation history up to (but not including) its last
//! message — so one log line answers "which region moved". The digests ride the
//! always-on, content-free request lifecycle record next to that request's own
//! cache-read count; they are hex strings, so no prompt text, tool schema or
//! conversation content reaches the log.
//!
//! Two properties make the digests readable:
//!
//! - Both history digests and the message count are reported together, because
//!   neither means anything alone. A growing count with a changing digest is an
//!   ordinary append. The same count with a changing digest is a REWRITE of
//!   content a provider already cached, which is the failure worth finding: an
//!   idle micro-compact clearing an old tool result, a media repair resizing a
//!   block, a model switch stripping thinking blocks signed by its predecessor.
//!   A shrinking count is a compaction. `prefixHistoryHash` — every message but
//!   the last — adds one positive confirmation at a turn boundary: when a
//!   request appends exactly one message, that digest equals the PREVIOUS
//!   request's `prefixHistoryHash` extended by one, so an append-only history is
//!   provable rather than inferred. Inside a tool loop a round appends an
//!   assistant message plus its results, so the comparison needs the count.
//! - Digests are SHAPE-AWARE. Text is hashed in full, because rewriting text in
//!   place is the one history edit that keeps a message's identity and size
//!   while invalidating everything after it. Media payloads are hashed by MIME
//!   type and length instead of content: an image block carries megabytes of
//!   base64, and re-encoding that per request would cost more than the request
//!   it measures. Length still catches the media edits that matter — a block
//!   shed by the media budget, or bytes replaced by a repair.
//!
//! Cost is therefore proportional to the prompt's TEXT, which the request
//! already serializes for the wire, so the pass is cheap enough to leave on.
//!
//! One limit worth stating: this fingerprints the request the engine hands to a
//! provider, not the bytes an adapter finally sends. A rewrite that happens
//! inside an adapter — stripping thinking blocks signed by a different model, or
//! joining the system parts into a single string — is therefore not visible
//! here. Those rewrites are themselves functions of the request's model and
//! parts, both of which this module does record.

use crate::runtime_values::{runtime_codec, runtime_json, RuntimeEncode, RuntimeString};
use crate::schemas::content::{ContentBlock, OutputBlock};
use crate::schemas::message::Message;
use crate::types::provider::{ModelRequest, SystemPromptPart, ToolSpec};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Marker fed to the hasher when a value cannot be canonically encoded. A
/// diagnostic must never fail a request, so an encoding error degrades to a
/// fixed marker: the digest still moves when the shape moves, and a reader can
/// tell a fingerprint that never resolved from one that did.
const UNENCODABLE: &[u8] = b"<unencodable>";

/// Region and field separator. Without it, moving a byte from the end of one
/// entry to the start of the next would hash identically.
const SEPARATOR: [u8; 1] = [0];

/// Hex characters kept per digest. 64 bits distinguishes region changes across
/// any realistic number of requests while keeping the log line readable.
const DIGEST_HEX_CHARS: usize = 16;

/// The three cached regions of one request, hashed independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixFingerprint {
    /// Tools in wire order. The count matters on its own: a deferred tool
    /// surfacing mid-session inserts an entry and shifts every tool behind it.
    pub tool_count: usize,
    pub tools_hash: String,
    pub system_parts: usize,
    /// Digest over the parts a provider cache breakpoint may cover (those marked
    /// cacheable), including their names and cache markers: a part that flips
    /// its marker changes which region the breakpoint lands on.
    pub stable_system_hash: String,
    /// Digest over the remaining parts, which sit outside any breakpoint on a
    /// wire that supports them. Churn here is expected per request; churn on a
    /// wire that joins the system parts into ONE string is what invalidates the
    /// whole system prefix, and this digest is what separates the two cases.
    pub volatile_system_hash: String,
    /// Digest over every message, i.e. the full history this request sends.
    pub history_messages: usize,
    pub history_hash: String,
    /// Digest over every message except the last.
    ///
    /// Compared against the PREVIOUS request's [`Self::history_hash`], this is
    /// what separates an append from a rewrite: when a request adds exactly one
    /// message, its prefix digest equals the earlier request's full digest, so
    /// the cached region is intact and the low hit rate lies elsewhere. When the
    /// count is unchanged but a digest moved, cached content was rewritten in
    /// place — an idle micro-compact clearing an old tool result, a media repair
    /// resizing a block, a model switch stripping thinking blocks signed by its
    /// predecessor — and every byte after that point was re-sent uncached.
    pub history_prefix_hash: String,
    /// Digest of the request's cache-routing key, when it has one. Reported
    /// separately from the session id because a sub-agent keys on its own agent
    /// id, and a key that changes per request defeats routing on the wires that
    /// use it.
    pub cache_key_hash: Option<String>,
}

impl PrefixFingerprint {
    /// Fingerprint a request as the engine hands it to a provider.
    pub fn of(request: &ModelRequest) -> Self {
        let (tool_count, tools_hash) = digest_tools(&request.tools);
        let (system_parts, stable_system_hash, volatile_system_hash) =
            digest_system(&request.system);
        let (history_messages, history_hash, history_prefix_hash) =
            digest_history(&request.messages);
        Self {
            tool_count,
            tools_hash,
            system_parts,
            stable_system_hash,
            volatile_system_hash,
            history_messages,
            history_hash,
            history_prefix_hash,
            cache_key_hash: request
                .cache_key
                .as_ref()
                .map(|key| short_hex(&Sha256::digest(key.to_node_utf8()))),
        }
    }

    /// The log fields, named to sit beside the request lifecycle record's own
    /// token counts.
    pub fn fields(&self) -> Value {
        json!({
            "prefixToolCount": self.tool_count,
            "prefixToolsHash": self.tools_hash,
            "prefixSystemParts": self.system_parts,
            "prefixStableSystemHash": self.stable_system_hash,
            "prefixVolatileSystemHash": self.volatile_system_hash,
            "prefixHistoryMessages": self.history_messages,
            "prefixHistoryHash": self.history_hash,
            "prefixHistoryPrefixHash": self.history_prefix_hash,
            "prefixCacheKeyHash": self.cache_key_hash,
        })
    }
}

fn digest_tools(tools: &[ToolSpec]) -> (usize, String) {
    let mut hasher = Sha256::new();
    for tool in tools {
        // A tool's schema is bounded and text-only, so it is hashed verbatim.
        feed(&mut hasher, tool);
    }
    (tools.len(), finish(&hasher))
}

fn digest_system(parts: &[SystemPromptPart]) -> (usize, String, String) {
    let mut stable = Sha256::new();
    let mut volatile = Sha256::new();
    for part in parts {
        // The cacheable marker is the region boundary, so it belongs to the
        // stable digest: a part that stops being cacheable leaves the region a
        // breakpoint may cover, and the digest must say so.
        if part.cacheable == Some(true) {
            feed(&mut stable, part);
        } else {
            feed(&mut volatile, part);
        }
    }
    (parts.len(), finish(&stable), finish(&volatile))
}

fn digest_history(messages: &[Message]) -> (usize, String, String) {
    let mut hasher = Sha256::new();
    let split = messages.len().saturating_sub(1);
    for message in &messages[..split] {
        feed_message(&mut hasher, message);
    }
    // SHA-256 is a streaming hash, so the state after the leading messages IS
    // the prefix digest: cloning it here costs one small struct copy instead of
    // a second pass over the whole history.
    let history_prefix_hash = finish(&hasher);
    for message in &messages[split..] {
        feed_message(&mut hasher, message);
    }
    (messages.len(), finish(&hasher), history_prefix_hash)
}

/// Hash one message's identity, kind and block shapes.
fn feed_message(hasher: &mut Sha256, message: &Message) {
    match message {
        Message::User(user) => {
            update(hasher, b"user");
            feed_text(hasher, &user.uuid);
            // An injected meta message occupies the same slot as a real turn but
            // is not one, so its marker is part of the shape.
            update(hasher, user.is_meta.unwrap_or(false).to_string().as_bytes());
            for block in &user.content {
                feed_block(hasher, block);
            }
        }
        Message::Assistant(assistant) => {
            update(hasher, b"assistant");
            feed_text(hasher, &assistant.uuid);
            // The producing model is part of the shape: a switch re-sends the
            // same history under a new model, which is when signed thinking from
            // the previous model gets stripped.
            feed_text(hasher, &assistant.model);
            for block in &assistant.content {
                feed_block(hasher, block);
            }
        }
        Message::ToolResult(result) => {
            update(hasher, b"tool_result");
            feed_text(hasher, &result.uuid);
            feed_text(hasher, &result.tool_use_id);
            update(hasher, result.is_error.to_string().as_bytes());
            for block in &result.content {
                feed_output_block(hasher, block);
            }
            // A PDF riding a tool result is a sibling block, so it belongs to the
            // same message's shape.
            if let Some(extra) = &result.extra_user_blocks {
                for block in extra {
                    feed_block(hasher, block);
                }
            }
        }
        Message::System(system) => {
            update(hasher, b"system");
            feed_text(hasher, &system.uuid);
            feed_text(hasher, &system.text);
        }
        Message::CompactBoundary(boundary) => {
            // A compaction replaces the whole history with one of these plus a
            // retained tail, so the boundary's own digest is the clearest marker
            // that the prefix was rewritten rather than appended to.
            update(hasher, b"compact_boundary");
            feed_text(hasher, &boundary.uuid);
            feed_text(hasher, &boundary.summary);
        }
    }
    hasher.update(SEPARATOR);
}

fn feed_block(hasher: &mut Sha256, block: &ContentBlock) {
    match block {
        ContentBlock::Text(text) => {
            update(hasher, b"text");
            feed_text(hasher, &text.text);
            // Display identity travels with the block and changes how a provider
            // groups it, so it is part of the shape.
            if let Some(id) = &text.message_id {
                feed_text(hasher, id);
            }
            if let Some(phase) = &text.phase {
                feed_text(hasher, phase);
            }
        }
        ContentBlock::Thinking(thinking) => {
            update(hasher, b"thinking");
            feed_text(hasher, &thinking.text);
            // Both payloads are opaque and potentially large; their LENGTH is
            // what changes when a provider re-issues or drops them. A stripped
            // signature also shows up as a missing field, so presence is fed
            // too.
            feed_presence_and_len(hasher, b"sig", thinking.signature.as_ref());
            feed_presence_and_len(hasher, b"enc", thinking.encrypted_content.as_ref());
        }
        ContentBlock::RedactedThinking(redacted) => {
            update(hasher, b"redacted_thinking");
            feed_presence_and_len(hasher, b"data", Some(&redacted.data));
        }
        ContentBlock::Image(image) => {
            update(hasher, b"image");
            feed_text(hasher, &image.mime_type);
            feed_presence_and_len(hasher, b"b64", Some(&image.base64));
        }
        ContentBlock::Document(document) => {
            update(hasher, b"document");
            feed_text(hasher, &document.mime_type);
            feed_presence_and_len(hasher, b"b64", Some(&document.base64));
            if let Some(filename) = &document.filename {
                feed_text(hasher, filename);
            }
        }
        ContentBlock::Media(media) => {
            update(hasher, b"media");
            feed_text(hasher, &media.mime_type);
            feed_presence_and_len(hasher, b"b64", Some(&media.base64));
            if let Some(filename) = &media.filename {
                feed_text(hasher, filename);
            }
        }
        ContentBlock::ToolUse(call) => {
            update(hasher, b"tool_use");
            feed_text(hasher, &call.id);
            feed_text(hasher, &call.name);
            // A call's arguments are bounded text, so they are hashed verbatim:
            // a re-serialized argument object is a real prefix change.
            feed(hasher, &call.input);
            feed_presence_and_len(hasher, b"thoughtsig", call.thought_signature.as_ref());
        }
        ContentBlock::ToolResult(result) => {
            update(hasher, b"tool_result_block");
            feed_text(hasher, &result.tool_use_id);
            update(
                hasher,
                result.is_error.unwrap_or(false).to_string().as_bytes(),
            );
            for block in &result.content {
                feed_output_block(hasher, block);
            }
        }
        ContentBlock::WebSearchCall(search) => {
            update(hasher, b"web_search_call");
            feed_text(hasher, &search.id);
            feed_text(hasher, &search.query);
            // Native results are large provider-owned payloads; their count and
            // per-result size are enough to see a re-issued search.
            update(hasher, search.results.len().to_string().as_bytes());
            hasher.update(SEPARATOR);
        }
    }
    hasher.update(SEPARATOR);
}

fn feed_output_block(hasher: &mut Sha256, block: &OutputBlock) {
    match block {
        OutputBlock::Text(text) => {
            update(hasher, b"out_text");
            feed_text(hasher, &text.text);
        }
        OutputBlock::Image(image) => {
            update(hasher, b"out_image");
            feed_text(hasher, &image.mime_type);
            feed_presence_and_len(hasher, b"b64", Some(&image.base64));
        }
    }
    hasher.update(SEPARATOR);
}

/// Feed presence plus length for an opaque payload: enough to see it appear,
/// disappear or change size, without copying its bytes.
fn feed_presence_and_len(hasher: &mut Sha256, label: &[u8], value: Option<&RuntimeString>) {
    update(hasher, label);
    match value {
        Some(text) => {
            update(hasher, b"1");
            update(hasher, text.len_utf16().to_string().as_bytes());
        }
        None => update(hasher, b"0"),
    }
    hasher.update(SEPARATOR);
}

fn feed_text(hasher: &mut Sha256, text: &RuntimeString) {
    hasher.update(text.to_node_utf8());
    hasher.update(SEPARATOR);
}

/// Hash one value's canonical encoding. Encoding failures fall back to a fixed
/// marker rather than propagating: this is a diagnostic.
fn feed<T: RuntimeEncode + ?Sized>(hasher: &mut Sha256, value: &T) {
    match runtime_codec::encode_value(value) {
        Ok(encoded) => hasher.update(runtime_json::stringify(&encoded).to_node_utf8()),
        Err(_) => hasher.update(UNENCODABLE),
    }
    hasher.update(SEPARATOR);
}

fn update(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(bytes);
    hasher.update(SEPARATOR);
}

fn finish(hasher: &Sha256) -> String {
    short_hex(&hasher.clone().finalize())
}

fn short_hex(digest: &[u8]) -> String {
    let mut out = String::with_capacity(DIGEST_HEX_CHARS);
    for byte in digest.iter().take(DIGEST_HEX_CHARS / 2) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimeJson;
    use crate::schemas::content::{ImageBlock, TextBlock};
    use crate::schemas::message::UserMessage;
    use crate::types::provider::{CacheScope, ToolSpecKind};

    fn request() -> ModelRequest {
        ModelRequest {
            supports_image_detail_original: false,
            model_verbosity: None,
            reasoning_summary: Default::default(),
            model: "model-a".into(),
            image_limits: None,
            max_media_blocks: None,
            system: vec![
                SystemPromptPart {
                    name: "identity".into(),
                    text: "stable identity".into(),
                    cacheable: Some(true),
                    cache_scope: Some(CacheScope::Global),
                },
                SystemPromptPart {
                    name: "reminder".into(),
                    text: "volatile reminder".into(),
                    cacheable: None,
                    cache_scope: None,
                },
            ],
            messages: vec![user("m-1", "first"), user("m-2", "second")],
            tools: vec![tool("read"), tool("write")],
            native_deferred_tools: None,
            deferred_tools: Vec::new(),
            tool_choice: None,
            max_output_tokens: 1024,
            task_budget: None,
            output_schema: None,
            web_search: None,
            thinking: None,
            effort: None,
            skip_cache_write: None,
            cache_ttl: None,
            cache_domain: None,
            cache_key: Some("session-1".into()),
            context_management: None,
            context_hints: None,
            thinking_context: None,
        }
    }

    fn user(uuid: &str, text: &str) -> Message {
        Message::User(UserMessage {
            uuid: RuntimeString::from(uuid),
            content: vec![ContentBlock::Text(TextBlock {
                text: RuntimeString::from(text),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
            question_answer: None,
        })
    }

    fn image(uuid: &str, base64: &str) -> Message {
        Message::User(UserMessage {
            uuid: RuntimeString::from(uuid),
            content: vec![ContentBlock::Image(ImageBlock {
                mime_type: "image/png".into(),
                base64: RuntimeString::from(base64),
                detail: None,
                source_path: None,
            })],
            is_meta: None,
            question_answer: None,
        })
    }

    fn tool(name: &str) -> ToolSpec {
        ToolSpec {
            name: RuntimeString::from(name),
            description: RuntimeString::from("does a thing"),
            input_schema: RuntimeJson::Null,
            kind: ToolSpecKind::Function,
            format: None,
        }
    }

    #[test]
    fn an_append_extends_the_digest_instead_of_moving_it() {
        let base = request();
        let mut grown = request();
        grown.messages.push(user("m-3", "third"));

        let a = PrefixFingerprint::of(&base);
        let b = PrefixFingerprint::of(&grown);

        // The whole point of reporting both digests: one request appends a
        // message, and its prefix digest then equals the earlier request's full
        // digest. A reader comparing two consecutive log lines can therefore
        // prove the cached region was intact, rather than inferring it from a
        // cache-read count that a low hit rate has already made ambiguous.
        assert_eq!(
            b.history_prefix_hash, a.history_hash,
            "an append must extend the previous history, not rewrite it"
        );
        assert_eq!(a.history_messages + 1, b.history_messages);
        assert_eq!(a.tools_hash, b.tools_hash);
        assert_eq!(a.stable_system_hash, b.stable_system_hash);
    }

    #[test]
    fn a_rewrite_keeps_the_count_and_moves_the_digest() {
        let base = request();
        let mut rewritten = request();
        rewritten.messages[0] = user("m-1", "first, edited in place");

        let a = PrefixFingerprint::of(&base);
        let b = PrefixFingerprint::of(&rewritten);

        // This is the failure worth finding: the same number of messages, so
        // nothing was appended, yet cached content changed and every byte after
        // it was re-sent uncached. An in-place rewrite keeps a message's identity
        // and size, which is why text is hashed in full.
        assert_eq!(a.history_messages, b.history_messages);
        assert_ne!(a.history_hash, b.history_hash);
        assert_ne!(
            b.history_prefix_hash, a.history_hash,
            "an unchanged count plus a moved digest is a rewrite, never an append"
        );
    }

    #[test]
    fn a_compaction_shrinks_the_count_and_replaces_the_digest() {
        let base = request();
        let mut compacted = request();
        compacted.messages = vec![user("boundary", "older work summary")];

        let a = PrefixFingerprint::of(&base);
        let b = PrefixFingerprint::of(&compacted);

        assert!(b.history_messages < a.history_messages);
        assert_ne!(a.history_hash, b.history_hash);
    }

    #[test]
    fn media_is_fingerprinted_by_length_without_copying_its_payload() {
        let mut base = request();
        base.messages.insert(0, image("img-1", &"A".repeat(4096)));
        let mut same_size = request();
        same_size
            .messages
            .insert(0, image("img-1", &"B".repeat(4096)));
        let mut resized = request();
        resized
            .messages
            .insert(0, image("img-1", &"A".repeat(2048)));

        // Same byte count, different content: the shape digest is stable, which
        // is the trade this module makes to stay cheap on media-heavy history.
        assert_eq!(
            PrefixFingerprint::of(&base).history_hash,
            PrefixFingerprint::of(&same_size).history_hash
        );
        // A shed or repaired image changes length, so it is still visible.
        assert_ne!(
            PrefixFingerprint::of(&base).history_hash,
            PrefixFingerprint::of(&resized).history_hash
        );
    }

    #[test]
    fn a_volatile_system_part_moves_only_the_volatile_digest() {
        let base = request();
        let mut changed = request();
        changed.system[1].text = RuntimeString::from("a different reminder");

        let a = PrefixFingerprint::of(&base);
        let b = PrefixFingerprint::of(&changed);

        assert_ne!(a.volatile_system_hash, b.volatile_system_hash);
        assert_eq!(
            a.stable_system_hash, b.stable_system_hash,
            "a reminder edit must not be reported as a stable-region change"
        );
    }

    #[test]
    fn a_part_leaving_the_cacheable_region_moves_both_digests() {
        let base = request();
        let mut flipped = request();
        flipped.system[0].cacheable = None;

        let a = PrefixFingerprint::of(&base);
        let b = PrefixFingerprint::of(&flipped);

        assert_ne!(a.stable_system_hash, b.stable_system_hash);
        assert_ne!(a.volatile_system_hash, b.volatile_system_hash);
    }

    #[test]
    fn inserting_a_tool_mid_array_moves_the_tools_digest() {
        let base = request();
        let mut inserted = request();
        // A deferred tool surfacing under its sorted name lands between the two
        // existing tools, which is the shift that invalidates the region.
        inserted.tools.insert(1, tool("mcp_alpha"));

        let a = PrefixFingerprint::of(&base);
        let b = PrefixFingerprint::of(&inserted);

        assert_ne!(a.tools_hash, b.tools_hash);
        assert_eq!(a.tool_count + 1, b.tool_count);
    }

    #[test]
    fn reordering_tools_moves_the_tools_digest() {
        let base = request();
        let mut reordered = request();
        reordered.tools.swap(0, 1);

        assert_ne!(
            PrefixFingerprint::of(&base).tools_hash,
            PrefixFingerprint::of(&reordered).tools_hash,
            "tool order is part of the cached prefix"
        );
    }

    #[test]
    fn the_cache_key_is_reported_as_a_digest_not_a_value() {
        let a = PrefixFingerprint::of(&request());
        let mut other = request();
        other.cache_key = Some("session-2".into());
        let b = PrefixFingerprint::of(&other);

        assert_ne!(a.cache_key_hash, b.cache_key_hash);
        let key = a.cache_key_hash.clone().expect("a key was present");
        assert_eq!(key.len(), DIGEST_HEX_CHARS);
        assert!(
            !key.contains("session"),
            "the log carries a digest, never the identifier itself"
        );

        let mut keyless = request();
        keyless.cache_key = None;
        assert_eq!(PrefixFingerprint::of(&keyless).cache_key_hash, None);
    }

    #[test]
    fn the_fields_are_digests_and_counts_only() {
        let fingerprint = PrefixFingerprint::of(&request());
        let fields = fingerprint.fields();

        assert_eq!(fields["prefixToolCount"], json!(2));
        assert_eq!(fields["prefixSystemParts"], json!(2));
        assert_eq!(fields["prefixHistoryMessages"], json!(2));
        assert!(fields["prefixHistoryPrefixHash"].is_string());
        let serialized = fields.to_string();
        for leaked in [
            "stable identity",
            "volatile reminder",
            "does a thing",
            "first",
            "session-1",
        ] {
            assert!(
                !serialized.contains(leaked),
                "{leaked:?} must not reach a content-free log"
            );
        }
    }

    #[test]
    fn an_empty_request_still_fingerprints() {
        let mut empty = request();
        empty.tools.clear();
        empty.system.clear();
        empty.messages.clear();

        let fingerprint = PrefixFingerprint::of(&empty);
        assert_eq!(fingerprint.tool_count, 0);
        assert_eq!(fingerprint.system_parts, 0);
        assert_eq!(fingerprint.history_messages, 0);
        assert_eq!(fingerprint.history_hash.len(), DIGEST_HEX_CHARS);
    }

    #[test]
    fn a_single_message_request_has_an_empty_history_prefix() {
        let mut single = request();
        single.messages = vec![user("m-1", "only")];

        let fingerprint = PrefixFingerprint::of(&single);
        assert_eq!(fingerprint.history_messages, 1);
        assert_eq!(
            fingerprint.history_prefix_hash,
            PrefixFingerprint::of(&{
                let mut none = request();
                none.messages.clear();
                none
            })
            .history_hash,
            "excluding the last of one message leaves the same empty digest"
        );
    }

    #[test]
    fn digests_are_stable_across_repeated_fingerprints_of_one_request() {
        // A reader compares digests across log lines, so the same request must
        // always produce the same value: nothing here may depend on map
        // iteration order or wall-clock time.
        let a = PrefixFingerprint::of(&request());
        let b = PrefixFingerprint::of(&request());
        assert_eq!(a, b);
    }
}
