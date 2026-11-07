//! Request-view media validation and aggregate shedding.
//!
//! The budget counts normalized user image/document blocks and their base64
//! payloads, including tool-result content. It is not a whole-JSON body limit.
//! A compliant slice is borrowed unchanged; shedding never edits persisted
//! messages and keeps the newest media until both hysteresis targets are met.

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::LazyLock;

use regex::Regex;

use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};

pub const DIRECT_MEDIA_BASE64_BYTES: usize = 24 * 1024 * 1024;
pub const OTHER_MEDIA_BASE64_BYTES: usize = 75 * 1024 * 1024;
pub const MEDIA_COUNT_HEADROOM: usize = 20;
pub const MEDIA_BASE64_HEADROOM: usize = 10 * 1024 * 1024;
pub const MEDIA_REMOVED_MARKER: &str = "[media removed: request limit]";

tokio::task_local! {
    // The watchdog observes the adapter's ledger even when it ends polling
    // before the adapter can emit its terminal error. No wire field is needed.
    pub(crate) static REQUEST_MEDIA_REPAIRED: Arc<AtomicBool>;
}

pub(crate) fn media_repair_observer() -> Option<Arc<AtomicBool>> {
    REQUEST_MEDIA_REPAIRED.try_with(Arc::clone).ok()
}

pub(crate) fn request_media_repaired() -> bool {
    REQUEST_MEDIA_REPAIRED
        .try_with(|state| state.load(Ordering::Acquire))
        .unwrap_or(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaBudget {
    pub max_blocks: usize,
    pub max_base64_bytes: usize,
    pub count_headroom: usize,
    pub base64_headroom: usize,
}

impl MediaBudget {
    pub fn for_anthropic(direct_endpoint: bool, max_media_blocks: Option<usize>) -> Self {
        Self {
            max_blocks: max_media_blocks.filter(|limit| *limit > 0).unwrap_or(100),
            max_base64_bytes: if direct_endpoint {
                DIRECT_MEDIA_BASE64_BYTES
            } else {
                OTHER_MEDIA_BASE64_BYTES
            },
            count_headroom: MEDIA_COUNT_HEADROOM,
            base64_headroom: MEDIA_BASE64_HEADROOM,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MediaUsage {
    pub blocks: usize,
    pub base64_bytes: usize,
}

fn is_user(message: &RuntimeJson) -> bool {
    message
        .get("role")
        .and_then(RuntimeJson::as_str)
        .is_some_and(|role| role == "user")
}

fn is_media(block: &RuntimeJson) -> bool {
    block
        .get("type")
        .and_then(RuntimeJson::as_str)
        .is_some_and(|kind| kind == "image" || kind == "document")
}

fn nested_content(block: &RuntimeJson) -> Option<&Vec<RuntimeJson>> {
    if block
        .get("type")
        .and_then(RuntimeJson::as_str)
        .is_some_and(|kind| kind == "tool_result")
    {
        block.get("content").and_then(RuntimeJson::as_array)
    } else {
        None
    }
}

fn media_base64_bytes(block: &RuntimeJson) -> usize {
    let Some(source) = block.get("source") else {
        return 0;
    };
    if source
        .get("type")
        .and_then(RuntimeJson::as_str)
        .is_none_or(|kind| kind != "base64")
    {
        return 0;
    }
    source
        .get("data")
        .and_then(RuntimeJson::as_str)
        .map_or(0, RuntimeString::len_utf16)
}

pub fn media_usage(messages: &[RuntimeJson]) -> MediaUsage {
    let mut usage = MediaUsage::default();
    for message in messages.iter().filter(|message| is_user(message)) {
        let Some(content) = message.get("content").and_then(RuntimeJson::as_array) else {
            continue;
        };
        for block in content {
            if is_media(block) {
                usage.blocks = usage.blocks.saturating_add(1);
                usage.base64_bytes = usage.base64_bytes.saturating_add(media_base64_bytes(block));
            }
            if let Some(nested) = nested_content(block) {
                for media in nested.iter().filter(|media| is_media(media)) {
                    usage.blocks = usage.blocks.saturating_add(1);
                    usage.base64_bytes =
                        usage.base64_bytes.saturating_add(media_base64_bytes(media));
                }
            }
        }
    }
    usage
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImagePayloadViolation {
    /// One-based image ordinal across the normalized user request.
    pub image_index: usize,
    pub message_index: usize,
    pub content_index: usize,
    pub tool_result_content_index: Option<usize>,
    pub base64_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImagePayloadError {
    pub max_base64_bytes: usize,
    pub violations: Vec<ImagePayloadViolation>,
}

impl fmt::Display for ImagePayloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Final image payload exceeds the {} base64-byte limit",
            self.max_base64_bytes
        )?;
        for violation in &self.violations {
            write!(
                formatter,
                "; image {} at messages[{}].content[{}]",
                violation.image_index, violation.message_index, violation.content_index
            )?;
            if let Some(index) = violation.tool_result_content_index {
                write!(formatter, ".content[{index}]")?;
            }
            write!(formatter, " has {} base64 bytes", violation.base64_bytes)?;
        }
        Ok(())
    }
}

impl std::error::Error for ImagePayloadError {}

/// Validate final encoded images before aggregate stripping. A historical image
/// exceeding its per-image ceiling must not be hidden by the aggregate gate.
pub fn validate_final_images(
    messages: &[RuntimeJson],
    max_base64_bytes: usize,
) -> Result<(), ImagePayloadError> {
    let mut image_index = 0;
    let mut violations = Vec::new();
    for (message_index, message) in messages
        .iter()
        .enumerate()
        .filter(|(_, message)| is_user(message))
    {
        let Some(content) = message.get("content").and_then(RuntimeJson::as_array) else {
            continue;
        };
        for (content_index, block) in content.iter().enumerate() {
            let mut check = |media: &RuntimeJson, tool_result_content_index| {
                if media
                    .get("type")
                    .and_then(RuntimeJson::as_str)
                    .is_none_or(|kind| kind != "image")
                {
                    return;
                }
                image_index += 1;
                let base64_bytes = media_base64_bytes(media);
                if base64_bytes > max_base64_bytes {
                    violations.push(ImagePayloadViolation {
                        image_index,
                        message_index,
                        content_index,
                        tool_result_content_index,
                        base64_bytes,
                    });
                }
            };
            check(block, None);
            if let Some(nested) = nested_content(block) {
                for (index, media) in nested.iter().enumerate() {
                    check(media, Some(index));
                }
            }
        }
    }
    if violations.is_empty() {
        Ok(())
    } else {
        Err(ImagePayloadError {
            max_base64_bytes,
            violations,
        })
    }
}

fn remove_media(
    block: &RuntimeJson,
    count_to_remove: &mut usize,
    bytes_to_remove: &mut usize,
) -> bool {
    if !is_media(block) {
        return false;
    }
    let bytes = media_base64_bytes(block);
    if *count_to_remove == 0 && (*bytes_to_remove == 0 || bytes == 0) {
        return false;
    }
    *count_to_remove = count_to_remove.saturating_sub(1);
    *bytes_to_remove = bytes_to_remove.saturating_sub(bytes);
    true
}

/// Oldest message first; within each message nested media precede top-level
/// media. Crossing one cap does not require crossing the other: both remaining
/// removal targets must be satisfied before stopping. URL/file sources count
/// toward the block limit but cannot help clear a base64-only excess.
pub fn trim_media(messages: &[RuntimeJson], budget: MediaBudget) -> Cow<'_, [RuntimeJson]> {
    let usage = media_usage(messages);
    let count_excess = usage.blocks.saturating_sub(budget.max_blocks);
    let bytes_excess = if budget.max_base64_bytes > 0 {
        usage.base64_bytes.saturating_sub(budget.max_base64_bytes)
    } else {
        0
    };
    if count_excess == 0 && bytes_excess == 0 {
        return Cow::Borrowed(messages);
    }
    let mut count_to_remove = if count_excess > 0 {
        count_excess.saturating_add(budget.count_headroom)
    } else {
        0
    };
    let mut bytes_to_remove = if bytes_excess > 0 {
        bytes_excess.saturating_add(budget.base64_headroom)
    } else {
        0
    };
    let mut trimmed = messages.to_vec();
    for message in &mut trimmed {
        if count_to_remove == 0 && bytes_to_remove == 0 {
            break;
        }
        if !is_user(message) {
            continue;
        }
        let Some(content) = message
            .get_mut("content")
            .and_then(RuntimeJson::as_array_mut)
        else {
            continue;
        };
        let mut removed = false;
        for block in content.iter_mut() {
            if count_to_remove == 0 && bytes_to_remove == 0 {
                break;
            }
            if nested_content(block).is_none() {
                continue;
            }
            let nested = block
                .get_mut("content")
                .and_then(RuntimeJson::as_array_mut)
                .expect("nested_content verified an array");
            nested.retain(|media| {
                let remove = remove_media(media, &mut count_to_remove, &mut bytes_to_remove);
                removed |= remove;
                !remove
            });
        }
        content.retain(|media| {
            let remove = remove_media(media, &mut count_to_remove, &mut bytes_to_remove);
            removed |= remove;
            !remove
        });
        if removed && content.is_empty() {
            let mut marker = RuntimeObject::new();
            marker.insert("type", RuntimeJson::from("text"));
            marker.insert("text", RuntimeJson::from(MEDIA_REMOVED_MARKER));
            content.push(RuntimeJson::Object(marker));
        }
    }
    Cow::Owned(trimmed)
}

/// A recognized HTTP400 media rejection, not a token or whole-request-size error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectedMediaKind {
    Image,
    Document,
}

impl RejectedMediaKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Document => "document",
        }
    }

    fn matches(self, block: &RuntimeJson, base64_only: bool) -> bool {
        if block
            .get("type")
            .and_then(RuntimeJson::as_str)
            .is_none_or(|kind| kind != self.as_str())
        {
            return false;
        }
        !base64_only
            || block
                .get("source")
                .and_then(|source| source.get("type"))
                .and_then(RuntimeJson::as_str)
                .is_some_and(|kind| kind == "base64")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MediaCoordinate {
    pub message_index: usize,
    pub content_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaRejection {
    pub kind: RejectedMediaKind,
    /// Coordinates in the normalized outgoing messages, never persisted history.
    pub coordinate: Option<MediaCoordinate>,
}

const IMAGE_REJECTION_MESSAGES: &[&str] = &[
    "could not process image",
    "image exceeds",
    "image dimensions exceed",
    "image does not match the provided media type",
    "image cannot be empty",
    "exceeds api limit",
    "images exceed the api limit",
    "unable to resize image",
    "unable to compress image",
    "image file is empty",
];
const DOCUMENT_REJECTION_MESSAGES: &[&str] = &[
    "could not process pdf",
    "could not process document",
    "pdf pages",
    "the pdf specified was not valid",
    "the pdf specified is password protected",
    "pdf cannot be empty",
    "too much media",
];

static MEDIA_COORDINATE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
    r"(?i)\bmessages(?:\.(?P<message_dot>[0-9]+)|\[(?P<message_bracket>[0-9]+)\])\.content(?:\.(?P<content_dot>[0-9]+)|\[(?P<content_bracket>[0-9]+)\])(?:(?:\.tool_result)?\.content(?:\.[0-9]+|\[[0-9]+\]))?(?:\.(?P<kind>image|document|pdf)\b)?"
).expect("media coordinate pattern is valid")
});

static MEDIA_TYPED_PATH_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
    r"(?i)\bmessages(?:\.(?P<message_dot>[0-9]+)|\[(?P<message_bracket>[0-9]+)\])\.content(?:\.(?P<content_dot>[0-9]+)|\[(?P<content_bracket>[0-9]+)\])(?:(?:\.tool_result)?\.content(?:\.[0-9]+|\[[0-9]+\]))?\.(?P<kind>image|document|pdf)\b"
).expect("typed media coordinate pattern is valid")
});

/// Reject unrelated HTTP400 wording and every non-400 status. A typed media
/// path identifies the kind directly; otherwise only recognized English media
/// refusals qualify. A bare input/context-size phrase does not qualify.
pub fn parse_media_rejection(status: Option<u16>, message: &str) -> Option<MediaRejection> {
    if status != Some(400) {
        return None;
    }
    let captures = MEDIA_TYPED_PATH_RE
        .captures(message)
        .or_else(|| MEDIA_COORDINATE_RE.captures(message));
    let path_kind = captures
        .as_ref()
        .and_then(|captures| captures.name("kind"))
        .map(|kind| kind.as_str().to_ascii_lowercase());
    let lower = message.to_ascii_lowercase();
    let kind = match path_kind.as_deref() {
        Some("image") => RejectedMediaKind::Image,
        Some("document" | "pdf") => RejectedMediaKind::Document,
        _ if IMAGE_REJECTION_MESSAGES
            .iter()
            .any(|pattern| lower.contains(pattern)) =>
        {
            RejectedMediaKind::Image
        }
        _ if DOCUMENT_REJECTION_MESSAGES
            .iter()
            .any(|pattern| lower.contains(pattern)) =>
        {
            RejectedMediaKind::Document
        }
        _ => return None,
    };
    let coordinate = captures.and_then(|captures| {
        let message_index = captures
            .name("message_dot")
            .or_else(|| captures.name("message_bracket"))?
            .as_str()
            .parse()
            .ok()?;
        let content_index = captures
            .name("content_dot")
            .or_else(|| captures.name("content_bracket"))?
            .as_str()
            .parse()
            .ok()?;
        Some(MediaCoordinate {
            message_index,
            content_index,
        })
    });
    Some(MediaRejection { kind, coordinate })
}

fn rejected_media_marker(block: &RuntimeJson, kind: RejectedMediaKind) -> RuntimeJson {
    let mut marker = RuntimeObject::new();
    marker.insert("type", RuntimeJson::from("text"));
    marker.insert("text", RuntimeJson::from(format!(
        "[{} removed: the API could not process this {}. The file may be unsupported or corrupt; do not retry reading it. Use a shell command if inspection is needed.]",
        if kind == RejectedMediaKind::Image { "Image" } else { "Document" }, kind.as_str(),
    )));
    if let Some(cache) = block.get("cache_control") {
        marker.insert("cache_control", cache.clone());
    }
    RuntimeJson::Object(marker)
}

fn replace_rejected_block(
    block: &mut RuntimeJson,
    kind: RejectedMediaKind,
    base64_only: bool,
) -> bool {
    if kind.matches(block, base64_only) {
        *block = rejected_media_marker(block, kind);
        return true;
    }
    if nested_content(block).is_none() {
        return false;
    }
    let mut changed = false;
    for media in block
        .get_mut("content")
        .and_then(RuntimeJson::as_array_mut)
        .expect("nested content is an array")
    {
        if kind.matches(media, base64_only) {
            *media = rejected_media_marker(media, kind);
            changed = true;
        }
    }
    changed
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MediaRepairReason {
    Targeted {
        kind: RejectedMediaKind,
        coordinate: MediaCoordinate,
    },
    LatestCarrier {
        kind: RejectedMediaKind,
        message_index: usize,
    },
}

impl fmt::Display for MediaRepairReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Targeted { kind, coordinate } => write!(
                formatter,
                "media-strip:{}:{}.{}",
                kind.as_str(),
                coordinate.message_index,
                coordinate.content_index
            ),
            Self::LatestCarrier {
                kind,
                message_index,
            } => write!(
                formatter,
                "media-strip-latest:{}:{}",
                kind.as_str(),
                message_index
            ),
        }
    }
}

#[derive(Debug)]
pub struct MediaRequestRepair {
    pub body: RuntimeJson,
    pub reason: MediaRepairReason,
}

/// State belongs to ONE adapter request, across ordinary establishment/auth
/// attempts. Its three untargeted media repairs are independent of transport,
/// request-size, and summary retries. Reasons are consumed only on real change.
#[derive(Debug, Default)]
pub struct MediaRepairState {
    untargeted_attempts: u8,
    reasons: HashSet<MediaRepairReason>,
}

impl MediaRepairState {
    pub fn has_repairs(&self) -> bool {
        !self.reasons.is_empty()
    }

    pub fn untargeted_attempts(&self) -> u8 {
        self.untargeted_attempts
    }

    pub fn repair_request(
        &mut self,
        body: &RuntimeJson,
        status: Option<u16>,
        message: &str,
    ) -> Option<MediaRequestRepair> {
        let rejection = parse_media_rejection(status, message)?;
        let messages = body.get("messages").and_then(RuntimeJson::as_array)?;
        if let Some(coordinate) = rejection.coordinate {
            let carrier = messages
                .get(coordinate.message_index)
                .filter(|message| is_user(message))
                .and_then(|message| message.get("content"))
                .and_then(RuntimeJson::as_array)
                .and_then(|content| content.get(coordinate.content_index));
            if let Some(carrier) = carrier {
                let reason = MediaRepairReason::Targeted {
                    kind: rejection.kind,
                    coordinate,
                };
                let mut replacement = carrier.clone();
                if !self.reasons.contains(&reason)
                    && replace_rejected_block(&mut replacement, rejection.kind, false)
                {
                    let mut repaired = body.clone();
                    repaired["messages"][coordinate.message_index]["content"]
                        [coordinate.content_index] = replacement;
                    self.reasons.insert(reason);
                    return Some(MediaRequestRepair {
                        body: repaired,
                        reason,
                    });
                }
            }
        }
        if self.untargeted_attempts >= 3 {
            return None;
        }
        for (message_index, carrier) in messages
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, message)| is_user(message))
        {
            let Some(content) = carrier.get("content").and_then(RuntimeJson::as_array) else {
                continue;
            };
            let mut content = content.clone();
            let mut changed = false;
            for block in &mut content {
                changed |= replace_rejected_block(block, rejection.kind, true);
            }
            if !changed {
                continue;
            }
            let reason = MediaRepairReason::LatestCarrier {
                kind: rejection.kind,
                message_index,
            };
            if self.reasons.contains(&reason) {
                return None;
            }
            let mut repaired = body.clone();
            repaired["messages"][message_index]["content"] = RuntimeJson::Array(content);
            self.untargeted_attempts += 1;
            self.reasons.insert(reason);
            return Some(MediaRequestRepair {
                body: repaired,
                reason,
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;

    fn wire(json: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(json)).unwrap()
    }

    fn small_budget(count: usize, bytes: usize) -> MediaBudget {
        MediaBudget {
            max_blocks: count,
            max_base64_bytes: bytes,
            count_headroom: 0,
            base64_headroom: 0,
        }
    }

    #[test]
    fn http400_parser_covers_known_media_wording_and_both_nested_path_carriers() {
        for message in IMAGE_REJECTION_MESSAGES {
            assert_eq!(
                parse_media_rejection(Some(400), message).unwrap().kind,
                RejectedMediaKind::Image
            );
        }
        for message in DOCUMENT_REJECTION_MESSAGES {
            assert_eq!(
                parse_media_rejection(Some(400), message).unwrap().kind,
                RejectedMediaKind::Document
            );
        }
        for path in [
            "messages.12.content.3.content.4.image",
            "messages[12].content[3].content[4].image",
            "messages.12.content.3.tool_result.content.4.image",
            "messages[12].content[3].tool_result.content[4].image",
        ] {
            assert_eq!(
                parse_media_rejection(Some(400), path),
                Some(MediaRejection {
                    kind: RejectedMediaKind::Image,
                    coordinate: Some(MediaCoordinate {
                        message_index: 12,
                        content_index: 3
                    }),
                })
            );
        }
        assert_eq!(
            parse_media_rejection(Some(400), "messages[1].content[2].pdf.source: invalid")
                .unwrap()
                .kind,
            RejectedMediaKind::Document
        );
        for status in [None, Some(413), Some(500)] {
            assert!(parse_media_rejection(
                status,
                "messages.0.content.0.image: Could not process image"
            )
            .is_none());
        }
        for ordinary in [
            "Input is too long.",
            "Invalid tool choice",
            "Context window exceeded",
        ] {
            assert!(parse_media_rejection(Some(400), ordinary).is_none());
        }
    }

    #[test]
    fn untargeted_repair_replaces_only_base64_of_the_rejected_kind_in_the_latest_user_carrier() {
        let body = wire(
            r#"{"model":"fixture","messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"OLD"}}]},{"role":"assistant","content":[{"type":"text","text":"answer"}]},{"role":"user","content":[{"type":"image","source":{"type":"url","url":"https://example.test/image"}},{"type":"document","source":{"type":"base64","data":"PDF"}},{"type":"tool_result","tool_use_id":"tool","content":[{"type":"text","text":"keep"},{"type":"image","source":{"type":"base64","data":""}},{"type":"image","source":{"type":"url","url":"https://example.test/nested"}},{"type":"document","source":{"type":"base64","data":"PDF"}}]}]}]}"#,
        );
        let original = body.clone();
        let mut state = MediaRepairState::default();
        let repair = state
            .repair_request(&body, Some(400), "Image cannot be empty")
            .unwrap();
        assert_eq!(
            repair.reason,
            MediaRepairReason::LatestCarrier {
                kind: RejectedMediaKind::Image,
                message_index: 2
            }
        );
        let content = &repair.body["messages"][2]["content"];
        assert_eq!(content[0]["source"]["type"], RuntimeJson::from("url"));
        assert_eq!(content[1]["type"], RuntimeJson::from("document"));
        assert_eq!(content[2]["tool_use_id"], RuntimeJson::from("tool"));
        assert_eq!(content[2]["content"][0]["text"], RuntimeJson::from("keep"));
        assert_eq!(content[2]["content"][1]["type"], RuntimeJson::from("text"));
        assert_eq!(
            content[2]["content"][2]["source"]["type"],
            RuntimeJson::from("url")
        );
        assert_eq!(
            content[2]["content"][3]["type"],
            RuntimeJson::from("document")
        );
        assert_eq!(repair.body["messages"][0], body["messages"][0]);
        assert_eq!(body, original);
        assert_eq!(state.untargeted_attempts(), 1);
    }

    #[test]
    fn targeted_carrier_preserves_identity_text_other_kinds_and_deduplicates_outer_coordinate() {
        let body = wire(
            r#"{"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"TOP"}},{"type":"tool_result","tool_use_id":"tool","is_error":false,"cache_control":{"type":"ephemeral"},"content":[{"type":"text","text":"keep"},{"type":"image","source":{"type":"base64","data":"A"}},{"type":"image","source":{"type":"url","url":"https://example.test/image"}},{"type":"document","source":{"type":"base64","data":"PDF"}}]}]}]}"#,
        );
        let mut state = MediaRepairState::default();
        let repair = state
            .repair_request(
                &body,
                Some(400),
                "messages.0.content.1.content.1.image: invalid",
            )
            .unwrap();
        let carrier = &repair.body["messages"][0]["content"][1];
        assert_eq!(carrier["tool_use_id"], RuntimeJson::from("tool"));
        assert_eq!(carrier["is_error"], RuntimeJson::Bool(false));
        assert_eq!(
            carrier["cache_control"]["type"],
            RuntimeJson::from("ephemeral")
        );
        assert_eq!(carrier["content"][0]["text"], RuntimeJson::from("keep"));
        assert_eq!(carrier["content"][1]["type"], RuntimeJson::from("text"));
        assert_eq!(carrier["content"][2]["type"], RuntimeJson::from("text"));
        assert_eq!(carrier["content"][3]["type"], RuntimeJson::from("document"));
        assert_eq!(
            repair.body["messages"][0]["content"][0],
            body["messages"][0]["content"][0]
        );
        assert_eq!(state.untargeted_attempts(), 0);
        let fallback = state
            .repair_request(
                &repair.body,
                Some(400),
                "messages[0].content[1].tool_result.content[2].image: invalid",
            )
            .expect("an unchanged target falls through to the remaining base64 carrier");
        assert_ne!(fallback.body, repair.body);
        assert_eq!(
            fallback.body["messages"][0]["content"][0]["type"],
            RuntimeJson::from("text")
        );
        assert_eq!(
            fallback.body["messages"][0]["content"][1],
            repair.body["messages"][0]["content"][1]
        );
        assert!(matches!(
            fallback.reason,
            MediaRepairReason::LatestCarrier {
                message_index: 0,
                ..
            }
        ));
        assert_eq!(state.untargeted_attempts(), 1);
        assert!(
            state
                .repair_request(&body, Some(400), "messages.0.content.1.image: invalid")
                .is_none(),
            "both targeted and fallback reasons have already been consumed"
        );
        let document_repair = state
            .repair_request(
                &fallback.body,
                Some(400),
                "messages.0.content.1.document: invalid",
            )
            .unwrap();
        assert_eq!(
            document_repair.body["messages"][0]["content"][1]["content"][3]["type"],
            RuntimeJson::from("text")
        );
        assert_eq!(
            state.untargeted_attempts(),
            1,
            "a different kind is a separate targeted reason"
        );
        assert!(state
            .repair_request(&document_repair.body, Some(400), "Could not process image")
            .is_none());
        assert_eq!(state.untargeted_attempts(), 1);
    }

    #[test]
    fn no_change_does_not_consume_an_attempt_and_unusable_coordinates_can_select_latest() {
        let body = wire(
            r#"{"messages":[{"role":"user","content":[{"type":"text","text":"keep"},{"type":"image","source":{"type":"base64","data":"A"}}]}]}"#,
        );
        let mut state = MediaRepairState::default();
        let empty =
            wire(r#"{"messages":[{"role":"user","content":[{"type":"text","text":"keep"}]}]}"#);
        assert!(state
            .repair_request(&empty, Some(400), "messages.0.content.0.image: invalid")
            .is_none());
        assert_eq!(state.untargeted_attempts(), 0);
        for path in [
            "messages.0.content.0.image: invalid",
            "messages.99.content.99.image: invalid",
        ] {
            let mut state = MediaRepairState::default();
            let repair = state.repair_request(&body, Some(400), path).unwrap();
            assert_ne!(repair.body, body);
            assert_eq!(
                repair.body["messages"][0]["content"][0],
                body["messages"][0]["content"][0]
            );
            assert_eq!(
                repair.body["messages"][0]["content"][1]["type"],
                RuntimeJson::from("text")
            );
            assert_eq!(state.untargeted_attempts(), 1);
            assert!(state
                .repair_request(&repair.body, Some(400), "Could not process image")
                .is_none());
            assert_eq!(state.untargeted_attempts(), 1);
        }
    }

    #[test]
    fn compliant_request_keeps_the_same_slice_and_bytes() {
        let messages = vec![wire(
            r#"{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"AAAA"}}]}"#,
        )];
        let trimmed = trim_media(&messages, small_budget(1, 4));
        assert!(matches!(trimmed, Cow::Borrowed(_)));
        assert!(std::ptr::eq(trimmed.as_ptr(), messages.as_ptr()));
    }

    #[test]
    fn byte_only_excess_keeps_non_base64_references_and_strips_nested_first() {
        let messages = vec![wire(
            r#"{"role":"user","content":[{"type":"image","source":{"type":"url","url":"https://example.test/image"}},{"type":"image","source":{"type":"base64","data":"AAAA"}},{"type":"tool_result","tool_use_id":"t","content":[{"type":"document","source":{"type":"base64","data":"AAAA"}},{"type":"text","text":"keep"}]}]}"#,
        )];
        let original = messages.clone();
        let trimmed = trim_media(&messages, small_budget(10, 4));
        let content = trimmed[0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 3);
        assert_eq!(content[2]["content"].as_array().unwrap().len(), 1);
        assert_eq!(
            media_usage(&trimmed),
            MediaUsage {
                blocks: 2,
                base64_bytes: 4
            }
        );
        assert_eq!(messages, original);
    }

    #[test]
    fn count_excess_uses_headroom_even_for_url_and_file_sources() {
        let messages = vec![wire(
            r#"{"role":"user","content":[{"type":"image","source":{"type":"url"}},{"type":"document","source":{"type":"file","file_id":"file"}},{"type":"image","source":{"type":"url"}}]}"#,
        )];
        let budget = MediaBudget {
            count_headroom: 1,
            ..small_budget(2, 4)
        };
        let trimmed = trim_media(&messages, budget);
        assert_eq!(
            media_usage(&trimmed),
            MediaUsage {
                blocks: 1,
                base64_bytes: 0
            }
        );
    }

    #[test]
    fn both_targets_must_be_met_before_shedding_stops() {
        let messages = vec![wire(
            r#"{"role":"user","content":[{"type":"image","source":{"type":"url"}},{"type":"document","source":{"type":"base64","data":"AAAA"}},{"type":"image","source":{"type":"base64","data":"AAAAAAAA"}}]}"#,
        )];
        let budget = MediaBudget {
            count_headroom: 1,
            base64_headroom: 4,
            ..small_budget(2, 8)
        };
        let trimmed = trim_media(&messages, budget);
        assert_eq!(media_usage(&trimmed), MediaUsage::default());
        assert_eq!(
            trimmed[0]["content"][0]["text"],
            RuntimeJson::from(MEDIA_REMOVED_MARKER)
        );
    }

    #[test]
    fn final_validation_reports_top_level_and_nested_images_not_documents() {
        let messages = vec![wire(
            r#"{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"AAAAAA"}},{"type":"tool_result","content":[{"type":"document","source":{"type":"base64","data":"AAAAAAAA"}},{"type":"image","source":{"type":"base64","data":"AAAAAA"}}]}]}"#,
        )];
        let error = validate_final_images(&messages, 4).unwrap_err();
        assert_eq!(error.violations.len(), 2);
        assert_eq!(error.violations[1].image_index, 2);
        assert_eq!(error.violations[1].tool_result_content_index, Some(1));
    }

    #[test]
    fn direct_byte_cap_clears_ten_mebibytes_of_headroom_not_a_fixed_recent_count() {
        let mut media = wire(r#"{"type":"image","source":{"type":"base64","data":"AAAA"}}"#);
        media["source"]["data"] = RuntimeJson::String(RuntimeString::from("A".repeat(600_000)));
        let mut message = wire(r#"{"role":"user","content":[]}"#);
        message["content"] = RuntimeJson::Array(
            (0..55)
                .map(|index| {
                    let mut block = media.clone();
                    block["fixture_index"] = RuntimeJson::from(index);
                    block
                })
                .collect(),
        );
        let messages = vec![message];
        let trimmed = trim_media(&messages, MediaBudget::for_anthropic(true, None));
        assert_eq!(
            media_usage(&trimmed),
            MediaUsage {
                blocks: 24,
                base64_bytes: 14_400_000
            }
        );
        assert_eq!(
            trimmed[0]["content"][0]["fixture_index"],
            RuntimeJson::from(31)
        );
        assert_eq!(messages[0]["content"].as_array().unwrap().len(), 55);

        let mut compliant = messages.clone();
        for block in compliant[0]["content"].as_array_mut().unwrap() {
            block["source"]["data"] = RuntimeJson::String(RuntimeString::from("A".repeat(100_000)));
        }
        let kept = trim_media(&compliant, MediaBudget::for_anthropic(true, None));
        assert!(matches!(kept, Cow::Borrowed(_)));
        assert_eq!(media_usage(&kept).blocks, 55);
    }

    #[test]
    fn endpoint_and_explicit_count_select_separate_policy_axes() {
        assert_eq!(
            MediaBudget::for_anthropic(true, None).max_base64_bytes,
            25_165_824
        );
        assert_eq!(
            MediaBudget::for_anthropic(false, None).max_base64_bytes,
            78_643_200
        );
        assert_eq!(MediaBudget::for_anthropic(true, None).max_blocks, 100);
        assert_eq!(MediaBudget::for_anthropic(false, Some(600)).max_blocks, 600);
    }
}
