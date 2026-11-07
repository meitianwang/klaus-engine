//! Aggregate native-media projection for one outgoing model request.

use std::borrow::Cow;

use crate::context::media_budget::{MediaBudget, MediaUsage, MEDIA_REMOVED_MARKER};
use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock};
use crate::schemas::message::Message;
use crate::types::errors::{InputValidationError, InputValidationErrorCode};

fn visit_outputs(
    blocks: &[OutputBlock],
    path: &str,
    visit: &mut impl FnMut(bool, &RuntimeString, &str),
) {
    for (index, block) in blocks.iter().enumerate() {
        if let OutputBlock::Image(image) = block {
            visit(true, &image.base64, &format!("{path}[{index}]"));
        }
    }
}

fn visit_content(
    blocks: &[ContentBlock],
    path: &str,
    visit: &mut impl FnMut(bool, &RuntimeString, &str),
) {
    for (index, block) in blocks.iter().enumerate() {
        let path = format!("{path}[{index}]");
        match block {
            ContentBlock::Image(image) => visit(true, &image.base64, &path),
            ContentBlock::Document(document) => visit(false, &document.base64, &path),
            ContentBlock::ToolResult(result) => {
                visit_outputs(&result.content, &format!("{path}.content"), visit)
            }
            _ => {}
        }
    }
}

fn visit_media(messages: &[Message], mut visit: impl FnMut(bool, &RuntimeString, &str)) {
    for (index, message) in messages.iter().enumerate() {
        let path = format!("messages[{index}]");
        match message {
            Message::User(user) => {
                visit_content(&user.content, &format!("{path}.content"), &mut visit)
            }
            Message::Assistant(assistant) => {
                visit_content(&assistant.content, &format!("{path}.content"), &mut visit)
            }
            Message::ToolResult(result) => {
                visit_outputs(&result.content, &format!("{path}.content"), &mut visit);
                if let Some(blocks) = &result.extra_user_blocks {
                    visit_content(blocks, &format!("{path}.extraUserBlocks"), &mut visit);
                }
            }
            _ => {}
        }
    }
}

/// Count image and document blocks and their encoded payload lengths. Native
/// audio/video follows its own admission policy and is not part of this budget.
pub fn native_media_usage(messages: &[Message]) -> MediaUsage {
    let mut usage = MediaUsage::default();
    visit_media(messages, |_, base64, _| {
        usage.blocks = usage.blocks.saturating_add(1);
        usage.base64_bytes = usage.base64_bytes.saturating_add(base64.len_utf16());
    });
    usage
}

struct RemovalTargets {
    blocks: usize,
    base64_bytes: usize,
}

impl RemovalTargets {
    fn complete(&self) -> bool {
        self.blocks == 0 && self.base64_bytes == 0
    }

    fn remove(&mut self, base64: &RuntimeString) -> bool {
        let bytes = base64.len_utf16();
        if self.blocks == 0 && (self.base64_bytes == 0 || bytes == 0) {
            return false;
        }
        self.blocks = self.blocks.saturating_sub(1);
        self.base64_bytes = self.base64_bytes.saturating_sub(bytes);
        true
    }
}

fn trim_outputs(blocks: &mut Vec<OutputBlock>, targets: &mut RemovalTargets) {
    blocks.retain(|block| match block {
        OutputBlock::Image(image) => !targets.remove(&image.base64),
        _ => true,
    });
}

fn trim_nested_content(blocks: &mut [ContentBlock], targets: &mut RemovalTargets) {
    for block in blocks {
        if let ContentBlock::ToolResult(result) = block {
            trim_outputs(&mut result.content, targets);
        }
    }
}

fn trim_sibling_content(
    blocks: &mut Vec<ContentBlock>,
    targets: &mut RemovalTargets,
    placeholder_if_empty: bool,
) {
    let before = blocks.len();
    blocks.retain(|block| match block {
        ContentBlock::Image(image) => !targets.remove(&image.base64),
        ContentBlock::Document(document) => !targets.remove(&document.base64),
        _ => true,
    });
    if placeholder_if_empty && before > 0 && blocks.is_empty() {
        blocks.push(ContentBlock::Text(TextBlock {
            text: MEDIA_REMOVED_MARKER.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        }));
    }
}

fn trim_nested_message(message: &mut Message, targets: &mut RemovalTargets) {
    match message {
        Message::User(user) => trim_nested_content(&mut user.content, targets),
        Message::Assistant(assistant) => trim_nested_content(&mut assistant.content, targets),
        Message::ToolResult(result) => {
            trim_outputs(&mut result.content, targets);
            if let Some(blocks) = &mut result.extra_user_blocks {
                trim_nested_content(blocks, targets);
            }
        }
        _ => {}
    }
}

fn trim_sibling_message(message: &mut Message, targets: &mut RemovalTargets) {
    match message {
        Message::User(user) => trim_sibling_content(&mut user.content, targets, true),
        Message::Assistant(assistant) => {
            trim_sibling_content(&mut assistant.content, targets, true)
        }
        Message::ToolResult(result) => {
            if let Some(blocks) = &mut result.extra_user_blocks {
                trim_sibling_content(blocks, targets, false);
            }
        }
        _ => {}
    }
}

fn user_carrier(message: &Message) -> bool {
    matches!(message, Message::User(_) | Message::ToolResult(_))
}

/// Validate every final image before aggregate shedding, then remove the oldest
/// media until both exceeded budgets have their requested headroom. A compliant
/// request borrows its original slice; every changed request owns a projection.
pub fn project_native_media(
    messages: &[Message],
    budget: MediaBudget,
    max_image_base64_bytes: usize,
) -> Result<Cow<'_, [Message]>, Box<InputValidationError>> {
    let mut image_index = 0;
    let mut violations = Vec::new();
    visit_media(messages, |is_image, base64, path| {
        if is_image {
            image_index += 1;
            let bytes = base64.len_utf16();
            if bytes > max_image_base64_bytes {
                violations.push(format!(
                    "image {image_index} at {path} has {bytes} base64 bytes"
                ));
            }
        }
    });
    if !violations.is_empty() {
        let mut error = InputValidationError::new(
            InputValidationErrorCode::RequestTooLarge,
            format!(
                "Final image payload exceeds the {max_image_base64_bytes} base64-byte limit; {}",
                violations.join("; ")
            ),
        );
        error.detail.feature = Some("image_payload".into());
        error.detail.limit_bytes = Some(max_image_base64_bytes as u64);
        return Err(Box::new(error));
    }
    let usage = native_media_usage(messages);
    let block_excess = usage.blocks.saturating_sub(budget.max_blocks);
    let byte_excess = if budget.max_base64_bytes > 0 {
        usage.base64_bytes.saturating_sub(budget.max_base64_bytes)
    } else {
        0
    };
    if block_excess == 0 && byte_excess == 0 {
        return Ok(Cow::Borrowed(messages));
    }
    let mut targets = RemovalTargets {
        blocks: if block_excess > 0 {
            block_excess.saturating_add(budget.count_headroom)
        } else {
            0
        },
        base64_bytes: if byte_excess > 0 {
            byte_excess.saturating_add(budget.base64_headroom)
        } else {
            0
        },
    };
    let mut projected = messages.to_vec();
    let mut start = 0;
    while start < projected.len() && !targets.complete() {
        let mut end = start + 1;
        if user_carrier(&projected[start]) {
            // Consecutive user input and tool results become one normalized
            // user carrier. Its nested output precedes every sibling attachment.
            while end < projected.len() && user_carrier(&projected[end]) {
                end += 1;
            }
        }
        for message in &mut projected[start..end] {
            trim_nested_message(message, &mut targets);
        }
        for message in &mut projected[start..end] {
            trim_sibling_message(message, &mut targets);
        }
        start = end;
    }
    Ok(Cow::Owned(projected))
}
