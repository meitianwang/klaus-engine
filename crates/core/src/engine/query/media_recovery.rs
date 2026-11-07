//! Request-local recovery for media that a service cannot decode.

use crate::provider::classify::{DOCUMENT_REJECTIONS, IMAGE_REJECTIONS};
use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock};
use crate::schemas::message::Message;
use crate::types::errors::{ProviderError, ProviderErrorKind, ProviderMediaOrigin};
use crate::types::provider::ModelRequest;
use regex::Regex;
use std::collections::HashSet;
use std::sync::LazyLock;

static PATH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"messages(?:\[|\.)(\d+)\]?\.content(?:\[|\.)(\d+)\]?(?:(?:\.tool_result)?\.content(?:\[|\.)(\d+)\]?)?(?:\.(image|document|pdf))?",
    )
    .unwrap()
});

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Image,
    Document,
}

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Document => "document",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Location {
    Content {
        message: usize,
        extra: bool,
        block: usize,
        output: Option<usize>,
    },
    Result {
        message: usize,
        output: usize,
    },
}

#[derive(Default)]
struct WireBlock {
    locations: Vec<(Kind, Location, Option<usize>)>,
    nested: bool,
    thinking: bool,
}
#[derive(Default)]
struct Carrier {
    assistant: bool,
    result: bool,
    blocks: Vec<WireBlock>,
}

#[derive(Default)]
pub(super) struct MediaRecovery {
    untargeted: usize,
    reasons: HashSet<String>,
}

impl MediaRecovery {
    pub(super) fn heal(
        &mut self,
        request: &mut ModelRequest,
        error: &ProviderError,
        _messages_wire: bool,
    ) -> Option<String> {
        if error.status != Some(400)
            || matches!(
                error.kind,
                ProviderErrorKind::PromptTooLong | ProviderErrorKind::RequestTooLarge
            )
        {
            return None;
        }
        let message = error.message.try_to_utf8().ok()?;
        let path = PATH.captures(&message);
        let lower = message.to_ascii_lowercase();
        let kind = path
            .as_ref()
            .and_then(|path| path.get(4))
            .map(|kind| {
                if kind.as_str() == "image" {
                    Kind::Image
                } else {
                    Kind::Document
                }
            })
            .or_else(|| {
                if IMAGE_REJECTIONS
                    .iter()
                    .any(|pattern| lower.contains(pattern))
                {
                    Some(Kind::Image)
                } else if DOCUMENT_REJECTIONS
                    .iter()
                    .any(|pattern| lower.contains(pattern))
                {
                    Some(Kind::Document)
                } else {
                    None
                }
            })?;
        let carriers = origin_carriers(request, error.media_origins.as_deref()?);
        let detail = message
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(200)
            .collect::<String>();
        let marker = TextBlock {
            text: format!("[{} removed: the service rejected this media ({detail}). Do not retry reading or resending the same file; use a local non-media inspection method if its contents are needed.]", kind.label()).into(),
            citations: None, harness_envelope: None, message_id: None, phase: None,
        };
        {
            if let Some((message_index, block_index)) = path.and_then(|path| {
                let message_index = path[1].parse::<usize>().ok()?;
                let block_index = path[2].parse::<usize>().ok()?;
                Some((message_index, block_index))
            }) {
                if let Some(carrier) = carriers
                    .get(message_index)
                    .filter(|carrier| !carrier.assistant)
                {
                    if let Some(block) = carrier.blocks.get(block_index) {
                        let targets = block
                            .locations
                            .iter()
                            .filter(|(candidate, _, _)| *candidate == kind)
                            .map(|(_, location, _)| *location)
                            .collect::<Vec<_>>();
                        let reason =
                            format!("media-strip:{}:{message_index}.{block_index}", kind.label());
                        if !targets.is_empty() && !self.reasons.contains(&reason) {
                            let mut next = request.messages.clone();
                            for target in targets {
                                replace(&mut next, target, &marker);
                            }
                            if next != request.messages {
                                self.reasons.insert(reason.clone());
                                request.messages = next;
                                return Some(reason);
                            }
                        }
                    }
                }
            }
        }
        if self.untargeted >= 3 {
            return None;
        }
        for (index, carrier) in carriers
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, carrier)| !carrier.assistant)
        {
            let targets = carrier
                .blocks
                .iter()
                .flat_map(|block| block.locations.iter())
                .filter(|(candidate, _, _)| *candidate == kind)
                .map(|(_, location, _)| *location)
                .collect::<Vec<_>>();
            if targets.is_empty() {
                continue;
            }
            let reason = format!("media-strip-latest:{}:{index}", kind.label());
            if self.reasons.contains(&reason) {
                continue;
            }
            let mut next = request.messages.clone();
            for target in targets {
                replace(&mut next, target, &marker);
            }
            if next != request.messages {
                request.messages = next;
                self.untargeted += 1;
                self.reasons.insert(reason.clone());
                return Some(reason);
            }
        }
        None
    }
}

fn content_blocks(
    blocks: &[ContentBlock],
    message: usize,
    extra: bool,
    stale: bool,
) -> Vec<WireBlock> {
    let mut out = Vec::new();
    for (block, value) in blocks.iter().enumerate() {
        let location = |output| Location::Content {
            message,
            extra,
            block,
            output,
        };
        let mut wire = WireBlock::default();
        match value {
            ContentBlock::Text(text) if text.text.is_empty() => continue,
            ContentBlock::Image(_) => wire.locations.push((Kind::Image, location(None), None)),
            ContentBlock::Document(_) => {
                wire.locations.push((Kind::Document, location(None), None))
            }
            ContentBlock::ToolResult(result) => {
                wire.nested = true;
                for (output, value) in result.content.iter().enumerate() {
                    if matches!(value, OutputBlock::Image(_)) {
                        wire.locations
                            .push((Kind::Image, location(Some(output)), Some(output)));
                    }
                }
            }
            ContentBlock::Thinking(thinking) => {
                if stale
                    || thinking
                        .signature
                        .as_ref()
                        .is_none_or(|signature| signature.is_empty())
                {
                    continue;
                }
                wire.thinking = true;
            }
            ContentBlock::RedactedThinking(_) => {
                if stale {
                    continue;
                }
                wire.thinking = true;
            }
            ContentBlock::WebSearchCall(search) => {
                if search.error.is_some()
                    || search.results.is_empty()
                    || search
                        .results
                        .iter()
                        .any(|result| result.encrypted_token.is_none())
                {
                    continue;
                }
                out.push(WireBlock::default());
            }
            _ => {}
        }
        out.push(wire);
    }
    if stale
        && out.is_empty()
        && blocks.iter().all(|block| {
            matches!(
                block,
                ContentBlock::Thinking(_) | ContentBlock::RedactedThinking(_)
            )
        })
    {
        out.push(WireBlock::default());
    }
    out
}

fn normalized_model(model: &RuntimeString) -> RuntimeString {
    let units = model.as_utf16();
    if units.last() == Some(&u16::from(b']')) {
        if let Some(start) = units.iter().rposition(|unit| *unit == u16::from(b'[')) {
            return model.slice_utf16(..start);
        }
    }
    model.clone()
}

fn push_plain(carriers: &mut Vec<Carrier>, assistant: bool, blocks: Vec<WireBlock>) {
    if blocks.is_empty() {
        return;
    }
    if let Some(last) = carriers.last_mut().filter(|carrier| {
        carrier.assistant == assistant
            && (assistant || (!carrier.result && !carrier.blocks.iter().any(|block| block.nested)))
    }) {
        last.blocks.extend(blocks);
    } else {
        carriers.push(Carrier {
            assistant,
            result: false,
            blocks,
        });
    }
}

fn carriers(request: &ModelRequest) -> Vec<Carrier> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < request.messages.len() {
        match &request.messages[i] {
            Message::User(user) => push_plain(
                &mut out,
                false,
                content_blocks(&user.content, i, false, false),
            ),
            Message::Assistant(assistant) => push_plain(
                &mut out,
                true,
                content_blocks(
                    &assistant.content,
                    i,
                    false,
                    normalized_model(&assistant.model) != normalized_model(&request.model),
                ),
            ),
            Message::CompactBoundary(_) => push_plain(&mut out, false, vec![WireBlock::default()]),
            Message::System(_) => {}
            Message::ToolResult(_) => {
                let mut blocks = Vec::new();
                let mut extras = Vec::new();
                while let Some(Message::ToolResult(result)) = request.messages.get(i) {
                    let mut block = WireBlock {
                        nested: true,
                        ..WireBlock::default()
                    };
                    for (output, value) in result.content.iter().enumerate() {
                        if matches!(value, OutputBlock::Image(_)) {
                            block.locations.push((
                                Kind::Image,
                                Location::Result { message: i, output },
                                Some(output),
                            ));
                        }
                    }
                    blocks.push(block);
                    if let Some(extra) = &result.extra_user_blocks {
                        extras.extend(content_blocks(extra, i, true, false));
                    }
                    i += 1;
                }
                blocks.extend(extras);
                out.push(Carrier {
                    assistant: false,
                    result: true,
                    blocks,
                });
                continue;
            }
        }
        i += 1;
    }
    if let Some(last) = out.last_mut().filter(|carrier| carrier.assistant) {
        while last.blocks.last().is_some_and(|block| block.thinking) {
            last.blocks.pop();
        }
        if last.blocks.is_empty() {
            last.blocks.push(WireBlock::default());
        }
    }
    out
}

fn origin_carriers(request: &ModelRequest, origins: &[ProviderMediaOrigin]) -> Vec<Carrier> {
    let baseline = carriers(request);
    let message_bound = request
        .messages
        .len()
        .saturating_add(request.system.len())
        .saturating_add(1);
    let block_bound = baseline
        .iter()
        .map(|carrier| carrier.blocks.len())
        .sum::<usize>()
        .saturating_add(1);
    let mut out: Vec<Carrier> = Vec::new();
    for origin in origins {
        if origin.wire_message_index >= message_bound || origin.wire_content_index >= block_bound {
            continue;
        }
        let location = match (origin.content_index, origin.output_index) {
            (Some(block), output) => Location::Content {
                message: origin.message_index,
                extra: origin.extra_user_blocks,
                block,
                output,
            },
            (None, Some(output)) if !origin.extra_user_blocks => Location::Result {
                message: origin.message_index,
                output,
            },
            _ => continue,
        };
        let Some(kind) = kind_at(&request.messages, location) else {
            continue;
        };
        out.resize_with(
            out.len().max(origin.wire_message_index + 1),
            Carrier::default,
        );
        let carrier = &mut out[origin.wire_message_index];
        carrier.assistant |= matches!(
            request.messages.get(origin.message_index),
            Some(Message::Assistant(_))
        );
        carrier.blocks.resize_with(
            carrier.blocks.len().max(origin.wire_content_index + 1),
            WireBlock::default,
        );
        carrier.blocks[origin.wire_content_index].locations.push((
            kind,
            location,
            origin.wire_output_index,
        ));
    }
    out
}

fn kind_at(messages: &[Message], target: Location) -> Option<Kind> {
    match target {
        Location::Result { message, output } => {
            let Message::ToolResult(result) = messages.get(message)? else {
                return None;
            };
            matches!(result.content.get(output)?, OutputBlock::Image(_)).then_some(Kind::Image)
        }
        Location::Content {
            message,
            extra,
            block,
            output,
        } => {
            let blocks = match messages.get(message)? {
                Message::User(user) if !extra => &user.content,
                Message::Assistant(assistant) if !extra => &assistant.content,
                Message::ToolResult(result) if extra => result.extra_user_blocks.as_ref()?,
                _ => return None,
            };
            let value = blocks.get(block)?;
            if let Some(output) = output {
                let ContentBlock::ToolResult(result) = value else {
                    return None;
                };
                return matches!(result.content.get(output)?, OutputBlock::Image(_))
                    .then_some(Kind::Image);
            }
            match value {
                ContentBlock::Image(_) => Some(Kind::Image),
                ContentBlock::Document(_) => Some(Kind::Document),
                _ => None,
            }
        }
    }
}

fn replace(messages: &mut [Message], target: Location, marker: &TextBlock) {
    match target {
        Location::Result { message, output } => {
            if let Some(Message::ToolResult(result)) = messages.get_mut(message) {
                if let Some(block) = result.content.get_mut(output) {
                    *block = OutputBlock::Text(marker.clone());
                }
            }
        }
        Location::Content {
            message,
            extra,
            block,
            output,
        } => {
            let blocks = match messages.get_mut(message) {
                Some(Message::User(user)) if !extra => Some(&mut user.content),
                Some(Message::Assistant(assistant)) if !extra => Some(&mut assistant.content),
                Some(Message::ToolResult(result)) if extra => result.extra_user_blocks.as_mut(),
                _ => None,
            };
            if let Some(value) = blocks.and_then(|blocks| blocks.get_mut(block)) {
                if let Some(output) = output {
                    if let ContentBlock::ToolResult(result) = value {
                        if let Some(block) = result.content.get_mut(output) {
                            *block = OutputBlock::Text(marker.clone());
                        }
                    }
                } else {
                    *value = ContentBlock::Text(marker.clone());
                }
            }
        }
    }
}
