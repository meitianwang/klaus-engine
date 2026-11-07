//! Complete request accounting and read-only review admission.

use super::ApprovalQueryBudgetExceeded;
use crate::engine::query::{ProviderCallAdmission, ProviderCallAdmissionReceipt};
use crate::runtime_values::{runtime_codec, runtime_json, RuntimeJson, RuntimeString};
use crate::schemas::content::{ContentBlock, OutputBlock};
use crate::schemas::message::Message;
use crate::sync_ext::MutexExt;
use crate::types::provider::{ModelProvider, ModelRequest, ProviderCapabilities, SystemPromptPart};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

// Images are accounted independently of their base64 transport payload.
const IMAGE_TOKEN_RESERVATION: usize = 10_000;

pub(crate) fn review_message_id(message: &Message) -> &RuntimeString {
    match message {
        Message::User(message) => &message.uuid,
        Message::Assistant(message) => &message.uuid,
        Message::ToolResult(message) => &message.uuid,
        Message::System(message) => &message.uuid,
        Message::CompactBoundary(message) => &message.uuid,
    }
}

fn strip_content_image_bytes(content: &mut [ContentBlock]) -> usize {
    content
        .iter_mut()
        .filter_map(|block| match block {
            ContentBlock::Image(image) => {
                image.base64 = RuntimeString::default();
                Some(1usize)
            }
            _ => None,
        })
        .fold(0usize, usize::saturating_add)
}

/// Accounts every declared input field, tool grammar and final output schema.
/// Binary image payloads reserve model tokens instead of being counted as text.
pub(crate) fn estimate_approval_request_tokens(
    request: &ModelRequest,
) -> Result<usize, RuntimeString> {
    let mut framed = request.clone();
    let mut images = 0usize;
    for message in &mut framed.messages {
        images = images.saturating_add(match message {
            Message::User(message) => strip_content_image_bytes(&mut message.content),
            Message::Assistant(message) => strip_content_image_bytes(&mut message.content),
            Message::ToolResult(message) => {
                let mut count = 0usize;
                for block in &mut message.content {
                    if let OutputBlock::Image(image) = block {
                        image.base64 = RuntimeString::default();
                        count = count.saturating_add(1);
                    }
                }
                if let Some(content) = &mut message.extra_user_blocks {
                    count = count.saturating_add(strip_content_image_bytes(content));
                }
                count
            }
            Message::System(_) | Message::CompactBoundary(_) => 0,
        });
    }
    let serialized = runtime_codec::encode_text(&framed).map_err(|error| {
        RuntimeString::from(format!(
            "Cannot account for approval review request: {error}"
        ))
    })?;
    Ok(serialized
        .to_node_utf8()
        .len()
        .div_ceil(4)
        .saturating_add(images.saturating_mul(IMAGE_TOKEN_RESERVATION)))
}

/// Counts one actual wire input item, retaining its role and content framing.
pub(crate) fn estimate_approval_wire_input_tokens(item: &RuntimeJson) -> usize {
    let mut item = item.clone();
    let mut adjustment = 0usize;
    if let Some(object) = item.as_object_mut() {
        if object
            .get("type")
            .and_then(RuntimeJson::as_str)
            .is_some_and(|kind| kind == "compaction" || kind == "context_compaction")
        {
            if let Some(ciphertext) = object.get_mut("encrypted_content") {
                if let Some(text) = ciphertext.as_str() {
                    adjustment = text.to_node_utf8().len().div_ceil(3).div_ceil(4);
                    *ciphertext = RuntimeJson::from("");
                }
            }
        }
        adjustment = adjustment.saturating_add(strip_wire_message_images(object));
        if let Some(messages) = object
            .get_mut("messages")
            .and_then(RuntimeJson::as_array_mut)
        {
            for message in messages {
                if let Some(message) = message.as_object_mut() {
                    adjustment = adjustment.saturating_add(strip_wire_message_images(message));
                }
            }
        }
    }
    runtime_json::stringify(&item)
        .to_node_utf8()
        .len()
        .div_ceil(4)
        .saturating_add(adjustment)
}

fn strip_wire_message_images(object: &mut crate::runtime_values::RuntimeObject) -> usize {
    let mut adjustment = 0usize;
    for field in ["content", "output"] {
        if let Some(content) = object.get_mut(field).and_then(RuntimeJson::as_array_mut) {
            adjustment = adjustment.saturating_add(strip_wire_content_images(content));
        }
    }
    adjustment
}

fn strip_wire_content_images(content: &mut [RuntimeJson]) -> usize {
    let mut adjustment = 0usize;
    for block in content {
        let Some(block) = block.as_object_mut() else {
            continue;
        };
        match block.get("type").and_then(RuntimeJson::as_str) {
            Some(kind) if kind == "input_image" => {
                if let Some(url) = block.get_mut("image_url") {
                    if url.as_str().is_some_and(|url| url.starts_with("data:")) {
                        *url = RuntimeJson::from("");
                    }
                }
                adjustment = adjustment.saturating_add(IMAGE_TOKEN_RESERVATION);
            }
            Some(kind) if kind == "image_url" => {
                if let Some(image) = block
                    .get_mut("image_url")
                    .and_then(RuntimeJson::as_object_mut)
                {
                    if let Some(url) = image.get_mut("url") {
                        if url.as_str().is_some_and(|url| url.starts_with("data:")) {
                            *url = RuntimeJson::from("");
                        }
                    }
                }
                adjustment = adjustment.saturating_add(IMAGE_TOKEN_RESERVATION);
            }
            Some(kind) if kind == "image" => {
                if let Some(source) = block.get_mut("source").and_then(RuntimeJson::as_object_mut) {
                    if source
                        .get("type")
                        .and_then(RuntimeJson::as_str)
                        .is_some_and(|kind| kind == "base64")
                    {
                        if let Some(data) = source
                            .get_mut("data")
                            .filter(|data| data.as_str().is_some())
                        {
                            *data = RuntimeJson::from("");
                            adjustment = adjustment.saturating_add(IMAGE_TOKEN_RESERVATION);
                        }
                    }
                }
            }
            Some(kind) if kind == "tool_result" => {
                if let Some(content) = block.get_mut("content").and_then(RuntimeJson::as_array_mut)
                {
                    adjustment = adjustment.saturating_add(strip_wire_content_images(content));
                }
            }
            _ => {}
        }
    }
    adjustment
}

/// Apply the selected target's media and tool declarations to this request copy.
fn bind_request_capabilities(request: &mut ModelRequest, capabilities: &ProviderCapabilities) {
    request.bind_media_capabilities(capabilities);
    request.max_output_tokens = request
        .max_output_tokens
        .min(capabilities.max_output_tokens);
    if !capabilities.tools {
        request.tools.clear();
        request.tool_choice = None;
    }
    if !capabilities.vision {
        crate::engine::query::request::omit_request_images(&mut request.messages);
    }
    crate::engine::query::tool_dispatch::gate_request_images(request);
}

pub(crate) struct ApprovalQueryRestoration {
    pub messages: Vec<Message>,
    pub system: Vec<SystemPromptPart>,
}

pub(crate) enum ApprovalQueryRestoreError {
    BudgetExceeded {
        estimated_tokens: usize,
        max_input_tokens: usize,
    },
    Invalid(RuntimeString),
}

pub(crate) type ApprovalQueryRestore = Arc<
    dyn Fn(&ModelRequest) -> Result<ApprovalQueryRestoration, ApprovalQueryRestoreError>
        + Send
        + Sync,
>;

pub(crate) struct ApprovalQueryAdmission {
    maximum: usize,
    originals: Vec<Message>,
    history: Arc<Mutex<Vec<Message>>>,
    provider: Arc<dyn ModelProvider>,
    rejected: Mutex<Option<ApprovalQueryBudgetExceeded>>,
    restore_retained: Option<ApprovalQueryRestore>,
    admitted: Mutex<Option<(Vec<Message>, Vec<SystemPromptPart>)>>,
}

impl ApprovalQueryAdmission {
    pub(crate) fn new(
        maximum: usize,
        protected_ids: &HashSet<RuntimeString>,
        history: Arc<Mutex<Vec<Message>>>,
        provider: Arc<dyn ModelProvider>,
        restore_retained: Option<ApprovalQueryRestore>,
    ) -> Arc<Self> {
        let originals = history
            .lock_recover()
            .iter()
            .filter(|message| protected_ids.contains(review_message_id(message)))
            .cloned()
            .collect();
        Arc::new(Self {
            maximum,
            originals,
            history,
            provider,
            rejected: Mutex::new(None),
            restore_retained,
            admitted: Mutex::new(None),
        })
    }
    pub(crate) fn rejected(&self) -> Option<ApprovalQueryBudgetExceeded> {
        *self.rejected.lock_recover()
    }
}

impl ProviderCallAdmission for ApprovalQueryAdmission {
    fn prepare_request(&self, request: &mut ModelRequest) -> Result<(), RuntimeString> {
        let present: HashSet<_> = request
            .messages
            .iter()
            .map(review_message_id)
            .cloned()
            .collect();
        let mut restored: Vec<_> = self
            .originals
            .iter()
            .filter(|message| !present.contains(review_message_id(message)))
            .cloned()
            .collect();
        request.messages.extend(restored.iter().cloned());
        let mut restored_system = Vec::new();
        if let Some(restore) = &self.restore_retained {
            let missing = restore(request).map_err(|error| match error {
                ApprovalQueryRestoreError::BudgetExceeded {
                    estimated_tokens,
                    max_input_tokens,
                } => {
                    *self.rejected.lock_recover() = Some(ApprovalQueryBudgetExceeded {
                        estimated_tokens,
                        max_input_tokens,
                    });
                    RuntimeString::from(
                        "Retained approval review context exceeds the available input budget.",
                    )
                }
                ApprovalQueryRestoreError::Invalid(error) => error,
            })?;
            request.messages.extend(missing.messages.iter().cloned());
            restored.extend(missing.messages);
            restored_system = missing.system;
            request.system.extend(restored_system.iter().cloned());
        }
        bind_request_capabilities(request, &self.provider.capabilities(&request.model));
        let estimated_tokens = self.provider.approval_request_tokens(request)?;
        if estimated_tokens > self.maximum {
            *self.rejected.lock_recover() = Some(ApprovalQueryBudgetExceeded {
                estimated_tokens,
                max_input_tokens: self.maximum,
            });
            return Err(
                "The complete approval review request exceeds the available input budget.".into(),
            );
        }
        if !restored.is_empty() || !restored_system.is_empty() {
            *self.admitted.lock_recover() = Some((restored.clone(), restored_system));
        }
        let mut history = self.history.lock_recover();
        let recorded: HashSet<_> = history.iter().map(review_message_id).cloned().collect();
        history.extend(
            restored
                .into_iter()
                .filter(|message| !recorded.contains(review_message_id(message))),
        );
        *self.rejected.lock_recover() = None;
        Ok(())
    }
    fn take_admitted_request_context(&self) -> Option<(Vec<Message>, Vec<SystemPromptPart>)> {
        self.admitted.lock_recover().take()
    }
    fn input_budget_exceeded(&self) -> bool {
        self.rejected().is_some()
    }
    fn clear_input_budget_failure(&self) {
        *self.rejected.lock_recover() = None;
    }
    fn before_call(&self) -> Result<(), RuntimeString> {
        Ok(())
    }
    fn call_returned(&self) -> ProviderCallAdmissionReceipt {
        Box::pin(async { Ok(()) })
    }
    fn call_panicked(&self) -> ProviderCallAdmissionReceipt {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::{ImageBlock, TextBlock};
    use crate::schemas::message::UserMessage;

    fn request() -> ModelRequest {
        runtime_codec::decode_text(&RuntimeString::from(r#"{"model":"review-model","system":[],"messages":[],"tools":[],"maxOutputTokens":1024}"#)).unwrap()
    }
    fn authority() -> Message {
        Message::User(UserMessage {
            uuid: "authority-1".into(),
            question_answer: None,
            is_meta: Some(true),
            content: vec![ContentBlock::Text(TextBlock {
                text: "Only inspect the requested local operation.".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
        })
    }
    #[test]
    fn image_budget_reserves_model_tokens_instead_of_base64_bytes() {
        let mut request = request();
        let empty = estimate_approval_request_tokens(&request).unwrap();
        request.messages = vec![Message::User(UserMessage {
            uuid: "image-1".into(),
            question_answer: None,
            is_meta: None,
            content: vec![ContentBlock::Image(ImageBlock {
                mime_type: "image/png".into(),
                base64: "eA==".into(),
                detail: None,
                source_path: None,
            })],
        })];
        let small = estimate_approval_request_tokens(&request).unwrap();
        if let Message::User(user) = &mut request.messages[0] {
            if let ContentBlock::Image(image) = &mut user.content[0] {
                image.base64 = "a".repeat(64_000).into();
            }
        }
        assert_eq!(small, estimate_approval_request_tokens(&request).unwrap());
        assert!(small >= empty.saturating_add(IMAGE_TOKEN_RESERVATION));
    }
    #[test]
    fn wire_image_budget_counts_nested_media_without_interpreting_tool_payloads() {
        let image = |data: &str| {
            let mut image = runtime_json::parse(&RuntimeString::from(
                r#"{"type":"image","source":{"type":"base64","media_type":"image/png","data":""}}"#,
            ))
            .unwrap();
            *image.get_mut("source").unwrap().get_mut("data").unwrap() = RuntimeJson::from(data);
            image
        };
        let small = runtime_json::parse(&RuntimeString::from(r#"{"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"eA=="}},{"type":"tool_result","tool_use_id":"inspection","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"eA=="}}]}]}]}"#)).unwrap();
        let mut large = small.clone();
        let content = large.get_mut("messages").unwrap().as_array_mut().unwrap()[0]
            .get_mut("content")
            .unwrap()
            .as_array_mut()
            .unwrap();
        content[0] = image(&"a".repeat(64_000));
        content[1]
            .get_mut("content")
            .unwrap()
            .as_array_mut()
            .unwrap()[0] = image(&"a".repeat(64_000));
        assert_eq!(
            estimate_approval_wire_input_tokens(&small),
            estimate_approval_wire_input_tokens(&large)
        );
        let serialized = runtime_json::stringify(&small)
            .to_node_utf8()
            .len()
            .div_ceil(4);
        assert!(
            estimate_approval_wire_input_tokens(&small)
                >= serialized.saturating_add(2 * IMAGE_TOKEN_RESERVATION - 10)
        );
        let opaque = runtime_json::parse(&RuntimeString::from(r#"{"tools":[{"input_schema":{"type":"image","source":{"type":"base64","data":"opaque-schema"}}}],"messages":[{"role":"assistant","content":[{"type":"tool_use","input":{"type":"image","source":{"type":"base64","data":"opaque-input"}}}]}]}"#)).unwrap();
        assert_eq!(
            estimate_approval_wire_input_tokens(&opaque),
            runtime_json::stringify(&opaque)
                .to_node_utf8()
                .len()
                .div_ceil(4)
        );
    }

    #[test]
    fn retained_budget_failure_stays_typed_and_never_records_unadmitted_authority() {
        let history = Arc::new(Mutex::new(Vec::new()));
        let guard = ApprovalQueryAdmission::new(
            1024,
            &HashSet::new(),
            history.clone(),
            Arc::new(crate::testing::fake_provider()),
            Some(Arc::new(|_| {
                Err(ApprovalQueryRestoreError::BudgetExceeded {
                    estimated_tokens: 2048,
                    max_input_tokens: 1024,
                })
            })),
        );
        assert!(guard.prepare_request(&mut request()).is_err());
        assert_eq!(
            guard.rejected(),
            Some(ApprovalQueryBudgetExceeded {
                estimated_tokens: 2048,
                max_input_tokens: 1024
            })
        );
        assert!(history.lock_recover().is_empty());
        assert!(guard.take_admitted_request_context().is_none());
    }

    #[test]
    fn missing_protected_authority_is_restored_before_a_continuation_is_admitted() {
        let original = authority();
        let history = Arc::new(Mutex::new(vec![original.clone()]));
        let ids = HashSet::from([RuntimeString::from("authority-1")]);
        let guard = ApprovalQueryAdmission::new(
            128_000,
            &ids,
            history.clone(),
            Arc::new(crate::testing::fake_provider()),
            None,
        );
        history.lock_recover().clear();
        let mut next = request();
        guard.prepare_request(&mut next).unwrap();
        assert_eq!(next.messages, vec![original.clone()]);
        assert_eq!(*history.lock_recover(), vec![original]);
        guard.prepare_request(&mut next).unwrap();
        assert_eq!(next.messages.len(), 1);
        assert_eq!(history.lock_recover().len(), 1);
    }
}
