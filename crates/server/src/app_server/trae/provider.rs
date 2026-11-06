//! Model- and account-scoped opaque replay on top of the normal chat provider.

use futures::StreamExt;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::{json, Value};
use std::sync::{Arc, OnceLock};

pub(super) fn marker() -> &'static str {
    static MARKER: OnceLock<String> = OnceLock::new();
    MARKER.get_or_init(|| format!("\u{1e}klaus-trae-{}:", uuid::Uuid::new_v4().simple()))
}

pub(super) fn envelope(model: &str, account: &str, extra: Value) -> Value {
    json!({"kind":"klaus.trae.extra-info.v1","source":"trae","model":model,"account":account,"extraInfo":extra})
}
fn own_envelope(value: &Value, model: &str, account: &str) -> bool {
    value["kind"] == "klaus.trae.extra-info.v1"
        && value["source"] == "trae"
        && value["model"].as_str() == Some(model)
        && value["account"].as_str() == Some(account)
        && value["extraInfo"].is_object()
}

#[cfg(test)]
pub(super) fn marker_extra(value: &str, model: &str, account: &str) -> Option<Value> {
    marker_context(value, model, account)?
        .get("extraInfo")
        .filter(|value| value.is_object())
        .cloned()
}
pub(super) fn marker_context(value: &str, model: &str, account: &str) -> Option<Value> {
    let (_, encoded) = value.rsplit_once(marker())?;
    if encoded.len() > 2 * 1024 * 1024 {
        return None;
    }
    let envelope: Value = serde_json::from_str(encoded).ok()?;
    if !own_envelope(&envelope, model, account) {
        return None;
    }
    Some(envelope)
}

pub(super) fn usage_envelope(model: &str, account: &str, usage: Value) -> Value {
    json!({"kind":"klaus.trae.usage.v1","source":"trae","model":model,"account":account,"usage":usage})
}

fn native_usage(value: &Value) -> Usage {
    let read = value["cache_read_input_tokens"].as_u64().unwrap_or(0);
    let creation = value["cache_creation_input_tokens"].as_u64().unwrap_or(0);
    Usage {
        input_tokens: value["prompt_tokens"]
            .as_u64()
            .unwrap_or(0)
            .saturating_sub(read)
            .saturating_sub(creation),
        output_tokens: value["completion_tokens"].as_u64().unwrap_or(0),
        cache_read_input_tokens: read,
        cache_creation_input_tokens: creation,
        total_tokens: value["total_tokens"].as_u64(),
    }
}

fn prepare_replay(request: &mut ModelRequest, account: &str) {
    let Ok(model) = request.model.try_to_utf8() else {
        return;
    };
    for message in &mut request.messages {
        let Message::Assistant(assistant) = message else {
            continue;
        };
        if assistant.model != request.model {
            continue;
        }
        for block in &mut assistant.content {
            let ContentBlock::Thinking(thinking) = block else {
                continue;
            };
            let Some(opaque) = thinking
                .encrypted_content
                .as_ref()
                .and_then(|value| value.try_to_utf8().ok())
                .filter(|value| value.len() <= 2 * 1024 * 1024)
            else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<Value>(&opaque) else {
                continue;
            };
            if !own_envelope(&value, &model, account) {
                continue;
            }
            thinking.text = RuntimeString::from(format!("{}{opaque}", marker()));
            thinking.signature = None;
            thinking.encrypted_content = None;
        }
    }
}

pub(super) fn decorate(inner: Arc<dyn ModelProvider>) -> Arc<dyn ModelProvider> {
    Arc::new(TraeProvider {
        inner,
        account: super::auth::account_scope().unwrap_or_default(),
    })
}
struct TraeProvider {
    inner: Arc<dyn ModelProvider>,
    account: String,
}

#[async_trait::async_trait]
impl ModelProvider for TraeProvider {
    fn info(&self) -> &ProviderInfo {
        self.inner.info()
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.inner.capabilities(model)
    }
    fn model_metadata(&self, model: &RuntimeString) -> ModelCapabilities {
        self.inner.model_metadata(model)
    }
    fn offline_model_catalog(&self) -> Vec<(RuntimeString, ModelCapabilities)> {
        self.inner.offline_model_catalog()
    }
    fn has_model_capabilities(&self, model: &RuntimeString) -> bool {
        self.inner.has_model_capabilities(model)
    }
    fn estimate_tokens(&self, request: &ModelRequest) -> Option<u64> {
        self.inner.estimate_tokens(request)
    }
    fn stream(&self, mut request: ModelRequest, signal: AbortSignal) -> ModelStream {
        prepare_replay(&mut request, &self.account);
        let model = request.model.try_to_utf8().unwrap_or_default();
        let account = self.account.clone();
        let mut stream = self.inner.stream(request, signal);
        Box::pin(async_stream::stream! {
            let mut thinking=RuntimeString::default();
            let mut native_usage_snapshot=None;
            while let Some(event)=stream.next().await{
                if let ModelStreamEvent::ThinkingDelta{text}=&event{
                    if let Ok(encoded)=text.try_to_utf8(){
                        if let Some(encoded)=encoded.strip_prefix(marker()){
                            if let Ok(value)=serde_json::from_str::<Value>(encoded){
                                if own_envelope(&value,&model,&account){
                                    yield ModelStreamEvent::ThinkingBlock{text:thinking.clone(),is_summary:false,signature:None,encrypted_content:Some(RuntimeString::from(encoded))};
                                    continue;
                                }
                                if value["kind"]=="klaus.trae.error.v1"&&value["source"]=="trae"
                                    &&value["model"].as_str()==Some(&model)&&value["account"].as_str()==Some(&account){
                                    if let Ok(error)=serde_json::from_value::<super::errors::ServiceError>(value["error"].clone()){
                                        yield ModelStreamEvent::Error{error:error.provider_error()};
                                        return;
                                    }
                                }
                                if value["kind"]=="klaus.trae.usage.v1"&&value["source"]=="trae"
                                    &&value["model"].as_str()==Some(&model)&&value["account"].as_str()==Some(&account)&&value["usage"].is_object(){
                                    let usage=native_usage(&value["usage"]);
                                    native_usage_snapshot=Some(usage.clone());
                                    yield ModelStreamEvent::UsageSnapshot{usage};
                                    continue;
                                }
                            }
                            yield ModelStreamEvent::Error{error:ProviderError{
                                kind:ProviderErrorKind::InvalidRequest,message:"trae_replay_metadata_invalid".into(),status:None,code:Some("trae_replay_metadata_invalid".into()),
                                error_type:None,request_id:None,media_origins:None,media_repair_complete: false, credential_recovery: None,retry_after_ms:None,
                            }};
                            return;
                        }
                    }
                    thinking=thinking.concat(text);
                }
                if let ModelStreamEvent::MessageStop{stop_reason,..}=&event{
                    if let Some(usage)=native_usage_snapshot.take(){
                        yield ModelStreamEvent::MessageStop{stop_reason:*stop_reason,usage};
                        continue;
                    }
                    yield event;
                    continue;
                }
                if matches!(&event,ModelStreamEvent::UsageSnapshot{..})&&native_usage_snapshot.is_some(){continue;}
                yield event;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klaus_agent_core::runtime_values::runtime_codec;
    use klaus_agent_core::schemas::content::ThinkingBlock;
    use klaus_agent_core::schemas::message::AssistantMessage;

    struct Scripted {
        inner: klaus_agent_core::testing::FakeProvider,
        events: Vec<ModelStreamEvent>,
        requests: std::sync::Mutex<Vec<ModelRequest>>,
    }
    impl ModelProvider for Scripted {
        fn info(&self) -> &ProviderInfo {
            self.inner.info()
        }
        fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
            self.inner.capabilities(model)
        }
        fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
            self.requests.lock().unwrap().push(request);
            Box::pin(futures::stream::iter(self.events.clone()))
        }
    }

    #[tokio::test]
    async fn opaque_metadata_and_native_usage_survive_stream_completion_and_history_restore() {
        use klaus_agent_core::types::provider::ModelStopReason;
        let context = envelope(
            "exact-model",
            "account-a",
            json!({"model":"native","version":1,"content":"opaque-signed-reasoning"}),
        );
        let usage = usage_envelope(
            "exact-model",
            "account-a",
            json!({"prompt_tokens":103,"completion_tokens":7,"cache_read_input_tokens":80,"cache_creation_input_tokens":3,"total_tokens":110}),
        );
        let inner = Arc::new(Scripted {
            inner: klaus_agent_core::testing::fake_provider(),
            requests: std::sync::Mutex::new(Vec::new()),
            events: vec![
                ModelStreamEvent::ThinkingDelta {
                    text: "Real thoughts".into(),
                },
                ModelStreamEvent::ThinkingDelta {
                    text: format!("{}{context}", marker()).into(),
                },
                ModelStreamEvent::ThinkingDelta {
                    text: format!("{}{usage}", marker()).into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: native_usage(&json!({})),
                },
            ],
        });
        let provider = TraeProvider {
            inner: inner.clone(),
            account: "account-a".into(),
        };
        let events = provider
            .stream(request("exact-model", "account-a"), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        let opaque = events
            .iter()
            .find_map(|event| {
                if let ModelStreamEvent::ThinkingBlock {
                    text,
                    encrypted_content,
                    ..
                } = event
                {
                    assert_eq!(text, "Real thoughts");
                    encrypted_content.clone()
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(opaque.try_to_utf8().unwrap(), context.to_string());
        assert!(events
            .iter()
            .filter_map(
                |event| if let ModelStreamEvent::ThinkingDelta { text } = event {
                    Some(text)
                } else {
                    None
                }
            )
            .all(|text| !text.try_to_utf8().unwrap().contains(marker())));
        let ModelStreamEvent::MessageStop { usage, .. } = events.last().unwrap() else {
            panic!("terminal")
        };
        assert_eq!(usage.input_tokens, 20);
        assert_eq!(usage.cache_read_input_tokens, 80);
        assert_eq!(usage.cache_creation_input_tokens, 3);
        assert_eq!(usage.total_tokens, Some(110));
        let mut replay = request("exact-model", "account-a");
        if let Message::Assistant(message) = &mut replay.messages[0] {
            if let ContentBlock::Thinking(thinking) = &mut message.content[0] {
                thinking.encrypted_content = Some(opaque);
            }
        }
        let _ = provider
            .stream(replay.clone(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        {
            let captured = inner.requests.lock().unwrap();
            let Message::Assistant(message) = &captured[1].messages[0] else {
                panic!("assistant")
            };
            let ContentBlock::Thinking(thinking) = &message.content[0] else {
                panic!("thinking")
            };
            assert_eq!(
                marker_context(
                    &thinking.text.try_to_utf8().unwrap(),
                    "exact-model",
                    "account-a"
                )
                .unwrap()["extraInfo"],
                context["extraInfo"]
            );
        }
        let ordinary = Scripted {
            inner: klaus_agent_core::testing::fake_provider(),
            events: vec![],
            requests: std::sync::Mutex::new(Vec::new()),
        };
        let _ = ordinary
            .stream(replay.clone(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(ordinary.requests.lock().unwrap()[0], replay);
    }

    #[tokio::test]
    async fn service_entitlement_failure_is_a_provider_error_without_a_successful_terminal() {
        let record = super::super::errors::ServiceError::from_native(
            &json!({"code":1005,"extra":"{\"plan\":3}"}),
            None,
            None,
        )
        .envelope("exact-model", "account-a");
        let inner = Arc::new(Scripted {
            inner: klaus_agent_core::testing::fake_provider(),
            requests: std::sync::Mutex::new(Vec::new()),
            events: vec![
                ModelStreamEvent::ThinkingDelta {
                    text: format!("{}{record}", marker()).into(),
                },
                ModelStreamEvent::TextDelta {
                    text: "must not be published".into(),
                },
            ],
        });
        let provider = TraeProvider {
            inner,
            account: "account-a".into(),
        };
        let events = provider
            .stream(request("exact-model", "account-a"), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(events.len(), 1);
        let ModelStreamEvent::Error { error } = &events[0] else {
            panic!("error")
        };
        assert_eq!(error.code.as_ref().unwrap(), "trae_model_access_denied");
        assert_eq!(error.error_type.as_ref().unwrap(), "1005");
        assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
        assert!(error
            .message
            .try_to_utf8()
            .unwrap()
            .contains("required plan 3"));
    }

    fn request(model: &str, account: &str) -> ModelRequest {
        let mut request: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
            json!({"model":model,"system":[],"messages":[],"tools":[],"maxOutputTokens":100})
                .to_string(),
        ))
        .unwrap();
        request.messages.push(Message::Assistant(AssistantMessage {
            uuid: "assistant".into(),
            model: model.into(),
            usage: None,
            content: vec![ContentBlock::Thinking(ThinkingBlock {
                message_id: None,
                text: "reasoning".into(),
                signature: None,
                encrypted_content: Some(
                    envelope(model, account, json!({"content":"byte-exact","version":1}))
                        .to_string()
                        .into(),
                ),
            })],
        }));
        request
    }
    #[test]
    fn opaque_replay_requires_the_same_source_model_and_account() {
        let mut own = request("exact-model", "account-a");
        prepare_replay(&mut own, "account-a");
        let Message::Assistant(message) = &own.messages[0] else {
            panic!("assistant")
        };
        let ContentBlock::Thinking(block) = &message.content[0] else {
            panic!("thinking")
        };
        assert!(block.encrypted_content.is_none());
        assert_eq!(
            marker_extra(
                &block.text.try_to_utf8().unwrap(),
                "exact-model",
                "account-a"
            )
            .unwrap()["content"],
            "byte-exact"
        );
        assert!(marker_extra(
            &block.text.try_to_utf8().unwrap(),
            "exact-model",
            "account-b"
        )
        .is_none());
        for (model, account) in [
            ("exact-model", "account-b"),
            ("different-model", "account-a"),
        ] {
            let mut foreign = request("exact-model", "account-a");
            foreign.model = model.into();
            prepare_replay(&mut foreign, account);
            let Message::Assistant(message) = &foreign.messages[0] else {
                panic!("assistant")
            };
            let ContentBlock::Thinking(block) = &message.content[0] else {
                panic!("thinking")
            };
            assert!(block.encrypted_content.is_some());
            assert_eq!(block.text, "reasoning");
        }
        let foreign = json!({"kind":"klaus.trae.extra-info.v1","source":"another-source","model":"exact-model","account":"account-a","extraInfo":{}});
        assert!(!own_envelope(&foreign, "exact-model", "account-a"));
    }
}
