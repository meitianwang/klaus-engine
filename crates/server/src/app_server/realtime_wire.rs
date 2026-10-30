//! Protocol-specific realtime messages and transcript normalization.
use super::super::exact_payload;
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeString};
use serde_json::{json, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Protocol {
    Public,
    V1,
    Frameless,
    Glm,
    Qoder,
}
impl Protocol {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "public" => Ok(Self::Public),
            "v1" => Ok(Self::V1),
            "frameless" => Ok(Self::Frameless),
            "glm" => Ok(Self::Glm),
            "qoder" => Ok(Self::Qoder),
            _ => Err("The realtime wire protocol is unsupported".into()),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::V1 => "v1",
            Self::Frameless => "frameless",
            Self::Glm => "glm",
            Self::Qoder => "qoder",
        }
    }
    pub fn voices(self) -> Vec<String> {
        match self {
            Self::Public | Self::Glm => Vec::new(),
            Self::Qoder => ["longanqian", "loongeva_v3.6", "longanlufeng"]
                .map(String::from)
                .to_vec(),
            _ => [
                "juniper", "maple", "spruce", "ember", "vale", "breeze", "arbor", "sol", "cove",
            ]
            .map(String::from)
            .to_vec(),
        }
    }
    pub fn session(
        self,
        instructions: &str,
        model: Option<&str>,
        voice: Option<&str>,
        transcription: Option<&str>,
    ) -> Value {
        let mut value = match self {
            Self::Public => {
                json!({"type":"realtime","audio":{"input":{"turn_detection":{"type":"server_vad","create_response":false,"interrupt_response":true}}},"instructions":instructions,"tools":[{"type":"function","name":"delegate_task","description":"Ask the task agent to carry out the user's request in the current conversation. The task follows normal tool permissions and reports a verified result.","parameters":{"type":"object","properties":{"prompt":{"type":"string","description":"The complete task request, with relevant spoken context."}},"required":["prompt"],"additionalProperties":false}}],"tool_choice":"auto"})
            }
            Self::V1 => {
                json!({"type":"quicksilver","instructions":instructions,"audio":{"input":{"format":{"type":"audio/pcm","rate":24000}},"output":{"voice":voice.unwrap_or("cove")}}})
            }
            Self::Frameless => {
                json!({"instructions":instructions,"audio":{"output":{"voice":voice.unwrap_or("cove")}},"delegation":{"type":"client"}})
            }
            Self::Glm => {
                json!({"instructions":instructions,"turn_detection":{"type":"server_vad"},"input_audio_format":"pcm24","output_audio_format":"pcm","beta_fields":{"chat_mode":"audio","tts_source":"e2e"},"tools":[{"type":"function","name":"delegate_task","description":"Ask the task agent to carry out the user's request in the current conversation. The task follows normal tool permissions and reports a verified result.","parameters":{"type":"object","properties":{"prompt":{"type":"string","description":"The complete task request, with relevant spoken context."}},"required":["prompt"],"additionalProperties":false}}]})
            }
            Self::Qoder => {
                json!({"instructions":instructions,"voice":voice.unwrap_or("longanqian"),"input_audio_format":"pcm","output_audio_format":"pcm","max_history_turns":50,"turn_detection":{"type":"smart_turn"},"tools":[{"type":"function","function":{"name":"delegate_task","description":"Ask the task agent to carry out the user's request in the current conversation. The task follows normal tool permissions and reports a verified result.","parameters":{"type":"object","properties":{"prompt":{"type":"string","description":"The complete task request, with relevant spoken context."}},"required":["prompt"],"additionalProperties":false}}}]})
            }
        };
        if let Some(model) = model {
            if !matches!(self, Self::Glm | Self::Qoder) {
                value["model"] = json!(model);
            }
        }
        match self {
            Self::Public => {
                if let Some(voice) = voice {
                    value["audio"]["output"]["voice"] = json!(voice);
                }
                if let Some(model) = transcription {
                    value["audio"]["input"]["transcription"] = json!({"model":model});
                }
            }
            Self::Glm => {
                if let Some(voice) = voice {
                    value["voice"] = json!(voice);
                }
            }
            _ => {}
        }
        value
    }
    pub fn context(
        self,
        text: &str,
        handoff: Option<&str>,
        speakable: bool,
        completed: bool,
    ) -> Vec<Value> {
        self.context_exact(&RuntimeString::from(text), handoff, speakable, completed)
            .iter()
            .map(|value| {
                exact_payload::scalar(value).expect("scalar realtime context stays scalar")
            })
            .collect()
    }

    pub fn context_exact(
        self,
        text: &RuntimeString,
        handoff: Option<&str>,
        speakable: bool,
        completed: bool,
    ) -> Vec<RuntimeJson> {
        let part = |text: RuntimeString| {
            exact_payload::object([("type", "input_text".into()), ("text", text.into())])
        };
        let message = |role: &str| {
            exact_payload::object([
                ("type", "conversation.item.create".into()),
                (
                    "item",
                    exact_payload::object([
                        ("type", "message".into()),
                        ("role", role.into()),
                        ("content", RuntimeJson::Array(vec![part(text.clone())])),
                    ]),
                ),
            ])
        };
        match self {
            Self::Public | Self::Glm | Self::Qoder => vec![if let Some(id) = handoff {
                exact_payload::object([
                    ("type", "conversation.item.create".into()),
                    (
                        "item",
                        exact_payload::object([
                            ("type", "function_call_output".into()),
                            ("call_id", id.into()),
                            ("output", text.clone().into()),
                        ]),
                    ),
                ])
            } else {
                message("system")
            }],
            Self::V1 => vec![if let Some(id) = handoff {
                let text = if completed {
                    RuntimeString::from("\"Agent Final Message\":\n\n").concat(text)
                } else {
                    text.clone()
                };
                exact_payload::object([
                    ("type", "conversation.handoff.append".into()),
                    ("handoff_id", id.into()),
                    ("output_text", text.into()),
                ])
            } else {
                message("developer")
            }],
            Self::Frameless => exact_chunks(text, 500)
                .into_iter()
                .map(|text| {
                    let mut value = exact_payload::object([
                        (
                            "type",
                            if handoff.is_some() {
                                "delegation.context.append".into()
                            } else {
                                "session.context.append".into()
                            },
                        ),
                        (
                            "channel",
                            if speakable {
                                "speakable".into()
                            } else {
                                "commentary".into()
                            },
                        ),
                        ("content", RuntimeJson::Array(vec![part(text)])),
                    ]);
                    if let Some(id) = handoff {
                        value
                            .as_object_mut()
                            .expect("context object")
                            .insert("delegation_item_id", id.into());
                    }
                    value
                })
                .collect(),
        }
    }
    pub fn outgoing(self, event: Value) -> Vec<Value> {
        if matches!(self, Self::Public | Self::Glm | Self::Qoder) {
            return vec![event];
        }
        match event["type"].as_str().unwrap_or("") {
            "response.cancel" => Vec::new(),
            "response.create" => event
                .pointer("/response/input/0/content/0/text")
                .and_then(Value::as_str)
                .map(|text| {
                    if self == Self::V1 {
                        self.context(text, Some("codex"), true, false)
                    } else {
                        self.context(text, None, true, false)
                    }
                })
                .unwrap_or_default(),
            "conversation.item.create" => event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                .map(|text| {
                    if self == Self::V1 {
                        let role = match event.pointer("/item/role").and_then(Value::as_str) {
                            Some("user") => "user",
                            Some("assistant") => "assistant",
                            _ => "developer",
                        };
                        vec![json!({"type":"conversation.item.create","item":{"type":"message","role":role,"content":[{"type":"input_text","text":text}]}})]
                    } else {
                        self.context(text, None, true, false)
                            .into_iter()
                            .map(|mut frame| {
                                frame.as_object_mut().unwrap().remove("channel");
                                frame
                            })
                            .collect()
                    }
                })
                .unwrap_or_default(),
            _ => vec![event],
        }
    }
    pub fn handoff(self, event: &Value) -> Option<(String, String)> {
        match self {
            Self::V1 if event["type"] == "conversation.handoff.requested" => Some((
                event["handoff_id"].as_str()?.into(),
                event["input_transcript"].as_str()?.into(),
            )),
            Self::Frameless
                if event["type"] == "delegation.created"
                    && event.pointer("/item/type") == Some(&json!("delegation"))
                    && event.pointer("/item/target") == Some(&json!("client")) =>
            {
                Some((
                    event.pointer("/item/id")?.as_str()?.into(),
                    event
                        .pointer("/item/content")?
                        .as_array()?
                        .iter()
                        .filter(|part| part["type"] == "input_text")
                        .filter_map(|part| part["text"].as_str())
                        .collect(),
                ))
            }
            _ => None,
        }
    }
}
/// Bound each context frame by its explicit UTF-8 display size while retaining
/// exact units. A valid pair stays together; isolated units remain unmodified.
fn exact_chunks(text: &RuntimeString, max: usize) -> Vec<RuntimeString> {
    let units = text.as_utf16();
    let mut result = Vec::new();
    let mut start = 0;
    while start < units.len() {
        let mut end = start;
        let mut bytes = 0;
        while end < units.len() {
            let unit = units[end];
            let paired = (0xd800..=0xdbff).contains(&unit)
                && units
                    .get(end + 1)
                    .is_some_and(|unit| (0xdc00..=0xdfff).contains(unit));
            let width = if paired {
                4
            } else if unit <= 0x7f {
                1
            } else if unit <= 0x7ff {
                2
            } else {
                3
            };
            if bytes + width > max {
                break;
            }
            bytes += width;
            end += if paired { 2 } else { 1 };
        }
        debug_assert!(end > start, "context frame budget holds one code point");
        result.push(RuntimeString::from_utf16(&units[start..end]));
        start = end;
    }
    if result.is_empty() {
        result.push(RuntimeString::default());
    }
    result
}

#[derive(Default)]
pub(super) struct TranscriptNormalizer {
    current: std::collections::HashMap<String, String>,
    finished: std::collections::HashSet<String>,
    finished_order: std::collections::VecDeque<String>,
    last_done: std::collections::HashMap<String, String>,
    partial: std::collections::HashMap<String, String>,
    recent: Vec<(String, String)>,
    fresh: std::collections::HashSet<String>,
    sequence: u64,
}
impl TranscriptNormalizer {
    pub fn normalize(&mut self, protocol: Protocol, event: &Value) -> Option<Value> {
        if protocol == Protocol::Public {
            return None;
        }
        let kind = event["type"].as_str()?;
        if kind == "input_audio_buffer.speech_started" {
            self.fresh.insert("user".into());
        }
        if kind == "response.created" {
            self.fresh.insert("assistant".into());
        }
        let (role, done, text) = match kind {
            "conversation.input_transcript.delta"
            | "conversation.item.input_audio_transcription.delta" => {
                ("user", false, event["delta"].as_str()?)
            }
            "conversation.input_transcript.turn_marked"
            | "conversation.item.input_audio_transcription.completed" => {
                ("user", true, event["transcript"].as_str()?)
            }
            "conversation.output_transcript.delta"
            | "response.output_text.delta"
            | "response.audio_transcript.delta"
            | "response.output_audio_transcript.delta" => {
                ("assistant", false, event["delta"].as_str()?)
            }
            "input_transcript.added" => ("user", false, event.pointer("/item/text")?.as_str()?),
            "output_transcript.added" => {
                ("assistant", false, event.pointer("/item/text")?.as_str()?)
            }
            "turn.done" => (
                event.pointer("/turn/role")?.as_str()?,
                true,
                event.pointer("/turn/transcript")?.as_str()?,
            ),
            "response.output_audio_transcript.done"
            | "response.audio_transcript.done"
            | "response.output_text.done" => (
                "assistant",
                true,
                event["transcript"]
                    .as_str()
                    .or_else(|| event["text"].as_str())?,
            ),
            _ => return None,
        };
        if !matches!(role, "user" | "assistant") {
            return None;
        }
        let accumulated = self.partial.get(role).cloned().unwrap_or_default();
        let final_text = if protocol == Protocol::Frameless
            && done
            && !accumulated.is_empty()
            && !text.starts_with(&accumulated)
        {
            accumulated
        } else {
            text.into()
        };
        let text = final_text.as_str();
        let alias = event["item_id"]
            .as_str()
            .or_else(|| event.pointer("/item/id").and_then(Value::as_str))
            .or_else(|| event.pointer("/turn/id").and_then(Value::as_str));
        if done && alias.is_some_and(|id| self.finished.contains(&format!("{role}:{id}"))) {
            return None;
        }
        if done
            && alias.is_none()
            && !self.current.contains_key(role)
            && !self.fresh.contains(role)
            && self.last_done.get(role).is_some_and(|prior| prior == text)
        {
            return None;
        }
        let id = if let Some(id) = self.current.get(role) {
            id.clone()
        } else {
            self.sequence += 1;
            let id = event["item_id"]
                .as_str()
                .or_else(|| event.pointer("/item/id").and_then(Value::as_str))
                .or_else(|| event.pointer("/turn/id").and_then(Value::as_str))
                .map(String::from)
                .unwrap_or_else(|| format!("transcript-{}-{role}", self.sequence));
            self.current.insert(role.into(), id.clone());
            id
        };
        if done {
            self.current.remove(role);
            self.partial.remove(role);
            self.last_done.insert(role.into(), text.into());
            if let Some(alias) = alias {
                let key = format!("{role}:{alias}");
                self.finished.insert(key.clone());
                self.finished_order.push_back(key);
                while self.finished_order.len() > 4096 {
                    if let Some(old) = self.finished_order.pop_front() {
                        self.finished.remove(&old);
                    }
                }
            }
            if !text.is_empty() {
                self.recent.push((role.into(), text.into()));
            }
            while self
                .recent
                .iter()
                .map(|(role, text)| role.len() + text.len() + 2)
                .sum::<usize>()
                > 8 * 1024
            {
                if self.recent.len() > 1 {
                    self.recent.remove(0);
                } else {
                    let text = &mut self.recent[0].1;
                    let mut start = text.len().saturating_sub(8 * 1024 - 16);
                    while !text.is_char_boundary(start) {
                        start += 1;
                    }
                    *text = text[start..].into();
                    break;
                }
            }
            self.fresh.remove(role);
        } else {
            let partial = self.partial.entry(role.into()).or_default();
            if partial.len() + text.len() <= 1024 * 1024 {
                partial.push_str(text);
            }
            self.fresh.remove(role);
        }
        Some(
            json!({"type":match(role,done){("user",false)=>"conversation.item.input_audio_transcription.delta",("user",true)=>"conversation.item.input_audio_transcription.completed",(_,false)=>"response.output_audio_transcript.delta",(_,true)=>"response.output_audio_transcript.done"},"item_id":id,(if done{"transcript"}else{"delta"}):text}),
        )
    }
    pub fn handoff_input(
        &mut self,
        protocol: Protocol,
        event: &Value,
        input: &str,
    ) -> Option<Value> {
        if input.trim().is_empty() || self.last_done.get("user").is_some_and(|text| text == input) {
            return None;
        }
        self.normalize(protocol,&json!({"type":"conversation.input_transcript.turn_marked","item_id":event["item_id"].as_str().or_else(||event.pointer("/item/id").and_then(Value::as_str)),"transcript":input}))
    }
    pub fn task_prompt(&mut self, input: &str) -> String {
        let mut lines = std::mem::take(&mut self.recent)
            .into_iter()
            .map(|(role, text)| format!("{role}: {text}"))
            .collect::<Vec<_>>();
        for role in ["user", "assistant"] {
            if let Some(text) = self.partial.get(role) {
                lines.push(format!("{role}: {text}"));
            }
        }
        let transcript = lines.join("\n");
        let mut start = transcript.len().saturating_sub(8 * 1024);
        while !transcript.is_char_boundary(start) {
            start += 1;
        }
        let prompt = if input.trim().is_empty() {
            transcript[start..].to_string()
        } else {
            input.into()
        };
        if transcript.is_empty() {
            prompt
        } else {
            format!(
                "Voice request:\n{prompt}\n\nRecent spoken context:\n{}",
                &transcript[start..]
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn appended_text_preserves_message_roles_and_default_context_delivery() {
        for (input, expected) in [
            ("user", "user"),
            ("assistant", "assistant"),
            ("system", "developer"),
        ] {
            let event = json!({"type":"conversation.item.create","item":{"type":"message","role":input,"content":[{"type":"input_text","text":"A text update"}]}});
            let output = Protocol::V1.outgoing(event.clone());
            assert_eq!(output[0]["item"]["role"], expected);
            let output = Protocol::Frameless.outgoing(event);
            assert_eq!(output[0]["type"], "session.context.append");
            assert!(output[0].get("channel").is_none());
            assert_eq!(output[0]["content"][0]["text"], "A text update");
        }
    }
    #[test]
    fn glm_configuration_declares_pcm_rates_and_audio_chat_mode() {
        let glm = Protocol::Glm.session(
            "Instructions",
            Some("glm-realtime-air"),
            Some("tongtong"),
            None,
        );
        assert_eq!(glm["input_audio_format"], "pcm24");
        assert_eq!(glm["output_audio_format"], "pcm");
        assert_eq!(
            glm.pointer("/turn_detection/type"),
            Some(&json!("server_vad"))
        );
        assert_eq!(glm.pointer("/beta_fields/chat_mode"), Some(&json!("audio")));
        assert_eq!(glm["voice"], "tongtong");
        assert!(glm.get("model").is_none());
        assert!(Protocol::Glm.voices().is_empty());
        assert!(matches!(Protocol::parse("glm"), Ok(Protocol::Glm)));
    }
    #[test]
    fn qoder_configuration_declares_pcm_and_smart_turn() {
        let qoder = Protocol::Qoder.session("Instructions", Some("ignored-model"), None, None);
        assert_eq!(qoder["input_audio_format"], "pcm");
        assert_eq!(qoder["output_audio_format"], "pcm");
        assert_eq!(qoder["voice"], "longanqian");
        assert_eq!(qoder["max_history_turns"], 50);
        assert_eq!(
            qoder.pointer("/turn_detection/type"),
            Some(&json!("smart_turn"))
        );
        assert_eq!(
            qoder.pointer("/tools/0/function/name"),
            Some(&json!("delegate_task"))
        );
        assert!(qoder.get("model").is_none());
        let voiced = Protocol::Qoder.session("Instructions", None, Some("longanlufeng"), None);
        assert_eq!(voiced["voice"], "longanlufeng");
        assert_eq!(
            Protocol::Qoder.voices().join(","),
            "longanqian,loongeva_v3.6,longanlufeng"
        );
        assert!(matches!(Protocol::parse("qoder"), Ok(Protocol::Qoder)));
        assert_eq!(Protocol::Qoder.name(), "qoder");
    }
    #[test]
    fn qoder_transcripts_normalize_from_response_audio_transcript_events() {
        let mut state = TranscriptNormalizer::default();
        let delta = state
            .normalize(
                Protocol::Qoder,
                &json!({"type":"response.audio_transcript.delta","delta":"Hel"}),
            )
            .unwrap();
        assert_eq!(delta["type"], "response.output_audio_transcript.delta");
        let done = state
            .normalize(
                Protocol::Qoder,
                &json!({"type":"response.audio_transcript.done","transcript":"Hello"}),
            )
            .unwrap();
        assert_eq!(done["type"], "response.output_audio_transcript.done");
        assert_eq!(done["item_id"], delta["item_id"]);
        let user = state
            .normalize(
                Protocol::Qoder,
                &json!({"type":"conversation.item.input_audio_transcription.completed","transcript":"Spoken request"}),
            )
            .unwrap();
        assert_eq!(
            user["type"],
            "conversation.item.input_audio_transcription.completed"
        );
        assert!(state
            .normalize(
                Protocol::Qoder,
                &json!({"type":"conversation.item.ambient_audio_transcription.delta","delta":"room tone"})
            )
            .is_none());
    }
    #[test]
    fn qoder_rides_openai_style_context_and_passthrough_frames() {
        let frames = Protocol::Qoder.context("Finished", Some("handoff-one"), true, true);
        assert_eq!(frames[0]["type"], "conversation.item.create");
        assert_eq!(
            frames[0].pointer("/item/type"),
            Some(&json!("function_call_output"))
        );
        assert_eq!(
            frames[0].pointer("/item/call_id"),
            Some(&json!("handoff-one"))
        );
        let event = json!({"type":"input_audio_buffer.append","audio":"AAA"});
        assert_eq!(Protocol::Qoder.outgoing(event.clone()), vec![event]);
    }
    #[test]
    fn hosted_configuration_declares_audio_and_client_delegation() {
        let v1 = Protocol::V1.session("Instructions", Some("voice-model"), Some("cove"), None);
        assert_eq!(v1["type"], "quicksilver");
        assert_eq!(v1.pointer("/audio/input/format/rate"), Some(&json!(24000)));
        assert_eq!(v1["model"], "voice-model");
        assert!(v1.get("tools").is_none());
        let bidi =
            Protocol::Frameless.session("Instructions", Some("live-model"), Some("maple"), None);
        assert_eq!(bidi.pointer("/delegation/type"), Some(&json!("client")));
        assert_eq!(bidi.pointer("/audio/output/voice"), Some(&json!("maple")));
    }
    #[test]
    fn context_chunks_preserve_utf8_and_phase_and_handoff_identity() {
        let text = "猫🙂".repeat(200);
        let frames = Protocol::Frameless.context(&text, Some("handoff-one"), false, false);
        let mut joined = String::new();
        for frame in frames {
            assert_eq!(frame["type"], "delegation.context.append");
            assert_eq!(frame["delegation_item_id"], "handoff-one");
            assert_eq!(frame["channel"], "commentary");
            let chunk = frame.pointer("/content/0/text").unwrap().as_str().unwrap();
            assert!(chunk.len() <= 500);
            joined.push_str(chunk);
        }
        assert_eq!(joined, text);
        assert_eq!(
            Protocol::V1.context("Finished", Some("handoff-one"), true, true)[0]["output_text"],
            "\"Agent Final Message\":\n\nFinished"
        );
    }
    #[test]
    fn idless_aliases_keep_one_stream_identity_and_completed_replays_are_ignored() {
        let mut state = TranscriptNormalizer::default();
        let delta=state.normalize(Protocol::V1,&json!({"type":"conversation.item.input_audio_transcription.delta","delta":"Hello"})).unwrap();
        let done = state
            .normalize(
                Protocol::V1,
                &json!({"type":"conversation.input_transcript.turn_marked","transcript":"Hello"}),
            )
            .unwrap();
        assert_eq!(delta["item_id"], done["item_id"]);
        assert!(state
            .normalize(
                Protocol::V1,
                &json!({"type":"conversation.input_transcript.turn_marked","transcript":"Hello"})
            )
            .is_none());
        state.normalize(
            Protocol::V1,
            &json!({"type":"input_audio_buffer.speech_started"}),
        );
        let next = state
            .normalize(
                Protocol::V1,
                &json!({"type":"conversation.input_transcript.turn_marked","transcript":"Hello"}),
            )
            .unwrap();
        assert_ne!(done["item_id"], next["item_id"]);
    }
    #[test]
    fn framed_done_alias_reuses_pending_item_and_deduplicates_turn_identity() {
        let mut state = TranscriptNormalizer::default();
        let delta=state.normalize(Protocol::Frameless,&json!({"type":"input_transcript.added","item":{"id":"input-one","text":"Spoken request"}})).unwrap();
        let done = json!({"type":"turn.done","turn":{"id":"turn-one","role":"user","transcript":"Spoken request"}});
        assert_eq!(
            state.normalize(Protocol::Frameless, &done).unwrap()["item_id"],
            delta["item_id"]
        );
        assert!(state.normalize(Protocol::Frameless, &done).is_none());
        assert!(state
            .handoff_input(
                Protocol::Frameless,
                &json!({"item":{"id":"handoff-one"}}),
                "Spoken request"
            )
            .is_none());
    }
    #[test]
    fn a_delayed_frameless_final_preserves_already_observed_spoken_text() {
        let mut state = TranscriptNormalizer::default();
        state.normalize(Protocol::Frameless,&json!({"type":"output_transcript.added","item":{"id":"speech-one","text":"The complete spoken sentence"}}));
        let done=state.normalize(Protocol::Frameless,&json!({"type":"turn.done","turn":{"id":"turn-one","role":"assistant","transcript":"The complete"}})).unwrap();
        assert_eq!(done["transcript"], "The complete spoken sentence");
        let mut v1 = TranscriptNormalizer::default();
        v1.normalize(
            Protocol::V1,
            &json!({"type":"conversation.output_transcript.delta","delta":"First draft"}),
        );
        assert_eq!(v1.normalize(Protocol::V1,&json!({"type":"response.output_audio_transcript.done","transcript":"Corrected final"})).unwrap()["transcript"],"Corrected final");
    }
    #[test]
    fn only_client_delegation_is_admitted_and_recent_context_is_bounded() {
        assert!(Protocol::Frameless.handoff(&json!({"type":"delegation.created","item":{"id":"one","type":"delegation","target":"server","content":[{"type":"input_text","text":"Do work"}]}})).is_none());
        assert_eq!(Protocol::Frameless.handoff(&json!({"type":"delegation.created","item":{"id":"one","type":"delegation","target":"client","content":[{"type":"input_text","text":"Do work"}]}})),Some(("one".into(),"Do work".into())));
        let mut state = TranscriptNormalizer::default();
        for number in 0..30 {
            state.normalize(Protocol::Frameless,&json!({"type":"turn.done","turn":{"id":format!("turn-{number}"),"role":"assistant","transcript":"🙂".repeat(4000)}}));
        }
        assert!(
            state
                .recent
                .iter()
                .map(|(role, text)| role.len() + text.len() + 2)
                .sum::<usize>()
                <= 8 * 1024
        );
        let prompt = state.task_prompt("Current request");
        assert!(prompt.contains("Current request"));
        assert!(prompt.len() < 9 * 1024);
        assert!(state.recent.is_empty());
    }
}
