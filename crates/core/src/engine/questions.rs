//! Optional questions stay answerable only in their originating admitted turn.
//! Presentation (expanded, deferred, collapsed) is deliberately client-owned.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeMap, RuntimeObject, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::content::ContentBlock;
use crate::schemas::message::UserMessage;
use crate::types::events::EngineEvent;
use crate::types::tool::ElicitRequest;

pub const MAX_ASYNC_QUESTIONS_PER_TURN: usize = 32;
const MAX_ANSWER_TEXT_UNITS: usize = 64 * 1024;

macro_rules! question_codec {
    ($ty:ty, {$($field:ident => $key:literal),* $(,)?}) => {
        impl RuntimeEncode for $ty {
            fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
                let mut object = RuntimeObjectEncoder::new();
                $(object.field($key, &self.$field)?;)*
                out.emit(RuntimeJson::Object(object.finish()))
            }
        }
        impl RuntimeDecode for $ty {
            fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                runtime_codec::decode_object(value, false, |object| Ok(Self {
                    $($field: object.required($key, &[])?,)*
                }))
            }
        }
    };
}

#[derive(Clone, Debug, PartialEq)]
pub struct QuestionRequest {
    pub prompt: RuntimeString,
    pub questions: Vec<RuntimeObject>,
    pub attachments_supported: bool,
    pub metadata: Option<RuntimeObject>,
}
question_codec!(QuestionRequest, {prompt => "prompt", questions => "questions", attachments_supported => "attachmentsSupported", metadata => "metadata"});

#[derive(Clone, Debug, PartialEq)]
pub struct AsyncQuestion {
    pub request_id: RuntimeString,
    pub turn_id: RuntimeString,
    pub tool_call_id: RuntimeString,
    pub request: QuestionRequest,
    pub state: RuntimeString,
    /// The full question array retains stable requestId:index identities.
    pub answers: RuntimeMap<RuntimeString>,
}
question_codec!(AsyncQuestion, {request_id => "requestId", turn_id => "turnId", tool_call_id => "toolCallId", request => "request", state => "state", answers => "answers"});

/// Typed provenance remains on the actual user message in durable history.
#[derive(Clone, Debug, PartialEq)]
pub struct QuestionAnswer {
    pub request_id: RuntimeString,
    pub turn_id: RuntimeString,
    pub tool_call_id: RuntimeString,
    pub client_response_id: RuntimeString,
    pub questions: Vec<RuntimeObject>,
    pub answers: RuntimeMap<RuntimeString>,
}
question_codec!(QuestionAnswer, {request_id => "requestId", turn_id => "turnId", tool_call_id => "toolCallId", client_response_id => "clientResponseId", questions => "questions", answers => "answers"});

#[derive(Clone, Debug, PartialEq)]
pub struct QuestionAnswerReceipt {
    pub accepted: bool,
    pub request_id: RuntimeString,
    pub turn_id: RuntimeString,
    pub user_message_id: RuntimeString,
    pub client_response_id: RuntimeString,
    pub state: RuntimeString,
}
question_codec!(QuestionAnswerReceipt, {accepted => "accepted", request_id => "requestId", turn_id => "turnId", user_message_id => "userMessageId", client_response_id => "clientResponseId", state => "state"});

#[derive(Clone, Debug, PartialEq)]
pub struct QuestionResolution {
    pub request_id: RuntimeString,
    pub turn_id: RuntimeString,
    pub state: RuntimeString,
    pub answers: RuntimeMap<RuntimeString>,
    pub user_message_id: Option<RuntimeString>,
    pub client_response_id: Option<RuntimeString>,
    /// A live client receives the same user message that history will project.
    pub user_message: Option<UserMessage>,
}
question_codec!(QuestionResolution, {request_id => "requestId", turn_id => "turnId", state => "state", answers => "answers", user_message_id => "userMessageId", client_response_id => "clientResponseId", user_message => "userMessage"});

type EventSink = Arc<dyn Fn(EngineEvent) + Send + Sync>;
type PersistQuestion =
    Arc<dyn Fn(AsyncQuestion) -> BoxFuture<'static, Result<(), RuntimeString>> + Send + Sync>;

#[derive(Clone)]
pub struct AsyncQuestionContext {
    pub registry: Arc<AsyncQuestionRegistry>,
    pub turn_id: RuntimeString,
    pub generation: u64,
    pub tool_call_id: RuntimeString,
    pub emit: Option<EventSink>,
    pub persist: PersistQuestion,
}

impl AsyncQuestionContext {
    pub async fn ask(
        &self,
        request: ElicitRequest,
        request_id: RuntimeString,
    ) -> Result<AsyncQuestion, RuntimeString> {
        let question = self.registry.register(self, request, request_id)?;
        if let Err(error) = (self.persist)(question.clone()).await {
            self.registry
                .remove_request(&question.turn_id, &question.request_id);
            return Err(error);
        }
        self.registry.publish(self, &question.request_id)
    }
}

struct SubmittedAnswer {
    metadata: QuestionAnswer,
    receipt: QuestionAnswerReceipt,
    message: UserMessage,
    ready: bool,
}

struct QuestionTurn {
    generation: u64,
    signal: CancellationToken,
    open: bool,
    requests: Vec<AsyncQuestion>,
    submissions: HashMap<RuntimeString, SubmittedAnswer>,
    submission_order: Vec<RuntimeString>,
    pending: VecDeque<RuntimeString>,
    emit: Option<EventSink>,
}

#[derive(Default)]
pub struct AsyncQuestionRegistry {
    turns: Mutex<HashMap<RuntimeString, QuestionTurn>>,
    changed: Notify,
}

pub enum PrepareAnswer {
    New(UserMessage),
    Existing(QuestionAnswerReceipt),
}

impl AsyncQuestionRegistry {
    pub fn open(&self, turn_id: RuntimeString, generation: u64, signal: CancellationToken) {
        let mut turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
        // Completed receipts survive until the next turn opens. In-flight writes
        // survive until their original terminal settlement completes.
        turns.retain(|_, turn| turn.open || turn.submissions.values().any(|answer| !answer.ready));
        turns.insert(
            turn_id,
            QuestionTurn {
                generation,
                signal,
                open: true,
                requests: Vec::new(),
                submissions: HashMap::new(),
                submission_order: Vec::new(),
                pending: VecDeque::new(),
                emit: None,
            },
        );
    }

    fn register(
        &self,
        context: &AsyncQuestionContext,
        request: ElicitRequest,
        request_id: RuntimeString,
    ) -> Result<AsyncQuestion, RuntimeString> {
        let mut turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
        let turn = turns
            .get_mut(&context.turn_id)
            .ok_or_else(|| RuntimeString::from("The originating question turn has ended"))?;
        if !turn.open || turn.signal.is_cancelled() || turn.generation != context.generation {
            return Err("The originating question turn has ended".into());
        }
        if turn.requests.len() >= MAX_ASYNC_QUESTIONS_PER_TURN {
            return Err("At most 32 optional question batches are allowed per turn".into());
        }
        let questions = request.questions.ok_or_else(|| {
            RuntimeString::from("Optional questions require structured questions")
        })?;
        crate::tools::builtins::ask_user::validate_questions(&questions)
            .map_err(|error| error.message)?;
        if request_id.is_empty()
            || turn
                .requests
                .iter()
                .any(|request| request.request_id == request_id)
        {
            return Err(
                "Optional question request identifiers must be nonempty and unique within a turn"
                    .into(),
            );
        }
        if !(1..=4).contains(&questions.len()) {
            return Err("Optional question batches must contain one to four questions".into());
        }
        let record = AsyncQuestion {
            request_id,
            turn_id: context.turn_id.clone(),
            tool_call_id: context.tool_call_id.clone(),
            request: QuestionRequest {
                prompt: request.prompt,
                questions,
                attachments_supported: true,
                metadata: request.metadata,
            },
            state: "pending".into(),
            answers: RuntimeMap::new(),
        };
        if crate::runtime_values::runtime_json::stringify(
            &runtime_codec::encode_value(&record.request).expect("question request codec"),
        )
        .len_utf16()
            > 128 * 1024
        {
            return Err(
                "An optional question request must not exceed 128 Ki characters of serialized text"
                    .into(),
            );
        }
        turn.emit = context.emit.clone();
        turn.requests.push(record.clone());
        Ok(record)
    }

    fn publish(
        &self,
        context: &AsyncQuestionContext,
        request_id: &RuntimeString,
    ) -> Result<AsyncQuestion, RuntimeString> {
        let turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
        let turn = turns
            .get(&context.turn_id)
            .filter(|turn| turn.open && !turn.signal.is_cancelled())
            .ok_or_else(|| RuntimeString::from("The originating question turn has ended"))?;
        let question = turn
            .requests
            .iter()
            .find(|request| request.request_id == *request_id)
            .cloned()
            .ok_or_else(|| RuntimeString::from("The optional question request has closed"))?;
        if question.state != "answered" {
            if let Some(emit) = &context.emit {
                // This sink only appends to the turn queue. Holding admission's
                // lock keeps a delayed requested snapshot ahead of later answers.
                emit(EngineEvent::QuestionRequested {
                    question: question.clone(),
                });
            }
        }
        Ok(question)
    }

    fn remove_request(&self, turn_id: &RuntimeString, request_id: &RuntimeString) {
        if let Some(turn) = self
            .turns
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(turn_id)
        {
            turn.requests
                .retain(|request| &request.request_id != request_id);
        }
    }

    pub fn pending(&self) -> Vec<AsyncQuestion> {
        self.turns
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|turn| turn.open && !turn.signal.is_cancelled())
            .flat_map(|turn| {
                turn.requests
                    .iter()
                    .filter(|request| request.state != "answered")
                    .cloned()
            })
            .collect()
    }

    pub fn prepare(
        &self,
        turn_id: &RuntimeString,
        request_id: &RuntimeString,
        client_response_id: &RuntimeString,
        answers: RuntimeMap<RuntimeString>,
        attachments: Option<Box<RuntimeMap<Vec<ContentBlock>>>>,
        message_id: RuntimeString,
    ) -> Result<PrepareAnswer, RuntimeString> {
        if client_response_id.is_empty() || client_response_id.len_utf16() > 256 {
            return Err(
                "clientResponseId must be a nonempty identifier of at most 256 characters".into(),
            );
        }
        let mut turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
        let turn = turns
            .get_mut(turn_id)
            .ok_or_else(|| RuntimeString::from("The originating question turn has ended"))?;
        let definitions = &turn
            .requests
            .iter()
            .find(|record| record.request_id == *request_id)
            .ok_or_else(|| {
                RuntimeString::from("No optional question request belongs to this turn")
            })?
            .request
            .questions;
        let answers =
            crate::tools::builtins::ask_user::canonical_question_map(definitions, answers)
                .map_err(|error| error.message)?;
        let attachments = attachments
            .map(|values| {
                crate::tools::builtins::ask_user::canonical_question_map(definitions, *values)
                    .map(Box::new)
            })
            .transpose()
            .map_err(|error| error.message)?;
        if let Some(previous) = turn.submissions.get(client_response_id) {
            if previous.metadata.request_id != *request_id || previous.metadata.answers != answers {
                return Err("clientResponseId was already used for a different answer".into());
            }
            let images = crate::tools::builtins::ask_user::validated_attachments(
                &previous.metadata.questions,
                &answers,
                attachments,
            )
            .map_err(|error| error.message)?
            .unwrap_or_default();
            let previous_images = previous
                .message
                .content
                .iter()
                .filter(|block| matches!(block, ContentBlock::Image(_)))
                .cloned()
                .collect::<Vec<_>>();
            if previous_images != images {
                return Err("clientResponseId was already used with different attachments".into());
            }
            return Ok(PrepareAnswer::Existing(previous.receipt.clone()));
        }
        if !turn.open || turn.signal.is_cancelled() {
            return Err("The originating question turn has ended".into());
        }
        let request = turn
            .requests
            .iter_mut()
            .find(|record| record.request_id == *request_id)
            .ok_or_else(|| {
                RuntimeString::from("No optional question request belongs to this turn")
            })?;
        if request.state == "submitting" {
            return Err(
                "An answer for this question request is being persisted; retry shortly".into(),
            );
        }
        if answers.is_empty() {
            return Err("At least one nonempty answer is required".into());
        }
        for (question, answer) in answers.entries() {
            if request.answers.get(&question).is_some() {
                return Err("This question has already been answered".into());
            }
            if !request.request.questions.iter().any(|entry| {
                crate::tools::builtins::ask_user::question_answer_key(entry) == Some(&question)
            }) {
                return Err("The response contains an unknown question".into());
            }
            if answer.len_utf16() > MAX_ANSWER_TEXT_UNITS
                || crate::tools::builtins::ask_user::runtime_is_blank(answer)
            {
                return Err("Answers must be nonempty and at most 64 KiB of text".into());
            }
        }
        let images = crate::tools::builtins::ask_user::validated_attachments(
            &request.request.questions,
            &answers,
            attachments,
        )
        .map_err(|error| error.message)?;
        let metadata = QuestionAnswer {
            request_id: request_id.clone(),
            turn_id: turn_id.clone(),
            tool_call_id: request.tool_call_id.clone(),
            client_response_id: client_response_id.clone(),
            questions: request.request.questions.clone(),
            answers,
        };
        let mut text = RuntimeString::default();
        for entry in &metadata.questions {
            let question = entry
                .get("question")
                .and_then(RuntimeJson::as_str)
                .expect("validated question");
            if let Some(answer) = metadata.answers.get(
                crate::tools::builtins::ask_user::question_answer_key(entry)
                    .expect("validated question"),
            ) {
                if !text.is_empty() {
                    text = text.concat(&RuntimeString::from("\n\n"));
                }
                text = text
                    .concat(question)
                    .concat(&RuntimeString::from("\n"))
                    .concat(answer);
            }
        }
        let mut content = vec![ContentBlock::Text(crate::schemas::content::TextBlock {
            text,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })];
        if let Some(images) = images {
            content.extend(images);
        }
        let message = UserMessage {
            uuid: message_id.clone(),
            content,
            is_meta: None,
            question_answer: Some(Box::new(metadata.clone())),
        };
        let receipt = QuestionAnswerReceipt {
            accepted: true,
            request_id: request_id.clone(),
            turn_id: turn_id.clone(),
            user_message_id: message_id,
            client_response_id: client_response_id.clone(),
            state: "submitting".into(),
        };
        request.state = "submitting".into();
        turn.pending.push_back(client_response_id.clone());
        turn.submission_order.push(client_response_id.clone());
        turn.submissions.insert(
            client_response_id.clone(),
            SubmittedAnswer {
                metadata,
                receipt,
                message: message.clone(),
                ready: false,
            },
        );
        Ok(PrepareAnswer::New(message))
    }

    pub async fn wait_receipt(
        &self,
        turn_id: &RuntimeString,
        client_response_id: &RuntimeString,
    ) -> Result<QuestionAnswerReceipt, RuntimeString> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
                let answer = turns
                    .get(turn_id)
                    .and_then(|turn| turn.submissions.get(client_response_id))
                    .ok_or_else(|| {
                        RuntimeString::from("The question answer could not be persisted")
                    })?;
                if answer.ready {
                    return Ok(answer.receipt.clone());
                }
            }
            changed.await;
        }
    }

    pub fn commit(
        &self,
        turn_id: &RuntimeString,
        client_response_id: &RuntimeString,
    ) -> Result<QuestionAnswerReceipt, RuntimeString> {
        let (receipt, emit, resolution) = {
            let mut turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
            let turn = turns
                .get_mut(turn_id)
                .ok_or_else(|| RuntimeString::from("The originating question turn has ended"))?;
            let answer = turn
                .submissions
                .get_mut(client_response_id)
                .ok_or_else(|| RuntimeString::from("Question answer submission disappeared"))?;
            answer.ready = true;
            let request = turn
                .requests
                .iter_mut()
                .find(|record| record.request_id == answer.metadata.request_id)
                .expect("registered request");
            for (key, value) in answer.metadata.answers.entries() {
                request.answers.insert(key, value.clone());
            }
            if turn.open && !turn.signal.is_cancelled() {
                request.state = if request.answers.len() == request.request.questions.len() {
                    "answered"
                } else {
                    "pending"
                }
                .into();
            } else if request.state != "expired" && request.state != "cancelled" {
                request.state = if turn.signal.is_cancelled() {
                    "cancelled"
                } else {
                    "expired"
                }
                .into();
            }
            answer.receipt.state = request.state.clone();
            let resolution = QuestionResolution {
                request_id: request.request_id.clone(),
                turn_id: turn_id.clone(),
                state: request.state.clone(),
                answers: request.answers.clone(),
                user_message_id: Some(answer.message.uuid.clone()),
                client_response_id: Some(client_response_id.clone()),
                user_message: Some(answer.message.clone()),
            };
            (answer.receipt.clone(), turn.emit.clone(), resolution)
        };
        self.changed.notify_waiters();
        if let Some(emit) = emit {
            emit(EngineEvent::QuestionResolved {
                question_resolved: resolution,
            });
        }
        Ok(receipt)
    }

    pub fn fail(&self, turn_id: &RuntimeString, client_response_id: &RuntimeString) {
        let notification = {
            let mut turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
            let mut notification = None;
            if let Some(turn) = turns.get_mut(turn_id) {
                if let Some(answer) = turn.submissions.remove(client_response_id) {
                    if let Some(request) = turn
                        .requests
                        .iter_mut()
                        .find(|record| record.request_id == answer.metadata.request_id)
                    {
                        request.state = if turn.open && !turn.signal.is_cancelled() {
                            "pending"
                        } else if turn.signal.is_cancelled() {
                            "cancelled"
                        } else {
                            "expired"
                        }
                        .into();
                        notification = Some((
                            turn.emit.clone(),
                            QuestionResolution {
                                request_id: request.request_id.clone(),
                                turn_id: turn_id.clone(),
                                state: request.state.clone(),
                                answers: request.answers.clone(),
                                user_message_id: None,
                                client_response_id: None,
                                user_message: None,
                            },
                        ));
                    }
                }
                turn.pending.retain(|id| id != client_response_id);
                turn.submission_order.retain(|id| id != client_response_id);
            }
            notification
        };
        self.changed.notify_waiters();
        if let Some((Some(emit), resolution)) = notification {
            emit(EngineEvent::QuestionResolved {
                question_resolved: resolution,
            });
        }
    }

    pub fn take_ready(&self, turn_id: &RuntimeString) -> Vec<UserMessage> {
        let mut turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
        let Some(turn) = turns.get_mut(turn_id) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        while turn
            .pending
            .front()
            .is_some_and(|id| turn.submissions.get(id).is_some_and(|answer| answer.ready))
        {
            let id = turn.pending.pop_front().expect("front exists");
            result.push(
                turn.submissions
                    .get(&id)
                    .expect("submitted answer")
                    .message
                    .clone(),
            );
        }
        result
    }

    /// Drain accepted answers, or atomically close admission before terminal output.
    /// A persistence operation already admitted wins over turn settlement.
    pub async fn drain_or_close(
        &self,
        turn_id: &RuntimeString,
        signal: &CancellationToken,
    ) -> Vec<UserMessage> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let ready = self.take_ready(turn_id);
            if !ready.is_empty() {
                return ready;
            }
            let closed = {
                let mut turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
                match turns.get_mut(turn_id) {
                    None => true,
                    Some(turn) if turn.pending.is_empty() => {
                        turn.open = false;
                        true
                    }
                    Some(_) => false,
                }
            };
            if closed {
                return Vec::new();
            }
            tokio::select! {
                _ = signal.cancelled() => {
                    if let Some(turn) = self.turns.lock().unwrap_or_else(|error| error.into_inner()).get_mut(turn_id) {
                        turn.open = false;
                    }
                    // Terminal instrumentation owns the ordered resolution events.
                    return Vec::new();
                },
                _ = changed => {}
            }
        }
    }

    /// Close admission and finish already admitted writes before publishing the
    /// terminal turn event. Repeat typed replies here so a write racing with the
    /// producer queue's close still reaches a live client; UUIDs are stable.
    pub async fn finish(&self, turn_id: &RuntimeString, state: &str) -> Vec<QuestionResolution> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let writing = {
                let mut turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
                let Some(turn) = turns.get_mut(turn_id) else {
                    return Vec::new();
                };
                turn.open = false;
                turn.submissions.values().any(|answer| !answer.ready)
            };
            if !writing {
                break;
            }
            changed.await;
        }
        let mut resolutions = self.close(turn_id, state);
        let turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(turn) = turns.get(turn_id) {
            for request in &turn.requests {
                if request.state != "answered"
                    && !resolutions
                        .iter()
                        .any(|resolution| resolution.request_id == request.request_id)
                {
                    resolutions.push(QuestionResolution {
                        request_id: request.request_id.clone(),
                        turn_id: turn_id.clone(),
                        state: request.state.clone(),
                        answers: request.answers.clone(),
                        user_message_id: None,
                        client_response_id: None,
                        user_message: None,
                    });
                }
            }
            for answer in turn
                .submission_order
                .iter()
                .filter_map(|id| turn.submissions.get(id))
                .filter(|answer| answer.ready)
            {
                let request = turn
                    .requests
                    .iter()
                    .find(|request| request.request_id == answer.metadata.request_id)
                    .expect("registered request");
                resolutions.push(QuestionResolution {
                    request_id: request.request_id.clone(),
                    turn_id: turn_id.clone(),
                    state: request.state.clone(),
                    answers: request.answers.clone(),
                    user_message_id: Some(answer.message.uuid.clone()),
                    client_response_id: Some(answer.metadata.client_response_id.clone()),
                    user_message: Some(answer.message.clone()),
                });
            }
        }
        resolutions
    }

    pub fn close(&self, turn_id: &RuntimeString, state: &str) -> Vec<QuestionResolution> {
        let resolutions = {
            let mut turns = self.turns.lock().unwrap_or_else(|error| error.into_inner());
            let Some(turn) = turns.get_mut(turn_id) else {
                return Vec::new();
            };
            turn.open = false;
            turn.pending.clear();
            let resolutions = turn
                .requests
                .iter_mut()
                .filter(|request| {
                    request.state != "answered"
                        && request.state != "expired"
                        && request.state != "cancelled"
                })
                .map(|request| {
                    request.state = state.into();
                    QuestionResolution {
                        request_id: request.request_id.clone(),
                        turn_id: turn_id.clone(),
                        state: state.into(),
                        answers: request.answers.clone(),
                        user_message_id: None,
                        client_response_id: None,
                        user_message: None,
                    }
                })
                .collect::<Vec<_>>();
            resolutions
        };
        self.changed.notify_waiters();
        resolutions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;

    fn request() -> ElicitRequest {
        let input=runtime_json::parse(&RuntimeString::from(r#"{"question":"Pick?","header":"Pick","options":[{"label":"A","description":"first"},{"label":"B","description":"second"}],"multiSelect":false}"#)).unwrap();
        ElicitRequest {
            prompt: "Pick?".into(),
            questions: Some(vec![input.as_object().unwrap().clone()]),
            attachments_supported: Some(true),
            metadata: None,
            options: None,
            multi_select: None,
        }
    }

    #[tokio::test]
    async fn async_question_publication_cannot_resurrect_an_already_answered_request() {
        let registry = Arc::new(AsyncQuestionRegistry::default());
        registry.open("turn".into(), 1, CancellationToken::new());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let events = Arc::new(Mutex::new(Vec::new()));
        let context = AsyncQuestionContext {
            registry: registry.clone(),
            turn_id: "turn".into(),
            generation: 1,
            tool_call_id: "call".into(),
            emit: Some(Arc::new({
                let events = events.clone();
                move |event| events.lock().unwrap().push(event)
            })),
            persist: Arc::new({
                let entered = entered.clone();
                let release = release.clone();
                move |_| {
                    let entered = entered.clone();
                    let release = release.clone();
                    Box::pin(async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                }
            }),
        };
        let asking = tokio::spawn(async move { context.ask(request(), "question".into()).await });
        entered.notified().await;
        assert_eq!(registry.pending().len(), 1);
        let mut answers = RuntimeMap::new();
        answers.insert("Pick?".into(), "A".into());
        registry
            .prepare(
                &"turn".into(),
                &"question".into(),
                &"response".into(),
                answers,
                None,
                "user".into(),
            )
            .unwrap();
        registry.commit(&"turn".into(), &"response".into()).unwrap();
        release.notify_one();
        let record = asking.await.unwrap().unwrap();
        assert_eq!(record.state, "answered");
        assert_eq!(record.answers.get(&"Pick?".into()), Some(&"A".into()));
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], EngineEvent::QuestionResolved { .. }));
    }

    #[tokio::test]
    async fn async_question_registry_is_bounded_and_closes_answer_admission() {
        let registry = Arc::new(AsyncQuestionRegistry::default());
        let signal = CancellationToken::new();
        registry.open("turn".into(), 4, signal.clone());
        let context = AsyncQuestionContext {
            registry: registry.clone(),
            turn_id: "turn".into(),
            generation: 4,
            tool_call_id: "call".into(),
            emit: None,
            persist: Arc::new(|_| Box::pin(async { Ok(()) })),
        };
        for index in 0..MAX_ASYNC_QUESTIONS_PER_TURN {
            context
                .ask(request(), format!("question-{index}").into())
                .await
                .unwrap();
        }
        assert!(context.ask(request(), "overflow".into()).await.is_err());
        assert_eq!(registry.pending().len(), MAX_ASYNC_QUESTIONS_PER_TURN);
        let events = registry.close(&context.turn_id, "expired");
        assert_eq!(events.len(), MAX_ASYNC_QUESTIONS_PER_TURN);
        assert!(registry.pending().is_empty());
        let mut answers = RuntimeMap::new();
        answers.insert("Pick?".into(), "A".into());
        assert!(registry
            .prepare(
                &context.turn_id,
                &"question-0".into(),
                &"late".into(),
                answers,
                None,
                "user".into()
            )
            .is_err());
        registry.open("next-turn".into(), 5, signal);
        assert!(context
            .ask(request(), "wrong-generation".into())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn async_question_duplicate_retry_waits_for_durability_before_success() {
        let registry = Arc::new(AsyncQuestionRegistry::default());
        registry.open("turn".into(), 1, CancellationToken::new());
        let context = AsyncQuestionContext {
            registry: registry.clone(),
            turn_id: "turn".into(),
            generation: 1,
            tool_call_id: "call".into(),
            emit: None,
            persist: Arc::new(|_| Box::pin(async { Ok(()) })),
        };
        context.ask(request(), "question".into()).await.unwrap();
        let mut answers = RuntimeMap::new();
        answers.insert("Pick?".into(), "A".into());
        assert!(matches!(
            registry
                .prepare(
                    &"turn".into(),
                    &"question".into(),
                    &"response".into(),
                    answers.clone(),
                    None,
                    "user".into()
                )
                .unwrap(),
            PrepareAnswer::New(_)
        ));
        assert!(matches!(
            registry
                .prepare(
                    &"turn".into(),
                    &"question".into(),
                    &"response".into(),
                    answers,
                    None,
                    "duplicate-user".into()
                )
                .unwrap(),
            PrepareAnswer::Existing(_)
        ));
        let waiting = {
            let registry = registry.clone();
            tokio::spawn(async move {
                registry
                    .wait_receipt(&"turn".into(), &"response".into())
                    .await
            })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        registry.commit(&"turn".into(), &"response".into()).unwrap();
        assert_eq!(waiting.await.unwrap().unwrap().user_message_id, "user");
        let pending = registry.take_ready(&"turn".into());
        assert_eq!(pending.len(), 1);
        assert!(registry.take_ready(&"turn".into()).is_empty());
        let event = EngineEvent::QuestionResolved {
            question_resolved: QuestionResolution {
                request_id: "question".into(),
                turn_id: "turn".into(),
                state: "answered".into(),
                answers: RuntimeMap::new(),
                user_message_id: Some("user".into()),
                client_response_id: Some("response".into()),
                user_message: Some(pending[0].clone()),
            },
        };
        let encoded = runtime_codec::encode_value(&event).unwrap();
        let decoded = runtime_codec::decode_value::<EngineEvent>(&encoded).unwrap();
        assert_eq!(runtime_codec::encode_value(&decoded).unwrap(), encoded);
    }
    #[tokio::test]
    async fn async_question_terminal_settlement_preserves_an_admitted_durable_reply() {
        let registry = Arc::new(AsyncQuestionRegistry::default());
        let signal = CancellationToken::new();
        registry.open("turn".into(), 1, signal.clone());
        let context = AsyncQuestionContext {
            registry: registry.clone(),
            turn_id: "turn".into(),
            generation: 1,
            tool_call_id: "call".into(),
            emit: None,
            persist: Arc::new(|_| Box::pin(async { Ok(()) })),
        };
        context.ask(request(), "question".into()).await.unwrap();
        let mut answers = RuntimeMap::new();
        answers.insert("Pick?".into(), "A".into());
        registry
            .prepare(
                &"turn".into(),
                &"question".into(),
                &"response".into(),
                answers,
                None,
                "user".into(),
            )
            .unwrap();
        signal.cancel();
        let finishing = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.finish(&"turn".into(), "cancelled").await })
        };
        tokio::task::yield_now().await;
        assert!(
            !finishing.is_finished(),
            "terminal events wait for an admitted write"
        );
        let receipt = registry.commit(&"turn".into(), &"response".into()).unwrap();
        assert_eq!(receipt.state, "cancelled");
        let events = finishing.await.unwrap();
        assert!(events.iter().any(|event| event.state == "cancelled"
            && event
                .user_message
                .as_ref()
                .is_some_and(|message| message.uuid == "user")));
        assert!(registry.pending().is_empty());
        assert!(
            registry.take_ready(&"turn".into()).is_empty(),
            "the answer cannot spill into another turn"
        );
    }
    #[tokio::test]
    async fn async_question_failed_write_notifies_reconnected_submitting_clients() {
        let registry = Arc::new(AsyncQuestionRegistry::default());
        registry.open("turn".into(), 1, CancellationToken::new());
        let events = Arc::new(Mutex::new(Vec::new()));
        let context = AsyncQuestionContext {
            registry: registry.clone(),
            turn_id: "turn".into(),
            generation: 1,
            tool_call_id: "call".into(),
            emit: Some(Arc::new({
                let events = events.clone();
                move |event| events.lock().unwrap().push(event)
            })),
            persist: Arc::new(|_| Box::pin(async { Ok(()) })),
        };
        context.ask(request(), "question".into()).await.unwrap();
        let mut answers = RuntimeMap::new();
        answers.insert("Pick?".into(), "A".into());
        registry
            .prepare(
                &"turn".into(),
                &"question".into(),
                &"response".into(),
                answers,
                None,
                "user".into(),
            )
            .unwrap();
        assert_eq!(registry.pending()[0].state, "submitting");
        registry.fail(&"turn".into(), &"response".into());
        assert_eq!(registry.pending()[0].state, "pending");
        assert!(registry
            .wait_receipt(&"turn".into(), &"response".into())
            .await
            .is_err());
        assert!(
            matches!(events.lock().unwrap().last(), Some(EngineEvent::QuestionResolved { question_resolved }) if question_resolved.state == "pending" && question_resolved.answers.is_empty() && question_resolved.user_message.is_none())
        );
    }
}
