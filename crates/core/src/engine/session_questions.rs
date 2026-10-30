use super::*;
use crate::engine::questions::{
    AsyncQuestion, AsyncQuestionContext, PrepareAnswer, QuestionAnswerReceipt,
};
use crate::runtime_values::RuntimeMap;
use crate::store::session_store::AppendEventOptions;

impl Session {
    pub(crate) fn async_question_context(
        self: &Arc<Self>,
        turn_id: RuntimeString,
        generation: u64,
    ) -> AsyncQuestionContext {
        let weak = Arc::downgrade(self);
        AsyncQuestionContext {
            registry: self.async_questions.clone(),
            turn_id,
            generation,
            tool_call_id: RuntimeString::default(),
            emit: None,
            persist: Arc::new(move |question| {
                let weak = weak.clone();
                Box::pin(async move {
                    let session = weak
                        .upgrade()
                        .ok_or_else(|| RuntimeString::from("The question session has closed"))?;
                    if let Some(rollout) = session.persistence.rollout() {
                        rollout
                            .append_event(
                                "async_question".into(),
                                Some(AppendEventOptions {
                                    turn_id: Some(question.turn_id.clone()),
                                    payload: Some(
                                        runtime_codec::encode_value(&question)
                                            .expect("question record codec"),
                                    ),
                                }),
                            )
                            .await;
                    }
                    session.flush_rollout().await.map_err(RuntimeString::from)
                })
            }),
        }
    }

    pub fn pending_questions(&self) -> Vec<AsyncQuestion> {
        self.async_questions.pending()
    }

    pub async fn answer_question(
        &self,
        turn_id: RuntimeString,
        request_id: RuntimeString,
        client_response_id: RuntimeString,
        answers: RuntimeMap<RuntimeString>,
        attachments: Option<Box<RuntimeMap<Vec<ContentBlock>>>>,
    ) -> Result<QuestionAnswerReceipt, RuntimeString> {
        let admission = self.async_questions.prepare(
            &turn_id,
            &request_id,
            &client_response_id,
            answers,
            attachments,
            (self.deps.uuid)().into(),
        )?;
        let user = match admission {
            PrepareAnswer::Existing(_) => {
                return self
                    .async_questions
                    .wait_receipt(&turn_id, &client_response_id)
                    .await
            }
            PrepareAnswer::New(user) => user,
        };
        if user
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Image(_)))
            && !self
                .inference_snapshot()
                .provider
                .capabilities(&self.steering_model())
                .vision
        {
            self.async_questions.fail(&turn_id, &client_response_id);
            return Err("The current model cannot read image attachments; select a vision-capable model before submitting".into());
        }
        let message = Message::User(user.clone());
        if let Some(rollout) = self.persistence.rollout() {
            rollout.append_message(message.clone()).await;
        }
        if let Err(error) = self.flush_rollout().await {
            self.async_questions.fail(&turn_id, &client_response_id);
            return Err(RuntimeString::from(format!(
                "Failed to persist the question answer: {error}"
            )));
        }
        self.push_history(message.clone());
        self.turn_index
            .lock_recover()
            .message_turn
            .insert(user.uuid.clone(), turn_id.clone());
        let committed = self.async_questions.commit(&turn_id, &client_response_id);
        if committed.is_ok() {
            self.capture_review_message(&message, &turn_id).await;
        }
        committed
    }
}
