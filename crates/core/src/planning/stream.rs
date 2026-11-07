//! Ordered projection of final assistant text into independent plan items.
use super::parser::{PlanSegment, ProposedPlanParser};
use super::runtime::{host, PlanningRuntime};
use super::types::CollaborationMode;
use crate::engine::session::{EngineEventStream, Session};
use crate::runtime_values::RuntimeString;
use crate::types::errors::TerminalReason;
use crate::types::events::{EngineEvent, InferenceIdentity, NotificationLevel, ThreadItem};
use async_stream::stream;
use futures::StreamExt;
use std::sync::Arc;

struct Projection {
    runtime: Arc<PlanningRuntime>,
    turn_id: RuntimeString,
    parser: ProposedPlanParser,
    current_plan: Option<RuntimeString>,
    plan_closed: bool,
    failed: bool,
    source_id: Option<RuntimeString>,
    source_phase: Option<RuntimeString>,
    source_inference: Option<InferenceIdentity>,
    source_segments: u64,
    fallback_ordinal: u64,
    text_id: RuntimeString,
    text: Vec<u16>,
    deferred_text: Vec<EngineEvent>,
}

impl Projection {
    fn source(
        &mut self,
        id: Option<&RuntimeString>,
        phase: &Option<RuntimeString>,
        inference: &Option<InferenceIdentity>,
    ) {
        if id.is_some() && self.source_id.as_ref() != id {
            self.source_id = id.cloned();
            self.source_segments = 0;
        }
        self.source_phase = phase.clone();
        self.source_inference = inference.clone();
    }

    fn finish_text(&mut self, output: &mut Vec<EngineEvent>, defer: bool) {
        if self.text.is_empty() {
            return;
        }
        let event = EngineEvent::ItemCompleted {
            item: ThreadItem::AgentMessage {
                id: self.text_id.clone(),
                text: RuntimeString::from_utf16(std::mem::take(&mut self.text)),
                phase: self.source_phase.clone(),
            },
            inference: self.source_inference.clone(),
        };
        if defer {
            self.deferred_text.push(event);
        } else {
            output.push(event);
        }
    }

    fn project(&mut self, event: EngineEvent) -> Vec<EngineEvent> {
        let mut output = Vec::new();
        let mut final_item = false;
        let mut terminal = false;
        let segments = match &event {
            EngineEvent::TextDelta {
                text,
                message_id,
                phase,
                inference,
            } if phase.as_ref().is_none_or(|p| p != "commentary") => {
                self.source(message_id.as_ref(), phase, inference);
                self.parser.push_runtime(text)
            }
            EngineEvent::ItemStarted {
                item: ThreadItem::AgentMessage { id, phase, .. },
                inference,
            } if phase.as_ref().is_none_or(|p| p != "commentary") => {
                self.source(Some(id), phase, inference);
                return output;
            }
            EngineEvent::ItemUpdated {
                item: ThreadItem::AgentMessage { phase, .. },
                ..
            } if phase.as_ref().is_none_or(|p| p != "commentary") => return output,
            EngineEvent::ItemCompleted {
                item: ThreadItem::AgentMessage { phase, .. },
                ..
            } if phase.as_ref().is_none_or(|p| p != "commentary") => {
                final_item = true;
                self.parser.finish_message()
            }
            EngineEvent::TurnCompleted { .. } | EngineEvent::TurnFailed { .. } => {
                terminal = true;
                self.parser.finish()
            }
            _ => {
                output.push(event);
                return output;
            }
        };
        for segment in segments {
            match segment {
                PlanSegment::Text(delta) => {
                    if delta.is_empty() {
                        continue;
                    }
                    if self.text.is_empty() {
                        self.source_segments += 1;
                        self.fallback_ordinal += 1;
                        self.text_id = match &self.source_id {
                            Some(id) if self.source_segments == 1 => id.clone(),
                            Some(id) => {
                                format!("{}-plan-text-{}", host(id), self.source_segments).into()
                            }
                            None => {
                                format!("{}-text-{}", host(&self.turn_id), self.fallback_ordinal)
                                    .into()
                            }
                        };
                        output.push(EngineEvent::ItemStarted {
                            item: ThreadItem::AgentMessage {
                                id: self.text_id.clone(),
                                text: RuntimeString::default(),
                                phase: self.source_phase.clone(),
                            },
                            inference: self.source_inference.clone(),
                        });
                    }
                    self.text.extend_from_slice(delta.as_utf16());
                    output.push(EngineEvent::TextDelta {
                        text: delta,
                        message_id: Some(self.text_id.clone()),
                        phase: self.source_phase.clone(),
                        inference: self.source_inference.clone(),
                    });
                }
                PlanSegment::Start => {
                    self.finish_text(&mut output, false);
                    let id: RuntimeString = uuid::Uuid::new_v4().to_string().into();
                    match self.runtime.start_plan(id.clone(), self.turn_id.clone()) {
                        Ok(plan) => {
                            self.current_plan = Some(id.clone());
                            output.push(EngineEvent::ItemStarted {
                                item: ThreadItem::ProposedPlan { id, plan },
                                inference: self.source_inference.clone(),
                            });
                        }
                        Err(error) => {
                            self.failed = true;
                            output.push(warning(error));
                        }
                    }
                }
                PlanSegment::Delta(delta) => {
                    if let Some(id) = &self.current_plan {
                        match self.runtime.append_plan_runtime(id, &delta) {
                            Ok(plan) => {
                                output.push(EngineEvent::PlanDelta {
                                    plan_id: id.clone(),
                                    turn_id: self.turn_id.clone(),
                                    revision: plan.revision,
                                    delta,
                                });
                                output.push(EngineEvent::ItemUpdated {
                                    item: ThreadItem::ProposedPlan {
                                        id: id.clone(),
                                        plan,
                                    },
                                    inference: self.source_inference.clone(),
                                });
                            }
                            Err(error) => {
                                self.failed = true;
                                output.push(warning(error));
                            }
                        }
                    }
                }
                PlanSegment::End => self.plan_closed = true,
            }
        }
        if final_item {
            self.finish_text(&mut output, self.current_plan.is_some());
        }
        if terminal {
            let success = matches!(
                &event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Completed,
                    ..
                }
            );
            if let Some(id) = self.current_plan.take() {
                let complete = success
                    && !self.failed
                    && self.plan_closed
                    && !self.parser.malformed()
                    && !self.parser.incomplete();
                match self.runtime.finish_plan(&id, complete) {
                    Ok(plan) => output.push(EngineEvent::ItemCompleted {
                        item: ThreadItem::ProposedPlan { id, plan },
                        inference: self.source_inference.clone(),
                    }),
                    Err(error) => {
                        self.failed = true;
                        output.push(warning(error));
                    }
                }
            }
            output.append(&mut self.deferred_text);
            self.finish_text(&mut output, false);
            output.push(event);
        }
        output
    }
}

fn warning(message: RuntimeString) -> EngineEvent {
    EngineEvent::Notification {
        level: NotificationLevel::Warn,
        message,
    }
}

impl Session {
    pub(crate) fn project_proposed_plans(
        self: &Arc<Self>,
        mut source: EngineEventStream,
        turn_id: RuntimeString,
        mode: CollaborationMode,
    ) -> EngineEventStream {
        if mode != CollaborationMode::Plan {
            return source;
        }
        let mut projection = Projection {
            runtime: self.planning.clone(),
            turn_id,
            parser: ProposedPlanParser::default(),
            current_plan: None,
            plan_closed: false,
            failed: false,
            source_id: None,
            source_phase: None,
            source_inference: None,
            source_segments: 0,
            fallback_ordinal: 0,
            text_id: RuntimeString::default(),
            text: Vec::new(),
            deferred_text: Vec::new(),
        };
        Box::pin(stream! {
            while let Some(event)=source.next().await {
                for event in projection.project(event) {yield event;}
            }
        })
    }
}
