//! Address goal state through loaded sessions or the saved-thread store.

use super::*;
use klaus_agent_core::goal::service::{HostGoalMutation, SavedGoalRequest};

pub(super) async fn handle_goal_request(
    state: &Arc<AppServerState>,
    id: &Value,
    method: &str,
    raw: RawJson,
) {
    let set = if method == "session/setGoal" {
        match raw.deserialize::<wire::SessionSetGoalParams>() {
            Ok(value) => Some(value),
            Err(error) => return send_invalid_params(state, id, &error).await,
        }
    } else {
        None
    };
    let target = match &set {
        Some(set) => set.target.clone(),
        None => match raw.deserialize::<wire::SessionGoalParams>() {
            Ok(value) => value,
            Err(error) => return send_invalid_params(state, id, &error).await,
        },
    };
    if method == "session/clearGoal" && target.expected_goal_id == Some(None) {
        return send_invalid_params_message(
            state,
            id,
            "clearGoal expectedGoalId must name a generation or be omitted",
        )
        .await;
    }
    if target.session_id.is_none() && target.thread_id.is_none() {
        return send_invalid_params_message(state, id, "sessionId or threadId is required").await;
    }
    if set.as_ref().is_some_and(|set| {
        set.owned_resources.is_some()
            && (target.origin != Some(wire::GoalOrigin::User)
                || set.objective.as_ref().is_none_or(runtime_text_is_blank))
    }) {
        return send_invalid_params_message(
            state,
            id,
            "ownedResources requires an explicit user objective",
        )
        .await;
    }
    let requested_thread = target.thread_id.as_deref();
    let handle = target
        .session_id
        .as_deref()
        .and_then(|session_id| state.session(session_id))
        .or_else(|| {
            requested_thread.and_then(|thread_id| {
                state
                    .sessions
                    .lock()
                    .unwrap()
                    .values()
                    .find(|handle| {
                        handle
                            .ps
                            .existing_session()
                            .is_some_and(|session| session.id == thread_id)
                    })
                    .cloned()
            })
        });
    if let Some(handle) = handle {
        if let (Some(thread_id), Some(session)) = (requested_thread, handle.ps.existing_session()) {
            if session.id != thread_id {
                return send_invalid_params_message(
                    state,
                    id,
                    "sessionId and threadId identify different threads",
                )
                .await;
            }
        }
        let mut op = RuntimeObject::new();
        op.insert(
            "type",
            match method {
                "session/goal" => "get_goal",
                "session/clearGoal" => "clear_goal",
                _ => "set_goal",
            }
            .into(),
        );
        if let Some(expected) = target.expected_goal_id {
            op.insert(
                "expectedGoalId",
                expected.map(RuntimeJson::from).unwrap_or(RuntimeJson::Null),
            );
        }
        if let Some(revision) = target.expected_goal_revision {
            op.insert("expectedGoalRevision", revision.into());
        }
        if let Some(origin) = target.origin {
            op.insert(
                "origin",
                match origin {
                    wire::GoalOrigin::User => "user",
                    wire::GoalOrigin::Automatic => "automatic",
                }
                .into(),
            );
        }
        if let Some(set) = set {
            if let Some(value) = set.owned_resources {
                op.insert(
                    "ownedResources",
                    exact_payload::runtime(
                        serde_json::to_value(value).expect("owned resource metadata is JSON"),
                    ),
                );
            }
            if let Some(value) = set.client_request_id {
                op.insert("clientRequestId", value.into());
            }
            if let Some(value) = set.objective {
                op.insert("objective", value.into());
            }
            if let Some(value) = set.status {
                op.insert("status", value.into());
            }
            if let Some(value) = set.token_budget {
                op.insert(
                    "tokenBudget",
                    value.map(RuntimeJson::from).unwrap_or(RuntimeJson::Null),
                );
            }
            if let Some(value) = set.submit {
                op.insert("submit", value.into());
            }
            if let Some(value) = set.display_text {
                op.insert("displayText", value.into());
            }
        }
        return forward_promoted_request(
            state.clone(),
            handle,
            id.clone(),
            RuntimeJson::Object(op),
            target.submission_id,
            None,
        )
        .await;
    }
    if let Some(session_id) = target.session_id.as_deref() {
        if target.thread_id.is_none() {
            return send_unknown_session(state, id, session_id).await;
        }
    }
    let thread_id = target.thread_id.unwrap();
    if thread_id.trim().is_empty() || thread_id.trim() != thread_id {
        return send_invalid_params_message(
            state,
            id,
            "threadId must be nonempty without surrounding whitespace",
        )
        .await;
    }
    let Some(handler) = state
        .lifecycle
        .as_ref()
        .and_then(|lifecycle| lifecycle.saved_goal_request.clone())
    else {
        return send_invalid_params_message(
            state,
            id,
            "saved goal access requires a session store",
        )
        .await;
    };
    let user_origin = target.origin == Some(wire::GoalOrigin::User);
    let request = match method {
        "session/goal" => SavedGoalRequest::Read,
        "session/clearGoal" => SavedGoalRequest::Clear {
            expected_goal_id: target.expected_goal_id.flatten().map(Into::into),
            expected_revision: target.expected_goal_revision,
            user_origin,
            automatic: target.origin == Some(wire::GoalOrigin::Automatic),
        },
        _ => {
            let set = set.unwrap();
            if set.submit == Some(true) {
                return send_invalid_params_message(
                    state,
                    id,
                    "load the thread before submitting a goal objective",
                )
                .await;
            }
            SavedGoalRequest::Set {
                mutation: HostGoalMutation {
                    owned_resources: set.owned_resources.map(Into::into),
                    objective: set.objective,
                    status: set.status.map(Into::into),
                    token_budget: set.token_budget,
                    expected_absent: target.expected_goal_id == Some(None),
                    expected_goal_id: target.expected_goal_id.flatten().map(Into::into),
                    expected_revision: target.expected_goal_revision,
                    require_safe_json: true,
                    control_guard: None,
                },
                user_origin,
            }
        }
    };
    let state = state.clone();
    let id = id.clone();
    let is_clear = method == "session/clearGoal";
    tokio::spawn(async move {
        let response = async {
            let eligibility = klaus_agent_core::goal::store::thread_goal_eligibility(&thread_id)
                .await
                .map_err(|error| error.to_string())?;
            let snapshot = handler(thread_id, request)
                .await
                .map_err(|error| error.to_string())?;
            let event = klaus_agent_protocol::types::ProtocolEvent {
                submission_id: Default::default(),
                thread_goal: snapshot
                    .goal
                    .as_ref()
                    .map(klaus_agent_protocol::types::ProtocolThreadGoal::from),
                goal_revision: Some(snapshot.revision),
                goal_cleared: (is_clear && snapshot.goal.is_none()).then_some(true),
                goal_pursuit_eligible: Some(eligibility.unwrap_or(false)),
                goal_pursuit_eligibility_notice: eligibility.is_none().then(|| {
                    "Open this thread to determine whether goal pursuit is available.".into()
                }),
                ..Default::default()
            };
            let mut value = klaus_agent_core::runtime_values::runtime_codec::encode_value(&event)
                .map_err(|error| error.to_string())?;
            value
                .as_object_mut()
                .expect("the protocol reply is an object")
                .remove("submissionId");
            Ok::<_, String>(value)
        }
        .await;
        match response {
            Ok(result) => state.send(exact_payload::response(&id, result)).await,
            Err(message) => {
                if let Some(message) =
                    message.strip_prefix(klaus_agent_core::goal::service::GOAL_CONFLICT_PREFIX)
                {
                    state
                        .send(wire::error_response_with_data(
                            &id,
                            wire::ENGINE_ERROR,
                            message,
                            serde_json::json!({"code":"GOAL_CONFLICT","feature":"goal"}),
                        ))
                        .await;
                } else {
                    state
                        .send(wire::error_response(&id, wire::ENGINE_ERROR, &message))
                        .await;
                }
            }
        }
    });
}
