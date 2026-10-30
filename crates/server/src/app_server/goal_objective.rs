//! Provider-free objective previews addressed by the current goal generation.

use super::*;

pub(super) async fn handle_read_goal_objective(
    state: &Arc<AppServerState>,
    id: &Value,
    raw: RawJson,
) {
    let params = match raw.deserialize::<wire::SessionReadGoalObjectiveParams>() {
        Ok(params) => params,
        Err(error) => return send_invalid_params(state, id, &error).await,
    };
    if params.expected_goal_id.trim().is_empty()
        || params.expected_goal_id.trim() != params.expected_goal_id
    {
        return send_invalid_params_message(
            state,
            id,
            "expectedGoalId must name a current goal generation",
        )
        .await;
    }
    if params.session_id.is_none() && params.thread_id.is_none() {
        return send_invalid_params_message(state, id, "sessionId or threadId is required").await;
    }
    if params
        .thread_id
        .as_ref()
        .is_some_and(|thread| thread.trim().is_empty() || thread.trim() != thread)
    {
        return send_invalid_params_message(
            state,
            id,
            "threadId must be nonempty without surrounding whitespace",
        )
        .await;
    }
    let handle = params
        .session_id
        .as_deref()
        .and_then(|session_id| state.session(session_id))
        .or_else(|| {
            params.thread_id.as_deref().and_then(|thread_id| {
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
    let thread_id = if let Some(handle) = handle {
        let Some(session) = handle.ps.existing_session() else {
            return send_invalid_params_message(
                state,
                id,
                "objective previews require a bound thread and do not initialize inference",
            )
            .await;
        };
        if params
            .thread_id
            .as_ref()
            .is_some_and(|thread| session.id != thread.as_str())
        {
            return send_invalid_params_message(
                state,
                id,
                "sessionId and threadId identify different threads",
            )
            .await;
        }
        match session.id.try_to_utf8() {
            Ok(thread_id) => thread_id,
            Err(error) => return send_invalid_params_message(state, id, &error.to_string()).await,
        }
    } else {
        let Some(thread_id) = params.thread_id else {
            return send_unknown_session(
                state,
                id,
                params
                    .session_id
                    .as_deref()
                    .expect("an address was required"),
            )
            .await;
        };
        if state
            .lifecycle
            .as_ref()
            .is_none_or(|lifecycle| lifecycle.saved_goal_request.is_none())
        {
            return send_invalid_params_message(
                state,
                id,
                "saved goal access requires a session store",
            )
            .await;
        }
        thread_id
    };
    let state = state.clone();
    let id = id.clone();
    tokio::spawn(async move {
        let result = async {
            let preview = klaus_agent_core::goal::objective_preview::read_objective(
                &thread_id,
                &params.expected_goal_id.into(),
            )
            .await?;
            Ok::<_, std::io::Error>(wire::SessionReadGoalObjectiveResult {
                goal_id: preview.goal_id.try_to_utf8().map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                })?,
                objective: preview.objective,
            })
        }
        .await;
        match result {
            Ok(result) => {
                state
                    .send(exact_payload::response(
                        &id,
                        Payload::Encoded(
                            RawJson::from_serialize(&result).expect("objective preview is JSON"),
                        ),
                    ))
                    .await
            }
            Err(error) => {
                let message = error.to_string();
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
