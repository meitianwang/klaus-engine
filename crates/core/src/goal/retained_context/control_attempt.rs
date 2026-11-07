//! Scoped human control attempts acquire authorization only through exact committed receipts.

use super::*;
use std::collections::HashSet;

pub const GOAL_CONTROL_ATTEMPT_EVENT: &str = "user_goal_control_attempt";
pub const GOAL_CONTROL_DISPOSITION_EVENT: &str = "user_goal_control_disposition";

#[derive(Debug, Clone)]
pub struct GoalControlAttempt {
    pub request_id: RuntimeString,
    pub fingerprint: String,
    pub foreground_submission_id: Option<RuntimeString>,
    pub instruction: GoalInstruction,
}

pub fn pause_control_attempt(
    id: RuntimeString,
    foreground_submission_id: Option<RuntimeString>,
    mutation: &crate::goal::service::HostGoalMutation,
) -> io::Result<GoalControlAttempt> {
    if mutation.objective.is_some()
        || mutation.token_budget.is_some()
        || mutation.owned_resources.is_some()
        || mutation
            .status
            .as_ref()
            .is_none_or(|status| *status != "paused")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a scoped pause attempt cannot grant other goal controls",
        ));
    }
    Ok(GoalControlAttempt {
        request_id: id.clone(),
        fingerprint: crate::goal::service::request_fingerprint(mutation)?,
        foreground_submission_id,
        instruction: instruction_for_set(id, None, Some(&RuntimeString::from("paused")))?
            .expect("pause has a status"),
    })
}

pub async fn append_control_attempt(
    rollout: &dyn LiveRollout,
    attempt: &GoalControlAttempt,
) -> io::Result<()> {
    let mut payload = RuntimeObject::new();
    payload.insert("version", RuntimeJson::from(1));
    payload.insert("origin", RuntimeJson::String("user".into()));
    payload.insert("requestId", attempt.request_id.clone().into());
    payload.insert(
        "fingerprint",
        RuntimeString::from(attempt.fingerprint.clone()).into(),
    );
    payload.insert("instruction", attempt.instruction.payload.clone());
    if let Some(foreground) = &attempt.foreground_submission_id {
        payload.insert("foregroundSubmissionId", foreground.clone().into());
    }
    rollout
        .append_event(
            GOAL_CONTROL_ATTEMPT_EVENT.into(),
            Some(AppendEventOptions {
                turn_id: None,
                payload: Some(RuntimeJson::Object(payload)),
            }),
        )
        .await;
    rollout.flush().await
}

pub async fn control_attempt_was_applied(
    thread: &str,
    attempt: &GoalControlAttempt,
) -> io::Result<bool> {
    let request = attempt
        .request_id
        .try_to_utf8()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    let receipt = crate::goal::store::get_goal_request_receipt(thread, &request).await?;
    match receipt {
        None => Ok(false),
        Some(receipt) if receipt.fingerprint == attempt.fingerprint => Ok(true),
        Some(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "goal control receipt does not match its retained intent",
        )),
    }
}

pub async fn append_control_disposition(
    rollout: &dyn LiveRollout,
    attempt: &GoalControlAttempt,
    accepted: bool,
    error: Option<&RuntimeString>,
) -> io::Result<()> {
    let mut payload = RuntimeObject::new();
    payload.insert("version", RuntimeJson::from(1));
    payload.insert("origin", RuntimeJson::String("user".into()));
    payload.insert("requestId", attempt.request_id.clone().into());
    payload.insert(
        "fingerprint",
        RuntimeString::from(attempt.fingerprint.clone()).into(),
    );
    payload.insert("accepted", RuntimeJson::Bool(accepted));
    if let Some(error) = error {
        let reason: String = error
            .try_to_utf8()
            .unwrap_or_else(|_| "goal control failed".into())
            .chars()
            .take(1024)
            .collect();
        payload.insert("reason", RuntimeString::from(reason).into());
    }
    rollout
        .append_event(
            GOAL_CONTROL_DISPOSITION_EVENT.into(),
            Some(AppendEventOptions {
                turn_id: None,
                payload: Some(RuntimeJson::Object(payload)),
            }),
        )
        .await;
    rollout.flush().await
}

fn parse_attempt(item: &RolloutItem) -> Option<GoalControlAttempt> {
    let RolloutItem::Event(event) = item else {
        return None;
    };
    if event.event != GOAL_CONTROL_ATTEMPT_EVENT {
        return None;
    }
    let payload = event.payload.as_ref()?;
    if payload.get("version")?.as_number()?.as_f64() != 1.0
        || payload.get("origin")?.as_str()? != "user"
    {
        return None;
    }
    let request_id = payload.get("requestId")?.as_str()?.clone();
    let fingerprint = payload.get("fingerprint")?.as_str()?.try_to_utf8().ok()?;
    if request_id.is_empty()
        || fingerprint.len() != 64
        || !fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let instruction_payload = payload.get("instruction")?;
    if instruction_payload.get("id")?.as_str()? != &request_id
        || instruction_payload.get("origin")?.as_str()? != "user"
        || instruction_payload.get("version")?.as_number()?.as_f64() != 1.0
        || instruction_payload.get("action")?.as_str()? != "set"
        || instruction_payload.get("status")?.as_str()? != "paused"
        || instruction_payload.get("objective").is_some()
    {
        return None;
    }
    Some(GoalControlAttempt {
        request_id: request_id.clone(),
        fingerprint,
        foreground_submission_id: payload
            .get("foregroundSubmissionId")
            .and_then(RuntimeJson::as_str)
            .cloned(),
        instruction: GoalInstruction {
            id: request_id,
            payload: instruction_payload.clone(),
        },
    })
}

pub fn applied_control_instruction(attempt: &GoalControlAttempt) -> GoalInstruction {
    let mut instruction = attempt.instruction.clone();
    if let RuntimeJson::Object(payload) = &mut instruction.payload {
        payload.insert("controlAttemptId", attempt.request_id.clone().into());
        payload.insert("disposition", RuntimeJson::String("applied".into()));
        if let Some(foreground) = &attempt.foreground_submission_id {
            payload.insert("foregroundSubmissionId", foreground.clone().into());
        }
    }
    instruction
}

pub(super) fn applied_dispositions(items: &[RolloutItem]) -> HashSet<(RuntimeString, String)> {
    items
        .iter()
        .filter_map(|item| {
            let RolloutItem::Event(event) = item else {
                return None;
            };
            if event.event != GOAL_CONTROL_DISPOSITION_EVENT {
                return None;
            }
            let payload = event.payload.as_ref()?;
            if payload.get("version")?.as_number()?.as_f64() != 1.0
                || payload.get("origin")?.as_str()? != "user"
                || payload.get("accepted")?.as_bool() != Some(true)
            {
                return None;
            }
            Some((
                payload.get("requestId")?.as_str()?.clone(),
                payload.get("fingerprint")?.as_str()?.try_to_utf8().ok()?,
            ))
        })
        .collect()
}

pub(super) fn project(
    items: &[RolloutItem],
    applied: &HashSet<(RuntimeString, String)>,
) -> Vec<Message> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for item in items {
        let instruction = super::normal_instruction(item).or_else(|| {
            let attempt = parse_attempt(item)?;
            applied
                .contains(&(attempt.request_id.clone(), attempt.fingerprint.clone()))
                .then(|| applied_control_instruction(&attempt))
        });
        if let Some(instruction) = instruction {
            if seen.insert(instruction.id.clone()) {
                result.push(super::instruction_message(&instruction));
            }
        }
    }
    result
}

/// Exact receipts resolve the commit/disposition crash gap without examining current goal state.
pub async fn instructions_from_rollout_with_receipts(
    thread: &str,
    items: &[RolloutItem],
) -> io::Result<Vec<Message>> {
    let mut applied = HashSet::new();
    let mut seen = HashSet::new();
    for attempt in items.iter().filter_map(parse_attempt) {
        if seen.insert(attempt.request_id.clone())
            && control_attempt_was_applied(thread, &attempt).await?
        {
            applied.insert((attempt.request_id, attempt.fingerprint));
        }
    }
    Ok(project(items, &applied))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goal::service::HostGoalMutation;
    use crate::schemas::rollout::EventItem;

    fn attempt_item(attempt: &GoalControlAttempt) -> RolloutItem {
        let mut payload = RuntimeObject::new();
        payload.insert("version", RuntimeJson::from(1));
        payload.insert("origin", "user".into());
        payload.insert("requestId", attempt.request_id.clone().into());
        payload.insert("fingerprint", attempt.fingerprint.clone().into());
        payload.insert("instruction", attempt.instruction.payload.clone());
        RolloutItem::Event(EventItem {
            event: GOAL_CONTROL_ATTEMPT_EVENT.into(),
            turn_id: None,
            payload: Some(RuntimeJson::Object(payload)),
        })
    }

    fn disposition(attempt: &GoalControlAttempt, accepted: bool) -> RolloutItem {
        let mut payload = RuntimeObject::new();
        payload.insert("version", RuntimeJson::from(1));
        payload.insert("origin", "user".into());
        payload.insert("requestId", attempt.request_id.clone().into());
        payload.insert("fingerprint", attempt.fingerprint.clone().into());
        payload.insert("accepted", RuntimeJson::Bool(accepted));
        RolloutItem::Event(EventItem {
            event: GOAL_CONTROL_DISPOSITION_EVENT.into(),
            turn_id: None,
            payload: Some(RuntimeJson::Object(payload)),
        })
    }

    #[test]
    fn pending_rejected_and_spoofed_pause_attempts_never_grant_authorization() {
        let mutation = HostGoalMutation {
            status: Some("paused".into()),
            ..Default::default()
        };
        let attempt =
            pause_control_attempt("attempt-id".into(), Some("foreground".into()), &mutation)
                .unwrap();
        let pending = attempt_item(&attempt);
        assert!(instructions_from_rollout(std::slice::from_ref(&pending)).is_empty());
        assert!(
            instructions_from_rollout(&[pending.clone(), disposition(&attempt, false)]).is_empty()
        );
        let mut forged = pending.clone();
        if let RolloutItem::Event(event) = &mut forged {
            event.event = "tool_output".into();
        }
        assert!(instructions_from_rollout(&[forged, disposition(&attempt, true)]).is_empty());
        let mut mismatch = disposition(&attempt, true);
        if let RolloutItem::Event(event) = &mut mismatch {
            if let Some(RuntimeJson::Object(payload)) = &mut event.payload {
                payload.insert("fingerprint", "a".repeat(64).into());
            }
        }
        assert!(instructions_from_rollout(&[pending, mismatch]).is_empty());
    }

    #[test]
    fn accepted_pause_is_projected_at_its_attempt_position_once() {
        let mutation = HostGoalMutation {
            status: Some("paused".into()),
            ..Default::default()
        };
        let attempt = pause_control_attempt("attempt-id".into(), None, &mutation).unwrap();
        let clear = clear_instruction("later-clear".into());
        let clear_item = RolloutItem::Event(EventItem {
            event: GOAL_INSTRUCTION_EVENT.into(),
            turn_id: None,
            payload: Some(clear.payload),
        });
        let items = [
            attempt_item(&attempt),
            clear_item,
            disposition(&attempt, true),
            attempt_item(&attempt),
        ];
        let projected = instructions_from_rollout(&items);
        let ids: Vec<_> = projected
            .iter()
            .filter_map(|message| match message {
                Message::User(user) => Some(user.uuid.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            ids,
            vec![
                RuntimeString::from("attempt-id"),
                RuntimeString::from("later-clear")
            ]
        );
        assert!(pause_control_attempt(
            "invalid".into(),
            None,
            &HostGoalMutation {
                objective: Some("unrelated objective".into()),
                ..mutation
            }
        )
        .is_err());
    }
}
