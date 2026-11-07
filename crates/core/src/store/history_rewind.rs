//! Conversation rollback math — the `drop_last_n_user_turns` history mechanic.
//! Rewinding is measured in USER TURNS,
//! not raw messages: one "turn" is a user message plus everything the assistant
//! produced in response to it, up to (but not including) the next user message.
//!
//! Cutting the history at a user-message boundary is what makes this safe: the span
//! immediately before a user message is always a COMPLETE assistant/tool exchange, so
//! the truncated prefix never ends on a dangling tool_use without its tool_result (nor
//! the reverse). No boundary repair is needed.
//!
//! This is the conversation half of the checkpoint feature only; the filesystem is a
//! separate concern (on-disk edits are left to the client).
//!
//! It lives in the store layer (alongside `projectForPrompt`) so both the live
//! rollback path (`Session.rollback`) and the resume projection can share ONE
//! implementation — the live truncation and the replayed `rollback` marker must agree
//! exactly, and a single source of truth is what guarantees that.

use crate::schemas::message::Message;

/// Indices of user-turn boundaries: every GENUINE `user` message starts a user turn.
/// Engine-injected re-prompts (`isMeta` — an outputSchema re-sample nudge, a Stop-hook
/// continuation) are user-role but are NOT turn boundaries, so a rewind undoes a
/// whole real turn
/// rather than stopping at an injected mid-turn re-prompt.
fn user_turn_positions(messages: &[Message]) -> Vec<usize> {
    let mut positions: Vec<usize> = Vec::new();
    for (i, m) in messages.iter().enumerate() {
        if let Message::User(u) = m {
            if u.is_meta != Some(true) {
                positions.push(i);
            }
        }
    }
    positions
}

/// How many user turns the history currently holds — the ceiling for a rollback.
pub fn count_user_turns(messages: &[Message]) -> usize {
    user_turn_positions(messages).len()
}

/// Return only the last `count` genuine user turns, dropping any prefix before
/// the first retained real user message. Every assistant, tool-result, and
/// meta-user message after that boundary is retained.
pub fn take_last_n_user_turns(messages: &[Message], count: usize) -> Vec<Message> {
    if count == 0 {
        return Vec::new();
    }
    let positions = user_turn_positions(messages);
    let Some(start) = positions
        .get(positions.len().saturating_sub(count))
        .copied()
    else {
        return Vec::new();
    };
    messages[start..].to_vec()
}

/// Return `messages` with its last `num_turns` user turns removed.
///
/// - `num_turns <= 0` or no user turns present → an unchanged copy.
/// - `num_turns >=` the number of user turns → drop from the first user turn on,
///   keeping only the pre-first-turn prefix (the first-instruction-turn
///   floor — e.g. a leading `compact_boundary` summary stays as context).
/// - otherwise → cut just before the Nth-from-last user turn.
///
/// Always returns a fresh vector; never mutates the input.
pub fn drop_last_n_user_turns(messages: &[Message], num_turns: f64) -> Vec<Message> {
    // Truncate defensively (the count is conceptually an integer): a fractional value reaching here —
    // e.g. from a corrupt rollout line — must not index the cut position to `undefined`
    // and collapse to 0, which would silently wipe the whole history.
    let n = num_turns.trunc();
    if n <= 0.0 {
        return messages.to_vec();
    }
    let positions = user_turn_positions(messages);
    if positions.is_empty() {
        return messages.to_vec();
    }
    // `n` is a positive, truncated (integral) value here; cast is safe.
    let n = n as usize;
    let cut_idx = if n >= positions.len() {
        positions[0]
    } else {
        // positions[positions.len() - n]; len >= n and n >= 1 so no underflow.
        positions[positions.len() - n]
    };
    messages[..cut_idx].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimeString;
    use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock};
    use crate::schemas::message::{
        AssistantMessage, CompactBoundaryMessage, SystemMessage, SystemSubtype, ToolResultMessage,
        UserMessage,
    };

    fn user(id: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: id.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: id.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })
    }

    fn asst(id: &str) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: id.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: id.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            model: "m".into(),
            usage: None,
        })
    }

    fn tool(id: &str) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: id.into(),
            tool_use_id: format!("tu-{id}").into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: id.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        })
    }

    fn boundary(id: &str) -> Message {
        Message::CompactBoundary(CompactBoundaryMessage {
            native_compaction: None,
            uuid: id.into(),
            summary: id.into(),
        })
    }

    fn meta_user(id: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: id.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: id.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: Some(true),
        })
    }

    fn user_display(id: &str) -> Message {
        Message::System(SystemMessage {
            uuid: format!("display-{id}").into(),
            text: format!(r#"{{"messageUuid":"{id}","localRender":{{}}}}"#).into(),
            subtype: SystemSubtype::UserDisplay,
        })
    }

    /// The uuid of any message variant.
    fn uuid(m: &Message) -> &RuntimeString {
        match m {
            Message::User(x) => &x.uuid,
            Message::Assistant(x) => &x.uuid,
            Message::ToolResult(x) => &x.uuid,
            Message::System(x) => &x.uuid,
            Message::CompactBoundary(x) => &x.uuid,
        }
    }

    fn uuids(msgs: &[Message]) -> Vec<RuntimeString> {
        msgs.iter().map(|m| uuid(m).clone()).collect()
    }

    /// A 3-turn conversation: each user turn is [user, assistant, tool_result].
    fn three_turns() -> Vec<Message> {
        vec![
            user("u1"),
            asst("a1"),
            tool("t1"),
            user("u2"),
            asst("a2"),
            tool("t2"),
            user("u3"),
            asst("a3"),
            tool("t3"),
        ]
    }

    // countUserTurns

    #[test]
    fn counts_user_messages_as_turn_boundaries() {
        assert_eq!(count_user_turns(&three_turns()), 3);
        assert_eq!(count_user_turns(&[]), 0);
        assert_eq!(count_user_turns(&[asst("a"), tool("t")]), 0);
    }

    // dropLastNUserTurns

    #[test]
    fn drops_the_last_user_turn_and_everything_after_it() {
        let r = drop_last_n_user_turns(&three_turns(), 1.0);
        assert_eq!(uuids(&r), vec!["u1", "a1", "t1", "u2", "a2", "t2"]);
    }

    #[test]
    fn drops_display_metadata_with_its_user_turn() {
        let history = vec![
            user("u1"),
            user_display("u1"),
            asst("a1"),
            user("u2"),
            user_display("u2"),
            asst("a2"),
        ];
        assert_eq!(
            uuids(&drop_last_n_user_turns(&history, 1.0)),
            vec!["u1", "display-u1", "a1"]
        );
    }

    #[test]
    fn drops_the_last_two_user_turns() {
        let r = drop_last_n_user_turns(&three_turns(), 2.0);
        assert_eq!(uuids(&r), vec!["u1", "a1", "t1"]);
    }

    #[test]
    fn cuts_at_a_user_message_boundary_so_no_dangling_tool_exchange_remains() {
        // Every kept prefix must end right before a user message (a complete exchange).
        let r = drop_last_n_user_turns(&three_turns(), 1.0);
        // t2, the close of turn 2
        assert!(matches!(r.last(), Some(Message::ToolResult(_))));
    }

    #[test]
    fn num_turns_ge_turn_count_drops_everything_from_the_first_user_turn_on() {
        assert_eq!(
            drop_last_n_user_turns(&three_turns(), 3.0),
            Vec::<Message>::new()
        );
        assert_eq!(
            drop_last_n_user_turns(&three_turns(), 99.0),
            Vec::<Message>::new()
        );
    }

    #[test]
    fn keeps_a_pre_first_turn_prefix_when_rolling_back_all_turns() {
        let hist: Vec<Message> = vec![
            boundary("b"),
            user("u1"),
            asst("a1"),
            user("u2"),
            asst("a2"),
        ];
        let r = drop_last_n_user_turns(&hist, 5.0);
        // the summary survives as context
        assert_eq!(uuids(&r), vec!["b"]);
    }

    #[test]
    fn returns_an_unchanged_copy_for_num_turns_le_0_or_no_user_turns() {
        let hist = three_turns();
        let same = drop_last_n_user_turns(&hist, 0.0);
        assert_eq!(same, hist);
        // a fresh vector is inherent to returning Vec<Message> by value in Rust.
        assert_eq!(uuids(&drop_last_n_user_turns(&[asst("a")], 1.0)), vec!["a"]);
    }

    #[test]
    fn never_mutates_its_input() {
        let hist = three_turns();
        let before = uuids(&hist);
        drop_last_n_user_turns(&hist, 2.0);
        assert_eq!(uuids(&hist), before);
    }

    #[test]
    fn truncates_a_fractional_num_turns_instead_of_wiping_the_whole_history() {
        // A fractional count must NOT index the cut to undefined and collapse to 0 (empty).
        assert_eq!(
            drop_last_n_user_turns(&three_turns(), 1.5),
            drop_last_n_user_turns(&three_turns(), 1.0)
        );
        // truncates to 0 → no-op
        assert_eq!(drop_last_n_user_turns(&three_turns(), 0.9), three_turns());
    }

    // takeLastNUserTurns

    #[test]
    fn takes_last_two_genuine_turns_and_keeps_following_meta_messages() {
        let hist = vec![
            boundary("summary"),
            user("u1"),
            asst("a1"),
            user("u2"),
            asst("a2"),
            meta_user("retry"),
            asst("a2b"),
            user("u3"),
            asst("a3"),
        ];

        assert_eq!(
            uuids(&take_last_n_user_turns(&hist, 2)),
            vec!["u2", "a2", "retry", "a2b", "u3", "a3"]
        );
    }

    #[test]
    fn recent_turn_projection_keeps_display_metadata_after_its_user() {
        let history = vec![
            user("u1"),
            user_display("u1"),
            asst("a1"),
            user("u2"),
            user_display("u2"),
            asst("a2"),
        ];
        assert_eq!(
            uuids(&take_last_n_user_turns(&history, 1)),
            vec!["u2", "display-u2", "a2"]
        );
    }

    #[test]
    fn oversized_recent_count_starts_at_first_real_user_and_drops_prefix() {
        let hist = vec![boundary("summary"), user("u1"), asst("a1")];

        assert_eq!(uuids(&take_last_n_user_turns(&hist, 99)), vec!["u1", "a1"]);
    }

    #[test]
    fn zero_recent_count_or_no_real_user_returns_empty() {
        assert!(take_last_n_user_turns(&three_turns(), 0).is_empty());
        assert!(take_last_n_user_turns(&[asst("a"), meta_user("retry")], 2).is_empty());
    }

    // isMeta exclusion — engine-injected re-prompts are not user turns

    #[test]
    fn does_not_count_an_is_meta_user_message_as_a_turn_boundary() {
        // [u1, a1, META, a2] — the META re-prompt must not split turn 1.
        let hist: Vec<Message> = vec![user("u1"), asst("a1"), meta_user("nudge"), asst("a2")];
        assert_eq!(count_user_turns(&hist), 1);
        // rollback(1) undoes the WHOLE real turn (back to empty), not just to the re-prompt.
        assert_eq!(drop_last_n_user_turns(&hist, 1.0), Vec::<Message>::new());
    }

    #[test]
    fn rolls_back_past_an_interleaved_re_prompt_to_the_prior_genuine_turn() {
        let hist: Vec<Message> = vec![
            user("u1"),
            asst("a1"),
            user("u2"),
            asst("bad"),
            meta_user("nudge"),
            asst("good"),
        ];
        // u1, u2 only — the nudge is not a turn
        assert_eq!(count_user_turns(&hist), 2);
        // rollback(1) drops turn 2 entirely, including the re-prompt and its re-sample.
        assert_eq!(uuids(&drop_last_n_user_turns(&hist, 1.0)), vec!["u1", "a1"]);
    }
}
