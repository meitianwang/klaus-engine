//! Defect: live history replay duplicated an accepted user input in temporary chats.
//! Root cause: turn openers and their display records bypassed the in-memory turn index.
//! Work item: 20261026 temporary transcript identity and history recovery.

use klaus_agent_core::engine::session::{Input, TurnOptions, UserContentPart};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::{Message, SystemSubtype};

use super::r20261019_native_media_input::fixture;

#[tokio::test]
async fn live_history_binds_each_structured_opener_and_display_record_to_its_turn() {
    let (session, _) = fixture();
    let mut turn_ids = Vec::new();
    for _ in 0..2 {
        let result = session
            .run(
                Input::Parts(vec![UserContentPart::Text {
                    text: "Inspect this selection".into(),
                }]),
                TurnOptions {
                    display_metadata: Some(
                        runtime_json::parse(&RuntimeString::from(
                            r#"{"displayText":"Inspect this selection","composerDocument":{"version":1,"content":{"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":"Inspect this selection"}]}]}}}"#,
                        ))
                        .unwrap(),
                    ),
                    ..Default::default()
                },
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        let history = session.visible_history();
        let user_id = history
            .iter()
            .rev()
            .find_map(|message| match message {
                Message::User(user) if user.is_meta != Some(true) => Some(&user.uuid),
                _ => None,
            })
            .unwrap();
        let display_id = history
            .iter()
            .rev()
            .find_map(|message| match message {
                Message::System(system) if system.subtype == SystemSubtype::UserDisplay => {
                    Some(&system.uuid)
                }
                _ => None,
            })
            .unwrap();
        let index = session.turn_index_snapshot();
        let turn_id = index
            .message_turn
            .get(user_id)
            .expect("live user opener must own a turn");
        assert_eq!(index.message_turn.get(display_id), Some(turn_id));
        assert!(index
            .turn_status
            .get(turn_id)
            .is_some_and(|status| status == "completed"));
        turn_ids.push(turn_id.clone());
    }
    assert_ne!(
        turn_ids[0], turn_ids[1],
        "identical text in separate turns keeps separate identities"
    );
}
