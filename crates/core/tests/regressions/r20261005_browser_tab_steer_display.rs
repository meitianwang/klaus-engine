//! Defect: a browser-tab human follow-up lost its composer text and chips when
//! an expanded operational prompt was steered into a running turn or resumed.
//! Root cause: session/steer and durable queued messages omitted displayMetadata.
//! Work item: 2026-10-05 browser tab mentions; persist host metadata beside user
//! messages without adding that opaque metadata to provider requests.

use super::r20260930_user_steer_persists_as_user_message::session_with_metadata;
use klaus_agent_core::engine::message_queue::SteerDelivery;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::message::{Message, SystemSubtype};

macro_rules! json {
    ($($json:tt)+) => {
        runtime_json::parse(&RuntimeString::from(stringify!($($json)+))).unwrap()
    };
}

#[tokio::test]
async fn browser_tab_steer_display_metadata_survives_history_without_entering_model_input() {
    let metadata = json!({
        "displayText":"Inspect [Orders](plugin://browser@klaus-bundled?mention=tab-v1&browserId=s1&tabId=42&title=Orders&url=https%3A%2F%2Fshop.test%2Forders)",
        "browserTabs":[{"browserId":"chrome:profile-1","tabId":42,"title":"Orders","url":"https://shop.test/orders"}]
    });
    let (session, requests) =
        session_with_metadata(Some(SteerDelivery::User), Some(metadata.clone()));
    let result = session
        .run(Input::Text("work".into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "turn error: {:?}", result.error);
    let history = session.history();
    let display_index = history.iter().position(|message| {
        matches!(message, Message::System(system) if system.subtype == SystemSubtype::UserDisplay)
    }).expect("an applied human steer keeps its display metadata");
    let Message::System(display) = &history[display_index] else {
        unreachable!()
    };
    let envelope = runtime_json::parse(&display.text).unwrap();
    assert_eq!(envelope.get("localRender"), Some(&metadata));
    let Message::User(user) = &history[display_index - 1] else {
        panic!("the display record must immediately follow its user message")
    };
    assert_eq!(
        envelope.get("messageUuid"),
        Some(&RuntimeJson::String(user.uuid.clone()))
    );
    assert!(requests.lock().unwrap().iter().all(|request| {
        request.messages.iter().all(|message| {
            !matches!(message, Message::System(system) if system.subtype == SystemSubtype::UserDisplay)
        })
    }), "host display metadata must not enter a provider request");
    session.shutdown_and_wait().await.unwrap();
}
