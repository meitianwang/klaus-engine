//! Defect: retained audio/video prevented later text-only model requests.
//! Root cause: replay capability checks treated admitted history as new input.
//! Work item: 20261021 request-only media projection with strict fresh admission.

use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::Message;

use super::r20261019_native_media_input::{fixture, part};

#[tokio::test]
async fn capable_text_only_capable_roundtrip_preserves_original_media_and_uuid() {
    let (session, requests) = fixture();
    let first = session
        .run(
            Input::Parts(vec![
                part("audio/wav", "UklGRg==", Some("voice.wav")),
                part("video/mp4", "AAAAHGZ0eXA=", Some("clip.mp4")),
            ]),
            TurnOptions::default(),
        )
        .await;
    assert!(first.error.is_none(), "{:?}", first.error);
    let original = session
        .history()
        .into_iter()
        .find(|message| {
            matches!(message, Message::User(user)
            if user.content.iter().any(|block| matches!(block, ContentBlock::Media(_))))
        })
        .expect("admitted native media must remain in canonical history");
    let Message::User(original_user) = &original else {
        unreachable!();
    };
    assert_eq!(original_user.content.len(), 2);

    let text_only = session
        .run(
            Input::Text("Continue with the available text.".into()),
            TurnOptions {
                model: Some("text-only".into()),
                ..Default::default()
            },
        )
        .await;
    assert!(text_only.error.is_none(), "{:?}", text_only.error);
    assert!(session.history().contains(&original));
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].model, "text-only");
        let projected = requests[1]
            .messages
            .iter()
            .find_map(|message| match message {
                Message::User(user) if user.uuid == original_user.uuid => Some(user),
                _ => None,
            })
            .expect("request projection preserves the original message identity");
        assert_eq!(projected.content.len(), 2);
        for (block, mime, filename) in [
            (&projected.content[0], "audio/wav", "voice.wav"),
            (&projected.content[1], "video/mp4", "clip.mp4"),
        ] {
            let ContentBlock::Text(note) = block else {
                panic!("unsupported native payload must be explicitly omitted");
            };
            assert!(note.text.contains("omitted from this request"));
            assert!(note.text.contains(mime));
            assert!(note.text.contains(filename));
            assert!(note.text.contains("Tell the user it was not read"));
            assert!(note.text.len_utf16() <= 2_000);
            assert!(!note.text.contains("UklGRg=="));
            assert!(!note.text.contains("AAAAHGZ0eXA="));
        }
    }

    let capable = session
        .run(
            Input::Text("Use the original media again.".into()),
            TurnOptions {
                model: Some("multimodal".into()),
                ..Default::default()
            },
        )
        .await;
    assert!(capable.error.is_none(), "{:?}", capable.error);
    assert!(session.history().contains(&original));
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].model, "multimodal");
        assert!(requests[0].messages.contains(&original));
        assert!(requests[2].messages.contains(&original));
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn fresh_unsupported_media_is_rejected_without_history_or_transport() {
    let (session, requests) = fixture();
    let before = session.history();
    let result = session
        .run(
            Input::Parts(vec![part("audio/wav", "UklGRg==", Some("voice.wav"))]),
            TurnOptions {
                model: Some("text-only".into()),
                ..Default::default()
            },
        )
        .await;
    assert!(result.error.is_some());
    assert!(requests.lock().unwrap().is_empty());
    assert!(!session.history().iter().any(|message| matches!(message,
        Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Media(_)))
    )));
    assert!(before
        .iter()
        .all(|message| session.history().contains(message)));
    session.shutdown_and_wait().await.unwrap();
}
