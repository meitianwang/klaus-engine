//! Defect: sending an image to a nonvisual model removed it from the transcript.
//! Root cause: input admission omitted images before history and rollout writes.
//! Work item: 2026-10-21 nonvisual image history retention.

use super::*;
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::store::thread_history::project_thread_history;

fn request_text(request: &ModelRequest) -> String {
    request
        .messages
        .iter()
        .filter_map(|message| match message {
            Message::User(user) => Some(&user.content),
            _ => None,
        })
        .flatten()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn nonvisual_user_image_stays_in_history_and_reaches_a_later_visual_model() {
    let original = image(512, 256, false, "retained-user-image.png");
    let fixture = Fixture::new("blind", Scenario::Text, original.clone(), Vec::new(), None).await;
    let mut parts = match input(&original) {
        Input::Parts(parts) => parts,
        _ => unreachable!(),
    };
    parts.insert(
        0,
        UserContentPart::Text {
            text: "Inspect this attachment.".into(),
        },
    );
    fixture.run(Input::Parts(parts)).await;

    let requests = fixture.recorded();
    assert_eq!(requests.len(), 1);
    assert!(images(&requests[0].messages).is_empty());
    let text = request_text(&requests[0]);
    assert!(text.contains("Inspect this attachment."));
    assert!(text.contains("cannot understand images") && text.contains("Tell the user"));
    assert!(text.contains("attachment remains in the conversation"));
    assert_eq!(images(&fixture.session.history()), vec![original.clone()]);

    fixture.session.set_model("wide").unwrap();
    fixture
        .run(Input::Text("Inspect the earlier attachment now.".into()))
        .await;
    let requests = fixture.recorded();
    assert_eq!(requests.len(), 2);
    assert_eq!(images(&requests[1].messages), vec![original.clone()]);
    assert!(!request_text(&requests[1]).contains("cannot understand images"));
    assert_eq!(images(&fixture.session.history()), vec![original]);
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn nonvisual_image_only_turn_survives_durable_history_reload() {
    let directory =
        std::env::temp_dir().join(format!("nonvisual-image-history-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let original = image(512, 256, false, "retained-image-only.png");
    let fixture = Fixture::new(
        "blind",
        Scenario::Text,
        original.clone(),
        Vec::new(),
        Some(&directory),
    )
    .await;
    fixture.run(input(&original)).await;
    let requests = fixture.recorded();
    assert_eq!(requests.len(), 1);
    assert!(images(&requests[0].messages).is_empty());
    assert!(request_text(&requests[0]).contains("cannot understand images"));
    let thread_id = fixture.session.id.try_to_utf8().unwrap();
    fixture.session.shutdown_and_wait().await.unwrap();

    let store = JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    });
    let saved = store.read_thread(&thread_id).await.unwrap();
    let history = project_thread_history(saved.raw_items).history;
    std::fs::remove_dir_all(directory).unwrap();
    assert_eq!(images(&history), vec![original]);
}

#[tokio::test]
async fn nonvisual_queued_user_images_survive_admission_and_consumption() {
    let original = image(512, 256, false, "retained-queued-image.png");
    let fixture = Fixture::new(
        "blind",
        Scenario::Tool { switch_to: None },
        original.clone(),
        Vec::new(),
        None,
    )
    .await;
    fixture
        .session
        .enqueue(
            input(&original),
            EnqueueOptions {
                priority: Some(QueuePriority::Now),
                delivery: Some(SteerDelivery::User),
                ..Default::default()
            },
        )
        .unwrap();
    let queued = fixture.session.queued_messages();
    assert_eq!(queued.len(), 1);
    let mut retained = Vec::new();
    content_images(
        &queued_value_to_content_blocks(&queued[0].value),
        &mut retained,
    );
    assert_eq!(retained, vec![original.clone()]);

    fixture
        .run(Input::Text("Consume the queued attachment.".into()))
        .await;
    assert!(fixture.session.queued_messages().is_empty());
    let requests = fixture.recorded();
    assert_eq!(requests.len(), 2);
    assert!(requests
        .iter()
        .all(|request| images(&request.messages).is_empty()));
    assert!(request_text(&requests[1]).contains("cannot understand images"));
    let history = fixture.session.history();
    let queued_user = history.iter().find(|message| {
        matches!(message, Message::User(user) if user.content.iter().any(|block| {
            matches!(block, ContentBlock::Image(image) if image == &original)
        }))
    });
    assert!(
        queued_user.is_some(),
        "the queued user turn retains its image"
    );
    assert_eq!(images(&history), vec![original; 2]);
    fixture.session.shutdown_and_wait().await.unwrap();
}
