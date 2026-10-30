//! Regression: read-only side questions must deliver inline content, never paths
//! or transcript mutations. Work item: 2026-09-22 side-question attachments.

use super::*;
use std::time::Duration;

#[tokio::test]
async fn readonly_side_question_does_not_invoke_registered_hooks() {
    let calls = Arc::new(AtomicUsize::new(0));
    let bus = setup_bus(Hook {
        event: HookEvent::UserPromptSubmit,
        matcher: None,
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: false,
    });
    let session = mk_setup_session(
        mk_deps(vec![text_turn("side answer"), text_turn("main answer")]),
        bus,
        None,
        None,
    );
    session
        .ask_side_question_input(Input::Parts(question_parts()))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(session.history_snapshot().is_empty());
    session
        .run(Input::Text("main input".into()), TurnOptions::default())
        .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the hook is actually registered"
    );
}

fn image_part() -> UserContentPart {
    UserContentPart::Image {
        base64: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==".into(),
        mime_type: "image/png".into(),
    source_path: None,
}
}

fn question_parts() -> Vec<UserContentPart> {
    vec![
        UserContentPart::Text {
            text: "Compare the image with this parsed Office/TXT/HTML text.".into(),
        },
        image_part(),
        pdf_part(b"%PDF-1.4\n%%EOF\n", "APPLICATION/PDF", Some("report.pdf")),
        UserContentPart::Text {
            text: RuntimeString::from_utf16([b'x' as u16, 0xd800]),
        },
    ]
}

#[tokio::test]
async fn readonly_side_question_rejects_unreadable_images_instead_of_generating_without_them() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.model = "text-only-model".into();
    let session = mk_session_with_provider_and_memory(
        Arc::new(SelectivePdfProvider::new()),
        recording_deps(requests.clone(), vec![]),
        config,
        Vec::new(),
        None,
    );
    let error = session
        .ask_side_question_input(Input::Parts(vec![image_part()]))
        .await
        .unwrap_err();
    assert!(error.contains("vision-capable"), "{error}");
    assert!(requests.lock().unwrap().is_empty());
    assert!(session.history_snapshot().is_empty());
}

#[tokio::test]
async fn readonly_side_question_blocks_reach_provider_without_tools_or_history_mutation() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.memory.enabled = false;
    let session = mk_session_with_deps(
        recording_deps(
            requests.clone(),
            vec![
                text_turn("seed"),
                text_turn("side answer"),
                text_turn("legacy"),
            ],
        ),
        config,
        vec![create_write_tool()],
    );
    session
        .run(Input::Text("parent task".into()), TurnOptions::default())
        .await;
    let before = session.history_snapshot();
    let parts = question_parts();
    let answer = session
        .ask_side_question_input(Input::Parts(parts.clone()))
        .await
        .unwrap();
    assert_eq!(answer.response, "side answer");
    assert_eq!(session.history_snapshot(), before);
    assert_eq!(
        session
            .ask_side_question(" legacy string ".into())
            .await
            .unwrap()
            .response,
        "legacy"
    );
    assert_eq!(session.history_snapshot(), before);
    let requests = requests.lock().unwrap();
    assert!(
        !requests[0].tools.is_empty(),
        "the parent has real tool specs"
    );
    let request = &requests[1];
    assert!(request.tools.is_empty());
    assert_eq!(request.skip_cache_write, Some(true));
    let Message::User(user) = request.messages.last().unwrap() else {
        panic!("user input");
    };
    assert_eq!(user.content.len(), parts.len() + 1);
    assert!(
        matches!(&user.content[0], ContentBlock::Text(text) if text.text.contains("Side conversation boundary."))
    );
    assert!(
        matches!(&user.content[2], ContentBlock::Image(image) if image.mime_type == "image/png")
    );
    assert!(
        matches!(&user.content[3], ContentBlock::Document(document) if document.mime_type == "application/pdf" && document.filename.as_ref().is_some_and(|name| name == "report.pdf"))
    );
    assert!(
        matches!(&user.content[4], ContentBlock::Text(text) if text.text == RuntimeString::from_utf16([b'x' as u16, 0xd800]))
    );
    assert_eq!(&request.messages[..before.len()], before.as_slice());
}

#[tokio::test]
async fn readonly_side_question_rejects_invalid_payload_before_provider_and_history() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_provider_and_memory(
        Arc::new(SelectivePdfProvider::new()),
        recording_deps(requests.clone(), vec![]),
        default_config(),
        Vec::new(),
        None,
    );
    let before = session.history_snapshot();
    let invalid = [
        Input::Text(" \n ".into()),
        Input::Parts(vec![]),
        Input::Parts(vec![UserContentPart::Text { text: " \t".into() }]),
        Input::Parts(vec![image_part(); 6]),
        Input::Parts(vec![
            image_part(),
            image_part(),
            image_part(),
            pdf_part(b"%PDF-1.4", "application/pdf", None),
            pdf_part(b"%PDF-1.4", "application/pdf", None),
            pdf_part(b"%PDF-1.4", "application/pdf", None),
        ]),
        Input::Parts(vec![UserContentPart::Image {
            base64: "%%%".into(),
            mime_type: "image/png".into(),
            source_path: None,
        }]),
        Input::Parts(vec![UserContentPart::Image {
            base64: RuntimeString::from_utf16([0xd800]),
            mime_type: "image/png".into(),
            source_path: None,
        }]),
        Input::Parts(vec![UserContentPart::Image {
            base64: "bm90IGEgcG5n".into(),
            mime_type: "image/png".into(),
            source_path: None,
        }]),
        Input::Parts(vec![UserContentPart::Image {
            base64: "iVBORw0KGgo=".into(),
            mime_type: "image/svg+xml".into(),
            source_path: None,
        }]),
        Input::Parts(vec![UserContentPart::Image {
            base64: "iVBORw0KGgo=".into(),
            mime_type: "image/jpeg".into(),
            source_path: None,
        }]),
        Input::Parts(vec![UserContentPart::Document {
            base64: "%%%".into(),
            mime_type: "application/pdf".into(),
            filename: None,
        }]),
        Input::Parts(vec![pdf_part(b"not PDF", "application/pdf", None)]),
        Input::Parts(vec![pdf_part(b"%PDF-1.4", "application/msword", None)]),
        Input::Parts(vec![pdf_part(
            b"%PDF-1.4",
            "application/pdf",
            Some("bad\nname.pdf"),
        )]),
    ];
    for input in invalid {
        assert!(session.ask_side_question_input(input).await.is_err());
        assert_eq!(session.history_snapshot(), before);
        assert!(requests.lock().unwrap().is_empty());
    }
    session.set_model("text-only-model").unwrap();
    assert!(session
        .ask_side_question_input(Input::Parts(vec![pdf_part(
            b"%PDF-1.4",
            "application/pdf",
            None
        ),]))
        .await
        .is_err());
    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(session.history_snapshot(), before);
}

#[tokio::test]
async fn readonly_side_question_five_ten_mib_documents_do_not_change_turn_limits() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_deps(
        recording_deps(requests.clone(), vec![text_turn("five PDFs")]),
        default_config(),
        Vec::new(),
    );
    let mut bytes = vec![b' '; 10 * 1024 * 1024];
    bytes[..8].copy_from_slice(b"%PDF-1.4");
    let part = pdf_part(&bytes, "application/pdf", None);
    let input = Input::Parts(vec![part.clone(); 5]);
    assert!(
        session
            .validate_input_for_turn(input.clone(), &TurnOptions::default())
            .is_err(),
        "the ordinary turn still has its existing 20 MiB total PDF limit"
    );
    assert_eq!(
        session
            .ask_side_question_input(input)
            .await
            .unwrap()
            .response,
        "five PDFs"
    );
    {
        let calls = requests.lock().unwrap();
        let Message::User(user) = calls[0].messages.last().unwrap() else {
            panic!("user input");
        };
        assert_eq!(
            user.content
                .iter()
                .filter(|block| matches!(block, ContentBlock::Document(_)))
                .count(),
            5
        );
        assert!(calls[0].tools.is_empty());
        assert_eq!(calls[0].skip_cache_write, Some(true));
    }
    bytes.push(b' ');
    for part in [
        pdf_part(&bytes, "application/pdf", None),
        UserContentPart::Image {
            base64: base64::engine::general_purpose::STANDARD
                .encode(&bytes)
                .into(),
            mime_type: "image/png".into(),
            source_path: None,
        },
    ] {
        let error = session
            .ask_side_question_input(Input::Parts(vec![part]))
            .await
            .unwrap_err();
        assert!(error.contains("10 MiB"), "{error}");
    }
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert!(session.history_snapshot().is_empty());
    assert!(
        session
            .validate_input_for_turn(
                Input::Parts(vec![pdf_part(b"%PDF-1.4", "application/pdf", None); 6]),
                &TurnOptions::default(),
            )
            .is_ok(),
        "the side question's five-attachment count must not affect turns"
    );
}

#[tokio::test]
async fn readonly_side_question_cancel_aborts_provider_and_drops_stream() {
    struct DropProbe(Arc<AtomicBool>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let entered = Arc::new(tokio::sync::Notify::new());
    let cancelled = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let mut deps = mk_deps(vec![]);
    deps.call_model = Box::new({
        let entered = entered.clone();
        let cancelled = cancelled.clone();
        let dropped = dropped.clone();
        move |request, signal| {
            assert!(request.tools.is_empty());
            assert_eq!(request.skip_cache_write, Some(true));
            entered.notify_one();
            let cancelled = cancelled.clone();
            tokio::spawn(async move {
                signal.cancelled().await;
                cancelled.notify_one();
            });
            let probe = DropProbe(dropped.clone());
            Box::pin(futures::stream::once(async move {
                let _probe = probe;
                futures::future::pending::<crate::types::provider::ModelStreamEvent>().await
            }))
        }
    });
    let session = mk_session_with_deps(deps, default_config(), vec![create_write_tool()]);
    let signal = AbortSignal::new();
    let task = tokio::spawn({
        let session = session.clone();
        let signal = signal.clone();
        async move {
            session
                .ask_side_question_input_with_signal(Input::Parts(question_parts()), signal)
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert!(session.history_snapshot().is_empty());
    signal.cancel();
    assert_eq!(task.await.unwrap().unwrap_err(), "side question cancelled");
    tokio::time::timeout(Duration::from_secs(5), cancelled.notified())
        .await
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert!(session.history_snapshot().is_empty());
}
