//! Defect: text-based identity prevented independent questions from sharing prompts.
//! Root cause: answer attribution depended on mutable question wording.
//! Work item: 2026-10-18 stable question identifiers with historical compatibility.

use super::r20260905_structured_user_questions::{context, RecordingElicit};
use super::r20261004_async_user_questions::harness_with_questions;
use super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeMap, RuntimeString};
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::tools::builtins::ask_user::{
    create_ask_user_async_tool, create_ask_user_tool,
};
use klaus_agent_core::types::tool::ElicitResponse;
use std::sync::{Arc, Mutex};

const QUESTIONS: &str = r#"{"questions":[{"id":"first","question":"Choose?","header":"First","options":[{"label":"A","description":"First choice"},{"label":"B","description":"Second choice"}],"multiSelect":false},{"id":"second","question":"Choose?","header":"Second","options":[{"label":"C","description":"Third choice"},{"label":"D","description":"Fourth choice"}],"multiSelect":false}]}"#;

#[test]
fn question_ids_schema_requires_stable_keys_and_accepts_identical_prompts() {
    for tool in [create_ask_user_tool(), create_ask_user_async_tool()] {
        let schema = tool.static_input_schema();
        assert!(schema["properties"]["questions"]["items"]["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "id"));
        let input = runtime_json::parse(&QUESTIONS.into()).unwrap();
        assert_eq!(tool.parse_input(&input).unwrap(), input);
        let cross_alias = QUESTIONS.replacen("Choose?", "second", 1);
        assert!(tool
            .parse_input(&runtime_json::parse(&cross_alias.into()).unwrap())
            .is_err());
        for broken in [
            QUESTIONS.replace("\"second\"", "\"first\""),
            QUESTIONS.replace("\"first\"", "\" \""),
        ] {
            assert!(tool
                .parse_input(&runtime_json::parse(&broken.into()).unwrap())
                .is_err());
        }
    }
}

#[tokio::test]
async fn question_ids_blocking_answers_do_not_overwrite_identical_prompts() {
    let mut answers = RuntimeMap::new();
    answers.insert("first".into(), "A".into());
    answers.insert("second".into(), "D".into());
    let handler = Arc::new(RecordingElicit {
        response: ElicitResponse::Answers {
            answers,
            attachments: None,
        },
        requests: Mutex::new(Vec::new()),
        cancel_before_return: false,
    });
    let ctx = context(
        RuntimeProcessContext::initialize(process_supervisor()).unwrap(),
        handler,
    );
    let result = create_ask_user_tool()
        .call(runtime_json::parse(&QUESTIONS.into()).unwrap(), &ctx, None)
        .await
        .unwrap();
    let answers = result.serializable.output.get("answers").unwrap();
    assert_eq!(
        answers.get("first").and_then(|value| value.as_str()),
        Some(&RuntimeString::from("A"))
    );
    assert_eq!(
        answers.get("second").and_then(|value| value.as_str()),
        Some(&RuntimeString::from("D"))
    );
}

#[tokio::test]
async fn question_ids_legacy_clients_resolve_only_unambiguous_prompts() {
    let input =
        QUESTIONS
            .replacen("Choose?", "First prompt?", 1)
            .replacen("Choose?", "Second prompt?", 1);
    let mut answers = RuntimeMap::new();
    answers.insert("First prompt?".into(), "A".into());
    answers.insert("Second prompt?".into(), "D".into());
    let handler = Arc::new(RecordingElicit {
        response: ElicitResponse::Answers {
            answers,
            attachments: None,
        },
        requests: Mutex::new(Vec::new()),
        cancel_before_return: false,
    });
    let ctx = context(
        RuntimeProcessContext::initialize(process_supervisor()).unwrap(),
        handler,
    );
    let result = create_ask_user_tool()
        .call(runtime_json::parse(&input.into()).unwrap(), &ctx, None)
        .await
        .unwrap();
    let answers = result.serializable.output.get("answers").unwrap();
    assert_eq!(
        answers.get("first").and_then(|value| value.as_str()),
        Some(&RuntimeString::from("A"))
    );
    assert_eq!(
        answers.get("second").and_then(|value| value.as_str()),
        Some(&RuntimeString::from("D"))
    );
}

#[tokio::test]
async fn question_ids_async_partial_answers_keep_distinct_durable_attribution() {
    let (session, _, started, release) = harness_with_questions(QUESTIONS);
    let running = {
        let session = session.clone();
        tokio::spawn(async move {
            session
                .run(Input::Text("work".into()), TurnOptions::default())
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), started.notified())
        .await
        .unwrap();
    let question = session.pending_questions()[0].clone();
    let mut ambiguous = RuntimeMap::new();
    ambiguous.insert("Choose?".into(), "A".into());
    assert!(session
        .answer_question(
            question.turn_id.clone(),
            question.request_id.clone(),
            "ambiguous".into(),
            ambiguous,
            None
        )
        .await
        .is_err());
    for (id, value, expected) in [("first", "A", "pending"), ("second", "D", "answered")] {
        let mut answers = RuntimeMap::new();
        answers.insert(id.into(), value.into());
        let reply = session
            .answer_question(
                question.turn_id.clone(),
                question.request_id.clone(),
                id.into(),
                answers,
                None,
            )
            .await
            .unwrap();
        assert_eq!(reply.state, expected);
    }
    release.notify_one();
    assert!(running.await.unwrap().error.is_none());
    let history = session.history();
    let answers = history
        .iter()
        .filter_map(|message| match message {
            Message::User(user) => user.question_answer.as_ref(),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(answers.len(), 2);
    assert_eq!(
        answers[0].answers.get(&RuntimeString::from("first")),
        Some(&RuntimeString::from("A"))
    );
    assert_eq!(
        answers[1].answers.get(&RuntimeString::from("second")),
        Some(&RuntimeString::from("D"))
    );
}
