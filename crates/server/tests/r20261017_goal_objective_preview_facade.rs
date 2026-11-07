//! Full objective previews retain current generation and saved ownership authority.

use super::*;
use sha2::{Digest, Sha256};

const REFERENCE_PREFIX: &str = "Read the complete goal objective from this Klaus-owned file: ";

struct PreviewHarness {
    client: TestClient,
    factory: CreateSession,
    lifecycle: klaus_agent_protocol::server::ProtocolLifecycle,
    creates: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
}

impl PreviewHarness {
    async fn new(root: &std::path::Path) -> Self {
        let creates = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let calls = requests.clone();
        let agent = make_agent_with_call_model_and_config(
            Box::new(move |_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                panic!("objective previews must not request inference")
            }),
            Some(json!({"goal":true,"autoMemory":false,"autoDream":false})),
        );
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(root.join("sessions").to_string_lossy().into_owned()),
                archive_dir: None,
            }));
        let factory_agent = agent.clone();
        let factory_store = store.clone();
        let factory_creates = creates.clone();
        let factory: CreateSession = Arc::new(move |ctx: SessionContext| {
            let agent = factory_agent.clone();
            let store = factory_store.clone();
            let creates = factory_creates.clone();
            Box::pin(async move {
                creates.fetch_add(1, Ordering::SeqCst);
                let options = StartSessionOptions {
                    id: ctx.init.thread_id,
                    approval_handler: Some(ctx.approval_handler),
                    elicit_handler: Some(ctx.elicit_handler),
                    mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                    config: Some(json!({"goal":true,"autoMemory":false,"autoDream":false})),
                    ..Default::default()
                };
                let session = if let Some(snapshot) = ctx.fork_live {
                    agent.fork_live_session(store, &snapshot, options).await
                } else {
                    agent.open_session(store, options).await
                };
                session.map_err(|error| Box::new(error) as BoxError)
            })
        });
        let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store));
        let mut client = TestClient::start_with(factory.clone(), Some(lifecycle.clone()));
        client.initialize().await;
        Self {
            client,
            factory,
            lifecycle,
            creates,
            requests,
        }
    }

    async fn start(&mut self, thread: &str) -> String {
        self.client
            .send(json!({"id":2,"method":"session/start","params":{
                "init":{"threadId":thread}
            }}))
            .await;
        let (reply, _) = self.client.response_for(2).await;
        assert!(reply.get("error").is_none(), "{reply}");
        reply["result"]["sessionId"].as_str().unwrap().to_owned()
    }

    async fn read(&mut self, id: u64, params: Value) -> Value {
        self.client
            .send(json!({"id":id,"method":"session/readGoalObjective","params":params}))
            .await;
        self.client.response_for(id).await.0
    }
}

fn preparation(root: &std::path::Path, thread: &str, bytes: &[u8]) -> (String, Value) {
    let preparation_id = uuid::Uuid::new_v4().to_string();
    let directory = root
        .join("goal-objectives")
        .join(format!("{:x}", Sha256::digest(thread.as_bytes())))
        .join(&preparation_id);
    std::fs::create_dir_all(&directory).unwrap();
    let objective = directory.join("objective.md");
    std::fs::write(&objective, bytes).unwrap();
    (
        format!("{REFERENCE_PREFIX}{}", objective.display()),
        json!({
            "preparationId":preparation_id,
            "files":[{"name":"objective.md","sizeBytes":bytes.len(),"sha256":format!("{:x}",Sha256::digest(bytes))}]
        }),
    )
}

#[tokio::test]
async fn named_live_and_saved_fork_previews_read_all_source_owned_text_without_inference() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let mut harness = PreviewHarness::new(root.path()).await;
    let source = harness.start("preview-source").await;
    let full = (0..160)
        .map(|index| {
            format!(
                "{index:03}: 全部目标细节与验证要求，以及完整的交互、底层实现和可靠恢复条件 🦀\n"
            )
        })
        .collect::<String>();
    assert!(full.chars().count() > 4_000);
    let (reference, manifest) = preparation(root.path(), "preview-source", full.as_bytes());
    harness
        .client
        .send(json!({"id":3,"method":"session/setGoal","params":{
            "sessionId":source,"objective":reference,"status":"paused","origin":"user",
            "expectedGoalId":null,"ownedResources":manifest
        }}))
        .await;
    let (created, _) = harness.client.response_for(3).await;
    assert!(created.get("error").is_none(), "{created}");
    let generation = created["result"]["threadGoal"]["goalId"].clone();
    let preview = harness
        .read(
            4,
            json!({"sessionId":source,"threadId":"preview-source","expectedGoalId":generation}),
        )
        .await;
    assert_eq!(
        preview["result"],
        json!({"goalId":generation,"objective":full}),
        "{preview}"
    );
    harness
        .client
        .send(json!({"id":5,"method":"session/goal","params":{"sessionId":source}}))
        .await;
    let (ordinary, _) = harness.client.response_for(5).await;
    assert_eq!(ordinary["result"]["threadGoal"]["objective"], reference);
    assert!(serde_json::to_vec(&ordinary).unwrap().len() < 4_000);
    harness
        .client
        .send(json!({"id":6,"method":"session/forkLive","params":{
            "sourceSessionId":source,"init":{"threadId":"preview-target"},"inheritGoal":true
        }}))
        .await;
    let (forked, _) = harness.client.response_for(6).await;
    assert!(forked.get("error").is_none(), "{forked}");
    let target = forked["result"]["sessionId"].clone();
    harness
        .client
        .send(json!({"id":7,"method":"session/clearGoal","params":{
            "sessionId":source,"expectedGoalId":generation,"origin":"user"
        }}))
        .await;
    let (cleared, _) = harness.client.response_for(7).await;
    assert_eq!(cleared["result"]["goalCleared"], true, "{cleared}");
    let inherited = harness
        .read(8, json!({"sessionId":target,"expectedGoalId":generation}))
        .await;
    assert_eq!(
        inherited["result"],
        json!({"goalId":generation,"objective":full}),
        "{inherited}"
    );
    assert!(!root
        .path()
        .join("goal-objectives")
        .join(format!("{:x}", Sha256::digest(b"preview-target")))
        .exists());
    assert_eq!(harness.creates.load(Ordering::SeqCst), 2);
    assert_eq!(harness.requests.load(Ordering::SeqCst), 0);
    harness.client.close().await;

    let mut saved = TestClient::start_with(harness.factory, Some(harness.lifecycle));
    saved.initialize().await;
    saved
        .send(
            json!({"id":9,"method":"session/readGoalObjective","params":{
                "threadId":"preview-target","expectedGoalId":generation
            }}),
        )
        .await;
    let (reply, _) = saved.response_for(9).await;
    assert_eq!(
        reply["result"],
        json!({"goalId":generation,"objective":full}),
        "{reply}"
    );
    saved
        .send(
            json!({"id":10,"method":"session/readGoalObjective","params":{
                "threadId":"preview-target","expectedGoalId":"wrong-generation"
            }}),
        )
        .await;
    let (wrong, _) = saved.response_for(10).await;
    assert_eq!(wrong["error"]["data"]["code"], "GOAL_CONFLICT", "{wrong}");
    assert_eq!(
        harness.creates.load(Ordering::SeqCst),
        2,
        "saved reads cannot construct a Session"
    );
    assert_eq!(harness.requests.load(Ordering::SeqCst), 0);
    saved.close().await;
}

#[tokio::test]
async fn literal_file_reference_without_a_capability_remains_plain_text() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let mut harness = PreviewHarness::new(root.path()).await;
    let session = harness.start("preview-literal").await;
    let external = root.path().join("unrelated-secret.txt");
    std::fs::write(&external, b"These unrelated bytes must never be returned").unwrap();
    let literal = format!("{REFERENCE_PREFIX}{}", external.display());
    harness.client.send(json!({"id":3,"method":"session/setGoal","params":{
        "sessionId":session,"objective":literal,"status":"paused","origin":"user","expectedGoalId":null
    }})).await;
    let (created, _) = harness.client.response_for(3).await;
    let generation = created["result"]["threadGoal"]["goalId"].clone();
    let preview = harness
        .read(4, json!({"sessionId":session,"expectedGoalId":generation}))
        .await;
    assert_eq!(
        preview["result"],
        json!({"goalId":generation,"objective":literal}),
        "{preview}"
    );
    assert_eq!(harness.requests.load(Ordering::SeqCst), 0);
    harness.client.close().await;
}

#[tokio::test]
async fn preview_rejects_missing_generation_arbitrary_paths_and_tampered_owned_bytes() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let mut harness = PreviewHarness::new(root.path()).await;
    let session = harness.start("preview-validation").await;
    let (reference, manifest) = preparation(
        root.path(),
        "preview-validation",
        b"Original complete objective",
    );
    harness
        .client
        .send(json!({"id":3,"method":"session/setGoal","params":{
            "sessionId":session,"objective":reference,"status":"paused","origin":"user",
            "expectedGoalId":null,"ownedResources":manifest
        }}))
        .await;
    let (created, _) = harness.client.response_for(3).await;
    assert!(created.get("error").is_none(), "{created}");
    let generation = created["result"]["threadGoal"]["goalId"].clone();
    for (offset, params) in [
        json!({"sessionId":session}),
        json!({"sessionId":session,"expectedGoalId":null}),
        json!({"sessionId":session,"expectedGoalId":""}),
        json!({"expectedGoalId":generation}),
        json!({"sessionId":session,"expectedGoalId":generation,"path":"/arbitrary/secret"}),
        json!({"sessionId":session,"threadId":"another-thread","expectedGoalId":generation}),
    ]
    .into_iter()
    .enumerate()
    {
        let reply = harness.read(10 + offset as u64, params).await;
        assert_eq!(reply["error"]["code"], wire::INVALID_PARAMS, "{reply}");
    }
    std::fs::write(
        reference.strip_prefix(REFERENCE_PREFIX).unwrap(),
        b"Tampered complete objective",
    )
    .unwrap();
    let reply = harness
        .read(30, json!({"sessionId":session,"expectedGoalId":generation}))
        .await;
    assert_eq!(reply["error"]["code"], wire::ENGINE_ERROR, "{reply}");
    assert!(reply.get("result").is_none());
    assert_eq!(harness.requests.load(Ordering::SeqCst), 0);
    harness.client.close().await;
}
