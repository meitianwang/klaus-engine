use super::*;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

struct PausedOutput {
    attempts: Arc<AtomicUsize>,
    broken: bool,
}
impl AsyncWrite for PausedOutput {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.broken {
            Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()))
        } else {
            Poll::Pending
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
fn options(root: &Path) -> AppServerOptions {
    AppServerOptions {
        windows_sandbox_context: None,
        create_session: Arc::new(|_| {
            Box::pin(async { Err("No session is requested by this fixture".into()) })
        }),
        credential_store: None,
        lifecycle: None,
        plugin_admin: None,
        permission_rules_path: Some(root.join("permissions.json")),
        config_path: Some(root.join("config.json")),
        startup_init_ack_budget: None,
        session_unload_delay: None,
    }
}
fn outgoing(index: usize) -> Outgoing {
    Outgoing {
        message: wire::response(
            &serde_json::json!(index),
            serde_json::json!({"index":index}),
        )
        .into(),
        cursor_operation: None,
        workbuddy_operation: None,
        provider_operation: None,
    }
}
async fn requests(input: &mut tokio::io::DuplexStream) {
    input
        .write_all(b"{\"id\":0,\"method\":\"initialize\",\"params\":{}}\n")
        .await
        .unwrap();
    for id in 1..=256 {
        input
            .write_all(
                format!(
                    "{{\"id\":{id},\"method\":\"unavailable-fixture-method\",\"params\":{{}}}}\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn eof_releases_blocked_publications_after_the_connection_drain_budget() {
    let root = tempfile::tempdir().unwrap();
    let (mut input, server_input) = tokio::io::duplex(64 * 1024);
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut server = tokio::spawn(run_app_server(
        server_input,
        PausedOutput {
            attempts: attempts.clone(),
            broken: false,
        },
        options(root.path()),
    ));
    requests(&mut input).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while attempts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    input.shutdown().await.unwrap();
    let result = tokio::time::timeout(
        CONNECTION_DRAIN_BUDGET + Duration::from_secs(1),
        &mut server,
    )
    .await;
    if result.is_err() {
        server.abort();
    }
    assert!(
        result.is_ok(),
        "EOF must release a blocked receiver after the bounded drain"
    );
    assert!(
        attempts.load(Ordering::SeqCst) > 0,
        "the fixture must reach real output backpressure"
    );
}

#[tokio::test]
async fn a_failed_writer_closes_the_reader_even_when_stdin_remains_open() {
    let root = tempfile::tempdir().unwrap();
    let (mut input, server_input) = tokio::io::duplex(64 * 1024);
    let mut server = tokio::spawn(run_app_server(
        server_input,
        PausedOutput {
            attempts: Arc::new(AtomicUsize::new(0)),
            broken: true,
        },
        options(root.path()),
    ));
    input
        .write_all(b"{\"id\":0,\"method\":\"initialize\",\"params\":{}}\n")
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), &mut server).await;
    if result.is_err() {
        server.abort();
    }
    assert!(
        result.is_ok(),
        "writer failure must wake pending publishers and the input loop"
    );
    drop(input);
}

#[tokio::test]
async fn request_admission_order_survives_a_full_output_channel() {
    let (tx, rx) = mpsc::channel::<Outgoing>(1);
    assert!(tx.send(outgoing(0)).await.is_ok());
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut tails = FuturesUnordered::<BoxFuture<'static, ()>>::new();
    for index in [1, 2] {
        let tx = tx.clone();
        let order = order.clone();
        admit_request_tail(
            &mut tails,
            Box::pin(async move {
                order.lock().unwrap().push(index);
                let _ = tx.send(outgoing(index)).await;
            }),
        )
        .await;
    }
    order.lock().unwrap().push(3);
    assert_eq!(
        *order.lock().unwrap(),
        [1, 2, 3],
        "control input follows admitted prefixes without waiting for output"
    );
    assert_eq!(
        tails.len(),
        2,
        "only the original suspended request futures remain"
    );
    drop(rx);
    tokio::time::timeout(Duration::from_secs(1), async {
        while tails.next().await.is_some() {}
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn a_paused_writer_bounds_admitted_frames_and_dropping_it_wakes_the_producer() {
    let (tx, rx) = mpsc::channel::<Outgoing>(OUTGOING_CAPACITY);
    let attempts = Arc::new(AtomicUsize::new(0));
    let writer = tokio::spawn(write_app_output(
        PausedOutput {
            attempts: attempts.clone(),
            broken: false,
        },
        rx,
    ));
    let admitted = Arc::new(AtomicUsize::new(0));
    let count = admitted.clone();
    let mut producer = tokio::spawn(async move {
        for index in 0..1024 {
            if tx.send(outgoing(index)).await.is_err() {
                return;
            }
            count.fetch_add(1, Ordering::SeqCst);
        }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while attempts.load(Ordering::SeqCst) == 0
            || admitted.load(Ordering::SeqCst) < OUTGOING_CAPACITY + 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        admitted.load(Ordering::SeqCst),
        OUTGOING_CAPACITY + 1,
        "one frame is in the writer and 128 are admitted"
    );
    writer.abort();
    let _ = writer.await;
    tokio::time::timeout(Duration::from_secs(1), &mut producer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn input_half_close_drains_accepted_replies_after_a_temporary_output_stall() {
    let root = tempfile::tempdir().unwrap();
    let (mut input, server_input) = tokio::io::duplex(64 * 1024);
    // One byte forces a real transport write to remain pending until this
    // half-closed client resumes reading its accepted replies.
    let (mut output, server_output) = tokio::io::duplex(1);
    let server = tokio::spawn(run_app_server(
        server_input,
        server_output,
        options(root.path()),
    ));
    requests(&mut input).await;
    input.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut bytes = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::io::AsyncReadExt::read_to_end(&mut output, &mut bytes),
    )
    .await
    .expect("accepted replies must drain while the connection budget remains")
    .unwrap();
    let text = String::from_utf8(bytes).unwrap();
    let frames: Vec<Value> = text
        .lines()
        .map(|line| {
            serde_json::from_str(line).expect("every drained frame must remain complete JSON")
        })
        .collect();
    let ids: std::collections::BTreeSet<_> = frames
        .iter()
        .map(|frame| frame["id"].as_u64().unwrap())
        .collect();
    assert_eq!(frames.len(), 257, "EOF must not drop buffered replies");
    assert_eq!(ids, (0..=256).collect());
    assert!(frames
        .iter()
        .any(|frame| frame["id"] == 0 && frame.get("result").is_some()));
    server.await.unwrap();
}
