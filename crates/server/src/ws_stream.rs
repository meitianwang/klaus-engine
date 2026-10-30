//! Adapt a single WebSocket connection to the newline-delimited line pair the
//! `ProtocolServer`/`ProtocolClient` speak — so the reviewed, tested Op/Event
//! server is reused verbatim, with WebSocket as just another transport.
//!
//! Each inbound WS text frame carries exactly one line (the protocol writes one
//! message per write), so we push each frame onto `input` (ensuring the trailing
//! newline the splitter needs). Each outbound `output` line — already a complete
//! `…\n` line — becomes one WS text frame.
//!
//! `input` ends (EOF) when the socket closes, which resolves
//! `ProtocolServer::start()` / terminates the client reader, so the owner can
//! tear down.

use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch, Notify};
use tokio::task::{AbortHandle, JoinHandle};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;

#[cfg(test)]
use crate::endpoint_activity::EndpointTestGate;
use crate::line_stream::{ChannelReader, ChannelWriter};

/// Cloneable owner for the two WebSocket transport pumps.
///
/// Closing is synchronous: both pumps are aborted before [`Self::close`]
/// returns, which destroys the split socket without waiting for a WebSocket
/// close handshake. [`Self::close_and_wait`] additionally waits for the
/// retained supervisor to observe both pump exits.
#[derive(Clone)]
pub struct WsTransportHandle(Arc<WsTransportInner>);

struct WsTransportInner {
    close: watch::Sender<bool>,
    read_abort: AbortHandle,
    write_abort: AbortHandle,
    completion: Arc<WsTransportCompletion>,
}

impl WsTransportInner {
    fn close(&self) {
        let _ = self.close.send(true);
        self.read_abort.abort();
        self.write_abort.abort();
    }
}

impl Drop for WsTransportInner {
    fn drop(&mut self) {
        // The inner value is shared by every handle, so this runs only when the
        // final owner disappears. A forgotten explicit close cannot leave the
        // socket pumps detached forever.
        self.close();
    }
}

impl WsTransportHandle {
    /// Abort both transport pumps immediately (idempotent).
    pub fn close(&self) {
        self.0.close();
    }

    /// Abort both pumps and wait until their retained supervisor has observed
    /// both exits. Multiple and cancelled waiters are safe.
    pub async fn close_and_wait(&self) {
        self.close();
        self.0.completion.wait().await;
    }
}

#[derive(Default)]
struct WsTransportCompletion {
    finished: Mutex<bool>,
    notify: Notify,
}

impl WsTransportCompletion {
    fn finish(&self) {
        let mut finished = self.finished.lock().unwrap();
        if !*finished {
            *finished = true;
            self.notify.notify_waiters();
        }
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Register before checking retained state so completion cannot land
            // in the check-to-await window. The state remains available to a
            // replacement waiter if an earlier waiting future is cancelled.
            notified.as_mut().enable();
            if *self.finished.lock().unwrap() {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Clone, Default)]
struct WsTransportTestGates {
    #[cfg(test)]
    before_completion: Option<Arc<EndpointTestGate>>,
}

impl WsTransportTestGates {
    async fn pause_before_completion(&self) {
        #[cfg(test)]
        if let Some(gate) = &self.before_completion {
            gate.pause().await;
        }
    }
}

/// The byte-stream pair a `ProtocolServer`/`ProtocolClient` is constructed from,
/// plus a handle to close the underlying socket (the split socket is owned by the
/// pump tasks, so closing is exposed explicitly rather than via a raw socket
/// handle).
pub struct WsStreams {
    /// Inbound byte stream of newline-delimited JSON (ops for a server, events for a client).
    pub input: Box<dyn AsyncRead + Unpin + Send>,
    /// Outbound byte stream of newline-delimited JSON (events for a server, ops for a client).
    pub output: Box<dyn AsyncWrite + Unpin + Send>,
    /// Owns and closes the socket read/write pumps.
    pub transport: WsTransportHandle,
}

impl WsStreams {
    /// Abort both underlying socket pumps immediately (idempotent).
    pub fn close(&self) {
        self.transport.close();
    }

    /// Abort both pumps and wait for their retained supervisor to observe exit.
    pub async fn close_and_wait(&self) {
        self.transport.close_and_wait().await;
    }
}

fn retain_pumps(
    close: watch::Sender<bool>,
    read_pump: JoinHandle<()>,
    write_pump: JoinHandle<()>,
    test_gates: WsTransportTestGates,
) -> WsTransportHandle {
    let read_abort = read_pump.abort_handle();
    let write_abort = write_pump.abort_handle();
    let completion = Arc::new(WsTransportCompletion::default());
    let supervisor_completion = completion.clone();

    // The supervisor is the sole owner of both JoinHandles. It records durable
    // shared completion only after both tasks have actually exited.
    tokio::spawn(async move {
        let _ = read_pump.await;
        let _ = write_pump.await;
        test_gates.pause_before_completion().await;
        supervisor_completion.finish();
    });

    WsTransportHandle(Arc::new(WsTransportInner {
        close,
        read_abort,
        write_abort,
        completion,
    }))
}

/// Adapt a WebSocket stream to a [`WsStreams`] line pair. Spawns a read pump
/// (frames → `input`, EOF on socket close) and a write pump (`output` → frames).
pub fn ws_to_streams<S>(ws: WebSocketStream<S>) -> WsStreams
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (input_tx, input_rx) = mpsc::unbounded_channel::<String>();
    let (output_tx, output_rx) = mpsc::unbounded_channel::<String>();
    let (close_tx, _close_rx0) = watch::channel(false);

    let (mut sink, mut stream) = ws.split();

    // Read pump: WS text frames → input lines. Socket close/error → EOF.
    let read_pump = {
        let mut close_rx = close_tx.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = close_rx.changed() => break,
                    frame = stream.next() => {
                        match frame {
                            Some(Ok(Message::Text(text))) => {
                                // Guarantee exactly one newline terminator so the
                                // reader yields one line per frame even if a
                                // client omits it.
                                let line = if text.ends_with('\n') { text } else { format!("{text}\n") };
                                if input_tx.send(line).is_err() { break; }
                            }
                            // Ignore non-text frames — the protocol is UTF-8 only.
                            Some(Ok(Message::Binary(_)))
                            | Some(Ok(Message::Ping(_)))
                            | Some(Ok(Message::Pong(_)))
                            | Some(Ok(Message::Frame(_))) => {}
                            // Close / stream end / error → end of stream.
                            _ => break,
                        }
                    }
                }
            }
            // Dropping input_tx signals EOF to the protocol input.
        })
    };

    // Write pump: output lines → WS text frames.
    let write_pump = {
        let mut close_rx = close_tx.subscribe();
        let mut output_rx = output_rx;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = close_rx.changed() => break,
                    line = output_rx.recv() => {
                        match line {
                            Some(l) => { if sink.send(Message::Text(l)).await.is_err() { break; } }
                            None => break, // output dropped → nothing more to send
                        }
                    }
                }
            }
            let _ = sink.close().await;
        })
    };

    let transport = retain_pumps(
        close_tx,
        read_pump,
        write_pump,
        WsTransportTestGates::default(),
    );

    WsStreams {
        input: Box::new(ChannelReader::new(input_rx)),
        output: Box::new(ChannelWriter::new(output_tx)),
        transport,
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;

    struct PumpDrop(Arc<AtomicUsize>);

    impl Drop for PumpDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn blocking_pump(drops: Arc<AtomicUsize>) -> (JoinHandle<()>, oneshot::Receiver<()>) {
        let (started_tx, started_rx) = oneshot::channel();
        let pump = tokio::spawn(async move {
            let _drop = PumpDrop(drops);
            let _ = started_tx.send(());
            pending::<()>().await;
        });
        (pump, started_rx)
    }

    #[tokio::test]
    async fn close_waits_for_supervisor_completion_and_replacement_waiter_is_safe() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (read_pump, read_started) = blocking_pump(drops.clone());
        let (write_pump, write_started) = blocking_pump(drops.clone());
        let (close, _) = watch::channel(false);
        let exit_gate = Arc::new(EndpointTestGate::default());
        let transport = retain_pumps(
            close,
            read_pump,
            write_pump,
            WsTransportTestGates {
                before_completion: Some(exit_gate.clone()),
            },
        );
        read_started.await.unwrap();
        write_started.await.unwrap();

        let first_waiter = {
            let transport = transport.clone();
            tokio::spawn(async move { transport.close_and_wait().await })
        };
        exit_gate.wait_until_entered().await;
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert!(!first_waiter.is_finished());

        // Cancelling one caller must not consume completion or cancel the
        // retained supervisor. A replacement waiter still observes the gate.
        first_waiter.abort();
        let _ = first_waiter.await;
        let replacement_waiter = {
            let transport = transport.clone();
            tokio::spawn(async move { transport.close_and_wait().await })
        };
        tokio::task::yield_now().await;
        assert!(!replacement_waiter.is_finished());

        // If close_and_wait returned after signalling/aborting rather than
        // awaiting supervisor completion, the assertions above would fail.
        exit_gate.release();
        tokio::time::timeout(Duration::from_secs(2), replacement_waiter)
            .await
            .expect("replacement waiter did not observe pump completion")
            .unwrap();
    }

    #[tokio::test]
    async fn dropping_the_final_transport_owner_aborts_both_pumps() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (read_pump, read_started) = blocking_pump(drops.clone());
        let (write_pump, write_started) = blocking_pump(drops.clone());
        let (close, _) = watch::channel(false);
        let exit_gate = Arc::new(EndpointTestGate::default());
        let transport = retain_pumps(
            close,
            read_pump,
            write_pump,
            WsTransportTestGates {
                before_completion: Some(exit_gate.clone()),
            },
        );
        let completion = transport.0.completion.clone();
        let final_owner = transport.clone();
        read_started.await.unwrap();
        write_started.await.unwrap();

        drop(transport);
        tokio::task::yield_now().await;
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        drop(final_owner);
        exit_gate.wait_until_entered().await;
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        exit_gate.release();
        tokio::time::timeout(Duration::from_secs(2), completion.wait())
            .await
            .expect("supervisor did not retain and observe both pump exits");
    }
}
