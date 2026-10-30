//! The engine, as this CLI is allowed to see it.
//!
//! Every request this process makes of the engine goes through here, and the
//! only way to make one is to name a row of the engine's declared method table.
//! That is the point: `EngineClient` has no `send(op)`, so there is nothing to
//! reach past. A capability the facade has not declared is not reachable from
//! this crate at all, which is what makes the method table a contract rather
//! than a suggestion — and what stops this CLI and the desktop client from
//! consuming two different engines.
//!
//! Three call shapes exist because the facade answers in three shapes, and the
//! difference is the engine's, not an accident of this layer:
//!
//!   * [`EngineClient::call`] — a method with a declared result struct
//!     (`initialize`, `session/start`, `turn/start`). The reply IS the answer.
//!   * [`EngineClient::query`] — a direct-result method. The engine already
//!     settled the op, so the reply is the inner protocol's reply envelope and
//!     the caller renders it like any other event.
//!   * [`EngineClient::settle`] / [`EngineClient::ack`] — an ack-reply method.
//!     The reply is only a receipt; the answer streams back on the event
//!     notification, correlated by the submission id the receipt names.
//!
//! Human renderers and turn control flow use a [`ProtocolEvent`] view. Direct
//! machine replies retain the facade's raw JSON so opaque control numbers keep
//! their original integer representation.
//!
//! [`EndpointClient`] is the same door with a narrower frame, for the part of
//! the surface that is not a session's: the config file, the hooks file, the
//! durable permission rules, the credential store, the compiled-in skill
//! roster. Those are ENDPOINT state — they outlive every session and their
//! params carry no session id — so binding a session to reach them would be a
//! fiction, and one with a cost: opening a session runs discovery, loads
//! plugins and starts MCP children, all to print a config value.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_protocol::ProtocolEvent;
use klaus_agent_server::app_server::runtime_wire::RawJson;
use klaus_agent_server::app_server::wire;
use klaus_agent_server::{AppServerClient, Method, Notification, Notifications, ReverseRequest};

use crate::error::{CliError, CliResult};
use crate::text;

/// A bound session on an engine, reached through the facade.
///
/// Cheap to clone-by-reference and deliberately not `Clone`: the facade client
/// aborts its reader when the last owner drops it, so exactly one thing in this
/// process owns the connection and everything else borrows this.
pub struct EngineClient {
    facade: Arc<AppServerClient>,
    session_id: String,
}

impl EngineClient {
    pub fn new(facade: Arc<AppServerClient>, session_id: String) -> Self {
        Self { facade, session_id }
    }

    /// The session every call below is scoped to.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Give up the connection. Teardown only: the facade client aborts its
    /// reader when the last owner drops it, so whoever ends the session has to
    /// be able to take this one back out.
    pub fn into_facade(self) -> Arc<AppServerClient> {
        self.facade
    }

    /// Another handle on the same connection, for starting a SECOND session on it
    /// while this one is still open. Only [`crate::session::CliSession`] needs it,
    /// to replace a session whose provider has to change: the endpoint resolves a
    /// model selection once, at session start, so the connection outlives the
    /// session bound to it.
    pub fn facade(&self) -> Arc<AppServerClient> {
        Arc::clone(&self.facade)
    }

    /// The params of a method that takes nothing but its session.
    ///
    /// `submission_id` is left unset here, and almost everywhere else in this
    /// crate: the engine mints one and reports it in the receipt. The one
    /// exception is [`Self::cancellable_submission_id`].
    pub fn scope(&self) -> wire::SessionRequestParams {
        wire::SessionRequestParams {
            session_id: Some(self.session_id.clone()),
            submission_id: None,
        }
    }

    /// A submission id for a call this process intends to be able to cancel.
    ///
    /// Needed only by a declared-result method that spends model calls, because
    /// `session/cancelRequest` names a SUBMISSION: the facade adopts a
    /// client-chosen id verbatim precisely so that one can be named, and a caller
    /// that let the engine mint it learns the id only in the reply — which is the
    /// one thing it is waiting for, so it would learn it too late to interrupt.
    ///
    /// Unique within the connection, which is all that is asked of it: the facade
    /// refuses an id already in flight, and none of these outlive the process.
    pub fn cancellable_submission_id(&self, what: &str) -> String {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        format!("cli-{what}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
    }

    /// Call one declared method and get its declared result.
    pub async fn call<M: Method>(&self, params: M::Params) -> CliResult<M::Result> {
        Ok(self.facade.request::<M>(params).await?)
    }

    /// Call a direct-result method and hand back the reply envelope.
    ///
    /// The facade strips `submissionId` from a direct reply, because there is no
    /// submission to correlate against — the request/response pair WAS the
    /// correlation. The envelope model requires the field, so it is restored as
    /// empty here rather than being made optional: loosening the model would
    /// also let a streamed event lose its id without anyone noticing, and that
    /// id is the only thing tying a streamed reply to the op that caused it.
    pub async fn query<M>(&self, params: M::Params) -> CliResult<Arc<ProtocolEvent>>
    where
        M: Method,
        M::Result: serde::Serialize,
    {
        decode_reply(&self.reply::<M>(params).await?)
    }

    /// Retain a direct reply's opaque values and scalar number tokens for
    /// machine output. Only the absent correlation control is supplied here.
    pub async fn reply<M>(&self, params: M::Params) -> CliResult<RawJson>
    where
        M: Method,
        M::Result: serde::Serialize,
    {
        let reply = RawJson::from_serialize(&self.call::<M>(params).await?)
            .map_err(|error| CliError::Codec(error.to_string()))?
            .with_default_string("submissionId", "")
            .map_err(CliError::Codec)?;
        if let Some(error) = reply.get("error").map_err(CliError::Codec)? {
            if error.as_str().trim() != "null" {
                let error = error.to_runtime().map_err(CliError::Codec)?;
                let message = error
                    .as_string()
                    .ok_or_else(|| CliError::Codec("a protocol error must be a string".into()))?;
                return Err(CliError::protocol(text::text(message)));
            }
        }
        Ok(reply)
    }

    /// Call an ack-reply method and hand back the first streamed envelope that
    /// satisfies `ready`.
    ///
    /// Subscribed BEFORE the call, because a submission id tags a stream rather
    /// than a value: the engine may write the first envelope of a submission
    /// before its receipt reaches this process. `ready` exists for the same
    /// reason — compaction, dreaming and review all report progress under the
    /// submission id before reporting the result, so "the first envelope with
    /// this id" is the wrong answer for them.
    pub async fn settle<M>(
        &self,
        params: M::Params,
        ready: impl Fn(&ProtocolEvent) -> bool,
    ) -> CliResult<Arc<ProtocolEvent>>
    where
        M: Method<Result = wire::SubmissionAck>,
    {
        let mut events = self.events();
        let submission_id = self.ack::<M>(params).await?;
        while let Some(event) = events.next().await {
            let event = event?;
            if event.submission_id != submission_id {
                continue;
            }
            refused(&event)?;
            if ready(event.as_ref()) {
                return Ok(event);
            }
        }
        Err(CliError::Protocol(
            "the engine closed the event stream before replying".to_string(),
        ))
    }

    /// Call an ack-reply method for its effect and hand back the submission id
    /// it was given, for a caller that follows the stream itself.
    pub async fn ack<M>(&self, params: M::Params) -> CliResult<RuntimeString>
    where
        M: Method<Result = wire::SubmissionAck>,
    {
        let ack = self.call::<M>(params).await?;
        Ok(RuntimeString::from(ack.submission_id))
    }

    /// This session's event stream, as inner-protocol envelopes.
    ///
    /// Subscribe before submitting whatever you intend to follow; see
    /// [`Self::settle`].
    pub fn events(&self) -> Events {
        Events {
            notifications: self.facade.notifications(),
            session_id: self.session_id.clone(),
            ended: false,
        }
    }

    /// The next question the engine is blocked on, or `None` once the
    /// connection has ended.
    ///
    /// Approvals and elicitations arrive here rather than on [`Self::events`]:
    /// the facade turns them into requests BACK to this process precisely so
    /// that one consumer must answer, instead of every event subscriber seeing
    /// an ask and none of them owning it. Cancel-safe, so it can sit in a
    /// `select!` beside the event stream.
    pub async fn next_ask(&self) -> Option<ReverseRequest> {
        self.facade.next_server_request().await
    }
}

/// An engine reached WITHOUT a session, for the endpoint-state methods.
///
/// Only [`Self::call`] exists, and that is the whole shape of this surface: every
/// method here has a declared result struct, none of them submits anything, so
/// there is no ack to settle and no event stream to correlate against.
pub struct EndpointClient {
    facade: Arc<AppServerClient>,
}

impl EndpointClient {
    pub fn new(facade: Arc<AppServerClient>) -> Self {
        Self { facade }
    }

    /// Call one declared method and get its declared result.
    pub async fn call<M: Method>(&self, params: M::Params) -> CliResult<M::Result> {
        Ok(self.facade.request::<M>(params).await?)
    }

    /// Give up the connection, for the same reason [`EngineClient::into_facade`]
    /// exists: the facade client aborts its reader when the last owner drops it,
    /// so teardown has to be able to take it back out.
    pub fn into_facade(self) -> Arc<AppServerClient> {
        self.facade
    }
}

/// This session's inner-protocol envelopes, decoded off the notification stream.
pub struct Events {
    notifications: Notifications,
    session_id: String,
    ended: bool,
}

impl Stream for Events {
    type Item = CliResult<Arc<ProtocolEvent>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut this.notifications).poll_next(cx) {
                Poll::Ready(Some(notification)) => {
                    if let Some(event) = envelope(&notification, &this.session_id) {
                        return Poll::Ready(Some(event));
                    }
                }
                Poll::Ready(None) => {
                    this.ended = true;
                    return Poll::Ready(
                        this.notifications
                            .terminal_error()
                            .map(|error| Err(CliError::from(error))),
                    );
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// One notification as this session's envelope, or nothing.
///
/// Filtered by session even though this CLI opens exactly one: a facade
/// connection multiplexes sessions, so the stream is only this session's
/// because it is filtered, not because of how the CLI happens to use it.
///
/// Malformed content is an explicit stream error. Valid exact native events
/// must never disappear between the facade subscription and the renderer.
fn envelope(
    notification: &Notification,
    session_id: &str,
) -> Option<CliResult<Arc<ProtocolEvent>>> {
    if notification.session_id() != Some(session_id) {
        return None;
    }
    match notification.event() {
        Ok(Some(event)) => Some(text::decode::<ProtocolEvent>(&event).map(Arc::new)),
        Ok(None) => None,
        Err(error) => Some(Err(CliError::from(error))),
    }
}

/// A submission-correlated `error` fails the call: an op the engine REFUSED must
/// not read as an op that replied with nothing.
fn refused(event: &ProtocolEvent) -> CliResult<()> {
    match &event.error {
        Some(error) => Err(CliError::protocol(text::text(error))),
        None => Ok(()),
    }
}

/// Decode a protocol view only when a caller needs its typed fields. The raw
/// reply remains authoritative for machine output and opaque numeric controls.
pub(crate) fn decode_reply(reply: &RawJson) -> CliResult<Arc<ProtocolEvent>> {
    let event: ProtocolEvent =
        runtime_codec::decode_text(&RuntimeString::from(reply.as_str())).map_err(CliError::from)?;
    refused(&event)?;
    Ok(Arc::new(event))
}
