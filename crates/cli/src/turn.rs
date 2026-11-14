//! One turn, driven to completion.
//!
//! `turn/start` is not request/response: it answers with a turn id that tags a
//! whole STREAM of events, and somewhere in that stream the engine may stop and
//! ask a question it will not proceed without. So a turn cannot be awaited as a
//! value — it has to be followed, answered, and only then settled. That is what
//! this module is.
//!
//! Everything the engine can block on is answered here: tool approvals, engine
//! elicitation, MCP form/URL elicitation, and MCP OAuth. Leaving any of them
//! unanswered does not fail loudly; it hangs the turn until the transport dies,
//! which is the worst possible failure mode for a CLI. When there is no terminal
//! to ask, each one is refused explicitly instead.
//!
//! Approvals and engine elicitation do not arrive on the event stream at all:
//! the facade turns them into requests back to this process, so exactly one
//! consumer owns answering them. They are therefore a THIRD thing this loop
//! selects on, and an unanswered one is a refusal rather than silence — see
//! [`EngineClient::next_ask`].
//!
//! The terminal is watched for the whole time, not only at the points the engine
//! asks something. Ctrl-C stops the op and a typed line steers it, and both have
//! to be noticed WHILE it runs — an interrupt observed after the turn ended is
//! not an interrupt.

use futures::{Stream, StreamExt};
use klaus_agent_server::app_server::runtime_wire::RawJson;
use std::future::Future;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader, Lines, Stdin};

use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::permission::{ApprovalRequest, ApprovalResponse};
use klaus_agent_protocol::types::{
    Input, McpAuthorizationResponseWire, McpElicitationRequest, McpElicitationResponse,
};
use klaus_agent_protocol::{
    ElicitResponseWire, Elicitation, McpElicitationAction, ProtocolEvent, RunOpOptions, RunResult,
};
use klaus_agent_server::app_server::methods::{
    SessionCancelRequest, SessionResolveMcpAuthorization, SessionResolveMcpElicitation,
    SessionSteer, TurnInterrupt, TurnStart,
};
use klaus_agent_server::app_server::wire;
use klaus_agent_server::{Method, ReverseRequest};

use crate::engine::EngineClient;
use crate::error::{CliError, CliResult};
use crate::output::Printer;
use crate::render::{self, Rendered};
use crate::text;

/// Translate numeric selections while preserving custom text, including commas.
fn structured_question_answer(
    answer: &str,
    options: &[RuntimeString],
    multi: bool,
) -> RuntimeString {
    let parts: Vec<&str> = answer.split(',').map(str::trim).collect();
    let selected: Option<Vec<&RuntimeString>> = parts
        .iter()
        .map(|part| {
            part.parse::<usize>()
                .ok()
                .and_then(|index| options.get(index.wrapping_sub(1)))
        })
        .collect();
    if let Some(selected) = selected.filter(|items| multi || items.len() == 1) {
        let mut value = RuntimeString::default();
        for (index, selected) in selected.into_iter().enumerate() {
            if index > 0 {
                value = value.concat(&RuntimeString::from(", "));
            }
            value = value.concat(selected);
        }
        value
    } else {
        RuntimeString::from(answer)
    }
}

#[cfg(test)]
mod structured_question_tests {
    use super::*;

    #[test]
    fn numeric_choices_are_mapped_and_custom_answers_are_exact() {
        let options = vec![RuntimeString::from("A, B"), RuntimeString::from("C")];
        assert_eq!(
            structured_question_answer("1,2", &options, true),
            RuntimeString::from("A, B, C")
        );
        assert_eq!(
            structured_question_answer("  custom, answer  ", &options, false),
            RuntimeString::from("  custom, answer  ")
        );
        assert_eq!(
            structured_question_answer("1", &options, false),
            RuntimeString::from("A, B")
        );
    }
}

/// How to answer a question the engine blocks on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalPolicy {
    /// Ask on the terminal. Only valid when stdin is one.
    Ask,
    /// Answer every request affirmatively without asking.
    Auto,
    /// Refuse every request, with a reason the model can act on.
    Refuse,
}

impl ApprovalPolicy {
    /// The policy for a non-interactive command: `--auto-approve` if asked for,
    /// otherwise refuse.
    ///
    /// Note this does NOT fall back to asking when a terminal happens to be
    /// attached. `klaus exec` is the scriptable entry point, so its behaviour must
    /// not change depending on where it was launched from.
    pub fn batch(auto_approve: bool) -> Self {
        if auto_approve {
            Self::Auto
        } else {
            Self::Refuse
        }
    }

    /// The policy for an interactive session: ask when there is a terminal,
    /// refuse when stdin is a pipe.
    pub fn interactive(printer: &Printer, auto_approve: bool) -> Self {
        if auto_approve {
            Self::Auto
        } else if printer.can_prompt() {
            Self::Ask
        } else {
            Self::Refuse
        }
    }
}

/// Turn how a turn ended into whether the command that ran it succeeded.
///
/// Only `Completed` means the engine did the thing it was asked to do. The other
/// five are it reporting that it did not — the credentials were rejected, the
/// prompt did not fit, the budget ran out — and a command that ends after one
/// turn has to say so in its exit status, or `klaus exec ... && next-step` runs
/// the next step on a turn that produced nothing.
///
/// The reason has already been PRINTED by the time this is reached, by the
/// turn's own result rendering. This decides the status, not the message.
///
/// The REPL deliberately does not call this. It showed the reason the same way,
/// and its recovery is the next prompt — turning a refused turn into an error
/// there would end a session the user still wants. That is also why the variant
/// is [`CliError::Turn`] and not [`CliError::Protocol`]: the session is intact,
/// so only a caller that was going to stop anyway treats this as fatal.
pub fn concluded(reason: TerminalReason) -> CliResult<()> {
    match reason {
        TerminalReason::Completed => Ok(()),
        refused => Err(CliError::turn(render::terminal_reason(refused))),
    }
}

/// The terminal: the lines it types and the ctrl-c it sends.
///
/// Exactly one of these exists per process. `tokio::io::stdin()` is buffered, so
/// a second reader over it would strand whatever the first one had already read
/// ahead — and both the REPL loop and a mid-turn approval need to read lines.
pub struct Console {
    lines: Lines<BufReader<Stdin>>,
    /// Installed on first use and never before.
    ///
    /// Registering a SIGINT handler REPLACES the default disposition for the rest
    /// of the process, so a command that installed one and then stopped listening
    /// would leave ctrl-c doing nothing at all — an unkillable `klaus`. Only the
    /// code that does listen at every point it can block installs it: the turn
    /// loop and the REPL prompt. Management commands never touch it, so ctrl-c
    /// keeps killing them the ordinary way.
    interrupts: Option<Interrupts>,
}

/// What the terminal did.
pub enum Typed {
    /// A line, exactly as typed.
    Line(String),
    /// End of input. Nothing more can ever be typed.
    End,
    /// Ctrl-C.
    Interrupted,
}

impl Console {
    pub fn new() -> Self {
        Self {
            lines: BufReader::new(tokio::io::stdin()).lines(),
            interrupts: None,
        }
    }

    /// Ask, then read one line. `None` means end of input, which every caller
    /// must treat as "stop", not as an empty answer.
    ///
    /// Ctrl-C is deliberately NOT handled here. This is the read used outside any
    /// op, where the default signal disposition is still in force and already
    /// does the right thing.
    pub async fn ask(&mut self, printer: &Printer, question: &str) -> CliResult<Option<String>> {
        printer.prompt(question)?;
        Ok(self.lines.next_line().await?)
    }

    /// Ask, then wait for either an answer or a ctrl-c.
    pub async fn ask_or_interrupt(
        &mut self,
        printer: &Printer,
        question: &str,
    ) -> CliResult<Typed> {
        printer.prompt(question)?;
        self.next_action(true).await
    }

    /// Wait for whatever the terminal does next.
    ///
    /// `read_line` false leaves stdin completely alone. That is not a
    /// micro-optimisation: during a piped run the bytes on stdin are the next
    /// thing the CALLER will read — a script's following prompt — and consuming
    /// one here would silently swallow a whole turn of it.
    ///
    /// Both arms are cancel-safe, which is what makes this usable as a `select!`
    /// arm beside an event stream. `Lines::next_line` keeps its partial line
    /// across a dropped poll, and interrupts are a stream rather than a
    /// re-registered one-shot for the same reason.
    pub async fn next_action(&mut self, read_line: bool) -> CliResult<Typed> {
        if self.interrupts.is_none() {
            self.interrupts = Some(Interrupts::install()?);
        }
        // Borrowed as two disjoint fields rather than through `&mut self`, so
        // both can be held by the select at once.
        let interrupts = self.interrupts.as_mut().expect("just installed");
        let lines = &mut self.lines;
        tokio::select! {
            line = lines.next_line(), if read_line => match line? {
                Some(line) => Ok(Typed::Line(line)),
                None => Ok(Typed::End),
            },
            _ = interrupts.recv() => Ok(Typed::Interrupted),
        }
    }

    /// Drain the rest of stdin as one string. Goes through the same reader as
    /// [`Self::ask`] on purpose: a prompt piped in on stdin and an approval typed
    /// at a terminal are the same byte stream, and a second reader over it would
    /// strand whatever this one buffered ahead.
    pub async fn read_to_end(&mut self) -> CliResult<String> {
        let mut lines = Vec::new();
        while let Some(line) = self.lines.next_line().await? {
            lines.push(line);
        }
        Ok(lines.join("\n"))
    }
}

impl Default for Console {
    fn default() -> Self {
        Self::new()
    }
}

/// Ctrl-C, as a stream.
///
/// A stream and not `tokio::signal::ctrl_c()`, because that builds a fresh
/// registration per call and a signal arriving while no registration is alive is
/// simply dropped — which is exactly the gap between two iterations of a
/// `select!` loop. Held open for the whole run, it cannot miss a press.
struct Interrupts {
    #[cfg(unix)]
    signal: tokio::signal::unix::Signal,
    #[cfg(windows)]
    signal: tokio::signal::windows::CtrlC,
}

impl Interrupts {
    fn install() -> CliResult<Self> {
        #[cfg(unix)]
        let signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        #[cfg(windows)]
        let signal = tokio::signal::windows::ctrl_c()?;
        Ok(Self { signal })
    }

    /// One press.
    ///
    /// Never resolves once the stream has ended: an ended signal stream means no
    /// further press can arrive, and reporting that as a press would abort an op
    /// nobody asked to stop.
    async fn recv(&mut self) {
        if self.signal.recv().await.is_none() {
            std::future::pending::<()>().await;
        }
    }
}

/// What ctrl-c sends for the op being followed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnInterrupt {
    /// Abort the session's running turn. This also releases every approval the
    /// engine has parked, which is what unwedges a turn interrupted while it was
    /// waiting for one.
    Turn,
    /// Cancel only this op, leaving any running turn alone. The engine tracks
    /// `side_question`, `review_session` and `generate_title` as detached
    /// read-only work cancellable this way and nothing else, and ignores the op
    /// for anything else — so it is opt-in per call site rather than the default.
    Request,
}

/// One iteration of [`Turn::drive`]: the engine spoke, the engine asked, or the
/// terminal did something.
enum Step {
    Event(Option<CliResult<Arc<ProtocolEvent>>>),
    Ask(Option<ReverseRequest>),
    Terminal(Typed),
}

/// A turn in flight.
pub struct Turn<'a> {
    engine: &'a EngineClient,
    printer: &'a Printer,
    console: &'a mut Console,
    policy: ApprovalPolicy,
    /// Whether the last thing written left the cursor mid-line, so a line-shaped
    /// event knows it owes a newline first. Without this, a status line lands in
    /// the middle of the model's sentence.
    partial: bool,
    /// Whether a line typed while this op runs is steered into it.
    steerable: bool,
    /// What ctrl-c sends.
    on_interrupt: OnInterrupt,
    /// The op being followed, so the reads nested inside the loop can name it
    /// when they cancel it.
    submission_id: RuntimeString,
    /// Whether ctrl-c has already been acted on, so a second press escalates
    /// instead of sending the same op again.
    interrupting: bool,
}

impl<'a> Turn<'a> {
    pub fn new(
        engine: &'a EngineClient,
        printer: &'a Printer,
        console: &'a mut Console,
        policy: ApprovalPolicy,
    ) -> Self {
        Self {
            engine,
            printer,
            console,
            policy,
            partial: false,
            steerable: false,
            on_interrupt: OnInterrupt::Turn,
            submission_id: RuntimeString::default(),
            interrupting: false,
        }
    }

    /// Let a line typed while the op runs steer it.
    ///
    /// Off by default, and deliberately not inferred from "stdin is a terminal":
    /// in a piped REPL the lines waiting on stdin are the FOLLOWING prompts, and
    /// reading one mid-turn would consume a whole turn of the script.
    pub fn steerable(mut self) -> Self {
        self.steerable = true;
        self
    }

    /// Make ctrl-c cancel just this op instead of the session's turn. Valid only
    /// for the ops the engine tracks as cancellable; see [`OnInterrupt::Request`].
    pub fn cancelling(mut self) -> Self {
        self.on_interrupt = OnInterrupt::Request;
        self
    }

    /// Submit one turn and follow it until the engine reports its result.
    ///
    /// The event subscription is taken BEFORE the call, because the turn id it
    /// answers with tags a stream the engine may already have started writing.
    pub async fn run(
        &mut self,
        input: Input,
        options: Option<RunOpOptions>,
    ) -> CliResult<RunResult> {
        let events = self.engine.events();
        let started = self
            .engine
            .call::<TurnStart>(wire::TurnStartParams {
                session_id: self.engine.session_id().to_string(),
                input: text::input(&input)?,
                options: options.as_ref().map(text::to_json).transpose()?,
                submission_id: None,
            })
            .await?;
        // The turn id IS the inner submission id: every `session/event` for this
        // turn echoes it, which is what makes it the thing to filter on.
        self.drive(
            RuntimeString::from(started.turn_id),
            events,
            std::future::pending(),
            |event| event.result.clone(),
        )
        .await
    }

    /// Follow an ack-reply method that is a turn under another name, settling on
    /// whichever reply field `settle` reads.
    ///
    /// `session/compactStart` and `session/reviewStart` both spend model calls,
    /// stream engine events and can raise approvals, so driving them through
    /// [`show`] would show nothing until they finished and would hang the moment
    /// one asked a question. The only thing they do not share with `turn/start`
    /// is which field carries the answer.
    ///
    /// [`show`]: crate::commands::show
    pub async fn follow<M, T>(
        &mut self,
        params: M::Params,
        settle: impl Fn(&ProtocolEvent) -> Option<T>,
    ) -> CliResult<T>
    where
        M: Method<Result = wire::SubmissionAck>,
    {
        let events = self.engine.events();
        let submission_id = self.engine.ack::<M>(params).await?;
        self.drive(submission_id, events, std::future::pending(), settle)
            .await
    }

    /// Follow a direct-result method that spends model calls, settling on the
    /// reply it eventually answers with rather than on an event.
    ///
    /// `session/generateTitle` is one call, not a stream, so it is declared with
    /// a result — and that reply is CLAIMED, so its terminal envelope never
    /// reaches the event stream and [`Self::follow`] would wait for it forever.
    /// Everything else about it is a turn: it costs model calls, it streams
    /// progress, and it is worth being able to stop. So it is still followed; the
    /// only difference is where the settling value comes from.
    ///
    /// `submission_id` must be the one in `params`, and it is the CALLER's rather
    /// than the facade's on purpose: a declared-result method adopts a
    /// client-chosen submission id verbatim precisely so `session/cancelRequest`
    /// has something to name, and a caller that let the facade name it could not
    /// interrupt what it started.
    pub async fn query<M>(&mut self, params: M::Params, submission_id: &str) -> CliResult<RawJson>
    where
        M: Method<Result = RawJson>,
    {
        let engine = self.engine;
        let events = engine.events();
        self.drive(
            RuntimeString::from(submission_id),
            events,
            engine.reply::<M>(params),
            // The reply is the settling value, so no event can be: the engine
            // hands the terminal envelope to the call and streams only progress.
            |_| None,
        )
        .await
    }

    /// Follow one op to its settling reply, answering everything it blocks on and
    /// staying responsive to the terminal throughout.
    ///
    /// `reply` is the pending answer to the call that started the op, for the
    /// methods whose settling value arrives that way; the ones that settle on an
    /// event pass a future that never resolves.
    async fn drive<T, S, R>(
        &mut self,
        submission_id: RuntimeString,
        mut events: S,
        reply: R,
        settle: impl Fn(&ProtocolEvent) -> Option<T>,
    ) -> CliResult<T>
    where
        S: Stream<Item = CliResult<Arc<ProtocolEvent>>> + Unpin,
        R: Future<Output = CliResult<T>>,
    {
        self.submission_id = submission_id;
        // Copied out so the ask arm borrows the client rather than `self`, which
        // the terminal arm needs mutably.
        let engine = self.engine;
        let mut reply = std::pin::pin!(reply);
        // The reverse channel can end while the event stream is still open, and a
        // stream that has ended yields `None` forever — so the arm is switched
        // off rather than being allowed to win every race from then on.
        let mut answering = true;
        loop {
            // Which branch won is bound to `step` and the select ends there, so
            // the futures — and the borrow of `self.console` one of them holds —
            // are dropped before the handlers below touch `self` again.
            let step = tokio::select! {
                event = events.next() => Step::Event(event),
                ask = engine.next_ask(), if answering => Step::Ask(ask),
                settled = reply.as_mut() => return settled,
                action = self.console.next_action(self.steerable) => Step::Terminal(action?),
            };
            match step {
                Step::Event(Some(event)) => {
                    let event = event?;
                    if let Some(value) = self.observe(&event, &settle).await? {
                        return Ok(value);
                    }
                }
                Step::Event(None) => {
                    return Err(CliError::Protocol(
                        "the engine closed the event stream before the turn finished".to_string(),
                    ))
                }
                Step::Ask(Some(ask)) => self.answer(ask).await?,
                // Nothing can block on a question that can no longer be asked, so
                // the op stays followable.
                Step::Ask(None) => answering = false,
                Step::Terminal(action) => self.terminal(action).await?,
            }
        }
    }

    /// React to what the terminal did while the op was running.
    async fn terminal(&mut self, action: Typed) -> CliResult<()> {
        match action {
            Typed::Interrupted => self.interrupt().await,
            Typed::Line(line) => {
                let line = line.trim();
                if line.is_empty() {
                    return Ok(());
                }
                self.finish_line()?;
                // Steering is fire-and-forget on purpose. The engine acks it with
                // `steered` only once a running step actually attaches the input,
                // and correlated to the STEER's own submission id — so the ack is
                // not part of this op's stream, and waiting for it here would
                // stall the loop that has to keep draining that stream.
                self.engine
                    .ack::<SessionSteer>(wire::SessionSteerParams {
                        session_id: self.engine.session_id().to_string(),
                        input: text::input(&Input::Text(RuntimeString::from(line)))?,
                        priority: None,
                        submission_id: None,
                        // See commands/goal.rs: the CLI keeps the legacy
                        // tool-result steer shape its renderer expects.
                        delivery: None,
                        display_metadata: None,
                        expected_turn_id: None,
                        queue_message_id: None,
                    })
                    .await?;
                self.printer
                    .note("klaus: steering — the engine takes this up at its next step");
                Ok(())
            }
            // Nothing more can ever be typed, so stop watching stdin — but keep
            // following the op. A turn whose input ended is not a turn that failed.
            Typed::End => {
                self.steerable = false;
                Ok(())
            }
        }
    }

    /// Act on one ctrl-c.
    async fn interrupt(&mut self) -> CliResult<()> {
        self.finish_line()?;
        if self.interrupting {
            // The engine was already asked to stop and has not settled the op.
            // Waiting on would trap the user inside something they have twice
            // asked to end, so this abandons the stream instead. `Turn` is the
            // recoverable variant, so a REPL survives it and keeps its session.
            return Err(CliError::turn(
                "interrupted: gave up waiting for the engine to stop",
            ));
        }
        self.interrupting = true;
        match self.on_interrupt {
            OnInterrupt::Turn => {
                self.engine
                    .call::<TurnInterrupt>(wire::TurnInterruptParams {
                        session_id: self.engine.session_id().to_string(),
                        submission_id: None,
                        origin: None,
                        expected_turn_id: None,
                    })
                    .await?;
            }
            // `requestId` is the target op's own submission id: this cancels the
            // detached read-only work being followed, not a reverse request.
            OnInterrupt::Request => {
                self.engine
                    .ack::<SessionCancelRequest>(wire::SessionRequestIdParams {
                        session_id: self.engine.session_id().to_string(),
                        request_id: text::text(&self.submission_id),
                        submission_id: None,
                    })
                    .await?;
            }
        }
        self.printer
            .note("klaus: interrupting — ctrl-c again to stop waiting");
        Ok(())
    }

    /// Handle one event. `Some` means the op has settled.
    async fn observe<T>(
        &mut self,
        event: &Arc<ProtocolEvent>,
        settle: &impl Fn(&ProtocolEvent) -> Option<T>,
    ) -> CliResult<Option<T>> {
        // An empty submission id marks a push the engine correlated to no op at
        // all (MCP transitions, extension failures). Anything else that is not
        // ours belongs to a different op — including the replies to the ops this
        // loop itself sends while answering a prompt, and the ack for a steer.
        if event.submission_id.is_empty() {
            return self.unsolicited(event).map(|()| None);
        }
        if event.submission_id != self.submission_id {
            return Ok(None);
        }

        // `--json` forwards the engine's own event, unprojected, before anything
        // is interpreted: that is the contract, and it must hold even for events
        // this loop goes on to answer.
        self.printer.json(&text::encode(event.as_ref())?)?;

        if let Some(message) = &event.error {
            self.finish_line()?;
            return Err(CliError::turn(text::text(message)));
        }
        if let Some(engine_event) = &event.event {
            self.engine_event(engine_event).await?;
        }
        if let Some(elicitation) = &event.mcp_elicitation {
            self.answer_mcp_elicitation(elicitation).await?;
        }
        if let Some(authorization) = &event.mcp_authorization {
            self.answer_mcp_authorization(authorization).await?;
        }
        if let Some(progress) = &event.compact_progress {
            self.write(Rendered::Line(format!(
                "[compacting] {}",
                render::wire_tag(&progress.phase)
            )))?;
        }
        if let Some(value) = settle(event.as_ref()) {
            self.finish_line()?;
            return Ok(Some(value));
        }
        Ok(None)
    }

    /// Show one engine event.
    ///
    /// Approvals are not among them: the facade intercepts `approval.requested`
    /// and re-asks it as a reverse request, so it is [`Self::answer`] that owns
    /// presenting and answering one — and the renderer deliberately formats it
    /// as nothing, because rendering must stay free of side effects.
    async fn engine_event(&mut self, event: &EngineEvent) -> CliResult<()> {
        let columns = self.printer.columns();
        self.write(render::engine_event(self.printer.painter(), columns, event))
    }

    /// Answer one question the engine is blocked on.
    ///
    /// An unrecognised method is refused rather than dropped. The engine parks
    /// until it is told something, and a facade that grew a new ask this CLI has
    /// not learned yet must not turn into a hung turn.
    async fn answer(&mut self, ask: ReverseRequest) -> CliResult<()> {
        match ask.method.as_str() {
            "approval/request" => self.answer_approval(ask).await,
            "elicitation/request" => self.answer_elicitation(ask).await,
            method => {
                let reason = format!("this CLI cannot answer {method}");
                self.printer.note(&format!("klaus: {reason}"));
                ask.refuse(&reason);
                Ok(())
            }
        }
    }

    /// A tool approval, asked as a reverse request.
    async fn answer_approval(&mut self, ask: ReverseRequest) -> CliResult<()> {
        let request: ApprovalRequest = match ask.params.get("request").map_err(CliError::Codec)? {
            Some(request) => text::decode(&request)?,
            None => {
                ask.refuse("approval/request carried no request");
                return Ok(());
            }
        };
        self.finish_line()?;
        // A panel is several lines; each goes out as its own note so the
        // diagnostics stream stays line-oriented.
        for line in
            render::approval_request(self.printer.painter(), self.printer.columns(), &request)
        {
            self.printer.note(&line);
        }
        let response = match self.decide("allow this?").await? {
            true => ApprovalResponse::Allow {
                updated_input: None,
                updated_permissions: None,
            },
            false => ApprovalResponse::Deny {
                message: Some(RuntimeString::from(self.refusal())),
            },
        };
        ask.respond(text::to_json(&response)?);
        Ok(())
    }

    async fn answer_elicitation(&mut self, ask: ReverseRequest) -> CliResult<()> {
        let elicitation: Elicitation =
            match ask.params.get("elicitation").map_err(CliError::Codec)? {
                Some(elicitation) => text::decode(&elicitation)?,
                None => {
                    ask.refuse("elicitation/request carried no elicitation");
                    return Ok(());
                }
            };
        if let Some(questions) = &elicitation.request.questions {
            if self.policy != ApprovalPolicy::Ask {
                ask.respond(text::to_json(&ElicitResponseWire::Decline {
                    reason: Some(RuntimeString::from("Interactive questions require user answers; automatic approval cannot select answers.")),
                })?);
                return Ok(());
            }
            let mut answers = klaus_agent_core::runtime_values::RuntimeMap::new();
            for question in questions {
                let prompt = question
                    .get("question")
                    .and_then(klaus_agent_core::runtime_values::RuntimeJson::as_str)
                    .cloned()
                    .unwrap_or_default();
                let options = question
                    .get("options")
                    .and_then(klaus_agent_core::runtime_values::RuntimeJson::as_array)
                    .map(|options| {
                        options
                            .iter()
                            .filter_map(|option| {
                                option
                                    .get("label")
                                    .and_then(klaus_agent_core::runtime_values::RuntimeJson::as_str)
                                    .cloned()
                            })
                            .collect()
                    });
                let request = klaus_agent_protocol::ElicitRequestWire {
                    prompt: prompt.clone(),
                    questions: None,
                    attachments_supported: None,
                    metadata: None,
                    options,
                    multi_select: question
                        .get("multiSelect")
                        .and_then(klaus_agent_core::runtime_values::RuntimeJson::as_bool),
                };
                self.finish_line()?;
                self.printer
                    .note(&render::elicitation(self.printer.painter(), &request));
                let Some(answer) = self.ask("> ").await? else {
                    break;
                };
                if !answer.trim().is_empty() {
                    let value = structured_question_answer(
                        &answer,
                        request.options.as_deref().unwrap_or_default(),
                        request.multi_select == Some(true),
                    );
                    let key = question
                        .get("id")
                        .and_then(klaus_agent_core::runtime_values::RuntimeJson::as_str)
                        .cloned()
                        .unwrap_or(prompt);
                    answers.insert(key, value);
                }
            }
            ask.respond(text::to_json(&ElicitResponseWire::Answers {
                answers,
                attachments: None,
            })?);
            return Ok(());
        }
        let request = &elicitation.request;
        self.finish_line()?;
        self.printer
            .note(&render::elicitation(self.printer.painter(), request));
        let response = match self.policy {
            ApprovalPolicy::Refuse => ElicitResponseWire::Decline {
                reason: Some(RuntimeString::from(self.refusal())),
            },
            // "Auto" cannot invent an answer to a free-text question, and
            // guessing one would put words in the user's mouth. Taking the first
            // offered option is the only defensible automatic answer, and only
            // when the engine offered a closed set.
            ApprovalPolicy::Auto => match request.options.as_ref().and_then(|o| o.first()) {
                Some(option) => ElicitResponseWire::Accept {
                    values: vec![option.clone()],
                },
                None => ElicitResponseWire::Decline {
                    reason: Some(RuntimeString::from(
                        "--auto-approve cannot answer a free-text prompt",
                    )),
                },
            },
            ApprovalPolicy::Ask => self.read_elicitation_answer(request).await?,
        };
        ask.respond(text::to_json(&response)?);
        Ok(())
    }

    /// Ask a question raised by the op in flight.
    ///
    /// `None` means no answer is coming: input ended, or the user pressed ctrl-c,
    /// which additionally stops the op. Every caller already has to handle "no
    /// answer" — the engine parks until it is told something, so silence is never
    /// an option — which is why an interrupt needs no separate path through them.
    async fn ask(&mut self, question: &str) -> CliResult<Option<String>> {
        match self
            .console
            .ask_or_interrupt(self.printer, question)
            .await?
        {
            Typed::Line(line) => Ok(Some(line)),
            Typed::End => Ok(None),
            Typed::Interrupted => {
                self.interrupt().await?;
                Ok(None)
            }
        }
    }

    /// Read an answer for an engine elicitation. Options are answered by index
    /// (or by exact text), because an option's own text may contain the comma
    /// that separates a multi-select answer.
    async fn read_elicitation_answer(
        &mut self,
        request: &klaus_agent_protocol::ElicitRequestWire,
    ) -> CliResult<ElicitResponseWire> {
        let Some(answer) = self.ask("> ").await? else {
            return Ok(ElicitResponseWire::Decline {
                reason: Some(RuntimeString::from("no answer: input ended")),
            });
        };
        let answer = answer.trim();
        if answer.is_empty() {
            return Ok(ElicitResponseWire::Decline {
                reason: Some(RuntimeString::from("no answer given")),
            });
        }
        let Some(options) = &request.options else {
            return Ok(ElicitResponseWire::Accept {
                values: vec![RuntimeString::from(answer)],
            });
        };
        let mut values = Vec::new();
        for part in answer.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            match part
                .parse::<usize>()
                .ok()
                .and_then(|index| options.get(index.wrapping_sub(1)))
            {
                Some(option) => values.push(option.clone()),
                None => values.push(RuntimeString::from(part)),
            }
        }
        if values.is_empty() {
            return Ok(ElicitResponseWire::Decline {
                reason: Some(RuntimeString::from("no answer given")),
            });
        }
        // A single-select prompt gets one value even if several were typed;
        // sending more would break the server's own contract.
        if request.multi_select != Some(true) {
            values.truncate(1);
        }
        Ok(ElicitResponseWire::Accept { values })
    }

    /// MCP elicitation is a server-authored form, so there is nothing sensible to
    /// synthesise for it: the CLI shows what was asked and either declines or
    /// takes a raw JSON object typed by the user. Declining is a real MCP answer,
    /// unlike silence, which strands the calling tool.
    async fn answer_mcp_elicitation(
        &mut self,
        event: &klaus_agent_protocol::McpElicitationEvent,
    ) -> CliResult<()> {
        self.finish_line()?;
        let (message, shape) = match &event.request {
            McpElicitationRequest::Form { message, .. } => (message, "form".to_string()),
            McpElicitationRequest::OpenAiForm { message, .. } => (message, "form".to_string()),
            McpElicitationRequest::Url { message, url, .. } => {
                (message, format!("open {}", text::text(url)))
            }
        };
        self.printer.note(&format!(
            "[mcp input: {} / {shape}] {}",
            text::text(&event.server_name),
            text::text(message)
        ));

        let response = match self.policy {
            ApprovalPolicy::Ask => match self.ask("JSON answer (empty to decline): ").await? {
                Some(answer) if !answer.trim().is_empty() => McpElicitationResponse {
                    action: McpElicitationAction::Accept,
                    content: Some(text::parse_json(answer.trim())?),
                    meta: None,
                },
                _ => McpElicitationResponse::decline(),
            },
            _ => McpElicitationResponse::decline(),
        };
        self.engine
            .ack::<SessionResolveMcpElicitation>(wire::SessionRequestResolutionParams {
                session_id: self.engine.session_id().to_string(),
                request_id: text::text(&event.request_id),
                response: Some(text::to_json(&response)?),
                submission_id: None,
            })
            .await?;
        Ok(())
    }

    /// MCP OAuth needs a browser round trip the CLI cannot perform for the user,
    /// so the URL is printed and the resulting `code` is pasted back. Declining
    /// still has to be sent, or the MCP connection attempt never settles.
    async fn answer_mcp_authorization(
        &mut self,
        request: &klaus_agent_protocol::types::McpAuthorizationRequestWire,
    ) -> CliResult<()> {
        self.finish_line()?;
        self.printer.note(&format!(
            "[mcp auth: {}] open {}\n  redirect: {}",
            text::text(&request.server_name),
            text::text(&request.authorization_url),
            text::text(&request.redirect_uri)
        ));
        let response = match self.policy {
            ApprovalPolicy::Ask => {
                match self.ask("authorization code (empty to decline): ").await? {
                    Some(code) if !code.trim().is_empty() => McpAuthorizationResponseWire::Code {
                        code: RuntimeString::from(code.trim()),
                        state: None,
                    },
                    _ => McpAuthorizationResponseWire::Decline { reason: None },
                }
            }
            _ => McpAuthorizationResponseWire::Decline {
                reason: Some(RuntimeString::from(self.refusal())),
            },
        };
        self.engine
            .ack::<SessionResolveMcpAuthorization>(wire::SessionRequestResolutionParams {
                session_id: self.engine.session_id().to_string(),
                request_id: text::text(&request.request_id),
                response: Some(text::to_json(&response)?),
                submission_id: None,
            })
            .await?;
        Ok(())
    }

    /// A yes/no decision under the active policy.
    async fn decide(&mut self, question: &str) -> CliResult<bool> {
        match self.policy {
            ApprovalPolicy::Auto => Ok(true),
            ApprovalPolicy::Refuse => Ok(false),
            ApprovalPolicy::Ask => {
                let answer = self.ask(&format!("{question} [y/N] ")).await?;
                Ok(matches!(
                    answer.as_deref().map(str::trim),
                    Some("y") | Some("Y") | Some("yes") | Some("YES")
                ))
            }
        }
    }

    /// Why a request was refused, phrased for the model rather than the user: it
    /// is the model that receives this and has to pick a different approach.
    fn refusal(&self) -> &'static str {
        match self.policy {
            ApprovalPolicy::Refuse => {
                "refused: this CLI is running non-interactively; re-run with --auto-approve to allow"
            }
            _ => "refused by the user",
        }
    }

    /// Events the engine correlated to no op. They are shown as diagnostics, not
    /// as turn output, because they can arrive between two unrelated turns.
    fn unsolicited(&self, event: &ProtocolEvent) -> CliResult<()> {
        self.printer.json(&text::encode(event)?)?;
        if let Some(failure) = &event.extension_failure {
            self.printer.note(&format!(
                "[extension failed] {} {}: {}",
                render::extension_failure_kind(failure.kind),
                text::text(&failure.id),
                text::text(&failure.message)
            ));
        }
        if let Some(status) = &event.mcp_server_status {
            let mut line = format!(
                "[mcp {}] {}",
                render::wire_tag(&status.state),
                text::text(&status.server_name)
            );
            if let Some(error) = &status.error {
                line.push_str(&format!(": {}", text::text(error)));
            }
            self.printer.note(&line);
        }
        Ok(())
    }

    fn write(&mut self, rendered: Rendered) -> CliResult<()> {
        match &rendered {
            Rendered::Nothing => Ok(()),
            Rendered::Stream(chunk) => {
                // A chunk that already ends in a newline leaves the cursor at
                // column zero, so it owes nothing.
                self.partial = !chunk.ends_with('\n');
                rendered.emit(self.printer)
            }
            Rendered::Line(_) => {
                self.finish_line()?;
                rendered.emit(self.printer)
            }
        }
    }

    /// Close off a partially written line so the next whole line starts clean.
    fn finish_line(&mut self) -> CliResult<()> {
        if self.partial {
            self.partial = false;
            self.printer.line("")?;
        }
        Ok(())
    }
}
