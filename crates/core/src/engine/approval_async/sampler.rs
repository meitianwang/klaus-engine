use futures::{Stream, StreamExt};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub const CLASSIFIER_MODEL: &str = "gpt-5.6-luna";
pub const MAX_CLASSIFIER_OUTPUT_BYTES: usize = 8 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ClassifierError {
    #[error("classification did not contain assistant output")]
    MissingOutput,
    #[error("classification exceeded its response allowance")]
    OutputTooLarge,
    #[error("classification was superseded")]
    Superseded,
    #[error("classification cannot reuse the parent checkpoint")]
    IncompatibleCompaction,
    #[error("classification exceeds the model input allowance")]
    InputTooLarge,
    #[error("the retained classification queue is full")]
    QueueFull,
    #[error("classification returned an invalid label")]
    InvalidOutput,
    #[error("classification transport failed")]
    Transport,
    #[error("classification account owner changed")]
    AccountChanged,
}

/// Provider output is normalized without granting the classifier any tools.
#[derive(Debug)]
pub enum SamplingEvent<H> {
    /// A failed, unscored attempt was drained before retrying identical input.
    Restart,
    OutputTextDelta(String),
    /// A complete assistant-text or encrypted-reasoning item for retained history.
    OutputItem {
        item: H,
        tokens: usize,
        valid_for_retention: bool,
        assistant_text: Option<String>,
    },
    Completed,
    Error(ClassifierError),
}

pub type SamplingStream<H> = Pin<Box<dyn Stream<Item = SamplingEvent<H>> + Send>>;

struct ActiveClassification {
    id: u64,
    cancel: CancellationToken,
    scored: Arc<AtomicBool>,
}

struct AdmissionState {
    active: Mutex<VecDeque<ActiveClassification>>,
    sequence: AtomicU64,
    permits: Arc<Semaphore>,
    drains: TaskTracker,
}

pub struct ClassifierSampler {
    admission: Arc<AdmissionState>,
}

impl Default for ClassifierSampler {
    fn default() -> Self {
        Self {
            admission: Arc::new(AdmissionState {
                active: Mutex::new(VecDeque::with_capacity(16)),
                sequence: AtomicU64::new(0),
                permits: Arc::new(Semaphore::new(16)),
                drains: TaskTracker::new(),
            }),
        }
    }
}

/// Admission remains owned through response draining after early publication.
pub struct ClassifierAdmission {
    id: Option<u64>,
    owner: Weak<AdmissionState>,
    pub cancellation: CancellationToken,
    scored: Arc<AtomicBool>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for ClassifierAdmission {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let (Some(owner), Some(id)) = (self.owner.upgrade(), self.id) {
            owner
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retain(|active| active.id != id);
        }
    }
}

impl ClassifierSampler {
    pub async fn shutdown(&self) {
        for active in self
            .admission
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            active.cancel.cancel();
        }
        self.admission.drains.close();
        self.admission.drains.wait().await;
    }

    pub fn admit(&self) -> ClassifierAdmission {
        let mut active = self
            .admission
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        active.retain(|request| !request.cancel.is_cancelled());
        if active.len() == 16 {
            let oldest = active
                .iter()
                .position(|request| request.scored.load(Ordering::Relaxed))
                .unwrap_or(0);
            if let Some(oldest) = active.remove(oldest) {
                oldest.cancel.cancel();
            }
        }
        let id = self.admission.sequence.fetch_add(1, Ordering::Relaxed);
        let cancellation = CancellationToken::new();
        let scored = Arc::new(AtomicBool::new(false));
        active.push_back(ActiveClassification {
            id,
            cancel: cancellation.clone(),
            scored: Arc::clone(&scored),
        });
        ClassifierAdmission {
            id: Some(id),
            owner: Arc::downgrade(&self.admission),
            cancellation,
            scored,
            permit: None,
        }
    }
    /// Conversation preparation already has a non-evicting reservation.
    pub fn admit_retained(&self) -> ClassifierAdmission {
        ClassifierAdmission {
            id: None,
            owner: Arc::downgrade(&self.admission),
            cancellation: CancellationToken::new(),
            scored: Arc::new(AtomicBool::new(false)),
            permit: None,
        }
    }
}

pub struct ClassifierOutput<H> {
    pub result: Result<String, ClassifierError>,
    /// None on partial, invalid, contradictory or oversized retained output.
    pub retained_output: Option<Vec<H>>,
}

struct ResponseCleanup {
    tasks: TaskTracker,
    future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    permit: Option<OwnedSemaphorePermit>,
}

impl ResponseCleanup {
    async fn finish(mut self) {
        if let Some(cleanup) = self.future.take() {
            cleanup.await;
        }
    }
}

impl Drop for ResponseCleanup {
    fn drop(&mut self) {
        if let Some(cleanup) = self.future.take() {
            let permit = self.permit.take();
            match tokio::runtime::Handle::try_current() {
                Ok(runtime) => {
                    self.tasks.spawn_on(
                        async move {
                            cleanup.await;
                            drop(permit);
                        },
                        &runtime,
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        "Classifier response cleanup could not run after runtime shutdown"
                    );
                }
            }
        }
    }
}

impl ClassifierAdmission {
    /// Waiting for a transport slot remains cancellable by expiry and admission.
    pub async fn acquire(&mut self) -> Result<(), ClassifierError> {
        if self.permit.is_some() {
            return Ok(());
        }
        let owner = self.owner.upgrade().ok_or(ClassifierError::Superseded)?;
        let permit = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(ClassifierError::Superseded),
            permit = Arc::clone(&owner.permits).acquire_owned() => permit,
        }
        .map_err(|_| ClassifierError::Superseded)?;
        self.permit = Some(permit);
        Ok(())
    }

    pub fn mark_scored(&self) {
        self.scored.store(true, Ordering::Relaxed);
    }

    /// The host owns transport retries, credential generation and usage accounting.
    /// `finished` drains/interrupts the existing response transport on cancellation.
    /// Snapshot publication returns before the drain, retaining the same admission.
    pub async fn sample_snapshot<H, Finished, FinishedFuture>(
        mut self,
        mut events: SamplingStream<H>,
        expired: impl Future<Output = ()> + Send + 'static,
        finished: Finished,
    ) -> Result<String, ClassifierError>
    where
        H: Send + 'static,
        Finished: FnOnce() -> FinishedFuture + Send + 'static,
        FinishedFuture: Future<Output = ()> + Send + 'static,
    {
        let mut cleanup = ResponseCleanup {
            tasks: self
                .owner
                .upgrade()
                .map(|owner| owner.drains.clone())
                .unwrap_or_default(),
            future: Some(Box::pin(finished())),
            permit: None,
        };
        self.acquire().await?;
        cleanup.permit = self.permit.take();
        let mut expired = Box::pin(expired);
        let mut output = String::new();
        loop {
            let event = tokio::select! {
                biased;
                () = self.cancellation.cancelled() => {
                    cleanup.finish().await;
                    return if self.scored.load(Ordering::Relaxed) && !output.is_empty() { Ok(output) } else { Err(ClassifierError::Superseded) };
                },
                () = &mut expired => {
                    cleanup.finish().await;
                    return if self.scored.load(Ordering::Relaxed) && !output.is_empty() { Ok(output) } else { Err(ClassifierError::Superseded) };
                },
                event = events.next() => event,
            };
            match event {
                Some(SamplingEvent::Restart) => output.clear(),
                Some(SamplingEvent::OutputTextDelta(delta)) if !delta.is_empty() => {
                    if delta.len() > MAX_CLASSIFIER_OUTPUT_BYTES {
                        cleanup.finish().await;
                        return Err(ClassifierError::OutputTooLarge);
                    }
                    self.mark_scored();
                    let tasks = cleanup.tasks.clone();
                    tasks.spawn(async move {
                        loop {
                            let event = tokio::select! {
                                biased;
                                () = self.cancellation.cancelled() => None,
                                () = &mut expired => None,
                                event = events.next() => event,
                            };
                            if matches!(
                                event,
                                None | Some(SamplingEvent::Completed | SamplingEvent::Error(_))
                            ) {
                                break;
                            }
                        }
                        cleanup.finish().await;
                        drop(self);
                    });
                    return Ok(delta);
                }
                Some(SamplingEvent::OutputItem {
                    assistant_text: Some(text),
                    ..
                }) => {
                    if output.len().saturating_add(text.len()) > MAX_CLASSIFIER_OUTPUT_BYTES {
                        cleanup.finish().await;
                        return Err(ClassifierError::OutputTooLarge);
                    }
                    output.push_str(&text);
                    if !output.is_empty() {
                        self.mark_scored();
                    }
                }
                Some(SamplingEvent::Completed) => {
                    cleanup.finish().await;
                    return if output.is_empty() {
                        Err(ClassifierError::MissingOutput)
                    } else {
                        Ok(output)
                    };
                }
                Some(SamplingEvent::Error(error)) => {
                    cleanup.finish().await;
                    return Err(error);
                }
                None => {
                    cleanup.finish().await;
                    return if self.cancellation.is_cancelled() {
                        if self.scored.load(Ordering::Relaxed) && !output.is_empty() {
                            Ok(output)
                        } else {
                            Err(ClassifierError::Superseded)
                        }
                    } else {
                        Err(ClassifierError::MissingOutput)
                    };
                }
                _ => {}
            }
        }
    }

    /// Retained admission holds its permit through the completed validated output.
    pub async fn sample_retained<H, Finished, FinishedFuture>(
        mut self,
        mut events: SamplingStream<H>,
        mut ready: Option<oneshot::Sender<Result<String, ClassifierError>>>,
        remaining_tokens: usize,
        expired: impl Future<Output = ()> + Send,
        finished: Finished,
    ) -> ClassifierOutput<H>
    where
        Finished: FnOnce() -> FinishedFuture,
        FinishedFuture: Future<Output = ()> + Send + 'static,
    {
        let mut cleanup = ResponseCleanup {
            tasks: self
                .owner
                .upgrade()
                .map(|owner| owner.drains.clone())
                .unwrap_or_default(),
            future: Some(Box::pin(finished())),
            permit: None,
        };
        if let Err(error) = self.acquire().await {
            if let Some(ready) = ready.take() {
                let _ = ready.send(Err(error.clone()));
            }
            return ClassifierOutput {
                result: Err(error),
                retained_output: None,
            };
        }
        cleanup.permit = self.permit.take();
        tokio::pin!(expired);
        let mut retained_output = Some(Vec::new());
        let retention_budget = remaining_tokens;
        let mut remaining_tokens = remaining_tokens;
        let mut output = String::new();
        let mut first = None;
        let result = loop {
            let event = tokio::select! {
                biased;
                () = self.cancellation.cancelled() => break Err(ClassifierError::Superseded),
                () = &mut expired => break Err(ClassifierError::Superseded),
                event = events.next() => event,
            };
            match event {
                Some(SamplingEvent::Restart) if first.is_none() => {
                    retained_output = Some(Vec::new());
                    remaining_tokens = retention_budget;
                    output.clear();
                }
                Some(SamplingEvent::OutputTextDelta(delta))
                    if first.is_none() && !delta.is_empty() =>
                {
                    if delta.len() > MAX_CLASSIFIER_OUTPUT_BYTES {
                        break Err(ClassifierError::OutputTooLarge);
                    }
                    first = Some(delta.clone());
                    self.mark_scored();
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(Ok(delta));
                    }
                }
                Some(SamplingEvent::OutputItem {
                    item,
                    tokens,
                    valid_for_retention,
                    assistant_text,
                }) => {
                    if tokens > remaining_tokens || tokens > 10_000 || !valid_for_retention {
                        retained_output = None;
                    } else if let Some(output) = retained_output.as_mut() {
                        remaining_tokens -= tokens;
                        output.push(item);
                    }
                    if let Some(text) = assistant_text {
                        if output.len().saturating_add(text.len()) > MAX_CLASSIFIER_OUTPUT_BYTES {
                            break Err(ClassifierError::OutputTooLarge);
                        }
                        output.push_str(&text);
                    }
                }
                Some(SamplingEvent::Completed) => {
                    break if output.is_empty() {
                        Err(ClassifierError::MissingOutput)
                    } else {
                        Ok(output)
                    }
                }
                Some(SamplingEvent::Error(error)) => break Err(error),
                None => break Err(ClassifierError::MissingOutput),
                _ => {}
            }
        };
        if let Some(ready) = ready {
            let _ = ready.send(result.clone());
        }
        let reusable = result.as_ref().is_ok_and(|output| {
            matches!(output.as_str(), "low" | "high")
                && first.as_ref().is_none_or(|first| first == output)
        });
        cleanup.finish().await;
        ClassifierOutput {
            result,
            retained_output: retained_output.filter(|output| reusable && !output.is_empty()),
        }
    }
}
