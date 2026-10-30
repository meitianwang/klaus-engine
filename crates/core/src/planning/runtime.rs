//! Durable planning snapshots. Mutations publish only after an atomic save.
use super::types::*;
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimeObject, RuntimeString,
};
use crate::types::tool::EmitFn;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
pub type PlanningObserver = Arc<dyn Fn(PlanningSnapshot) + Send + Sync>;
struct State {
    snapshot: PlanningSnapshot,
    turn_id: RuntimeString,
    active_mode: Option<CollaborationMode>,
    error: Option<String>,
}
pub struct PlanningRuntime {
    thread_id: RuntimeString,
    path: Option<PathBuf>,
    state: Mutex<State>,
    mutation_gate: Mutex<()>,
    observer: Mutex<Option<PlanningObserver>>,
    default_questions: bool,
}
impl PlanningRuntime {
    pub fn new(
        thread_id: RuntimeString,
        root: Option<PathBuf>,
        legacy_plan: bool,
        default_questions: bool,
    ) -> Self {
        let path = root.map(|p| {
            p.join("planning")
                .join(format!("{}.json", safe_id(&thread_id)))
        });
        let mut snapshot = PlanningSnapshot {
            mode: if legacy_plan {
                CollaborationMode::Plan
            } else {
                CollaborationMode::Default
            },
            ..Default::default()
        };
        let mut error = None;
        if let Some(path) = &path {
            match std::fs::read_to_string(path) {
                Ok(text) => match runtime_codec::decode_text::<PlanningSnapshot>(&text.into()) {
                    Ok(mut restored) => {
                        for p in &mut restored.plans {
                            if p.status == "writing" {
                                p.status = "interrupted".into();
                                p.review_pending = false;
                            }
                        }
                        for q in &mut restored.questions {
                            if q.get("status")
                                .and_then(RuntimeJson::as_str)
                                .is_some_and(|s| s == "pending")
                            {
                                if let Some(obj) = q.as_object_mut() {
                                    obj.insert("status", RuntimeJson::String("expired".into()));
                                }
                            }
                        }
                        snapshot = restored
                    }
                    Err(e) => error = Some(format!("Cannot decode planning state: {e}")),
                },
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => error = Some(format!("Cannot read planning state: {e}")),
            }
        }
        #[cfg(any(test, feature = "testing"))]
        crate::goal::testing_phase::mark(
            "planning.loaded",
            &format!(
                "thread={thread_id:?} path={path:?} mode={:?} load_error={}",
                snapshot.mode,
                error.is_some()
            ),
        );
        Self {
            thread_id,
            path,
            state: Mutex::new(State {
                snapshot,
                turn_id: RuntimeString::default(),
                active_mode: None,
                error,
            }),
            mutation_gate: Mutex::new(()),
            observer: Mutex::new(None),
            default_questions,
        }
    }
    pub fn mode(&self) -> CollaborationMode {
        {
            let s = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            s.active_mode.unwrap_or(s.snapshot.mode)
        }
    }
    pub fn desired_mode(&self) -> CollaborationMode {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot
            .mode
    }
    pub fn default_questions_enabled(&self) -> bool {
        self.default_questions
    }
    pub fn snapshot(&self) -> Result<PlanningSnapshot, RuntimeString> {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(e) = &s.error {
            return Err(e.clone().into());
        }
        Ok(s.snapshot.clone())
    }
    pub fn set_observer(&self, observer: Option<PlanningObserver>) {
        *self
            .observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = observer;
    }
    pub fn begin_turn(&self, turn_id: RuntimeString) -> CollaborationMode {
        let mut s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        s.turn_id = turn_id;
        s.active_mode = Some(s.snapshot.mode);
        s.snapshot.mode
    }
    pub(crate) fn begin_turn_in_mode(
        &self,
        turn_id: RuntimeString,
        mode: CollaborationMode,
    ) -> CollaborationMode {
        let mut s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        s.turn_id = turn_id;
        s.active_mode = Some(mode);
        mode
    }
    fn mutate<T>(
        &self,
        f: impl FnOnce(&mut PlanningSnapshot) -> Result<T, RuntimeString>,
    ) -> Result<T, RuntimeString> {
        let _publication = self
            .mutation_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (result, snapshot) = {
            let mut s = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(e) = &s.error {
                return Err(e.clone().into());
            }
            let _lease = if let Some(path) = &self.path {
                std::fs::create_dir_all(path.parent().expect("planning file has a parent"))
                    .map_err(io_error)?;
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(path.with_extension("lock"))
                    .map_err(io_error)?;
                fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
                    RuntimeString::from(format!("Planning state is busy: {error}"))
                })?;
                match std::fs::read_to_string(path) {
                    Ok(text) => {
                        let latest: PlanningSnapshot = runtime_codec::decode_text(&text.into())
                            .map_err(|error| {
                                RuntimeString::from(format!(
                                    "Cannot decode planning state: {error}"
                                ))
                            })?;
                        if latest.revision > s.snapshot.revision {
                            s.snapshot = latest;
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(io_error(error)),
                }
                Some(file)
            } else {
                None
            };
            let mut next = s.snapshot.clone();
            let result = f(&mut next)?;
            next.revision = next.revision.saturating_add(1);
            self.persist(&next)?;
            s.snapshot = next.clone();
            (result, next)
        };
        let observer = self
            .observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(observer) = observer {
            observer(snapshot)
        }
        Ok(result)
    }
    fn persist(&self, snapshot: &PlanningSnapshot) -> Result<(), RuntimeString> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let parent = path
            .parent()
            .ok_or_else(|| RuntimeString::from("Planning directory is missing"))?;
        std::fs::create_dir_all(parent).map_err(io_error)?;
        let data = runtime_codec::encode_value(snapshot)
            .map_err(|e| RuntimeString::from(e.to_string()))?;
        let data = runtime_json::stringify(&data);
        let tmp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, data.to_node_utf8()).map_err(io_error)?;
        if let Err(e) = std::fs::rename(&tmp, path) {
            let _ = std::fs::remove_file(tmp);
            return Err(io_error(e));
        }
        Ok(())
    }
    /// Forked planning history is owned by its new thread and carries no live admission.
    pub fn inherit_from(
        &self,
        source: &PlanningSnapshot,
        retained_turns: &std::collections::HashSet<RuntimeString>,
    ) -> Result<(), RuntimeString> {
        self.mutate(|s| {
            if s.revision != 0 || !s.plans.is_empty() || s.implementation.is_some() {
                return Err("Fork planning state already exists".into());
            }
            s.mode = source.mode;
            s.plans = source
                .plans
                .iter()
                .filter(|plan| retained_turns.contains(&plan.turn_id))
                .cloned()
                .map(|mut plan| {
                    plan.thread_id = self.thread_id.clone();
                    plan.document_path = None;
                    if plan.status == "writing" {
                        plan.status = "interrupted".into();
                        plan.review_pending = false;
                    }
                    plan
                })
                .collect();
            s.progress = None;
            s.implementation = None;
            s.questions = source
                .questions
                .iter()
                .filter(|question| {
                    question
                        .get("status")
                        .and_then(RuntimeJson::as_str)
                        .is_some_and(|status| status != "pending")
                        && question
                            .get("turnId")
                            .and_then(RuntimeJson::as_str)
                            .is_some_and(|turn| retained_turns.contains(turn))
                })
                .cloned()
                .collect();
            Ok(())
        })
    }
    pub fn set_mode(
        &self,
        mode: CollaborationMode,
        expected: Option<u64>,
    ) -> Result<(), RuntimeString> {
        self.mutate(|s| {
            if expected.is_some_and(|r| r != s.revision) {
                return Err("Planning state changed; reload before changing mode".into());
            }
            s.mode = mode;
            Ok(())
        })
    }
    pub async fn update_progress(
        &self,
        progress: RuntimeJson,
        emit: &EmitFn,
    ) -> Result<(), RuntimeString> {
        if self.mode() == CollaborationMode::Plan {
            return Err("update_plan is unavailable in Plan mode".into());
        }
        self.mutate(|s| {
            s.progress = Some(progress.clone());
            Ok(())
        })?;
        emit(crate::types::events::ToolDomainEvent::PlanUpdated { progress }).await;
        Ok(())
    }
    pub fn start_plan(
        &self,
        id: RuntimeString,
        turn_id: RuntimeString,
    ) -> Result<ProposedPlan, RuntimeString> {
        self.mutate(|s| {
            let revision = s
                .plans
                .iter()
                .map(|p| p.revision)
                .max()
                .unwrap_or(0)
                .saturating_add(1);
            if s.plans.iter().any(|p| p.id == id) {
                return Err("Plan identity already exists".into());
            }
            let plan = ProposedPlan {
                id,
                thread_id: self.thread_id.clone(),
                turn_id,
                revision,
                content: RuntimeString::default(),
                status: "writing".into(),
                review_pending: false,
                document_path: None,
            };
            s.plans.push(plan.clone());
            Ok(plan)
        })
    }
    pub fn append_plan(
        &self,
        id: &RuntimeString,
        delta: &str,
    ) -> Result<ProposedPlan, RuntimeString> {
        self.append_plan_runtime(id, &RuntimeString::from_utf8(delta))
    }

    /// Append normalized text without projecting its UTF-16 identity to UTF-8.
    pub(crate) fn append_plan_runtime(
        &self,
        id: &RuntimeString,
        delta: &RuntimeString,
    ) -> Result<ProposedPlan, RuntimeString> {
        self.mutate(|s| {
            let plan = s
                .plans
                .iter_mut()
                .find(|p| p.id == *id && p.status == "writing")
                .ok_or_else(|| RuntimeString::from("Plan is no longer being written"))?;
            if plan.content.len_utf16().saturating_add(delta.len_utf16()) > 2_000_000 {
                return Err("Plan content exceeds 2000000 characters".into());
            }
            plan.content = plan.content.concat(delta);
            Ok(plan.clone())
        })
    }
    pub fn finish_plan(
        &self,
        id: &RuntimeString,
        complete: bool,
    ) -> Result<ProposedPlan, RuntimeString> {
        self.mutate(|s| {
            let index = s
                .plans
                .iter()
                .position(|p| p.id == *id)
                .ok_or_else(|| RuntimeString::from("Plan not found"))?;
            if complete && s.plans[index].status != "writing" {
                return Err("Only a writing plan can be finalized".into());
            }
            let complete = complete && !host(&s.plans[index].content).trim().is_empty();
            if complete {
                for p in &mut s.plans {
                    if p.status == "proposed" {
                        p.status = "superseded".into();
                        p.review_pending = false;
                    }
                }
            }
            s.plans[index].status = if complete { "proposed" } else { "interrupted" }.into();
            s.plans[index].review_pending = complete;
            Ok(s.plans[index].clone())
        })
    }
    fn checked_plan<'a>(
        s: &'a mut PlanningSnapshot,
        id: &RuntimeString,
        revision: u64,
    ) -> Result<&'a mut ProposedPlan, RuntimeString> {
        let latest = s
            .plans
            .iter()
            .rfind(|p| p.status == "proposed" || p.status == "accepted")
            .map(|p| p.id.clone());
        let p = s
            .plans
            .iter_mut()
            .find(|p| p.id == *id && p.revision == revision)
            .ok_or_else(|| RuntimeString::from("Plan identity or revision has changed"))?;
        if latest.as_ref() != Some(id) || !(p.status == "proposed" || p.status == "accepted") {
            return Err("Plan has been replaced or is incomplete".into());
        }
        Ok(p)
    }
    pub fn read_document(
        &self,
        id: &RuntimeString,
        revision: u64,
    ) -> Result<RuntimeJson, RuntimeString> {
        self.mutate(|s| {
            let pending = s.implementation.as_ref().is_some_and(|r| {
                r.plan_id == *id && r.revision == revision && r.admission == "pending"
            });
            let p = Self::checked_plan(s, id, revision)?;
            let path = self.document_path(p)?;
            self.guard_document_path(&path)?;
            if !path.exists() {
                std::fs::create_dir_all(path.parent().unwrap()).map_err(io_error)?;
                let mut options = std::fs::OpenOptions::new();
                use std::io::Write;
                match options.write(true).create_new(true).open(&path) {
                    Ok(mut f) => f.write_all(&p.content.to_node_utf8()).map_err(io_error)?,
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(e) => return Err(io_error(e)),
                }
            }
            let content = std::fs::read_to_string(&path).map_err(io_error)?;
            // The file is a UTF-8 projection, not the exact stored identity.
            if content.as_bytes() != p.content.to_node_utf8().as_slice() {
                if p.status == "accepted" {
                    return Err("An admitted plan document changed on disk; the reviewed artifact is unchanged".into());
                }
                if pending {
                    return Err(
                        "The plan document changed while an action is awaiting admission".into(),
                    );
                }
                p.content = content.clone().into();
                p.revision = p.revision.saturating_add(1);
            }
            p.document_path = Some(path.to_string_lossy().into_owned().into());
            Ok(document_json(p))
        })
    }
    pub fn save_document(
        &self,
        id: &RuntimeString,
        revision: u64,
        content: &RuntimeString,
    ) -> Result<RuntimeJson, RuntimeString> {
        self.save_document_expected(id, revision, content, None)
    }
    pub fn save_document_expected(
        &self,
        id: &RuntimeString,
        revision: u64,
        content: &RuntimeString,
        expected_content: Option<&RuntimeString>,
    ) -> Result<RuntimeJson, RuntimeString> {
        validate_content(content)?;
        self.mutate(|s| {
            if s.implementation.as_ref().is_some_and(|r| {
                r.plan_id == *id && r.revision == revision && r.admission == "pending"
            }) {
                return Err(
                    "A plan action is awaiting admission; finish or cancel it before editing"
                        .into(),
                );
            }
            let p = Self::checked_plan(s, id, revision)?;
            if p.status == "accepted" {
                return Err("An admitted plan cannot be edited".into());
            }
            if expected_content.is_some_and(|expected| expected != &p.content) {
                return Err("Plan content has changed; reload it before saving".into());
            }
            let path = self.document_path(p)?;
            self.guard_document_path(&path)?;
            if path.exists()
                && std::fs::read_to_string(&path).map_err(io_error)?.as_bytes()
                    != expected_content
                        .unwrap_or(&p.content)
                        .to_node_utf8()
                        .as_slice()
            {
                return Err("The plan document changed on disk; reload it before saving".into());
            }
            std::fs::create_dir_all(path.parent().unwrap()).map_err(io_error)?;
            let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
            std::fs::write(&temporary, content.to_node_utf8()).map_err(io_error)?;
            if path.exists() {
                let backup =
                    path.with_file_name(format!("PLAN.conflict-{}.md", uuid::Uuid::new_v4()));
                std::fs::copy(&path, &backup).map_err(io_error)?;
            }
            std::fs::rename(&temporary, &path).map_err(io_error)?;
            p.content = content.clone();
            p.revision = p.revision.saturating_add(1);
            p.document_path = Some(path.to_string_lossy().into_owned().into());
            Ok(document_json(p))
        })
    }
    fn document_path(&self, p: &ProposedPlan) -> Result<PathBuf, RuntimeString> {
        let root = self.path.as_ref().and_then(|p| p.parent()).ok_or_else(|| {
            RuntimeString::from(
                "Persistent plan documents are unavailable for an ephemeral session",
            )
        })?;
        Ok(root
            .join(safe_id(&self.thread_id))
            .join(safe_id(&p.id))
            .join("PLAN.md"))
    }
    fn guard_document_path(&self, path: &std::path::Path) -> Result<(), RuntimeString> {
        let root = self
            .path
            .as_ref()
            .and_then(|path| path.parent())
            .ok_or_else(|| RuntimeString::from("Persistent plan documents are unavailable"))?;
        let relative = path
            .strip_prefix(root)
            .map_err(|_| RuntimeString::from("Plan document escaped its owned directory"))?;
        let mut current = root.to_path_buf();
        for component in std::iter::once(None).chain(relative.components().map(Some)) {
            if let Some(component) = component {
                current.push(component.as_os_str());
            }
            match std::fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err("Plan document paths must not contain symbolic links".into())
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(io_error(error)),
            }
        }
        Ok(())
    }
    pub fn accept(
        &self,
        id: &RuntimeString,
        revision: u64,
        request_id: RuntimeString,
        content: Option<RuntimeString>,
    ) -> Result<(bool, RuntimeString), RuntimeString> {
        if request_id.is_empty() {
            return Err("An implementation request identity is required".into());
        }
        self.mutate(|s| {
            if let Some(r) = &s.implementation {
                if r.client_request_id == request_id {
                    if r.plan_id != *id || r.revision != revision || r.kind != "implementation" {
                        return Err("Implementation request identity was reused".into());
                    }
                    if content.as_ref().is_some_and(|content| {
                        r.input
                            != RuntimeString::from("PLEASE IMPLEMENT THIS PLAN:\n").concat(content)
                    }) {
                        return Err(
                            "Implementation request identity was reused with changed content"
                                .into(),
                        );
                    }
                    if r.admission == "pending" || r.admission == "started" {
                        return Ok((false, r.input.clone()));
                    }
                }
            }
            if s.implementation
                .as_ref()
                .is_some_and(|r| r.admission == "pending")
            {
                return Err("Another plan action is awaiting admission".into());
            }
            let p = Self::checked_plan(s, id, revision)?;
            if p.status == "accepted" {
                return Err(
                    "This plan is already implemented or implementation has started".into(),
                );
            }
            let content = if p.document_path.is_some() {
                let path = self.document_path(p)?;
                self.guard_document_path(&path)?;
                let current = std::fs::read_to_string(&path).map_err(io_error)?;
                if current.as_bytes() != p.content.to_node_utf8().as_slice()
                    || content
                        .as_ref()
                        .is_some_and(|provided| provided != &p.content)
                {
                    return Err("The plan document changed; reload it before acceptance".into());
                }
                p.content.clone()
            } else {
                content.unwrap_or_else(|| p.content.clone())
            };
            validate_content(&content)?;
            p.content = content.clone();
            p.review_pending = false;
            let input = RuntimeString::from("PLEASE IMPLEMENT THIS PLAN:\n").concat(&content);
            s.mode = CollaborationMode::Default;
            s.implementation = Some(ImplementationReceipt {
                implementation_id: format!("plan-implementation-{}", host(&request_id)).into(),
                kind: "implementation".into(),
                client_request_id: request_id,
                plan_id: id.clone(),
                revision,
                input: input.clone(),
                admission: "pending".into(),
            });
            Ok((true, input))
        })
    }
    /// Claim the durable plan receipt before the ordinary turn can call a provider.
    pub fn claim_submission(
        &self,
        submission_id: &RuntimeString,
        input: Option<&crate::engine::session::Input>,
    ) -> Result<bool, RuntimeString> {
        let snapshot = self.snapshot()?;
        let matching = snapshot
            .implementation
            .as_ref()
            .is_some_and(|r| r.implementation_id == *submission_id);
        if !matching {
            if host(submission_id).starts_with("plan-implementation-")
                || host(submission_id).starts_with("plan-feedback-")
            {
                return Err("Plan admission receipt is missing or has been replaced".into());
            }
            return Ok(false);
        }
        self.mutate(|s| {
            let r = s
                .implementation
                .as_mut()
                .filter(|r| r.implementation_id == *submission_id)
                .ok_or_else(|| RuntimeString::from("Plan admission receipt changed"))?;
            if r.admission != "pending" {
                return Err(
                    "This plan action has already been admitted or requires an explicit retry"
                        .into(),
                );
            }
            if !matches!(input,Some(crate::engine::session::Input::Text(text)) if text==&r.input) {
                return Err("Plan admission input does not match the reviewed content".into());
            }
            let id = r.plan_id.clone();
            let revision = r.revision;
            let implementation = r.kind == "implementation";
            r.admission = "started".into();
            let plan = Self::checked_plan(s, &id, revision)?;
            plan.review_pending = false;
            if implementation {
                plan.status = "accepted".into();
            }
            Ok(true)
        })
    }
    /// Safe rollback is available only to the admission owner before provider execution.
    pub fn fail_unstarted_submission(
        &self,
        submission_id: &RuntimeString,
    ) -> Result<(), RuntimeString> {
        self.mutate(|s| {
            let r = s
                .implementation
                .as_mut()
                .filter(|r| r.implementation_id == *submission_id && r.admission == "started")
                .ok_or_else(|| RuntimeString::from("Plan admission receipt changed"))?;
            r.admission = "failed".into();
            let id = r.plan_id.clone();
            let revision = r.revision;
            let plan = Self::checked_plan(s, &id, revision)?;
            plan.status = "proposed".into();
            plan.review_pending = true;
            Ok(())
        })
    }
    pub fn settle(
        &self,
        id: &RuntimeString,
        revision: u64,
        request_id: &RuntimeString,
        outcome: &RuntimeString,
    ) -> Result<(), RuntimeString> {
        if !["started", "failed", "cancelled"]
            .iter()
            .any(|s| outcome == *s)
        {
            return Err("Invalid implementation admission outcome".into());
        }
        self.mutate(|s| {
            let r = s
                .implementation
                .as_mut()
                .filter(|r| {
                    r.client_request_id == *request_id && r.plan_id == *id && r.revision == revision
                })
                .ok_or_else(|| RuntimeString::from("Implementation request is stale"))?;
            if r.admission == "started" && outcome != "started" {
                return Err("An admitted implementation cannot be rolled back".into());
            }
            if outcome == "started" && r.admission != "pending" && r.admission != "started" {
                return Err("Retry the plan action before admitting a terminal receipt".into());
            }
            r.admission = outcome.clone();
            let implementation = r.kind == "implementation";
            if outcome == "failed" {
                Self::checked_plan(s, id, revision)?.review_pending = true;
            }
            if outcome == "started" && implementation {
                Self::checked_plan(s, id, revision)?.status = "accepted".into();
            }
            Ok(())
        })
    }
    pub fn feedback(
        &self,
        id: &RuntimeString,
        revision: u64,
        request_id: RuntimeString,
        feedback: RuntimeString,
    ) -> Result<(bool, RuntimeString), RuntimeString> {
        if request_id.is_empty() {
            return Err("A feedback request identity is required".into());
        }
        validate_content(&feedback)?;
        self.mutate(|s| {
            if let Some(r) = &s.implementation {
                if r.client_request_id == request_id {
                    if r.plan_id != *id || r.revision != revision || r.kind != "feedback" {
                        return Err("Feedback request identity was reused".into());
                    }
                    if r.input
                        != RuntimeString::from("Plan feedback for the current proposed plan:\n\n")
                            .concat(&feedback)
                    {
                        return Err(
                            "Feedback request identity was reused with changed content".into()
                        );
                    }
                    if r.admission == "pending" || r.admission == "started" {
                        return Ok((false, r.input.clone()));
                    }
                }
            }
            if s.implementation
                .as_ref()
                .is_some_and(|r| r.admission == "pending")
            {
                return Err("Another plan action is awaiting admission".into());
            }
            Self::checked_plan(s, id, revision)?.review_pending = false;
            s.mode = CollaborationMode::Plan;
            let input = RuntimeString::from("Plan feedback for the current proposed plan:\n\n")
                .concat(&feedback);
            s.implementation = Some(ImplementationReceipt {
                implementation_id: format!("plan-feedback-{}", host(&request_id)).into(),
                kind: "feedback".into(),
                client_request_id: request_id,
                plan_id: id.clone(),
                revision,
                input: input.clone(),
                admission: "pending".into(),
            });
            Ok((true, input))
        })
    }
    pub fn register_question(&self, request: RuntimeJson) -> Result<(), RuntimeString> {
        self.mutate(|s| {
            s.questions.push(request);
            Ok(())
        })
    }
    pub fn resolve_question(
        &self,
        request_id: &RuntimeString,
        status: RuntimeString,
        answers: Option<RuntimeJson>,
    ) -> Result<(), RuntimeString> {
        self.mutate(|s| {
            for q in &mut s.questions {
                if q.get("requestId").and_then(RuntimeJson::as_str) == Some(request_id) {
                    if let Some(obj) = q.as_object_mut() {
                        obj.insert("status", RuntimeJson::String(status.clone()));
                        if let Some(answers) = &answers {
                            obj.insert("answers", answers.clone());
                        }
                    }
                }
            }
            Ok(())
        })
    }
    pub fn finish_pending_questions(
        &self,
        turn_id: Option<&RuntimeString>,
        status: &str,
    ) -> Result<(), RuntimeString> {
        let snapshot = self.snapshot()?;
        if !snapshot.questions.iter().any(|q| {
            q.get("status")
                .and_then(RuntimeJson::as_str)
                .is_some_and(|s| s == "pending")
                && turn_id.is_none_or(|t| q.get("turnId").and_then(RuntimeJson::as_str) == Some(t))
        }) {
            return Ok(());
        }
        self.mutate(|s| {
            for q in &mut s.questions {
                if q.get("status")
                    .and_then(RuntimeJson::as_str)
                    .is_some_and(|s| s == "pending")
                    && turn_id
                        .is_none_or(|t| q.get("turnId").and_then(RuntimeJson::as_str) == Some(t))
                {
                    if let Some(obj) = q.as_object_mut() {
                        obj.insert("status", RuntimeJson::String(status.into()));
                    }
                }
            }
            Ok(())
        })
    }

    pub fn cancel(&self, id: &RuntimeString, revision: u64) -> Result<(), RuntimeString> {
        self.mutate(|s| {
            Self::checked_plan(s, id, revision)?.review_pending = false;
            if let Some(r) = s.implementation.as_mut() {
                if r.plan_id == *id && r.admission == "pending" {
                    r.admission = "cancelled".into();
                }
            }
            s.mode = CollaborationMode::Default;
            Ok(())
        })
    }
}
fn validate_content(s: &RuntimeString) -> Result<(), RuntimeString> {
    if host(s).trim().is_empty() || s.len_utf16() > 2_000_000 {
        return Err("Plan content must be nonempty and at most 2000000 characters".into());
    }
    Ok(())
}
fn io_error(e: std::io::Error) -> RuntimeString {
    format!("Planning persistence failed: {e}").into()
}
pub fn host(s: &RuntimeString) -> String {
    String::from_utf8_lossy(&s.to_node_utf8()).into_owned()
}
fn safe_id(s: &RuntimeString) -> String {
    s.to_node_utf8()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn document_json(p: &ProposedPlan) -> RuntimeJson {
    let mut document = RuntimeObject::new();
    document.insert("planId", RuntimeJson::String(p.id.clone()));
    document.insert("revision", RuntimeJson::Number(p.revision.into()));
    document.insert(
        "path",
        p.document_path
            .clone()
            .map(RuntimeJson::String)
            .unwrap_or(RuntimeJson::Null),
    );
    document.insert("content", RuntimeJson::String(p.content.clone()));
    RuntimeJson::Object(document)
}
