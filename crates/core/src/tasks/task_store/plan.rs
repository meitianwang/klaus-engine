//! Persisted current-plan membership over durable task identities.
//!
//! Replacing a plan changes one atomic ledger pointer. Old task files and plan
//! membership remain available to in-flight tools and dependency resolution.
use super::*;
use std::collections::HashSet;

const PLAN_FILE: &str = ".task-plans.json";

#[derive(Debug, Clone, PartialEq)]
struct PlanEntry {
    task_id: RuntimeString,
    entry_id: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq)]
struct PlanRecord {
    id: RuntimeString,
    kind: RuntimeString,
    owner: Option<RuntimeString>,
    host_exposed: bool,
    entries: Vec<PlanEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct PlanLedger {
    current: RuntimeString,
    plans: Vec<PlanRecord>,
    incarnations: Vec<TaskIncarnation>,
}

#[derive(Debug, Clone, PartialEq)]
struct TaskIncarnation {
    id: RuntimeString,
    token: RuntimeString,
}

macro_rules! plan_codec {
    ($ty:ident, $( $field:ident => $wire:literal ),+ $(,)?) => {
        impl RuntimeEncode for $ty {
            fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
                let mut object = RuntimeObjectEncoder::new();
                $(object.field($wire, &self.$field)?;)+
                out.emit(RuntimeJson::Object(object.finish()))
            }
        }
        impl RuntimeDecode for $ty {
            fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                runtime_codec::decode_object(value, false, |object| Ok(Self {
                    $($field: object.required($wire, &[])?,)+
                }))
            }
        }
    };
}
plan_codec!(PlanEntry, task_id => "taskId", entry_id => "entryId");
impl RuntimeEncode for PlanRecord {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("kind", &self.kind)?;
        object.field("owner", &self.owner)?;
        object.field("hostExposed", &self.host_exposed)?;
        object.field("entries", &self.entries)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}
impl RuntimeDecode for PlanRecord {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                kind: object.required("kind", &[])?,
                owner: object.optional("owner", &[])?,
                host_exposed: object.defaulted("hostExposed", &[], || true)?,
                entries: object.required("entries", &[])?,
            })
        })
    }
}
plan_codec!(TaskIncarnation, id => "id", token => "token");
impl RuntimeEncode for PlanLedger {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("current", &self.current)?;
        object.field("plans", &self.plans)?;
        object.field("incarnations", &self.incarnations)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}
impl RuntimeDecode for PlanLedger {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                current: object.required("current", &[])?,
                plans: object.required("plans", &[])?,
                incarnations: object.defaulted("incarnations", &[], Vec::new)?,
            })
        })
    }
}

fn decode_ledger(bytes: &[u8], dir: &Path) -> io::Result<PlanLedger> {
    let value = parse_json_bytes(bytes, "task plan ledger", &dir.join(PLAN_FILE))?;
    let ledger: PlanLedger = runtime_codec::decode_value(&value).map_err(io::Error::other)?;
    if ledger.current.is_empty()
        || !ledger.plans.iter().any(|plan| plan.id == ledger.current)
        || ledger
            .plans
            .iter()
            .any(|plan| plan.id.is_empty() || (plan.kind != "plan" && plan.kind != "tasks"))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid task plan ledger",
        ));
    }
    Ok(ledger)
}

async fn read_ledger(dir: &Path) -> io::Result<Option<PlanLedger>> {
    match tokio::fs::read(dir.join(PLAN_FILE)).await {
        Ok(bytes) => decode_ledger(&bytes, dir).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

async fn write_ledger(dir: &Path, ledger: &PlanLedger) -> io::Result<()> {
    let bytes = runtime_codec::encode_bytes(ledger).map_err(io::Error::other)?;
    write_leaf_atomic(&dir.join(PLAN_FILE), &bytes).await
}

fn legacy_plan(tasks: &[Task]) -> PlanRecord {
    PlanRecord {
        id: RuntimeString::from(Uuid::new_v4().to_string()),
        kind: "tasks".into(),
        owner: None,
        host_exposed: tasks.iter().any(task_is_host_visible),
        entries: tasks
            .iter()
            .map(|task| PlanEntry {
                task_id: task.id.clone(),
                entry_id: Some(task.id.clone()),
            })
            .collect(),
    }
}

impl PlanLedger {
    fn is_initial_main_plan(&self) -> bool {
        self.plans.first().is_some_and(|plan| {
            plan.owner.is_none() && plan.kind == "tasks" && plan.id == self.current
        })
    }
    fn for_owner(&self, owner: Option<&RuntimeString>) -> Option<&PlanRecord> {
        match owner {
            None => Some(self.current()),
            Some(owner) => self
                .plans
                .iter()
                .rev()
                .find(|plan| plan.owner.as_ref() == Some(owner)),
        }
    }
    fn plan_mut(&mut self, id: &RuntimeString) -> &mut PlanRecord {
        self.plans
            .iter_mut()
            .find(|plan| &plan.id == id)
            .expect("selected plan exists")
    }
    fn token(&self, id: &RuntimeString) -> RuntimeString {
        self.incarnations
            .iter()
            .find(|entry| &entry.id == id)
            .map(|entry| entry.token.clone())
            .unwrap_or_else(|| RuntimeString::from("legacy:").concat(id))
    }
    fn reincarnate(&mut self, id: &RuntimeString) -> RuntimeString {
        let token: RuntimeString = Uuid::new_v4().to_string().into();
        self.incarnations.retain(|entry| &entry.id != id);
        self.incarnations.push(TaskIncarnation {
            id: id.clone(),
            token: token.clone(),
        });
        for plan in &mut self.plans {
            plan.entries.retain(|entry| &entry.task_id != id);
        }
        token
    }
    fn current(&self) -> &PlanRecord {
        self.plans
            .iter()
            .find(|plan| plan.id == self.current)
            .expect("validated current plan exists")
    }
    fn current_mut(&mut self) -> &mut PlanRecord {
        self.plans
            .iter_mut()
            .find(|plan| plan.id == self.current)
            .expect("validated current plan exists")
    }
}

/// One originating run's observed plan identities. Clones share updates within
/// that run; `fork` freezes observations and `for_agent` selects child ownership.
type ObservedPlans = HashMap<RuntimeString, Result<Option<RuntimeString>, String>>;
type ObservedTasks = HashMap<RuntimeString, HashMap<RuntimeString, RuntimeString>>;

#[derive(Debug, Clone)]
struct InheritedPlan {
    owner: Option<RuntimeString>,
    identity: Result<Option<RuntimeString>, String>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskPlanBinding {
    owner: Option<RuntimeString>,
    inherited: Arc<Mutex<HashMap<RuntimeString, InheritedPlan>>>,
    identities: Arc<Mutex<ObservedPlans>>,
    task_identities: Arc<Mutex<ObservedTasks>>,
}

impl TaskPlanBinding {
    /// Capture before a run can be delayed by a provider or background scheduler.
    pub fn capture(list_id: &RuntimeString) -> Self {
        let binding = Self::default();
        binding.observe(list_id);
        binding
    }

    pub fn observe(&self, list_id: &RuntimeString) {
        let mut identities = self.identities.lock().unwrap();
        identities.entry(list_id.clone()).or_insert_with(|| {
            let dir = dir_of(list_id);
            let mut observed = HashMap::new();
            if let Ok(entries) = std::fs::read_dir(&dir) {
                for entry in entries.flatten() {
                    if let Some(name) = entry.file_name().to_str() {
                        if let Some(id) = task_file_id(&RuntimeString::from(name)) {
                            observed.insert(id.clone(), RuntimeString::from("legacy:").concat(&id));
                        }
                    }
                }
            }
            let mut ledger = match std::fs::read(dir.join(PLAN_FILE)) {
                Ok(bytes) => Some(decode_ledger(&bytes, &dir).map_err(|error| error.to_string())?),
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.to_string()),
            };
            match std::fs::read(dir.join(TRANSACTION_FILE)) {
                Ok(bytes) => {
                    let value = parse_json_bytes(
                        &bytes,
                        "task plan transaction",
                        &dir.join(TRANSACTION_FILE),
                    )
                    .map_err(|error| error.to_string())?;
                    let transaction: PlanRollback =
                        runtime_codec::decode_value(&value).map_err(|error| error.to_string())?;
                    for id in &transaction.created {
                        observed.remove(id);
                    }
                    for task in &transaction.tasks {
                        observed.insert(
                            task.id.clone(),
                            RuntimeString::from("legacy:").concat(&task.id),
                        );
                    }
                    ledger = transaction.ledger;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.to_string()),
            }
            if let Some(ledger) = &ledger {
                for entry in &ledger.incarnations {
                    if let Some(token) = observed.get_mut(&entry.id) {
                        *token = entry.token.clone();
                    }
                }
            }
            // A resumed owner learns its persisted steps, but an ID already
            // observed by the originating run never silently rebinds after reuse.
            let mut all_tasks = self.task_identities.lock().unwrap();
            let known = all_tasks.entry(list_id.clone()).or_default();
            for (id, token) in observed {
                known.entry(id).or_insert(token);
            }
            if self.owner.is_some() {
                self.inherited
                    .lock()
                    .unwrap()
                    .entry(list_id.clone())
                    .or_insert_with(|| InheritedPlan {
                        owner: None,
                        identity: Ok(ledger.as_ref().map(|ledger| ledger.current.clone())),
                    });
            }
            Ok(ledger.as_ref().and_then(|ledger| {
                ledger
                    .for_owner(self.owner.as_ref())
                    .map(|plan| plan.id.clone())
            }))
        });
    }

    pub fn fork(&self) -> Self {
        let inherited = self.inherited.lock().unwrap().clone();
        let identities = self.identities.lock().unwrap().clone();
        let task_identities = self.task_identities.lock().unwrap().clone();
        Self {
            owner: self.owner.clone(),
            inherited: Arc::new(Mutex::new(inherited)),
            identities: Arc::new(Mutex::new(identities)),
            task_identities: Arc::new(Mutex::new(task_identities)),
        }
    }

    /// Scope a spawned run to its stable agent identity while retaining its
    /// parent's captured view until it explicitly writes a private plan.
    pub fn for_agent(&self, owner: &RuntimeString) -> Self {
        if self.owner.as_ref() == Some(owner) {
            return self.clone();
        }
        let mut child = self.fork();
        let inherited: HashMap<_, _> = self
            .identities
            .lock()
            .unwrap()
            .iter()
            .map(|(list_id, identity)| {
                let effective = if self.owner.is_some() && matches!(identity, Ok(None)) {
                    self.inherited
                        .lock()
                        .unwrap()
                        .get(list_id)
                        .cloned()
                        .unwrap_or(InheritedPlan {
                            owner: None,
                            identity: Ok(None),
                        })
                } else {
                    InheritedPlan {
                        owner: self.owner.clone(),
                        identity: identity.clone(),
                    }
                };
                (list_id.clone(), effective)
            })
            .collect();
        let lists: Vec<_> = inherited.keys().cloned().collect();
        child.owner = Some(owner.clone());
        child.inherited = Arc::new(Mutex::new(inherited));
        child.identities = Arc::new(Mutex::new(HashMap::new()));
        for list_id in lists {
            child.observe(&list_id);
        }
        child
    }

    fn effective_identity(&self, list_id: &RuntimeString) -> io::Result<Option<RuntimeString>> {
        self.observe(list_id);
        let identity = self
            .identities
            .lock()
            .unwrap()
            .get(list_id)
            .cloned()
            .expect("observed task list")
            .map_err(io::Error::other)?;
        if identity.is_none() && self.owner.is_some() {
            return self
                .inherited
                .lock()
                .unwrap()
                .get(list_id)
                .map(|entry| entry.identity.clone())
                .unwrap_or(Ok(None))
                .map_err(io::Error::other);
        }
        Ok(identity)
    }

    fn check(&self, list_id: &RuntimeString, current: Option<&RuntimeString>) -> io::Result<()> {
        self.observe(list_id);
        let identities = self.identities.lock().unwrap();
        match identities.get(list_id).expect("observed task list") {
            Ok(expected) if expected.as_ref() == current => Ok(()),
            Ok(_) => Err(io::Error::other("Task plan changed since this run started; stale plan writes are rejected. Existing tasks remain addressable with TaskGet/TaskUpdate.")),
            Err(error) => Err(io::Error::other(error.clone())),
        }
    }

    fn observe_task(
        &self,
        list_id: &RuntimeString,
        id: &RuntimeString,
        token: &RuntimeString,
        may_learn: bool,
    ) -> io::Result<()> {
        self.observe(list_id);
        let mut all = self.task_identities.lock().unwrap();
        let tasks = all.entry(list_id.clone()).or_default();
        match tasks.get(id) {
            Some(expected) if expected != token => Err(io::Error::other(
                "Task ID now names a different incarnation; stale task access is rejected",
            )),
            Some(_) => Ok(()),
            None if may_learn => {
                tasks.insert(id.clone(), token.clone());
                Ok(())
            }
            None => Err(io::Error::other(
                "Read the new task with TaskGet or TaskList before updating it",
            )),
        }
    }
    fn advance_task(&self, list_id: &RuntimeString, id: &RuntimeString, token: RuntimeString) {
        self.task_identities
            .lock()
            .unwrap()
            .entry(list_id.clone())
            .or_default()
            .insert(id.clone(), token);
    }
    fn advance(&self, list_id: &RuntimeString, plan_id: &RuntimeString) {
        self.identities
            .lock()
            .unwrap()
            .insert(list_id.clone(), Ok(Some(plan_id.clone())));
    }
}

/// An entry's `entry_id` is a stable merge key, never a task ID or a display number.
#[derive(Debug, Clone)]
pub struct TaskPlanWriteEntry {
    pub entry_id: Option<RuntimeString>,
    pub subject: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub active_form: Option<RuntimeString>,
    pub status: Option<TaskStatus>,
}

pub(super) async fn project_current_plan(
    dir: &Path,
    tasks: Vec<Task>,
) -> io::Result<(Option<RuntimeString>, RuntimeString, Vec<Task>)> {
    let completed: HashSet<_> = tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Completed)
        .map(|task| task.id.clone())
        .collect();
    let mut tasks = tasks;
    tasks.retain(task_is_host_visible);
    for task in &mut tasks {
        task.blocked_by.retain(|id| !completed.contains(id));
    }
    let Some(mut ledger) = read_ledger(dir).await? else {
        return Ok((None, "tasks".into(), tasks));
    };
    let plan = ledger.current();
    let mut by_id: HashMap<_, _> = tasks
        .into_iter()
        .map(|task| (task.id.clone(), task))
        .collect();
    let tasks: Vec<_> = plan
        .entries
        .iter()
        .filter_map(|entry| by_id.remove(&entry.task_id))
        .collect();
    let id = plan.id.clone();
    let kind = plan.kind.clone();
    if !plan.host_exposed {
        if tasks.is_empty() {
            return Ok((None, kind, tasks));
        }
        ledger.current_mut().host_exposed = true;
        write_ledger(dir, &ledger).await?;
    }
    Ok((Some(id), kind, tasks))
}

fn bound_plan<'a>(
    ledger: &'a PlanLedger,
    list_id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<&'a PlanRecord> {
    let observed = binding
        .map(|binding| binding.effective_identity(list_id))
        .transpose()?;
    let plan = match observed {
        None => ledger.current(),
        Some(None) => ledger.plans.first().expect("legacy plan retained"),
        Some(Some(id)) => ledger
            .plans
            .iter()
            .find(|plan| plan.id == id)
            .ok_or_else(|| io::Error::other("Captured task plan is unavailable"))?,
    };
    Ok(plan)
}

/// A coherent model-facing projection from one list transaction.
pub struct TaskPlanView {
    pub tasks: Vec<Task>,
    pub entry_ids: HashMap<RuntimeString, RuntimeString>,
}

fn entry_ids(plan: &PlanRecord) -> HashMap<RuntimeString, RuntimeString> {
    if plan.kind == "tasks" {
        return HashMap::new();
    }
    plan.entries
        .iter()
        .filter_map(|entry| {
            entry
                .entry_id
                .as_ref()
                .map(|key| (entry.task_id.clone(), key.clone()))
        })
        .collect()
}

/// List the current plan, or the archived plan captured by an older run.
pub async fn list_plan_tasks(
    list_id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<Vec<Task>> {
    Ok(list_plan_view(list_id, binding).await?.tasks)
}

pub async fn list_plan_view(
    list_id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<TaskPlanView> {
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let _publication = publication_gate.lock().await;
    let lock_path = task_list_lock_path(&dir);
    with_lock(
        lock_path.to_str().expect("UTF-8 task lock"),
        || list_plan_view_locked(&dir, list_id, binding),
        LOCK_OPTS,
    )
    .await
}

async fn list_plan_view_locked(
    dir: &Path,
    list_id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<TaskPlanView> {
    recover_transaction(dir).await?;
    let mut tasks = list_tasks_in_dir(dir).await?;
    let completed: HashSet<_> = tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Completed)
        .map(|task| task.id.clone())
        .collect();
    for task in &mut tasks {
        task.blocked_by.retain(|id| !completed.contains(id));
    }
    let Some(ledger) = read_ledger(dir).await? else {
        if let Some(binding) = binding {
            for task in &tasks {
                binding.observe_task(
                    list_id,
                    &task.id,
                    &RuntimeString::from("legacy:").concat(&task.id),
                    true,
                )?;
            }
        }
        return Ok(TaskPlanView {
            tasks,
            entry_ids: HashMap::new(),
        });
    };
    let plan = bound_plan(&ledger, list_id, binding)?;
    if let Some(binding) = binding {
        for task in &tasks {
            if plan.entries.iter().any(|entry| entry.task_id == task.id) {
                binding.observe_task(list_id, &task.id, &ledger.token(&task.id), true)?;
            }
        }
    }
    let mut by_id: HashMap<_, _> = tasks
        .into_iter()
        .map(|task| (task.id.clone(), task))
        .collect();
    Ok(TaskPlanView {
        tasks: plan
            .entries
            .iter()
            .filter_map(|entry| by_id.remove(&entry.task_id))
            .collect(),
        entry_ids: entry_ids(plan),
    })
}

pub(super) async fn append_membership(
    dir: &Path,
    list_id: &RuntimeString,
    task: &Task,
    binding: Option<&TaskPlanBinding>,
    prior: Option<PlanLedger>,
) -> io::Result<()> {
    let mut ledger = match prior {
        Some(ledger) => ledger,
        None => {
            let mut tasks = list_tasks_in_dir(dir).await?;
            tasks.retain(|old| old.id != task.id);
            let plan = legacy_plan(&tasks);
            PlanLedger {
                current: plan.id.clone(),
                plans: vec![plan],
                incarnations: Vec::new(),
            }
        }
    };
    let owner = binding.and_then(|binding| binding.owner.as_ref());
    let private = owner.is_some() && ledger.for_owner(owner).is_some();
    let target = if private || owner.is_none() {
        ledger
            .for_owner(owner)
            .expect("owned or main plan exists")
            .id
            .clone()
    } else {
        binding
            .unwrap()
            .effective_identity(list_id)?
            .unwrap_or_else(|| ledger.current.clone())
    };
    let token = ledger.reincarnate(&task.id);
    ledger.plan_mut(&target).host_exposed |= task_is_host_visible(task);
    ledger.plan_mut(&target).entries.push(PlanEntry {
        task_id: task.id.clone(),
        entry_id: Some(task.id.clone()),
    });
    commit_transaction(
        dir,
        list_id,
        &PlanTransaction {
            ledger: ledger.clone(),
            tasks: vec![task.clone()],
            deleted: Vec::new(),
        },
    )
    .await?;
    if let Some(binding) = binding {
        if private || owner.is_none() {
            binding.advance(list_id, &target);
        } else {
            binding
                .inherited
                .lock()
                .unwrap()
                .entry(list_id.clone())
                .or_insert_with(|| InheritedPlan {
                    owner: None,
                    identity: Ok(None),
                })
                .identity = Ok(Some(target));
        }
        binding.advance_task(list_id, &task.id, token);
    }
    Ok(())
}

pub(super) async fn check_binding(
    dir: &Path,
    list_id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<Option<PlanLedger>> {
    recover_transaction(dir).await?;
    let ledger = read_ledger(dir).await?;
    if let Some(binding) = binding {
        let initial_tasks = ledger
            .as_ref()
            .filter(|ledger| binding.owner.is_none() && ledger.is_initial_main_plan());
        if let Some(initial) = initial_tasks {
            binding.observe(list_id);
            let legacy = matches!(
                binding.identities.lock().unwrap().get(list_id),
                Some(Ok(None))
            );
            if legacy {
                binding.advance(list_id, &initial.current);
            }
        }
        binding.check(
            list_id,
            ledger
                .as_ref()
                .and_then(|ledger| ledger.for_owner(binding.owner.as_ref()))
                .map(|plan| &plan.id),
        )?;
    }
    Ok(ledger)
}

/// A child without a private plan may deliberately append to its captured
/// parent plan, but must not follow a parent's later replacement implicitly.
pub(super) async fn check_append_binding(
    dir: &Path,
    list_id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<Option<PlanLedger>> {
    let ledger = check_binding(dir, list_id, binding).await?;
    let Some(binding) = binding.filter(|binding| binding.owner.is_some()) else {
        return Ok(ledger);
    };
    if ledger
        .as_ref()
        .is_some_and(|ledger| ledger.for_owner(binding.owner.as_ref()).is_some())
    {
        return Ok(ledger);
    }
    let mut inherited = binding.inherited.lock().unwrap();
    let entry = inherited
        .entry(list_id.clone())
        .or_insert_with(|| InheritedPlan {
            owner: None,
            identity: Ok(None),
        });
    let actual = ledger
        .as_ref()
        .and_then(|ledger| ledger.for_owner(entry.owner.as_ref()))
        .map(|plan| plan.id.clone());
    let expected = entry
        .identity
        .as_ref()
        .map_err(|error| io::Error::other(error.clone()))?;
    let bootstrap = expected.is_none()
        && entry.owner.is_none()
        && ledger
            .as_ref()
            .is_some_and(|ledger| ledger.is_initial_main_plan());
    if bootstrap {
        entry.identity = Ok(actual.clone());
    }
    if entry.identity.as_ref().ok() != Some(&actual) {
        return Err(io::Error::other("Captured parent plan changed; write a private plan with TaskWrite or update an existing durable task"));
    }
    Ok(ledger)
}

#[derive(Debug, Clone)]
pub struct TaskPlanChange {
    pub previous: Option<Task>,
    pub task: Task,
}

pub type TaskPlanApprover = Box<
    dyn FnOnce(Vec<TaskPlanChange>) -> futures::future::BoxFuture<'static, io::Result<()>> + Send,
>;

#[derive(Debug, Clone, PartialEq)]
struct PlanTransaction {
    ledger: PlanLedger,
    tasks: Vec<Task>,
    deleted: Vec<RuntimeString>,
}
#[derive(Debug, Clone, PartialEq)]
struct PlanRollback {
    ledger: Option<PlanLedger>,
    tasks: Vec<Task>,
    created: Vec<RuntimeString>,
    snapshot: Option<TaskSnapshotLedger>,
}
plan_codec!(PlanRollback, ledger => "ledger", tasks => "tasks", created => "created", snapshot => "snapshot");
const TRANSACTION_FILE: &str = ".task-plan-transaction.json";

/// An undo journal fences all facade reads during an interrupted batch. Until
/// the journal is removed durably the batch has not committed; recovery restores
/// the complete prior plan. Failed rollback keeps the journal so reads fail
/// explicitly instead of exposing a partially restored plan.
pub(super) async fn recover_transaction(dir: &Path) -> io::Result<()> {
    let path = dir.join(TRANSACTION_FILE);
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let value = parse_json_bytes(&bytes, "task plan transaction", &path)?;
    let rollback: PlanRollback = runtime_codec::decode_value(&value).map_err(io::Error::other)?;
    for task in &rollback.tasks {
        write_task_file(dir, task).await?;
    }
    for id in &rollback.created {
        match tokio::fs::remove_file(task_path(dir, id)).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    if let Some(ledger) = &rollback.ledger {
        write_ledger(dir, ledger).await?;
    } else {
        match tokio::fs::remove_file(dir.join(PLAN_FILE)).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    if let Some(snapshot) = &rollback.snapshot {
        write_snapshot_ledger_atomic(dir, snapshot).await?;
    } else {
        match tokio::fs::remove_file(snapshot_ledger_path(dir)).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    sync_directory(dir).await?;
    tokio::fs::remove_file(&path).await?;
    sync_directory(dir).await
}

async fn commit_transaction(
    dir: &Path,
    list_id: &RuntimeString,
    transaction: &PlanTransaction,
) -> io::Result<()> {
    let mut rollback = PlanRollback {
        ledger: read_ledger(dir).await?,
        tasks: Vec::new(),
        created: Vec::new(),
        snapshot: read_snapshot_ledger(dir).await?,
    };
    for task in &transaction.tasks {
        match read_task_file(dir, &task.id).await? {
            Some(old) => rollback.tasks.push(old),
            None => rollback.created.push(task.id.clone()),
        }
    }
    for id in &transaction.deleted {
        if let Some(task) = read_task_file(dir, id).await? {
            rollback.tasks.push(task);
        }
    }
    let bytes = runtime_codec::encode_bytes(&rollback).map_err(io::Error::other)?;
    let journal = dir.join(TRANSACTION_FILE);
    write_leaf_atomic(&journal, &bytes).await?;
    let result = async {
        for id in &transaction.deleted {
            tokio::fs::remove_file(task_path(dir, id)).await?;
        }
        for task in &transaction.tasks {
            write_task_file(dir, task).await?;
        }
        write_ledger(dir, &transaction.ledger).await?;
        refresh_task_state_without_recovery_locked(list_id, dir).await?;
        tokio::fs::remove_file(&journal).await?;
        sync_directory(dir).await
    }
    .await;
    if let Err(error) = result {
        // Recreate after an uncertain journal-removal fsync, if necessary.
        write_leaf_atomic(&journal, &bytes).await?;
        recover_transaction(dir).await?;
        return Err(error);
    }
    Ok(())
}

/// Replace the current plan atomically, or merge entries by their plan-local key.
/// Replaced entries remain durable and ordinary plan steps do not consume task numbers.
pub async fn write_task_plan(
    list_id: &RuntimeString,
    entries: Vec<TaskPlanWriteEntry>,
    merge: bool,
    binding: Option<&TaskPlanBinding>,
) -> io::Result<Vec<Task>> {
    write_task_plan_approved(list_id, entries, merge, binding, None)
        .await
        .map(|view| view.tasks)
}

/// Hook approval runs without a list lock. A compare-and-swap after approval
/// rejects racing changes instead of rerunning hooks or overwriting newer work.
pub async fn write_task_plan_approved(
    list_id: &RuntimeString,
    entries: Vec<TaskPlanWriteEntry>,
    merge: bool,
    binding: Option<&TaskPlanBinding>,
    approve: Option<TaskPlanApprover>,
) -> io::Result<TaskPlanView> {
    if entries.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TaskWrite requires at least one entry",
        ));
    }
    let mut keys = HashSet::new();
    for entry in &entries {
        if entry.subject.as_ref().is_some_and(|subject| {
            subject
                .as_utf16()
                .iter()
                .all(|unit| is_ecmascript_whitespace(*unit))
        }) || entry
            .entry_id
            .as_ref()
            .is_some_and(|id| id.is_empty() || !keys.insert(id.clone()))
            || (entry.subject.is_none()
                && entry.status.is_none()
                && entry.description.is_none()
                && entry.active_form.is_none())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TaskWrite entries require content or status and unique non-empty IDs",
            ));
        }
    }
    let dir = dir_of(list_id);
    ensure_dir(&dir).await?;
    let publication_gate = task_publication_gate(&dir);
    let lock_path = task_list_lock_path(&dir);
    let (expected, transaction, changes) = {
        let _publication = publication_gate.lock().await;
        with_lock(
            lock_path.to_str().expect("UTF-8 task lock"),
            || async {
                recover_transaction(&dir).await?;
                let expected = check_binding(&dir, list_id, binding).await?;
                let mut ledger = match expected.clone() {
                    Some(ledger) => ledger,
                    None => {
                        let plan = legacy_plan(&list_tasks_in_dir(&dir).await?);
                        PlanLedger {
                            current: plan.id.clone(),
                            plans: vec![plan],
                            incarnations: Vec::new(),
                        }
                    }
                };
                let owner = binding.and_then(|binding| binding.owner.clone());
                let existing_plan = merge
                    .then(|| ledger.for_owner(owner.as_ref()).cloned())
                    .flatten();
                let mut plan = existing_plan.unwrap_or_else(|| PlanRecord {
                    id: Uuid::new_v4().to_string().into(),
                    kind: "plan".into(),
                    owner: owner.clone(),
                    host_exposed: true,
                    entries: Vec::new(),
                });
                plan.kind = "plan".into();
                plan.host_exposed = true;
                let mut changes = Vec::with_capacity(entries.len());
                for entry in entries {
                    let existing = entry.entry_id.as_ref().and_then(|id| {
                        plan.entries
                            .iter()
                            .position(|old| old.entry_id.as_ref() == Some(id))
                    });
                    let previous = match existing {
                        Some(index) => read_task_file(&dir, &plan.entries[index].task_id).await?,
                        None => None,
                    };
                    let task = if let Some(mut task) = previous.clone() {
                        check_task_binding(&dir, list_id, &task.id, binding, false).await?;
                        if let Some(subject) = entry.subject {
                            task.subject = subject;
                        }
                        if let Some(description) = entry.description {
                            task.description = description;
                        }
                        if let Some(active_form) = entry.active_form {
                            task.active_form = Some(active_form);
                        }
                        if let Some(status) = entry.status {
                            task.status = status;
                        }
                        task
                    } else {
                        let subject = entry.subject.ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "New TaskWrite entries require a non-empty subject",
                            )
                        })?;
                        let id: RuntimeString = format!("plan-step-{}", Uuid::new_v4()).into();
                        ledger.reincarnate(&id);
                        let member = PlanEntry {
                            task_id: id.clone(),
                            entry_id: entry.entry_id,
                        };
                        if let Some(index) = existing {
                            plan.entries[index] = member;
                        } else {
                            plan.entries.push(member);
                        }
                        Task {
                            id,
                            subject,
                            description: entry.description.unwrap_or_default(),
                            active_form: entry.active_form,
                            status: entry.status.unwrap_or(TaskStatus::Pending),
                            owner: None,
                            blocks: Vec::new(),
                            blocked_by: Vec::new(),
                            metadata: None,
                        }
                    };
                    changes.push(TaskPlanChange { previous, task });
                }
                if owner.is_none() {
                    ledger.current = plan.id.clone();
                }
                if let Some(existing) = ledger
                    .plans
                    .iter_mut()
                    .find(|existing| existing.id == plan.id)
                {
                    *existing = plan;
                } else {
                    ledger.plans.push(plan);
                }
                let transaction = PlanTransaction {
                    ledger,
                    tasks: changes.iter().map(|change| change.task.clone()).collect(),
                    deleted: Vec::new(),
                };
                Ok((expected, transaction, changes))
            },
            LOCK_OPTS,
        )
        .await?
    };
    if let Some(approve) = approve {
        approve(changes.clone()).await?;
    }
    let _publication = publication_gate.lock().await;
    let (state, view) = with_lock(
        lock_path.to_str().expect("UTF-8 task lock"),
        || async {
            recover_transaction(&dir).await?;
            let actual = check_binding(&dir, list_id, binding).await?;
            if actual != expected {
                return Err(io::Error::other(
                    "Task plan changed during batch preparation; retry the plan explicitly",
                ));
            }
            for change in &changes {
                if read_task_file(&dir, &change.task.id).await? != change.previous {
                    return Err(io::Error::other(
                        "A task changed during batch preparation; retry the plan explicitly",
                    ));
                }
            }
            commit_transaction(&dir, list_id, &transaction).await?;
            if let Some(binding) = binding {
                let target = transaction
                    .ledger
                    .for_owner(binding.owner.as_ref())
                    .expect("written plan exists");
                binding.advance(list_id, &target.id);
                for change in &changes {
                    if change.previous.is_none() {
                        binding.advance_task(
                            list_id,
                            &change.task.id,
                            transaction.ledger.token(&change.task.id),
                        );
                    }
                }
            }
            let (state, _) = refresh_task_state_locked(list_id, &dir).await?;
            let view = list_plan_view_locked(&dir, list_id, binding).await?;
            Ok((state, view))
        },
        LOCK_OPTS,
    )
    .await?;
    publish_task_state(&state);
    Ok(view)
}

pub(super) async fn check_task_binding(
    dir: &Path,
    list_id: &RuntimeString,
    id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
    may_learn: bool,
) -> io::Result<()> {
    if binding.is_some() {
        task_identity_token(dir, list_id, id, binding, may_learn).await?;
    }
    Ok(())
}

pub(super) async fn task_identity_token(
    dir: &Path,
    list_id: &RuntimeString,
    id: &RuntimeString,
    binding: Option<&TaskPlanBinding>,
    may_learn: bool,
) -> io::Result<RuntimeString> {
    let token = read_ledger(dir)
        .await?
        .map(|ledger| ledger.token(id))
        .unwrap_or_else(|| RuntimeString::from("legacy:").concat(id));
    if let Some(binding) = binding {
        binding.observe_task(list_id, id, &token, may_learn)?;
    }
    Ok(token)
}

/// Remove the task, archived memberships, and dependency edges in one transaction.
pub(super) async fn delete_task_transaction(
    dir: &Path,
    list_id: &RuntimeString,
    id: &RuntimeString,
) -> io::Result<bool> {
    if read_task_file(dir, id).await?.is_none() {
        return Ok(false);
    }
    let all = list_tasks_in_dir(dir).await?;
    let mut ledger = match read_ledger(dir).await? {
        Some(ledger) => ledger,
        None => {
            let plan = legacy_plan(&all);
            PlanLedger {
                current: plan.id.clone(),
                plans: vec![plan],
                incarnations: Vec::new(),
            }
        }
    };
    for plan in &mut ledger.plans {
        plan.entries.retain(|entry| &entry.task_id != id);
    }
    let mut tasks = Vec::new();
    for mut task in all {
        if &task.id == id {
            continue;
        }
        let before = (task.blocks.len(), task.blocked_by.len());
        task.blocks.retain(|edge| edge != id);
        task.blocked_by.retain(|edge| edge != id);
        if before != (task.blocks.len(), task.blocked_by.len()) {
            tasks.push(task);
        }
    }
    commit_transaction(
        dir,
        list_id,
        &PlanTransaction {
            ledger,
            tasks,
            deleted: vec![id.clone()],
        },
    )
    .await?;
    Ok(true)
}
