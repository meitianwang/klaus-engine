//! Routes files to language servers by extension and owns their lifecycle.
//!
//! Lazily starts one [`LspClient`] per configured language on first matching
//! file, restarts (bounded) a crashed server, and disposes all on session end.
//! Server availability/transport remains fail-soft; typed document eligibility
//! failures from a routed client are preserved for user-facing LSP tools.

use crate::fs::node_path::{host_path_dialect, resolve_runtime};
use crate::lsp::lsp_client::{LspClient, LspClientOptions};
use crate::lsp::types::{
    LspCall, LspCallHierarchyItem, LspController, LspDiagnostic, LspDocumentMutationGuard,
    LspHoverResult, LspLocation, LspQueryResult, LspServerConfig, LspSeverity, LspSymbol,
    LspWorkspaceSymbol,
};
use crate::runtime_process::{path_extname, path_lowercase, RuntimeProcessContext};
use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::types::reminder::{ReminderContext, ReminderProvider};
use async_trait::async_trait;
use futures::future::{self, BoxFuture, FutureExt, Shared};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use tokio::sync::{watch, RwLock as TokioRwLock};

/// Bound restart attempts per server so a crash-looping server can't churn forever.
const MAX_RESTARTS: u32 = 3;

type StartFuture = Shared<BoxFuture<'static, Option<Arc<LspClient>>>>;

struct SlotState {
    client: Option<Arc<LspClient>>,
    /// A shared in-flight start, so concurrent callers coalesce onto one spawn.
    starting: Option<StartFuture>,
    restarts: u32,
}

struct Slot {
    config: LspServerConfig,
    state: StdMutex<SlotState>,
    #[cfg(test)]
    disposed_late_starts: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    client_start_attempts: std::sync::atomic::AtomicU64,
}

struct LifecycleState {
    /// Retained completion for the manager-owned cleanup task. Keeping the
    /// receiver here makes repeated callers join the same teardown operation.
    dispose_completion: Option<watch::Receiver<bool>>,
}

struct ManagerLifecycle {
    disposed: AtomicBool,
    /// Linearizes lazy-start admission/publication with the transition to
    /// disposed. The lock is never held across an await.
    state: StdMutex<LifecycleState>,
}

pub struct LspManager {
    process_context: RuntimeProcessContext,
    slots: Vec<Arc<Slot>>,
    cwd: RuntimePath,
    opts: LspClientOptions,
    /// Files edited this session, drained by the diagnostics reminder each turn.
    edited: StdMutex<Vec<RuntimePath>>,
    /// Pre-edit diagnostics snapshot per edited path, so the reminder can surface
    /// only NEWLY-introduced problems. Captured in `before_edited` before the
    /// change reaches the server; consumed (deleted) by `new_diagnostics`.
    baseline: StdMutex<HashMap<RuntimePath, Vec<LspDiagnostic>>>,
    /// Canonical per-document reader/writer gates. File-backed queries hold a
    /// read lease through the LSP request; filesystem mutation tools hold a
    /// write lease from before the OS mutation through didClose/didChange.
    document_gates: StdMutex<HashMap<RuntimePath, Weak<TokioRwLock<()>>>>,
    lifecycle: Arc<ManagerLifecycle>,
}

impl LspManager {
    pub fn new(
        process_context: RuntimeProcessContext,
        configs: Vec<LspServerConfig>,
        cwd: RuntimePath,
        opts: LspClientOptions,
    ) -> Self {
        let slots = configs
            .into_iter()
            .map(|config| {
                Arc::new(Slot {
                    config,
                    state: StdMutex::new(SlotState {
                        client: None,
                        starting: None,
                        restarts: 0,
                    }),
                    #[cfg(test)]
                    disposed_late_starts: std::sync::atomic::AtomicU64::new(0),
                    #[cfg(test)]
                    client_start_attempts: std::sync::atomic::AtomicU64::new(0),
                })
            })
            .collect();
        Self {
            process_context,
            slots,
            cwd,
            opts,
            edited: StdMutex::new(Vec::new()),
            baseline: StdMutex::new(HashMap::new()),
            document_gates: StdMutex::new(HashMap::new()),
            lifecycle: Arc::new(ManagerLifecycle {
                disposed: AtomicBool::new(false),
                state: StdMutex::new(LifecycleState {
                    dispose_completion: None,
                }),
            }),
        }
    }

    /// True when at least one server is configured.
    pub fn available(&self) -> bool {
        !self.slots.is_empty()
    }

    fn disposed(&self) -> bool {
        self.lifecycle.disposed.load(Ordering::SeqCst)
    }

    /// Whether any configured server handles this file's extension.
    pub fn handles(&self, path: &RuntimePath) -> bool {
        self.slot_for(path).is_some()
    }

    fn document_key(&self, path: &RuntimePath) -> RuntimePath {
        resolve_runtime(&self.cwd, std::slice::from_ref(path)).unwrap_or_else(|_| path.clone())
    }

    fn document_gate(&self, path: &RuntimePath) -> Arc<TokioRwLock<()>> {
        let key = self.document_key(path);
        let mut gates = self.document_gates.lock().unwrap();
        if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
            return gate;
        }
        gates.retain(|_, gate| gate.strong_count() > 0);
        let gate = Arc::new(TokioRwLock::new(()));
        gates.insert(key, Arc::downgrade(&gate));
        gate
    }

    #[cfg(test)]
    fn prune_document_gates(&self) {
        self.document_gates
            .lock()
            .unwrap()
            .retain(|_, gate| gate.strong_count() > 0);
    }

    fn query_document_gate(&self, path: &RuntimePath) -> Option<Arc<TokioRwLock<()>>> {
        if self.disposed() || !self.handles(path) {
            return None;
        }
        Some(self.document_gate(path))
    }

    pub async fn begin_document_mutation(&self, paths: &[RuntimePath]) -> LspDocumentMutationGuard {
        if self.disposed() || paths.is_empty() {
            return LspDocumentMutationGuard::empty();
        }
        let mut paths = paths
            .iter()
            .map(|path| self.document_key(path))
            .filter(|path| self.handles(path))
            .collect::<Vec<_>>();
        paths.sort_by(|left, right| left.as_runtime_string().cmp(right.as_runtime_string()));
        paths.dedup();

        if paths.is_empty() {
            return LspDocumentMutationGuard::empty();
        }

        let gates = paths
            .iter()
            .map(|path| self.document_gate(path))
            .collect::<Vec<_>>();
        let mut guards = Vec::with_capacity(gates.len());
        for gate in gates {
            guards.push(gate.write_owned().await);
        }
        LspDocumentMutationGuard::new(guards)
    }

    /// Snapshot the PRE-edit diagnostics baseline. MUST run before the write: a
    /// pull server reading a not-yet-opened file resolves diagnostics off disk,
    /// so a post-write snapshot would capture post-edit state. The presence guard
    /// keeps the original baseline across multiple edits to the same file.
    pub async fn before_edited(&self, path: &RuntimePath) {
        if self.disposed() {
            return;
        }
        let has = self.baseline.lock().unwrap().contains_key(path);
        if !has {
            let diags = self.diagnostics(path).await.unwrap_or_default();
            self.baseline.lock().unwrap().insert(path.clone(), diags);
        }
    }

    pub async fn notify_edited(&self, path: &RuntimePath, text: Option<&RuntimeString>) {
        if self.disposed() {
            return;
        }
        self.edited_add(path);
        if let Some(client) = self.client_for(path).await {
            client.notify_edited(path, text).await;
        }
    }

    /// Close a document that was previously opened on its routed server.
    /// Deletion must not lazily start a brand-new server; it only joins an
    /// already-running/in-flight connection and clears reminder state.
    pub async fn notify_deleted(&self, path: &RuntimePath) {
        self.edited.lock().unwrap().retain(|edited| edited != path);
        self.baseline.lock().unwrap().remove(path);
        if self.disposed() {
            return;
        }
        let Some(slot) = self.slot_for(path) else {
            return;
        };
        let (client, starting) = {
            let state = slot.state.lock().unwrap();
            (state.client.clone(), state.starting.clone())
        };
        let client = match client.filter(|client| !client.is_dead()) {
            Some(client) => Some(client),
            None => match starting {
                Some(starting) => starting.await,
                None => None,
            },
        };
        if let Some(client) = client {
            client.notify_deleted(path).await;
        }
    }

    /// Diagnostics for `path` that were NOT present before its most recent edit
    /// (post-edit set minus the pre-edit baseline). Consumes the baseline so a
    /// later turn re-snapshots. With no baseline returns the full current set.
    pub async fn new_diagnostics(&self, path: &RuntimePath) -> Vec<LspDiagnostic> {
        let current = self.diagnostics(path).await.unwrap_or_default();
        let base = self.baseline.lock().unwrap().remove(path);
        match base {
            None => current,
            Some(base) => current
                .into_iter()
                .filter(|d| !base.iter().any(|b| diagnostics_equal(d, b)))
                .collect(),
        }
    }

    pub async fn diagnostics(&self, path: &RuntimePath) -> LspQueryResult<Vec<LspDiagnostic>> {
        let Some(gate) = self.query_document_gate(path) else {
            return Ok(Vec::new());
        };
        let _query = gate.read().await;
        match self.client_for(path).await {
            Some(c) => c.diagnostics(path).await,
            None => Ok(Vec::new()),
        }
    }

    pub async fn hover(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Option<LspHoverResult>> {
        let Some(gate) = self.query_document_gate(path) else {
            return Ok(None);
        };
        let _query = gate.read().await;
        match self.client_for(path).await {
            Some(c) => c.hover(path, line, character).await,
            None => Ok(None),
        }
    }

    pub async fn definition(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        let Some(gate) = self.query_document_gate(path) else {
            return Ok(Vec::new());
        };
        let _query = gate.read().await;
        match self.client_for(path).await {
            Some(c) => c.definition(path, line, character).await,
            None => Ok(Vec::new()),
        }
    }

    pub async fn references(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        let Some(gate) = self.query_document_gate(path) else {
            return Ok(Vec::new());
        };
        let _query = gate.read().await;
        match self.client_for(path).await {
            Some(c) => c.references(path, line, character).await,
            None => Ok(Vec::new()),
        }
    }

    pub async fn implementation(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        let Some(gate) = self.query_document_gate(path) else {
            return Ok(Vec::new());
        };
        let _query = gate.read().await;
        match self.client_for(path).await {
            Some(c) => c.implementation(path, line, character).await,
            None => Ok(Vec::new()),
        }
    }

    pub async fn document_symbols(&self, path: &RuntimePath) -> LspQueryResult<Vec<LspSymbol>> {
        let Some(gate) = self.query_document_gate(path) else {
            return Ok(Vec::new());
        };
        let _query = gate.read().await;
        match self.client_for(path).await {
            Some(c) => c.document_symbols(path).await,
            None => Ok(Vec::new()),
        }
    }

    /// workspace/symbol has no file to route on. Query every ALREADY-LIVE server
    /// and merge; if none is live yet, lazily start the first configured slot and
    /// query it. Collapses duplicates across servers.
    pub async fn workspace_symbol(&self, query: &RuntimeString) -> Vec<LspWorkspaceSymbol> {
        if self.disposed() {
            return Vec::new();
        }
        let mut clients: Vec<Arc<LspClient>> = self
            .slots
            .iter()
            .filter_map(|s| s.state.lock().unwrap().client.clone())
            .filter(|c| !c.is_dead())
            .collect();
        if clients.is_empty() {
            match self.slots.first() {
                None => return Vec::new(),
                Some(first) => {
                    if let Some(c) = self.start_slot(first).await {
                        clients = vec![c];
                    }
                }
            }
        }
        let mut seen: HashSet<(RuntimeString, RuntimeString, RuntimeString, u32, u32)> =
            HashSet::new();
        let mut out: Vec<LspWorkspaceSymbol> = Vec::new();
        for c in &clients {
            for s in c.workspace_symbol(query).await {
                let key = (
                    s.name.clone(),
                    s.kind.clone(),
                    s.path.clone(),
                    s.line,
                    s.character,
                );
                if seen.insert(key) {
                    out.push(s);
                }
            }
        }
        out
    }

    pub async fn prepare_call_hierarchy(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCallHierarchyItem>> {
        let Some(gate) = self.query_document_gate(path) else {
            return Ok(Vec::new());
        };
        let _query = gate.read().await;
        match self.client_for(path).await {
            Some(c) => c.prepare_call_hierarchy(path, line, character).await,
            None => Ok(Vec::new()),
        }
    }

    pub async fn incoming_calls(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCall>> {
        let Some(gate) = self.query_document_gate(path) else {
            return Ok(Vec::new());
        };
        let _query = gate.read().await;
        match self.client_for(path).await {
            Some(c) => c.incoming_calls(path, line, character).await,
            None => Ok(Vec::new()),
        }
    }

    pub async fn outgoing_calls(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCall>> {
        let Some(gate) = self.query_document_gate(path) else {
            return Ok(Vec::new());
        };
        let _query = gate.read().await;
        match self.client_for(path).await {
            Some(c) => c.outgoing_calls(path, line, character).await,
            None => Ok(Vec::new()),
        }
    }

    /// Drain + return the files edited since the last call (for the reminder).
    pub fn drain_edited(&self) -> Vec<RuntimePath> {
        let mut e = self.edited.lock().unwrap();
        let out = e.clone();
        e.clear();
        out
    }

    /// The `ctx.lsp` controller view.
    pub fn controller(self: &Arc<Self>) -> Arc<dyn LspController> {
        Arc::new(LspControllerHandle {
            manager: Arc::clone(self),
        })
    }

    pub async fn dispose_all(&self) {
        let mut completion = {
            // The lifecycle lock is also held by start admission and start
            // publication. Once `disposed` is stored while holding it, no new
            // shared start can be installed and no completed start can publish
            // a client into a slot.
            let mut lifecycle = self.lifecycle.state.lock().unwrap();
            if let Some(completion) = &lifecycle.dispose_completion {
                completion.clone()
            } else {
                self.lifecycle.disposed.store(true, Ordering::SeqCst);

                // Detach every currently-published client and in-flight start
                // before the first await. This is essential when Session wraps
                // disposal in a deadline: cancelling that waiter must not leave
                // a client reachable from the disposed manager.
                let mut clients = Vec::new();
                let mut starts = Vec::new();
                for slot in &self.slots {
                    let mut state = slot.state.lock().unwrap();
                    if let Some(client) = state.client.take() {
                        clients.push(client);
                    }
                    if let Some(starting) = state.starting.take() {
                        starts.push(starting);
                    }
                }

                let (completion_tx, completion_rx) = watch::channel(false);
                lifecycle.dispose_completion = Some(completion_rx.clone());

                // This task, rather than the caller, owns cleanup. A timed-out
                // or cancelled first caller therefore cannot abandon a client
                // dispose or drop the sole driver of an in-flight shared start.
                tokio::spawn(async move {
                    let client_cleanup = clients.into_iter().map(|client| async move {
                        client.dispose().await;
                    });
                    let start_cleanup = starts.into_iter().map(|starting| async move {
                        // A start completing after disposal observes the same
                        // lifecycle gate in `complete_start`, disposes its new
                        // client/process, and refuses publication.
                        let _ = starting.await;
                    });
                    future::join(
                        future::join_all(client_cleanup),
                        future::join_all(start_cleanup),
                    )
                    .await;
                    completion_tx.send_replace(true);
                });

                completion_rx
            }
        };

        self.edited.lock().unwrap().clear();
        self.baseline.lock().unwrap().clear();
        self.document_gates.lock().unwrap().clear();

        while !*completion.borrow() {
            if completion.changed().await.is_err() {
                break;
            }
        }
    }

    // --- internals ---

    fn edited_add(&self, path: &RuntimePath) {
        let mut e = self.edited.lock().unwrap();
        if !e.iter().any(|p| p == path) {
            e.push(path.clone());
        }
    }

    fn slot_for(&self, path: &RuntimePath) -> Option<Arc<Slot>> {
        let ext = extname_runtime(path.as_runtime_string());
        if ext.is_empty() {
            return None;
        }
        self.slots
            .iter()
            .find(|slot| {
                slot.config
                    .extensions
                    .iter()
                    .any(|candidate| runtime_extension_matches(&ext, candidate))
            })
            .cloned()
    }

    async fn client_for(&self, path: &RuntimePath) -> Option<Arc<LspClient>> {
        if self.disposed() {
            return None;
        }
        match self.slot_for(path) {
            Some(slot) => self.start_slot(&slot).await,
            None => None,
        }
    }

    /// Get a live client for a slot, lazily starting (or bounded-restarting) it.
    fn start_slot(&self, slot: &Arc<Slot>) -> BoxFuture<'static, Option<Arc<LspClient>>> {
        // Keep admission under the same lifecycle lock as disposal and start
        // publication. A caller that raced the initial fast-path check cannot
        // register a lazy start after the manager has transitioned to disposed.
        let lifecycle = self.lifecycle.state.lock().unwrap();
        if self.disposed() {
            return future::ready(None::<Arc<LspClient>>).boxed();
        }
        let mut st = slot.state.lock().unwrap();
        // Live client → reuse.
        if let Some(c) = &st.client {
            if !c.is_dead() {
                let c = c.clone();
                return future::ready(Some(c)).boxed();
            }
        }
        // A dead client may be restarted up to the cap.
        if st.client.as_ref().is_some_and(|c| c.is_dead()) {
            if st.restarts >= MAX_RESTARTS {
                return future::ready(None::<Arc<LspClient>>).boxed();
            }
            st.restarts += 1;
            st.client = None;
            st.starting = None;
        }
        // Coalesce onto an in-flight start.
        if let Some(s) = &st.starting {
            return s.clone().boxed();
        }
        let fut = run_start(
            self.process_context.clone(),
            slot.clone(),
            self.cwd.clone(),
            self.opts.clone(),
            self.lifecycle.clone(),
        )
        .boxed()
        .shared();
        st.starting = Some(fut.clone());
        drop(st);
        drop(lifecycle);
        fut.boxed()
    }
}

/// Create + start one client, then record it on the slot if manager disposal
/// has not won the lifecycle race (including a dead client, so a later call can
/// bounded-restart it).
async fn run_start(
    process_context: RuntimeProcessContext,
    slot: Arc<Slot>,
    cwd: RuntimePath,
    opts: LspClientOptions,
    lifecycle: Arc<ManagerLifecycle>,
) -> Option<Arc<LspClient>> {
    // A `Shared` future is lazy: it can be installed before disposal without
    // ever having been polled. The manager-owned cleanup task must not turn
    // such a dormant admission into a brand-new process after disposal. An
    // already-polled start linearizes here before disposal and is instead
    // handled by `complete_start` if it finishes late.
    let admitted = {
        let _lifecycle = lifecycle.state.lock().unwrap();
        let mut state = slot.state.lock().unwrap();
        if lifecycle.disposed.load(Ordering::SeqCst) {
            state.starting = None;
            false
        } else {
            true
        }
    };
    if !admitted {
        return None;
    }

    #[cfg(test)]
    slot.client_start_attempts.fetch_add(1, Ordering::SeqCst);

    let root = slot
        .config
        .root_dir
        .clone()
        .map(RuntimePath::from)
        .unwrap_or(cwd);
    let client = Arc::new(LspClient::new(
        process_context,
        slot.config.clone(),
        root,
        opts,
    ));
    client.start().await;
    complete_start(lifecycle, slot, client).await
}

/// Publish a completed lazy start only while the manager still admits clients.
/// A start that finishes after disposal owns disposal of the client it created;
/// this closes the process even when the original `dispose_all` waiter timed out.
async fn complete_start(
    lifecycle: Arc<ManagerLifecycle>,
    slot: Arc<Slot>,
    client: Arc<LspClient>,
) -> Option<Arc<LspClient>> {
    let publish = {
        let _lifecycle = lifecycle.state.lock().unwrap();
        let mut state = slot.state.lock().unwrap();
        state.starting = None;
        if lifecycle.disposed.load(Ordering::SeqCst) {
            false
        } else {
            state.client = Some(client.clone());
            true
        }
    };

    if !publish {
        client.dispose().await;
        #[cfg(test)]
        slot.disposed_late_starts.fetch_add(1, Ordering::SeqCst);
        return None;
    }

    if client.is_dead() {
        None
    } else {
        Some(client)
    }
}

/// The controller object handed to tools as `ctx.lsp`.
struct LspControllerHandle {
    manager: Arc<LspManager>,
}

#[async_trait]
impl LspController for LspControllerHandle {
    fn available(&self) -> bool {
        self.manager.available()
    }
    async fn diagnostics(&self, path: &RuntimePath) -> Vec<LspDiagnostic> {
        self.manager.diagnostics(path).await.unwrap_or_default()
    }
    async fn try_diagnostics(&self, path: &RuntimePath) -> LspQueryResult<Vec<LspDiagnostic>> {
        self.manager.diagnostics(path).await
    }
    async fn hover(&self, path: &RuntimePath, line: u32, character: u32) -> Option<LspHoverResult> {
        self.manager
            .hover(path, line, character)
            .await
            .unwrap_or_default()
    }
    async fn try_hover(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Option<LspHoverResult>> {
        self.manager.hover(path, line, character).await
    }
    async fn definition(&self, path: &RuntimePath, line: u32, character: u32) -> Vec<LspLocation> {
        self.manager
            .definition(path, line, character)
            .await
            .unwrap_or_default()
    }
    async fn try_definition(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        self.manager.definition(path, line, character).await
    }
    async fn references(&self, path: &RuntimePath, line: u32, character: u32) -> Vec<LspLocation> {
        self.manager
            .references(path, line, character)
            .await
            .unwrap_or_default()
    }
    async fn try_references(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        self.manager.references(path, line, character).await
    }
    async fn implementation(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> Vec<LspLocation> {
        self.manager
            .implementation(path, line, character)
            .await
            .unwrap_or_default()
    }
    async fn try_implementation(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        self.manager.implementation(path, line, character).await
    }
    async fn document_symbols(&self, path: &RuntimePath) -> Vec<LspSymbol> {
        self.manager
            .document_symbols(path)
            .await
            .unwrap_or_default()
    }
    async fn try_document_symbols(&self, path: &RuntimePath) -> LspQueryResult<Vec<LspSymbol>> {
        self.manager.document_symbols(path).await
    }
    async fn workspace_symbol(&self, query: &RuntimeString) -> Vec<LspWorkspaceSymbol> {
        self.manager.workspace_symbol(query).await
    }
    async fn prepare_call_hierarchy(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> Vec<LspCallHierarchyItem> {
        self.manager
            .prepare_call_hierarchy(path, line, character)
            .await
            .unwrap_or_default()
    }
    async fn try_prepare_call_hierarchy(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCallHierarchyItem>> {
        self.manager
            .prepare_call_hierarchy(path, line, character)
            .await
    }
    async fn incoming_calls(&self, path: &RuntimePath, line: u32, character: u32) -> Vec<LspCall> {
        self.manager
            .incoming_calls(path, line, character)
            .await
            .unwrap_or_default()
    }
    async fn try_incoming_calls(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCall>> {
        self.manager.incoming_calls(path, line, character).await
    }
    async fn outgoing_calls(&self, path: &RuntimePath, line: u32, character: u32) -> Vec<LspCall> {
        self.manager
            .outgoing_calls(path, line, character)
            .await
            .unwrap_or_default()
    }
    async fn try_outgoing_calls(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCall>> {
        self.manager.outgoing_calls(path, line, character).await
    }
    async fn begin_document_mutation(&self, paths: &[RuntimePath]) -> LspDocumentMutationGuard {
        self.manager.begin_document_mutation(paths).await
    }
    async fn before_edited(&self, path: &RuntimePath) {
        self.manager.before_edited(path).await
    }
    async fn notify_edited(&self, path: &RuntimePath, text: Option<&RuntimeString>) {
        self.manager.notify_edited(path, text).await
    }
    async fn notify_deleted(&self, path: &RuntimePath) {
        self.manager.notify_deleted(path).await
    }
}

/// Two diagnostics are the SAME iff message, severity, source, code AND the full
/// range (start + end) all match. Used to diff a post-edit set against the
/// pre-edit baseline so only newly-introduced problems surface.
fn diagnostics_equal(a: &LspDiagnostic, b: &LspDiagnostic) -> bool {
    a.message == b.message
        && a.severity == b.severity
        && a.source == b.source
        && a.code == b.code
        && a.line == b.line
        && a.character == b.character
        && a.end_line == b.end_line
        && a.end_character == b.end_character
}

fn extname_runtime(path: &RuntimeString) -> RuntimeString {
    path_extname(host_path_dialect(), &RuntimePath::from(path.clone())).into_runtime_string()
}

fn runtime_extension_matches(actual: &RuntimeString, configured: &RuntimeString) -> bool {
    let configured = if configured.starts_with(".") {
        configured.clone()
    } else {
        RuntimeString::from(".").concat(configured)
    };
    lowercase_runtime(actual) == lowercase_runtime(&configured)
}

fn lowercase_runtime(value: &RuntimeString) -> RuntimeString {
    path_lowercase(value)
}

fn severity_label(s: LspSeverity) -> &'static str {
    match s {
        LspSeverity::Error => "error",
        LspSeverity::Warning => "warning",
        LspSeverity::Information => "information",
        LspSeverity::Hint => "hint",
    }
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

fn render_diagnostic_line(
    path: &RuntimeString,
    line: u32,
    character: u32,
    severity: LspSeverity,
    source: Option<RuntimeString>,
    message: RuntimeString,
) -> RuntimeString {
    let where_ = path
        .concat(&RuntimeString::from(format!(":{}:", line + 1)))
        .concat(&RuntimeString::from((character + 1).to_string()));
    let source = source
        .map(|value| {
            RuntimeString::from(" [")
                .concat(&value)
                .concat(&RuntimeString::from("]"))
        })
        .unwrap_or_default();
    RuntimeString::from("- ")
        .concat(&where_)
        .concat(&RuntimeString::from(" "))
        .concat(&RuntimeString::from(severity_label(severity)))
        .concat(&source)
        .concat(&RuntimeString::from(": "))
        .concat(&message)
}

/// Surfaces diagnostics for files edited in the previous turn as a per-turn
/// `<system-reminder>`. Drains the edited set, so persistent errors aren't
/// re-nagged every turn unless the file is touched again. Any failure injects
/// nothing rather than throwing.
pub struct LspDiagnosticsReminderProvider {
    get_manager: Box<dyn Fn() -> Option<Arc<LspManager>> + Send + Sync>,
}

pub fn create_lsp_diagnostics_reminder_provider(
    get_manager: impl Fn() -> Option<Arc<LspManager>> + Send + Sync + 'static,
) -> LspDiagnosticsReminderProvider {
    LspDiagnosticsReminderProvider {
        get_manager: Box::new(get_manager),
    }
}

#[async_trait]
impl ReminderProvider for LspDiagnosticsReminderProvider {
    async fn reminder(&self, _ctx: &ReminderContext) -> Option<RuntimeString> {
        let mgr = (self.get_manager)()?;
        if !mgr.available() {
            return None;
        }
        let edited = mgr.drain_edited();
        if edited.is_empty() {
            return None;
        }
        let mut lines: Vec<RuntimeString> = Vec::new();
        for path in edited {
            // Only NEW diagnostics (post-edit minus the pre-edit baseline) — a
            // pre-existing error on an untouched line is not re-nagged.
            let diags = mgr.new_diagnostics(&path).await;
            let runtime_path = path.as_runtime_string().clone();
            for d in diags {
                lines.push(render_diagnostic_line(
                    &runtime_path,
                    d.line,
                    d.character,
                    d.severity,
                    d.source,
                    d.message,
                ));
            }
        }
        if lines.is_empty() {
            return None;
        }
        Some(
            RuntimeString::from("New language-server diagnostics for files you just edited:\n")
                .concat(&join_runtime_strings(&lines, "\n")),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::test_runtime_process_context;
    use std::time::Duration;
    use tokio::sync::oneshot;

    fn config(exts: &[&str]) -> LspServerConfig {
        LspServerConfig {
            language: "x".into(),
            extensions: exts.iter().copied().map(RuntimeString::from).collect(),
            command: "true".into(),
            args: None,
            root_dir: None,
        }
    }

    fn ctx() -> ReminderContext {
        ReminderContext {
            messages: Vec::new(),
            cwd: "/w".into(),
            task_list_id: None,
            task_plan: None,
            signal: None,
            plan_file_path: None,
            plan_file_exists: None,
            has_task_tool: false,
        }
    }

    #[test]
    fn extname_and_normalize() {
        assert_eq!(extname_runtime(&RuntimeString::from("a/b/foo.TS")), ".TS");
        assert_eq!(extname_runtime(&RuntimeString::from("foo")), "");
        assert_eq!(extname_runtime(&RuntimeString::from(".bashrc")), "");
        assert!(runtime_extension_matches(
            &RuntimeString::from(".TS"),
            &RuntimeString::from("ts")
        ));
        assert!(runtime_extension_matches(
            &RuntimeString::from(".tsx"),
            &RuntimeString::from(".TSX")
        ));
        assert!(runtime_extension_matches(
            &RuntimeString::from(".ä"),
            &RuntimeString::from("Ä")
        ));
    }

    #[test]
    fn extension_matching_uses_pinned_node20_case_mapping_and_preserves_surrogates() {
        // Garay uppercase U+10D50 -> lowercase U+10D70 was added in Unicode 16,
        // the version pinned by Node 20.19.4 / ICU 77.1 for this repository.
        assert!(runtime_extension_matches(
            &RuntimeString::from_utf16(vec![b'.' as u16, 0xd803, 0xdd70]),
            &RuntimeString::from_utf16(vec![0xd803, 0xdd50])
        ));

        let actual = RuntimeString::from_utf16(vec![b'.' as u16, b'a' as u16, 0xd800]);
        assert!(runtime_extension_matches(
            &actual,
            &RuntimeString::from_utf16(vec![b'A' as u16, 0xd800])
        ));
        assert!(!runtime_extension_matches(
            &actual,
            &RuntimeString::from_utf16(vec![b'A' as u16, 0xdc00])
        ));
    }

    #[test]
    fn handles_routes_by_extension() {
        let m = LspManager::new(
            test_runtime_process_context(),
            vec![config(&[".ts", "tsx"])],
            "/w".into(),
            LspClientOptions::default(),
        );
        assert!(m.available());
        assert!(m.handles(&RuntimePath::from("/w/a.ts")));
        // Case-insensitive + leading-dot-optional config entry.
        assert!(m.handles(&RuntimePath::from("/w/a.TSX")));
        assert!(!m.handles(&RuntimePath::from("/w/a.js")));
        assert!(!m.handles(&RuntimePath::from("/w/noext")));
    }

    #[test]
    fn empty_manager_not_available() {
        let m = LspManager::new(
            test_runtime_process_context(),
            vec![],
            "/w".into(),
            LspClientOptions::default(),
        );
        assert!(!m.available());
        assert!(!m.handles(&RuntimePath::from("/w/a.ts")));
    }

    #[tokio::test]
    async fn document_mutation_guard_excludes_same_canonical_path_queries() {
        let manager = LspManager::new(
            test_runtime_process_context(),
            vec![config(&[".ts"])],
            "/w".into(),
            LspClientOptions::default(),
        );
        let relative = RuntimePath::from("a.ts");
        let absolute = RuntimePath::from("/w/a.ts");
        let other = RuntimePath::from("/w/b.ts");

        let mutation = manager
            .begin_document_mutation(&[relative, absolute.clone()])
            .await;
        assert!(manager.document_gate(&absolute).try_read().is_err());
        assert!(manager.document_gate(&other).try_read().is_ok());

        drop(mutation);
        assert!(manager.document_gate(&absolute).try_read().is_ok());
    }

    #[tokio::test]
    async fn document_mutation_waits_for_query_read_window_then_excludes_new_queries() {
        let manager = LspManager::new(
            test_runtime_process_context(),
            vec![config(&[".ts"])],
            "/w".into(),
            LspClientOptions::default(),
        );
        let path = RuntimePath::from("/w/race.ts");
        let gate = manager.document_gate(&path);
        let query_read_window = gate.clone().read_owned().await;
        let mut mutation = Box::pin(manager.begin_document_mutation(std::slice::from_ref(&path)));

        assert!(
            mutation.as_mut().now_or_never().is_none(),
            "the OS mutation boundary must wait until the in-flight query leaves its read window"
        );
        drop(query_read_window);

        let mutation = mutation.await;
        assert!(gate.try_read().is_err());
        drop(mutation);
        assert!(gate.try_read().is_ok());
    }

    #[tokio::test]
    async fn unsupported_document_mutation_does_not_allocate_a_gate() {
        let manager = LspManager::new(
            test_runtime_process_context(),
            vec![config(&[".ts"])],
            "/w".into(),
            LspClientOptions::default(),
        );

        let mutation = manager
            .begin_document_mutation(&[
                RuntimePath::from("ignored.txt"),
                RuntimePath::from("/w/also-ignored.md"),
            ])
            .await;

        assert!(manager
            .diagnostics(&RuntimePath::from("queried-but-ignored.txt"))
            .await
            .unwrap()
            .is_empty());
        assert!(manager.document_gates.lock().unwrap().is_empty());
        drop(mutation);
    }

    #[tokio::test]
    async fn document_gate_cache_stays_bounded_across_long_sessions_and_dispose() {
        let manager = LspManager::new(
            test_runtime_process_context(),
            vec![config(&[".ts"])],
            "/w".into(),
            LspClientOptions::default(),
        );

        for index in 0..2_048 {
            let path = RuntimePath::from(format!("/w/generated-{index}.ts"));
            let mutation = manager
                .begin_document_mutation(std::slice::from_ref(&path))
                .await;
            drop(mutation);
            assert!(
                manager.document_gates.lock().unwrap().len() <= 1,
                "dead weak entries must be pruned on the next document"
            );
        }
        manager.prune_document_gates();
        assert!(manager.document_gates.lock().unwrap().is_empty());

        let live_gate = manager.document_gate(&RuntimePath::from("/w/live.ts"));
        assert_eq!(manager.document_gates.lock().unwrap().len(), 1);
        manager.dispose_all().await;
        assert!(manager.document_gates.lock().unwrap().is_empty());
        assert!(live_gate.try_read().is_ok());
    }

    #[test]
    fn diagnostics_equal_predicate() {
        let a = LspDiagnostic {
            line: 1,
            character: 2,
            end_line: Some(1),
            end_character: Some(5),
            severity: LspSeverity::Error,
            message: "m".into(),
            source: Some("s".into()),
            code: Some("C".into()),
        };
        let mut b = a.clone();
        assert!(diagnostics_equal(&a, &b));
        b.message = "n".into();
        assert!(!diagnostics_equal(&a, &b));
        let mut c = a.clone();
        c.end_character = Some(6);
        assert!(!diagnostics_equal(&a, &c));
    }

    #[test]
    fn reminder_line_preserves_non_scalar_path_source_and_message_units() {
        let path = RuntimeString::from_utf16(vec![b'/' as u16, b'w' as u16, 0xd800]);
        let source = RuntimeString::from_utf16(vec![b's' as u16, 0xdc00]);
        let message = RuntimeString::from_utf16(vec![b'm' as u16, 0xd801]);
        let rendered =
            render_diagnostic_line(&path, 0, 1, LspSeverity::Error, Some(source), message);
        assert!(rendered.as_utf16().contains(&0xd800));
        assert!(rendered.as_utf16().contains(&0xdc00));
        assert!(rendered.as_utf16().contains(&0xd801));
        assert!(!rendered.as_utf16().contains(&0xfffd));
    }

    #[tokio::test]
    async fn drain_edited_dedups_and_clears() {
        // `.txt` matches no server → notify only records the edited path (no spawn).
        let m = LspManager::new(
            test_runtime_process_context(),
            vec![config(&[".py"])],
            "/w".into(),
            LspClientOptions::default(),
        );
        m.notify_edited(
            &RuntimePath::from("/w/a.txt"),
            Some(&RuntimeString::from("x")),
        )
        .await;
        m.notify_edited(
            &RuntimePath::from("/w/a.txt"),
            Some(&RuntimeString::from("x")),
        )
        .await;
        m.notify_edited(
            &RuntimePath::from("/w/b.txt"),
            Some(&RuntimeString::from("y")),
        )
        .await;
        assert_eq!(
            m.drain_edited(),
            vec![RuntimePath::from("/w/a.txt"), RuntimePath::from("/w/b.txt")]
        );
        assert!(m.drain_edited().is_empty());
    }

    #[tokio::test]
    async fn reminder_none_when_unavailable_or_no_edits() {
        let empty = Arc::new(LspManager::new(
            test_runtime_process_context(),
            vec![],
            "/w".into(),
            LspClientOptions::default(),
        ));
        let p = create_lsp_diagnostics_reminder_provider({
            let m = empty.clone();
            move || Some(m.clone())
        });
        assert_eq!(p.reminder(&ctx()).await, None);

        let configured = Arc::new(LspManager::new(
            test_runtime_process_context(),
            vec![config(&[".py"])],
            "/w".into(),
            LspClientOptions::default(),
        ));
        let p2 = create_lsp_diagnostics_reminder_provider({
            let m = configured.clone();
            move || Some(m.clone())
        });
        // Available but nothing edited → nothing injected.
        assert_eq!(p2.reminder(&ctx()).await, None);
    }

    #[tokio::test]
    async fn reminder_none_when_manager_missing() {
        let p = create_lsp_diagnostics_reminder_provider(|| None);
        assert_eq!(p.reminder(&ctx()).await, None);
    }

    #[tokio::test]
    async fn dispose_does_not_drive_an_unpolled_lazy_start() {
        let manager = LspManager::new(
            test_runtime_process_context(),
            vec![config(&[".ts"])],
            "/w".into(),
            LspClientOptions::default(),
        );
        let slot = manager.slots[0].clone();

        // `start_slot` installs a lazy Shared future. Do not poll the returned
        // clone: disposal becomes the first code that could drive it.
        let dormant_start = manager.start_slot(&slot);
        assert_eq!(slot.client_start_attempts.load(Ordering::SeqCst), 0);

        manager.dispose_all().await;
        assert_eq!(
            slot.client_start_attempts.load(Ordering::SeqCst),
            0,
            "cleanup must not spawn a process for an admitted but dormant start"
        );
        assert!(dormant_start.await.is_none());
        let state = slot.state.lock().unwrap();
        assert!(state.client.is_none());
        assert!(state.starting.is_none());
    }

    #[tokio::test]
    async fn cancelled_dispose_waiter_cannot_publish_a_gated_late_start() {
        let manager = Arc::new(LspManager::new(
            test_runtime_process_context(),
            vec![config(&[".ts"])],
            "/w".into(),
            LspClientOptions::default(),
        ));
        let slot = manager.slots[0].clone();

        // Install a deterministic synthetic start at exactly the lifecycle
        // boundary after a client/process is ready but before it is published.
        // It calls the same completion path as `run_start` once released.
        let client = Arc::new(LspClient::new(
            test_runtime_process_context(),
            slot.config.clone(),
            "/w".into(),
            LspClientOptions::default(),
        ));
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let lifecycle = manager.lifecycle.clone();
        let completion_slot = slot.clone();
        let gated_start = async move {
            let _ = entered_tx.send(());
            let _ = release_rx.await;
            complete_start(lifecycle, completion_slot, client).await
        }
        .boxed()
        .shared();
        slot.state.lock().unwrap().starting = Some(gated_start);

        // A real lazy-start waiter takes a clone of the shared start. This is
        // the condition that let the old implementation outlive a cancelled
        // `dispose_all` future and write the client back into the slot.
        let start_waiter = tokio::spawn({
            let manager = manager.clone();
            let slot = slot.clone();
            async move { manager.start_slot(&slot).await }
        });
        tokio::time::timeout(Duration::from_secs(1), entered_rx)
            .await
            .expect("gated start must be polled")
            .expect("gated start entry signal must remain connected");

        let first_dispose_waiter = tokio::spawn({
            let manager = manager.clone();
            async move { manager.dispose_all().await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let start_detached = slot.state.lock().unwrap().starting.is_none();
                let cleanup_owned = manager
                    .lifecycle
                    .state
                    .lock()
                    .unwrap()
                    .dispose_completion
                    .is_some();
                if manager.disposed() && start_detached && cleanup_owned {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("manager must synchronously close publication before awaiting cleanup");

        // Reproduce Session's outer timeout by cancelling the first waiter.
        // The manager-owned cleanup operation must survive it, and a duplicate
        // caller must join that retained operation rather than report success.
        first_dispose_waiter.abort();
        assert!(first_dispose_waiter.await.unwrap_err().is_cancelled());
        let second_dispose_waiter = tokio::spawn({
            let manager = manager.clone();
            async move { manager.dispose_all().await }
        });
        tokio::task::yield_now().await;
        assert!(!second_dispose_waiter.is_finished());

        release_tx
            .send(())
            .expect("gated start receiver must remain owned by cleanup");
        assert!(start_waiter
            .await
            .expect("lazy-start waiter must not panic")
            .is_none());
        tokio::time::timeout(Duration::from_secs(1), second_dispose_waiter)
            .await
            .expect("retained LSP cleanup must finish after the gate opens")
            .expect("duplicate dispose waiter must not panic");

        let state = slot.state.lock().unwrap();
        assert!(state.client.is_none());
        assert!(state.starting.is_none());
        drop(state);
        assert_eq!(
            slot.disposed_late_starts.load(Ordering::SeqCst),
            1,
            "the unpublished client/process must be disposed exactly once"
        );
    }
}
