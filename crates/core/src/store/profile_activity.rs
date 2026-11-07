//! Read-only activity metrics from durable request usage and turn clocks.
//!
//! Transcripts are append-only, so a fingerprint cache (path, modification
//! time, length) kept beside the session root lets repeat scans reuse the
//! metrics already extracted from unchanged sessions. The cache stores one
//! pre-folded record set per session (usage records with resolved clocks,
//! longest-turn candidate, explicit gaps), never transcript content. It is a
//! pure performance artifact: stale, corrupt, or missing entries fall back to
//! reading the transcript, and a failed cache write never taints a result.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use chrono::{Datelike, NaiveDate};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

use crate::engine::agent::usage_ledger_from_rollout;
use crate::runtime_values::{
    runtime_codec, runtime_json, NodeBufferUtf8, RuntimeJson, RuntimeString,
};
use crate::schemas::message::Message;
use crate::schemas::rollout::{RolloutItem, SessionMeta};
use crate::store::jsonl_store::rollout_metadata;

const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
const CACHE_FILE_NAME: &str = ".profile-activity-cache.json";
const CACHE_VERSION: u32 = 2;
const READ_CONCURRENCY: usize = 16;
const SAVE_THROTTLE: std::time::Duration = std::time::Duration::from_secs(60);

/// Per-store cache document and its disk write throttle. Reconciliation is
/// serialized on the owning store's lock, so concurrent profile reads against
/// one store share a single scan.
#[derive(Default)]
pub(crate) struct MemoryCache {
    document: Option<CacheDocument>,
    last_save: Option<std::time::Instant>,
    dirty: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileActivityDay {
    pub date: String,
    pub tokens: u64,
}

/// Read-only lifetime activity measurements.
///
/// `incomplete` means a transcript could not be read or decoded, so some
/// usage may be missing entirely and a later read can recover it. `partial`
/// means every readable record was counted, but some of it predates turn
/// clocks and therefore cannot be placed on the calendar or timed; no retry
/// can recover clocks the transcript never recorded.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileActivity {
    pub days: Vec<ProfileActivityDay>,
    pub total_tokens: u64,
    pub peak_daily_tokens: u64,
    pub longest_task_ms: u64,
    pub longest_streak_days: u64,
    pub current_streak_days: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub incomplete: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub partial: bool,
}

/// One billed provider request with its resolved turn clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CachedRecord {
    request_id: String,
    assistant_id: String,
    tokens: u64,
    started_at_ms: Option<i64>,
}

/// Everything the fold needs from one session transcript tree.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionMetrics {
    session_id: String,
    longest_candidate_ms: u64,
    records: Vec<CachedRecord>,
    incomplete: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct ChildFingerprint {
    mtime_ns: u64,
    len: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct CachedFile {
    mtime_ns: u64,
    len: u64,
    metrics: SessionMetrics,
    children: BTreeMap<String, ChildFingerprint>,
}

#[derive(Default, Serialize, Deserialize)]
struct CacheDocument {
    version: u32,
    files: BTreeMap<String, CachedFile>,
}

struct SessionPlan<'a> {
    path: PathBuf,
    key: String,
    mtime_ns: u64,
    len: u64,
    cached: Option<&'a CachedFile>,
    children: Vec<(String, PathBuf, u64, u64)>,
    children_incomplete: bool,
}

/// Borrowed or freshly extracted session metrics, so cache hits never clone
/// the process-local document.
enum HeldMetrics<'a> {
    Cached(&'a SessionMetrics),
    Fresh(SessionMetrics),
}

impl HeldMetrics<'_> {
    fn get(&self) -> &SessionMetrics {
        match self {
            HeldMetrics::Cached(metrics) => metrics,
            HeldMetrics::Fresh(metrics) => metrics,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct RecordTiming {
    started_at_ms: Option<i64>,
}

#[derive(Default)]
struct ActivityFold {
    days: BTreeMap<NaiveDate, u64>,
    assistants: HashSet<String>,
    requests: HashSet<String>,
    result: ProfileActivity,
}

impl ActivityFold {
    fn add_session(&mut self, metrics: &SessionMetrics, today: NaiveDate) {
        self.result.longest_task_ms = self.result.longest_task_ms.max(safe_add(
            0,
            metrics.longest_candidate_ms,
            &mut self.result.incomplete,
        ));
        for record in &metrics.records {
            // Child requests are billed in the parent ledger and also appear
            // in saved child messages. Count each provider request once.
            if !self.assistants.insert(record.assistant_id.clone())
                || !self.requests.insert(record.request_id.clone())
            {
                continue;
            }
            let tokens = safe_add(0, record.tokens, &mut self.result.incomplete);
            self.result.total_tokens = safe_add(
                self.result.total_tokens,
                tokens,
                &mut self.result.incomplete,
            );
            if tokens == 0 {
                continue;
            }
            let date = record
                .started_at_ms
                .and_then(utc_date)
                .filter(|date| *date <= today);
            if let Some(date) = date {
                let value = self.days.entry(date).or_default();
                *value = safe_add(*value, tokens, &mut self.result.incomplete);
            } else {
                // Older records can prove usage without proving their calendar
                // day. Preserve the total, leave those tokens undated and mark
                // the calendar partial: the transcripts are readable, only the
                // clocks they never recorded are absent.
                self.result.partial = true;
            }
        }
    }

    fn finish(mut self, today: NaiveDate) -> ProfileActivity {
        let mut previous: Option<NaiveDate> = None;
        let mut streak = 0;
        for (date, tokens) in self.days {
            streak = if previous.and_then(|day| day.succ_opt()) == Some(date) {
                streak + 1
            } else {
                1
            };
            previous = Some(date);
            self.result.longest_streak_days = self.result.longest_streak_days.max(streak);
            self.result.peak_daily_tokens = self.result.peak_daily_tokens.max(tokens);
            self.result.days.push(ProfileActivityDay {
                date: date.format("%Y-%m-%d").to_string(),
                tokens,
            });
        }
        if previous.is_some_and(|last| last == today || Some(last) == today.pred_opt()) {
            self.result.current_streak_days = streak;
        }
        self.result
    }
}

fn record_timing(items: &[RolloutItem]) -> (HashMap<RuntimeString, RecordTiming>, Vec<u64>) {
    let (_, _, index, _) = rollout_metadata(items);
    let mut message_turn = index.message_turn;
    let mut current_turn = None;
    let mut assistants = HashSet::new();
    for item in items {
        match item {
            RolloutItem::TurnContext { ctx } => current_turn = ctx.turn_id.clone(),
            RolloutItem::Message {
                message: Message::Assistant(assistant),
            } => {
                assistants.insert(assistant.uuid.clone());
            }
            RolloutItem::UsageLedger(record) => {
                assistants.insert(record.assistant_message_id.clone());
                if let Some(turn) = &current_turn {
                    message_turn
                        .entry(record.assistant_message_id.clone())
                        .or_insert_with(|| turn.clone());
                }
            }
            _ => {}
        }
    }
    let timing = assistants
        .into_iter()
        .map(|id| {
            let turn = message_turn.get(&id);
            let started = turn
                .and_then(|turn| index.turn_started_at_ms.get(turn))
                .copied();
            (
                id,
                RecordTiming {
                    started_at_ms: started,
                },
            )
        })
        .collect();
    (timing, index.turn_duration_ms.into_values().collect())
}

fn lifecycle_payload_incomplete(items: &[RolloutItem]) -> bool {
    items.iter().any(|item| match item {
        RolloutItem::Event(event) if event.event == "turn_lifecycle" => event
            .payload
            .as_ref()
            .and_then(|value| value.as_object())
            .is_some_and(|payload| {
                ["startedAtMs", "finishedAtMs", "durationMs"]
                    .iter()
                    .any(|key| {
                        payload.get(*key).is_some_and(|value| {
                            value
                                .as_f64()
                                .is_none_or(|number| !number.is_finite() || number < 0.0)
                        })
                    })
            }),
        _ => false,
    })
}

fn cached_record(
    record: crate::schemas::rollout::UsageLedgerItem,
    clock: &RecordTiming,
) -> CachedRecord {
    CachedRecord {
        request_id: id_key(&record.request_id),
        assistant_id: id_key(&record.assistant_message_id),
        tokens: record.usage.total_tokens.unwrap_or_else(|| {
            record
                .usage
                .input_tokens
                .saturating_add(record.usage.output_tokens)
                .saturating_add(record.usage.cache_read_input_tokens)
                .saturating_add(record.usage.cache_creation_input_tokens)
        }),
        started_at_ms: clock.started_at_ms,
    }
}

fn id_key(id: &RuntimeString) -> String {
    match id.try_to_utf8() {
        Ok(id) => format!("u:{id}"),
        Err(_) => format!("w:{:x?}", id.as_utf16()),
    }
}

/// Reduce one session transcript tree to the inputs the fold consumes. Child
/// clocks join the parent map for requests the parent ledger bills, while
/// clocks the parent transcript owns stay authoritative.
fn extract_session(
    meta: &SessionMeta,
    parent_items: &[RolloutItem],
    child_items: &[Vec<RolloutItem>],
    scan_incomplete: bool,
) -> SessionMetrics {
    let mut incomplete = scan_incomplete || lifecycle_payload_incomplete(parent_items);
    let (parent_timing, parent_durations) = record_timing(parent_items);
    let own: HashSet<RuntimeString> = parent_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Message {
                message: Message::Assistant(assistant),
            } => Some(assistant.uuid.clone()),
            _ => None,
        })
        .collect();
    let mut merged = parent_timing;
    let mut longest = parent_durations.into_iter().max().unwrap_or(0);
    let mut child_clocks = Vec::new();
    for items in child_items {
        incomplete |= lifecycle_payload_incomplete(items);
        let (timing, durations) = record_timing(items);
        longest = longest.max(durations.into_iter().max().unwrap_or(0));
        child_clocks.push(timing.clone());
        // Child ledgers are billed in the parent file, but their clocks belong
        // to their own transcript. An absent child clock remains absent.
        merged.extend(timing.into_iter().filter(|(id, _)| !own.contains(id)));
    }
    let mut records = Vec::new();
    for record in usage_ledger_from_rollout(meta, parent_items, None) {
        let clock = merged
            .get(&record.assistant_message_id)
            .copied()
            .unwrap_or_default();
        records.push(cached_record(record, &clock));
    }
    let mut child_meta = meta.clone();
    child_meta.forked_from_id = None;
    for (items, timing) in child_items.iter().zip(child_clocks) {
        for record in usage_ledger_from_rollout(&child_meta, items, None) {
            let clock = timing
                .get(&record.assistant_message_id)
                .copied()
                .unwrap_or_default();
            records.push(cached_record(record, &clock));
        }
    }
    let session_id = id_key(&meta.id);
    SessionMetrics {
        session_id,
        longest_candidate_ms: longest,
        records,
        incomplete,
    }
}

fn utc_date(millis: i64) -> Option<NaiveDate> {
    if millis <= 0 {
        return None;
    }
    chrono::DateTime::from_timestamp_millis(millis)
        .map(|time| time.date_naive())
        .filter(|date| (1..=9999).contains(&date.year()))
}

fn safe_add(left: u64, right: u64, incomplete: &mut bool) -> u64 {
    let value = left.saturating_add(right);
    if value > MAX_SAFE_INTEGER {
        *incomplete = true;
        MAX_SAFE_INTEGER
    } else {
        value
    }
}

async fn fingerprint(path: &Path) -> io::Result<(u64, u64)> {
    let meta = tokio::fs::metadata(path).await?;
    let nanos = meta
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64;
    Ok((nanos, meta.len()))
}

async fn load_cache(path: &Path) -> CacheDocument {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(_) => return CacheDocument::default(),
    };
    match serde_json::from_slice::<CacheDocument>(&bytes) {
        Ok(document) if document.version == CACHE_VERSION => document,
        _ => CacheDocument::default(),
    }
}

async fn save_cache(path: &Path, document: &CacheDocument) -> io::Result<()> {
    let bytes = serde_json::to_vec(document)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    let tmp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    tokio::fs::write(&tmp, &bytes).await?;
    let result = tokio::fs::rename(&tmp, path).await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    result
}

/// Scan only the usage and timing records needed by the profile. No provider,
/// writer lease, lifecycle recovery, or prompt/history projection is created.
/// Sessions whose fingerprints match the cache contribute their stored record
/// set without being read again; everything else is read with bounded
/// parallelism and folded in deterministic path order.
pub(crate) async fn read_profile_activity(
    memory: &mut MemoryCache,
    roots: &[&Path],
    now_ms: i64,
) -> io::Result<ProfileActivity> {
    let today = utc_date(now_ms)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid activity clock"))?;
    let cache_path = roots.first().map(|root| root.join(CACHE_FILE_NAME));
    if memory.document.is_none() {
        let mut loaded = match cache_path.as_deref() {
            Some(path) => load_cache(path).await,
            None => CacheDocument::default(),
        };
        loaded.version = CACHE_VERSION;
        memory.document = Some(loaded);
    }
    let mut plans = Vec::new();
    let mut targets = Vec::new();
    let loaded = memory.document.as_ref().expect("document seeded above");
    for root in roots {
        for path in jsonl_files(root).await? {
            let (mtime_ns, len) = fingerprint(&path).await.unwrap_or((0, 0));
            let key = path.to_string_lossy().into_owned();
            let cached = loaded
                .files
                .get(&key)
                .filter(|entry| entry.mtime_ns == mtime_ns && entry.len == len);
            let mut children = Vec::new();
            let mut children_incomplete = false;
            let child_root = crate::store::compression::plain_path(&path)
                .with_extension("")
                .join("subagents");
            match jsonl_files(&child_root).await {
                Ok(paths) => {
                    for child in paths {
                        let (child_mtime, child_len) = fingerprint(&child).await.unwrap_or((0, 0));
                        children.push((
                            child.to_string_lossy().into_owned(),
                            child,
                            child_mtime,
                            child_len,
                        ));
                    }
                }
                Err(_) => children_incomplete = true,
            }
            let child_fingerprints_match = cached.is_some_and(|entry| {
                entry.children.len() == children.len()
                    && children.iter().all(|(key, _, mtime, len)| {
                        entry
                            .children
                            .get(key)
                            .is_some_and(|fp| fp.mtime_ns == *mtime && fp.len == *len)
                    })
            });
            let hit = cached.is_some() && child_fingerprints_match && !children_incomplete;
            if !hit {
                targets.push(path.clone());
                targets.extend(children.iter().map(|(_, path, _, _)| path.clone()));
            }
            plans.push(SessionPlan {
                path,
                key,
                mtime_ns,
                len,
                cached: cached.filter(|_| hit),
                children,
                children_incomplete,
            });
        }
    }
    let reads = stream::iter(targets.into_iter())
        .map(|path| async move {
            let read = read_metric_items(&path).await;
            (path, read)
        })
        .buffer_unordered(READ_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<HashMap<_, _>>();
    let mut fold = ActivityFold::default();
    let mut sessions = HashSet::new();
    let mut fresh = BTreeMap::new();
    let mut enumerated = HashSet::new();
    let mut dirty = false;
    for plan in plans {
        enumerated.insert(plan.key.clone());
        let mut cacheable = !plan.children_incomplete;
        let held = match plan.cached {
            Some(entry) => HeldMetrics::Cached(&entry.metrics),
            None => {
                dirty = true;
                let mut scan_incomplete = plan.children_incomplete;
                let parent_items = match reads.get(&plan.path) {
                    Some(Ok((items, incomplete))) => {
                        scan_incomplete |= *incomplete;
                        items
                    }
                    _ => {
                        fold.result.incomplete = true;
                        continue;
                    }
                };
                let Some(meta) = parent_items.iter().find_map(|item| match item {
                    RolloutItem::SessionMeta { meta, .. } => Some(meta.clone()),
                    _ => None,
                }) else {
                    fold.result.incomplete = true;
                    continue;
                };
                let mut child_items = Vec::new();
                for (_, child, _, _) in &plan.children {
                    match reads.get(child) {
                        Some(Ok((items, incomplete))) => {
                            scan_incomplete |= *incomplete;
                            child_items.push(items.clone());
                        }
                        _ => {
                            cacheable = false;
                            scan_incomplete = true;
                            fold.result.incomplete = true;
                        }
                    }
                }
                HeldMetrics::Fresh(extract_session(
                    &meta,
                    parent_items,
                    &child_items,
                    scan_incomplete,
                ))
            }
        };
        let metrics = held.get();
        fold.result.incomplete |= metrics.incomplete;
        // An archive move may expose the same session in both snapshots.
        let duplicate = !sessions.insert(metrics.session_id.clone());
        if !duplicate {
            fold.add_session(metrics, today);
        }
        if plan.cached.is_none() && cacheable {
            if let HeldMetrics::Fresh(metrics) = held {
                let children = plan
                    .children
                    .iter()
                    .map(|(key, _, mtime_ns, len)| {
                        (
                            key.clone(),
                            ChildFingerprint {
                                mtime_ns: *mtime_ns,
                                len: *len,
                            },
                        )
                    })
                    .collect();
                fresh.insert(
                    plan.key,
                    CachedFile {
                        mtime_ns: plan.mtime_ns,
                        len: plan.len,
                        metrics,
                        children,
                    },
                );
            }
        }
    }
    let throttle_ok = memory
        .last_save
        .map(|last| last.elapsed() >= SAVE_THROTTLE)
        .unwrap_or(true);
    let document = memory.document.as_mut().expect("document seeded above");
    dirty |= document.files.len() != enumerated.len();
    for (key, entry) in fresh {
        document.files.insert(key, entry);
    }
    document.files.retain(|key, _| enumerated.contains(key));
    memory.dirty |= dirty;
    if memory.dirty && throttle_ok {
        if let Some(path) = cache_path.as_deref() {
            if save_cache(path, document).await.is_ok() {
                memory.last_save = Some(std::time::Instant::now());
                memory.dirty = false;
            }
        }
    }
    Ok(fold.finish(today))
}

async fn jsonl_files(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut directory = match tokio::fs::read_dir(root).await {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut files = Vec::new();
    while let Some(entry) = directory.next_entry().await? {
        if entry.file_type().await?.is_file()
            && crate::store::compression::is_rollout_name(&entry.file_name().to_string_lossy())
        {
            files.push(entry.path());
        }
    }
    files.sort();
    let plain = files
        .iter()
        .filter(|path| path.extension().is_some_and(|value| value == "jsonl"))
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    files.retain(|path| {
        !path.to_string_lossy().ends_with(".jsonl.zst")
            || !plain.contains(&crate::store::compression::plain_path(path))
    });
    Ok(files)
}

#[derive(Deserialize)]
struct MetricEnvelope<'a> {
    kind: String,
    event: Option<String>,
    #[serde(borrow)]
    message: Option<MetricMessage<'a>>,
    #[serde(borrow)]
    ctx: Option<MetricContext<'a>>,
}

#[derive(Deserialize, Serialize)]
struct MetricMessage<'a> {
    #[serde(rename = "type")]
    kind: String,
    #[serde(borrow, serialize_with = "empty_content")]
    content: &'a serde_json::value::RawValue,
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    uuid: Option<&'a serde_json::value::RawValue>,
    #[serde(
        borrow,
        default,
        deserialize_with = "raw_field",
        skip_serializing_if = "Option::is_none"
    )]
    model: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    usage: Option<&'a serde_json::value::RawValue>,
}

fn empty_content<S: serde::Serializer>(
    _content: &&serde_json::value::RawValue,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    [0_u8; 0].serialize(serializer)
}

fn raw_field<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<&'de serde_json::value::RawValue>, D::Error> {
    <&serde_json::value::RawValue>::deserialize(deserializer).map(Some)
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetricContext<'a> {
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    cwd: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    model: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    permission_mode: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    turn_id: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    started_at_ms: Option<&'a serde_json::value::RawValue>,
}

/// Project fields with the JSON parser so whitespace, escaping and field order
/// cannot change the measurements. Large content/history fields are skipped
/// without constructing the runtime's UTF-16 value tree.
fn metric_projection(line: &str) -> Result<Option<String>, ()> {
    let envelope: MetricEnvelope<'_> = serde_json::from_str(line).map_err(|_| ())?;
    match envelope.kind.as_str() {
        "message" => {
            let message = envelope.message.ok_or(())?;
            if message.kind != "assistant" {
                return Ok(None);
            }
            if !message.content.get().starts_with('[') {
                return Err(());
            }
            let message = serde_json::to_string(&message).map_err(|_| ())?;
            Ok(Some(format!(
                "{{\"kind\":\"message\",\"message\":{message}}}"
            )))
        }
        "turn_context" => {
            let ctx = serde_json::to_string(&envelope.ctx.ok_or(())?).map_err(|_| ())?;
            Ok(Some(format!("{{\"kind\":\"turn_context\",\"ctx\":{ctx}}}")))
        }
        "event" if envelope.event.as_deref() == Some("turn_lifecycle") => {
            Ok(Some(line.to_string()))
        }
        "session_meta" | "usage_ledger" => Ok(Some(line.to_string())),
        _ => Ok(None),
    }
}

fn parse_metric_item(line: &str) -> Option<RolloutItem> {
    let mut value = runtime_json::parse(&RuntimeString::from(line)).ok()?;
    if value
        .get("kind")
        .and_then(RuntimeJson::as_str)
        .is_some_and(|kind| kind == "message")
    {
        if let Some(message) = value
            .get_mut("message")
            .and_then(RuntimeJson::as_object_mut)
        {
            if message
                .get("type")
                .and_then(RuntimeJson::as_str)
                .is_some_and(|kind| kind == "assistant")
                && !message.contains_key("model")
                && message.get("usage").is_none_or(RuntimeJson::is_null)
            {
                // Older unmetered messages omit the model. Retain their IDs for
                // clock ownership without adding usage or rewriting history.
                message.insert("model", RuntimeJson::String("".into()));
            }
        }
    }
    runtime_codec::decode_value(&value).ok()
}

async fn read_metric_items(path: &Path) -> io::Result<(Vec<RolloutItem>, bool)> {
    let file = crate::store::compression::open_async(path).await?;
    let length = file.metadata().await?.len();
    let mut reader = BufReader::with_capacity(1 << 20, file.take(length));
    let mut line = Vec::new();
    let mut items = Vec::new();
    let mut incomplete = false;
    while reader.read_until(b'\n', &mut line).await? != 0 {
        let text = match std::str::from_utf8(&line) {
            Ok(text) => std::borrow::Cow::Borrowed(text),
            Err(_) => {
                std::borrow::Cow::Owned(NodeBufferUtf8::decode(&line).try_to_utf8().map_err(
                    |error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()),
                )?)
            }
        };
        let text = text.trim();
        if !text.is_empty() {
            let projected = match metric_projection(text) {
                Ok(None) => {
                    line.clear();
                    continue;
                }
                Ok(Some(projected)) => projected,
                // The runtime codec accepts older UTF-16 string encodings
                // which a native JSON projection can reject.
                Err(()) => text.to_string(),
            };
            match parse_metric_item(&projected) {
                Some(RolloutItem::Message {
                    message: Message::Assistant(mut assistant),
                }) => {
                    assistant.content.clear();
                    items.push(RolloutItem::Message {
                        message: Message::Assistant(assistant),
                    });
                }
                Some(
                    item @ (RolloutItem::SessionMeta { .. }
                    | RolloutItem::TurnContext { .. }
                    | RolloutItem::UsageLedger(_)),
                ) => items.push(item),
                Some(RolloutItem::Event(event)) if event.event == "turn_lifecycle" => {
                    items.push(RolloutItem::Event(event));
                }
                Some(_) => {}
                None => incomplete = true,
            }
        }
        line.clear();
    }
    Ok((items, incomplete))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
    use crate::store::session_store::SessionStore;
    use serde_json::{json, Value};

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("profile-activity-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn millis(date: &str) -> i64 {
        NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis()
    }

    fn header(id: &str, fork: Option<&str>) -> Value {
        json!({"kind":"session_meta","schemaVersion":2,"meta":{
            "id":id,"createdAt":millis("2024-01-01"),"cwd":"/fixture","model":"fixture",
            "forkedFromId":fork
        }})
    }

    fn turn(id: &str, date: &str, duration: u64) -> Vec<Value> {
        vec![
            json!({"kind":"turn_context","ctx":{"cwd":"/fixture","model":"fixture",
                "permissionMode":"default","turnId":id,"startedAtMs":millis(date)}}),
            json!({"kind":"event","event":"turn_lifecycle","turnId":id,"payload":{
                "status":"completed","startedAtMs":millis(date),"durationMs":duration,
                "finishedAtMs":millis(date)+duration as i64}}),
        ]
    }

    fn usage(id: &str, tokens: u64) -> Value {
        json!({"kind":"usage_ledger","requestId":id,"assistantMessageId":id,"model":"fixture",
            "usage":{"inputTokens":tokens,"outputTokens":0,"cacheReadInputTokens":0,
            "cacheCreationInputTokens":0},"costUsd":0})
    }

    fn assistant(id: &str, tokens: u64) -> Value {
        json!({"kind":"message","message":{"type":"assistant","uuid":id,"model":"fixture",
            "content":[],"usage":{"inputTokens":tokens,"outputTokens":0,
            "cacheReadInputTokens":0,"cacheCreationInputTokens":0}}})
    }

    fn write(path: &Path, items: &[Value]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            path,
            items
                .iter()
                .map(|value| format!("{value}\n"))
                .collect::<String>(),
        )
        .unwrap();
    }

    fn store(root: &Path) -> JsonlSessionStore {
        JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(root.to_string_lossy().into_owned()),
            archive_dir: None,
        })
    }

    #[tokio::test]
    async fn profile_activity_counts_archives_and_deduplicates_forks_and_child_usage_read_only() {
        let root = TestDirectory::new();
        let main = root.path().join("1-main.jsonl");
        let mut items = vec![header("main", None)];
        items.extend(turn("old", "2024-01-01", 100));
        items.push(usage("old-request", 10));
        items.extend(turn("recent", "2026-10-17", 7_000));
        items.push(assistant("main-request", 900));
        items.push(usage("main-request", 20));
        items.push(usage("child-request", 30));
        write(&main, &items);
        let before = std::fs::read(&main).unwrap();
        let mut child = turn("child-turn", "2026-10-16", 3_000);
        child.push(assistant("child-request", 30));
        write(
            &root.path().join("1-main/subagents/agent-child.jsonl"),
            &child,
        );
        let mut archived = vec![header("archived", None)];
        archived.extend(turn("archive-turn", "2026-10-16", 500));
        archived.push(assistant("legacy", 40));
        write(&root.path().join("archived/2-archived.jsonl"), &archived);
        let mut fork = vec![header("fork", Some("main")), usage("inherited", 8_000)];
        fork.extend(turn("fork-turn", "2026-10-18", 200));
        fork.push(usage("fork-request", 5));
        write(&root.path().join("3-fork.jsonl"), &fork);
        let journal = root.path().join(".unarchive-journal/pending/intent.json");
        std::fs::create_dir_all(journal.parent().unwrap()).unwrap();
        std::fs::write(&journal, "unfinished transaction").unwrap();

        let result = store(root.path())
            .profile_activity(millis("2026-10-18"))
            .await
            .unwrap();
        assert_eq!(result.total_tokens, 105);
        assert_eq!(result.peak_daily_tokens, 70);
        assert_eq!(result.longest_task_ms, 7_000);
        assert_eq!(result.longest_streak_days, 3);
        assert_eq!(result.current_streak_days, 3);
        assert!(!result.incomplete, "{result:?}");
        assert_eq!(result.days.first().unwrap().date, "2024-01-01");
        assert_eq!(result.days.last().unwrap().date, "2026-10-18");
        assert_eq!(std::fs::read(&main).unwrap(), before);
        assert_eq!(
            std::fs::read_to_string(journal).unwrap(),
            "unfinished transaction"
        );
        assert!(!root.path().join("1-main.jsonl.lock").exists());
        assert!(root.path().join("archived/2-archived.jsonl").exists());
    }

    #[tokio::test]
    async fn profile_activity_preserves_known_totals_and_marks_missing_or_corrupt_timing() {
        let root = TestDirectory::new();
        let file = root.path().join("1-partial.jsonl");
        let mut items = vec![header("partial", None), usage("undated", 7)];
        items.extend(turn("known", "2026-10-17", 500));
        items.push(usage("known-request", 11));
        items.push(
            json!({"kind":"event","event":"turn_lifecycle","turnId":"bad", "payload":{
                "status":"completed","startedAtMs":-1,"durationMs":-100
            }}),
        );
        write(&file, &items);
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&file)
            .unwrap()
            .write_all(b"{\"kind\":\"usage_ledger\", truncated\n")
            .unwrap();
        let result = store(root.path())
            .profile_activity(millis("2026-10-18"))
            .await
            .unwrap();
        assert!(result.incomplete);
        assert_eq!(result.total_tokens, 18);
        assert_eq!(
            result.days,
            vec![ProfileActivityDay {
                date: "2026-10-17".into(),
                tokens: 11
            }]
        );
        assert_eq!(
            result.current_streak_days, 1,
            "yesterday keeps the active streak"
        );
        assert_eq!(result.longest_task_ms, 500);
        let expired = store(root.path())
            .profile_activity(millis("2026-10-19"))
            .await
            .unwrap();
        assert_eq!(expired.current_streak_days, 0);
    }

    #[tokio::test]
    async fn profile_activity_does_not_lend_parent_clocks_to_children_without_context() {
        let root = TestDirectory::new();
        let mut main = vec![header("main", None)];
        main.extend(turn("parent-turn", "2026-10-17", 900));
        main.push(assistant("parent-request", 10));
        main.push(usage("parent-request", 10));
        main.push(usage("child-message", 20));
        main.push(usage("child-ledger", 30));
        write(&root.path().join("1-main.jsonl"), &main);
        write(
            &root.path().join("1-main/subagents/agent-no-context.jsonl"),
            &[assistant("child-message", 20), usage("child-ledger", 30)],
        );
        let result = store(root.path())
            .profile_activity(millis("2026-10-18"))
            .await
            .unwrap();
        assert_eq!(result.total_tokens, 60);
        assert_eq!(
            result.days,
            vec![ProfileActivityDay {
                date: "2026-10-17".into(),
                tokens: 10
            }]
        );
        assert_eq!(result.peak_daily_tokens, 10);
        assert_eq!(result.longest_task_ms, 900);
        assert!(
            result.partial,
            "usage without any clock stays undated and explicit"
        );
        assert!(
            !result.incomplete,
            "missing clocks are a coverage gap, not a read failure: {result:?}"
        );
    }

    #[tokio::test]
    async fn profile_activity_serves_unchanged_transcripts_from_cache_and_rereads_appends() {
        let root = TestDirectory::new();
        let file = root.path().join("1-main.jsonl");
        let mut items = vec![header("main", None)];
        items.extend(turn("t1", "2026-10-17", 500));
        items.push(usage("r1", 10));
        write(&file, &items);
        let store = store(root.path());
        let first = store.profile_activity(millis("2026-10-18")).await.unwrap();
        assert_eq!(first.total_tokens, 10);
        let cache = std::fs::read(root.path().join(CACHE_FILE_NAME)).unwrap();
        assert!(String::from_utf8_lossy(&cache).contains("1-main.jsonl"));
        #[cfg(unix)]
        {
            // A cache hit must not reopen the transcript: revoke read access
            // and expect the cached numbers instead of a read failure.
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000)).unwrap();
            let cached = store.profile_activity(millis("2026-10-18")).await.unwrap();
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert_eq!(cached, first);
            assert!(!cached.incomplete);
        }
        let mut appended = items.clone();
        appended.extend(turn("t2", "2026-10-18", 700));
        appended.push(usage("r2", 5));
        write(&file, &appended);
        let second = store.profile_activity(millis("2026-10-18")).await.unwrap();
        assert_eq!(second.total_tokens, 15);
        assert_eq!(second.longest_task_ms, 700);
    }

    #[tokio::test]
    async fn profile_activity_falls_back_to_full_scan_when_cache_is_corrupt_or_foreign() {
        let root = TestDirectory::new();
        let file = root.path().join("1-main.jsonl");
        let mut items = vec![header("main", None)];
        items.extend(turn("t1", "2026-10-17", 500));
        items.push(usage("r1", 10));
        write(&file, &items);
        for poison in [
            b"{not json".to_vec(),
            b"{\"version\":999,\"files\":{}}".to_vec(),
        ] {
            std::fs::write(root.path().join(CACHE_FILE_NAME), &poison).unwrap();
            let store = store(root.path());
            let result = store.profile_activity(millis("2026-10-18")).await.unwrap();
            assert_eq!(result.total_tokens, 10);
            assert!(!result.incomplete);
        }
        let rebuilt = std::fs::read(root.path().join(CACHE_FILE_NAME)).unwrap();
        assert!(String::from_utf8_lossy(&rebuilt).contains("1-main.jsonl"));
    }

    #[tokio::test]
    async fn profile_activity_reconciles_child_only_changes_and_removals() {
        let root = TestDirectory::new();
        let file = root.path().join("1-main.jsonl");
        let mut items = vec![header("main", None)];
        items.extend(turn("t1", "2026-10-17", 500));
        items.push(usage("r1", 10));
        write(&file, &items);
        let store = store(root.path());
        assert_eq!(
            store
                .profile_activity(millis("2026-10-18"))
                .await
                .unwrap()
                .total_tokens,
            10
        );
        let child = root.path().join("1-main/subagents/child.jsonl");
        let mut child_items = turn("child", "2026-10-18", 800);
        child_items.push(assistant("child-request", 20));
        write(&child, &child_items);
        let result = store.profile_activity(millis("2026-10-18")).await.unwrap();
        assert_eq!(result.total_tokens, 30);
        assert_eq!(result.longest_task_ms, 800);
        std::fs::remove_file(child).unwrap();
        assert_eq!(
            store
                .profile_activity(millis("2026-10-18"))
                .await
                .unwrap()
                .total_tokens,
            10
        );
        std::fs::remove_file(file).unwrap();
        assert_eq!(
            store.profile_activity(millis("2026-10-18")).await.unwrap(),
            ProfileActivity::default()
        );
    }

    #[tokio::test]
    async fn profile_activity_keeps_deferred_cache_writes_dirty_until_saved() {
        let root = TestDirectory::new();
        let file = root.path().join("1-main.jsonl");
        let mut items = vec![header("main", None)];
        items.extend(turn("t1", "2026-10-17", 500));
        items.push(usage("r1", 10));
        write(&file, &items);
        let mut memory = MemoryCache::default();
        let roots = [root.path()];
        read_profile_activity(&mut memory, &roots, millis("2026-10-18"))
            .await
            .unwrap();
        let saved = std::fs::read(root.path().join(CACHE_FILE_NAME)).unwrap();
        items.push(usage("r2", 5));
        write(&file, &items);
        read_profile_activity(&mut memory, &roots, millis("2026-10-18"))
            .await
            .unwrap();
        assert!(memory.dirty);
        assert_eq!(
            std::fs::read(root.path().join(CACHE_FILE_NAME)).unwrap(),
            saved
        );
        memory.last_save = Some(std::time::Instant::now() - SAVE_THROTTLE);
        let result = read_profile_activity(&mut memory, &roots, millis("2026-10-18"))
            .await
            .unwrap();
        assert_eq!(result.total_tokens, 15);
        assert!(!memory.dirty);
        assert_ne!(
            std::fs::read(root.path().join(CACHE_FILE_NAME)).unwrap(),
            saved
        );
    }

    #[tokio::test]
    async fn profile_activity_projects_reordered_fields_and_preserves_utf16_ids() {
        let root = TestDirectory::new();
        let file = root.path().join("1-main.jsonl");
        let mut items = vec![header("main", None)];
        items.extend(turn("t1", "2026-10-17", 500));
        write(&file, &items);
        use std::io::Write;
        let mut output = std::fs::OpenOptions::new()
            .append(true)
            .open(&file)
            .unwrap();
        output.write_all(br#"{"message":{"usage":{"inputTokens":10,"outputTokens":0,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"content":[{"type":"text","text":"nested \"model\": \"wrong\""}],"model":"fixture","uuid":"reordered","type" : "assistant"},"kind":"message"}
{"kind":"usage_ledger","requestId":"\ud800","assistantMessageId":"ledger","model":"fixture","usage":{"inputTokens":5,"outputTokens":0,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"costUsd":0}
"#).unwrap();
        let store = store(root.path());
        let first = store.profile_activity(millis("2026-10-18")).await.unwrap();
        assert_eq!(first.total_tokens, 15);
        assert!(!first.incomplete, "{first:?}");
        assert_eq!(
            store.profile_activity(millis("2026-10-18")).await.unwrap(),
            first
        );
    }

    #[tokio::test]
    #[ignore = "manual: measure against a real sessions root"]
    async fn measure_real_sessions_root() {
        let Ok(root) = std::env::var("KLAUS_PROFILE_MEASURE_ROOT") else {
            eprintln!("MEASURE skipped: set KLAUS_PROFILE_MEASURE_ROOT");
            return;
        };
        let root = PathBuf::from(root);
        let archive = root.join("archived");
        let roots: Vec<&Path> = vec![root.as_path(), archive.as_path()];
        let now = chrono::Utc::now().timestamp_millis();
        let mut memory = MemoryCache::default();
        let started = std::time::Instant::now();
        let cold = read_profile_activity(&mut memory, &roots, now)
            .await
            .unwrap();
        let cold_elapsed = started.elapsed();
        let started = std::time::Instant::now();
        let warm = read_profile_activity(&mut memory, &roots, now)
            .await
            .unwrap();
        let warm_elapsed = started.elapsed();
        let cache_len = std::fs::metadata(root.join(CACHE_FILE_NAME))
            .map(|m| m.len())
            .unwrap_or(0);
        println!(
            "MEASURE cold={cold_elapsed:?} cold_tokens={} warm={warm_elapsed:?} warm_tokens={} cache_bytes={cache_len} days={} incomplete={}",
            cold.total_tokens,
            warm.total_tokens,
            cold.days.len(),
            cold.incomplete
        );
    }

    #[tokio::test]
    async fn profile_activity_handles_empty_history_and_limits_unsafe_usage_totals() {
        let root = TestDirectory::new();
        let store = store(root.path());
        assert_eq!(
            store.profile_activity(millis("2026-10-18")).await.unwrap(),
            ProfileActivity::default()
        );
        let mut items = vec![header("large", None)];
        items.extend(turn("large-turn", "2026-10-18", 1));
        items.push(usage("unrepresentable", u64::MAX));
        items.push(usage("large", MAX_SAFE_INTEGER));
        for index in 0..2_100 {
            items.push(assistant(
                &format!("large-legacy-{index}"),
                MAX_SAFE_INTEGER,
            ));
        }
        write(&root.path().join("1-large.jsonl"), &items);
        let result = store.profile_activity(millis("2026-10-18")).await.unwrap();
        assert!(result.incomplete);
        assert_eq!(result.total_tokens, MAX_SAFE_INTEGER);
        assert_eq!(result.peak_daily_tokens, MAX_SAFE_INTEGER);
        assert_eq!(result.days[0].tokens, MAX_SAFE_INTEGER);
    }
}
