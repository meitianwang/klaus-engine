//! Filesystem-backed store of typed memories.

use crate::fs::paths::config_home;
use crate::memory::memory_age::{memory_section, MemorySectionInput};
use crate::memory::types::{MemoryRecord, MemoryType};
use crate::runtime_values::{
    runtime_codec, NodeBufferUtf8, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder,
    RuntimeJson, RuntimeObjectEncoder, RuntimePath, RuntimeString,
};
use crate::schemas::content::{ContentBlock, TextBlock};
use crate::schemas::message::{Message, UserMessage};
use crate::secrets::sanitizer::sanitize_secrets;
use crate::types::abort::AbortSignal;
use crate::types::provider::{CallModel, ModelRequest, ModelStreamEvent, SystemPromptPart};
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use tokio::fs;

fn runtime_memory_path_to_fs_host(path: &RuntimePath) -> String {
    String::from_utf8(path.as_runtime_string().to_node_utf8())
        .expect("Node UTF-8 memory path projection is valid UTF-8")
}

fn runtime_memory_key_to_host(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8())
        .expect("Node UTF-8 memory selector projection is valid UTF-8")
}

fn runtime_memory_secret_scan_to_host(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8())
        .expect("Node UTF-8 memory secret-scan projection is valid UTF-8")
}

fn runtime_memory_file_text_to_scalar(value: &RuntimeString) -> String {
    value
        .try_to_utf8()
        .expect("Node Buffer UTF-8 memory file decoding always produces scalar text")
}

fn lowercase_runtime(value: &RuntimeString) -> RuntimeString {
    let mut units = Vec::with_capacity(value.len_utf16());
    for point in value.code_points() {
        if let Some(character) = char::from_u32(point) {
            for lower in character.to_lowercase() {
                let mut encoded = [0; 2];
                units.extend_from_slice(lower.encode_utf16(&mut encoded));
            }
        } else {
            units.push(point as u16);
        }
    }
    RuntimeString::from_utf16(units)
}

fn tokenize_runtime(value: &RuntimeString) -> Vec<Vec<u16>> {
    let lower = lowercase_runtime(value);
    let mut tokens = Vec::new();
    let mut current = Vec::new();
    for unit in lower.as_utf16().iter().copied() {
        if (b'a' as u16..=b'z' as u16).contains(&unit)
            || (b'0' as u16..=b'9' as u16).contains(&unit)
        {
            current.push(unit);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn count_non_overlapping(haystack: &[u16], needle: &[u16]) -> usize {
    let mut count = 0;
    let mut from = 0;
    while needle.len() <= haystack.len().saturating_sub(from) {
        if haystack[from..].starts_with(needle) {
            count += 1;
            from += needle.len();
        } else {
            from += 1;
        }
    }
    count
}

fn is_ecmascript_whitespace(unit: u16) -> bool {
    matches!(
        unit,
        0x0009..=0x000d
            | 0x0020
            | 0x00a0
            | 0x1680
            | 0x2000..=0x200a
            | 0x2028
            | 0x2029
            | 0x202f
            | 0x205f
            | 0x3000
            | 0xfeff
    )
}

fn trim_runtime(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_ecmascript_whitespace(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_ecmascript_whitespace(*unit))
        .map_or(start, |index| index + 1);
    value.slice_utf16(start..end)
}

fn parse_wanted_names(value: &RuntimeString) -> Vec<RuntimeString> {
    let mut names = Vec::new();
    let mut start = 0;
    for (index, unit) in value.as_utf16().iter().copied().enumerate() {
        if unit != b',' as u16 && unit != b'\n' as u16 {
            continue;
        }
        let part = trim_runtime(&value.slice_utf16(start..index));
        let part = if matches!(part.as_utf16().first(), Some(unit) if *unit == b'-' as u16 || *unit == b'*' as u16)
        {
            trim_runtime(&part.slice_utf16(1..))
        } else {
            part
        };
        if !part.is_empty() {
            names.push(lowercase_runtime(&part));
        }
        start = index + 1;
    }
    let part = trim_runtime(&value.slice_utf16(start..));
    let part = if matches!(part.as_utf16().first(), Some(unit) if *unit == b'-' as u16 || *unit == b'*' as u16)
    {
        trim_runtime(&part.slice_utf16(1..))
    } else {
        part
    };
    if !part.is_empty() {
        names.push(lowercase_runtime(&part));
    }
    names
}

fn join_exact_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    values
        .iter()
        .enumerate()
        .fold(RuntimeString::default(), |output, (index, value)| {
            let output = if index == 0 {
                output
            } else {
                output.concat(&separator)
            };
            output.concat(value)
        })
}

fn parse_memory_type(v: &str) -> Option<MemoryType> {
    match v {
        "user" => Some(MemoryType::User),
        "feedback" => Some(MemoryType::Feedback),
        "project" => Some(MemoryType::Project),
        "reference" => Some(MemoryType::Reference),
        _ => None,
    }
}

/// A record to persist: a [`MemoryRecord`] without the store-populated `path`
/// and `mtime_ms` fields.
#[derive(Debug, Clone, PartialEq)]
pub struct NewMemoryRecord {
    pub name: RuntimeString,
    pub description: RuntimeString,
    pub memory_type: MemoryType,
    pub body: RuntimeString,
}

impl RuntimeEncode for NewMemoryRecord {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("description", &self.description)?;
        object.field("type", &self.memory_type)?;
        object.field("body", &self.body)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for NewMemoryRecord {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                description: object.required("description", &[])?,
                memory_type: object.required("type", &[])?,
                body: object.required("body", &[])?,
            })
        })
    }
}

/// Parsed frontmatter: a flat string map plus the markdown body that follows it.
struct Parsed {
    fields: HashMap<String, String>,
    body: String,
}

/// Minimal frontmatter reader (no YAML dependency). Recognizes a leading
/// `---\n...\n---\n` block, parses each `key: value` line (stripping matching
/// surrounding quotes from the value), and treats everything after the closing
/// fence as the body. Files without a leading fence are all body, no fields.
fn parse_frontmatter(raw: &str) -> Parsed {
    let text = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    if !text.starts_with("---") {
        return Parsed {
            fields: HashMap::new(),
            body: text.to_string(),
        };
    }

    // Locate the line-delimited opening and closing fences.
    let lines: Vec<&str> = text.split('\n').collect();
    if lines.first().map(|l| l.trim()) != Some("---") {
        return Parsed {
            fields: HashMap::new(),
            body: text.to_string(),
        };
    }

    let mut close_idx: Option<usize> = None;
    for (i, line) in lines.iter().enumerate().skip(1) {
        if line.trim() == "---" {
            close_idx = Some(i);
            break;
        }
    }
    let Some(close_idx) = close_idx else {
        return Parsed {
            fields: HashMap::new(),
            body: text.to_string(),
        };
    };

    let mut fields: HashMap<String, String> = HashMap::new();
    for line in &lines[1..close_idx] {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some(colon) = trimmed.find(':') else {
            continue;
        };
        let key = trimmed[..colon].trim();
        if key.is_empty() {
            continue;
        }
        fields.insert(
            key.to_string(),
            strip_quotes(trimmed[colon + 1..].trim()).to_string(),
        );
    }

    let joined = lines[close_idx + 1..].join("\n");
    let body = joined.strip_prefix('\n').unwrap_or(&joined).to_string();
    Parsed { fields, body }
}

fn strip_quotes(v: &str) -> &str {
    let bytes = v.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return &v[1..v.len() - 1];
        }
    }
    v
}

fn serialize_frontmatter(rec: &NewMemoryRecord) -> RuntimeString {
    RuntimeString::from("---\nname: ")
        .concat(&rec.name)
        .concat(&RuntimeString::from("\ndescription: "))
        .concat(&rec.description)
        .concat(&RuntimeString::from("\ntype: "))
        .concat(&RuntimeString::from(rec.memory_type.as_str()))
        .concat(&RuntimeString::from("\n---\n"))
        .concat(&rec.body)
        .concat(&RuntimeString::from("\n"))
}

/// Options for [`TypedMemoryStore::new`].
#[derive(Debug, Clone, Default)]
pub struct TypedMemoryStoreOptions {
    pub dir: Option<RuntimePath>,
    pub redact_secrets: Option<bool>,
}

/// Options for [`TypedMemoryStore::recall_semantic`].
pub struct RecallSemanticOptions<'a> {
    pub call_model: &'a CallModel,
    pub model: RuntimeString,
    pub limit: Option<usize>,
    pub recent_tools: Option<&'a [RuntimeString]>,
    /// Names to omit from the catalog BEFORE selection (so the
    /// selector's budget is never spent re-picking memories shown earlier this session).
    pub exclude: Option<&'a [RuntimeString]>,
    /// Abort signal for the side-query; cancels the recall when the turn aborts.
    pub signal: Option<AbortSignal>,
}

impl std::fmt::Debug for RecallSemanticOptions<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecallSemanticOptions")
            .field("model", &self.model)
            .field("limit", &self.limit)
            .field("recent_tools", &self.recent_tools)
            .field("exclude", &self.exclude)
            .finish_non_exhaustive()
    }
}

/// Filesystem-backed store of typed memories, one markdown file per record. The
/// directory is created lazily on `write`; reads tolerate its absence.
#[derive(Debug, Clone)]
pub struct TypedMemoryStore {
    dir: RuntimePath,
    redact_secrets: bool,
}

impl TypedMemoryStore {
    pub fn new(opts: TypedMemoryStoreOptions) -> Self {
        let dir = opts.dir.unwrap_or_else(|| {
            RuntimePath::from(
                Path::new(&config_home())
                    .join("memory")
                    .to_string_lossy()
                    .into_owned(),
            )
        });
        // Redact secrets from persisted memory bodies by default (opt-out for hosts that need raw text).
        let redact_secrets = opts.redact_secrets.unwrap_or(true);
        TypedMemoryStore {
            dir,
            redact_secrets,
        }
    }

    /// The directory this store reads from and writes to.
    pub fn dir(&self) -> &RuntimePath {
        &self.dir
    }

    /// Read and parse every `*.md` file in the directory, NEWEST-FIRST by mtime
    /// (recency is a first-class signal). Each record carries
    /// its file mtime for freshness surfacing. Missing fields fall back to sensible
    /// defaults (name from filename, type `reference`). A missing directory yields an
    /// empty list rather than erroring.
    pub async fn list(&self) -> io::Result<Vec<MemoryRecord>> {
        let dir = runtime_memory_path_to_fs_host(&self.dir);
        let mut read_dir = match fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };
        let mut entries: Vec<String> = Vec::new();
        while let Some(entry) = read_dir.next_entry().await? {
            entries.push(entry.file_name().to_string_lossy().into_owned());
        }

        let mut records: Vec<MemoryRecord> = Vec::new();
        for entry in entries {
            if !entry.ends_with(".md") {
                continue;
            }
            let path = Path::new(&dir).join(&entry).to_string_lossy().into_owned();
            // Read the file and stat it in parallel (two syscalls) to get the
            // body and its mtime together.
            let (raw_res, stat_res) = tokio::join!(fs::read(&path), fs::metadata(&path));
            let raw = runtime_memory_file_text_to_scalar(&NodeBufferUtf8::decode(&raw_res?));
            let stat = stat_res?;
            let Parsed { fields, body } = parse_frontmatter(&raw);
            // The filename minus its `.md` extension.
            let fallback_name = entry.strip_suffix(".md").unwrap_or(&entry).to_string();
            let mtime_ms = stat
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            records.push(MemoryRecord {
                name: fields.get("name").cloned().unwrap_or(fallback_name).into(),
                description: fields
                    .get("description")
                    .cloned()
                    .unwrap_or_default()
                    .into(),
                memory_type: fields
                    .get("type")
                    .and_then(|t| parse_memory_type(t))
                    .unwrap_or(MemoryType::Reference),
                body: body.into(),
                path: path.into(),
                mtime_ms,
            });
        }
        records.sort_by(|a, b| b.mtime_ms.cmp(&a.mtime_ms));
        Ok(records)
    }

    /// Persist a record as `<dir>/<name>.md`, creating the directory if needed.
    /// Overwrites any existing file with the same name.
    pub async fn write(&self, rec: &NewMemoryRecord) -> io::Result<()> {
        let dir = runtime_memory_path_to_fs_host(&self.dir);
        fs::create_dir_all(&dir).await?;
        let path = Path::new(&dir).join(format!("{}.md", runtime_memory_key_to_host(&rec.name)));
        // Redact detected secrets from the persisted body AND description so a leaked key can't survive
        // across sessions and re-enter the model's context on reload (both are model-visible; see
        // secrets/sanitizer.rs for scope). `name` is the filename, left as-is.
        let clean = if self.redact_secrets {
            NewMemoryRecord {
                name: rec.name.clone(),
                description: sanitize_secrets(&runtime_memory_secret_scan_to_host(
                    &rec.description,
                ))
                .into(),
                memory_type: rec.memory_type,
                body: sanitize_secrets(&runtime_memory_secret_scan_to_host(&rec.body)).into(),
            }
        } else {
            rec.clone()
        };
        fs::write(&path, serialize_frontmatter(&clean).to_node_utf8()).await
    }

    /// Rank stored memories against a free-text `query` by keyword frequency. The
    /// query is lowercased and tokenized; each record is scored by the total number
    /// of token occurrences across its name, description, and body. Returns the
    /// top `limit` (default 5) records with a non-zero score, most relevant first.
    /// For semantic matching beyond literal keywords, see
    /// [`TypedMemoryStore::recall_semantic`].
    pub async fn recall(&self, query: &str, limit: Option<usize>) -> io::Result<Vec<MemoryRecord>> {
        let limit = limit.unwrap_or(5);
        let tokens = tokenize_runtime(&RuntimeString::from(query));
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let records = self.list().await?;
        let mut scored: Vec<(MemoryRecord, usize)> = Vec::new();
        for record in records {
            let haystack = lowercase_runtime(
                &record
                    .name
                    .concat(&RuntimeString::from(" "))
                    .concat(&record.description)
                    .concat(&RuntimeString::from(" "))
                    .concat(&record.body),
            );
            let score = tokens
                .iter()
                .map(|token| count_non_overlapping(haystack.as_utf16(), token))
                .sum();
            if score > 0 {
                scored.push((record, score));
            }
        }

        scored.sort_by(|a, b| b.1.cmp(&a.1));
        Ok(scored.into_iter().take(limit).map(|s| s.0).collect())
    }

    /// LLM-based side-query recall: a small model picks the most relevant memories
    /// by NAME from the catalog, capturing semantic matches keyword scoring misses.
    /// Falls back to [`TypedMemoryStore::recall`] if the model returns nothing
    /// usable. The model call is cheap (names + descriptions only, no bodies).
    ///
    /// TOOL-AWARE: when `recent_tools` is supplied, the selector is
    /// told to skip usage/API-reference memories for tools the agent is already
    /// exercising (their docs are noise mid-use) while still surfacing warnings/gotchas
    /// about them (active use is exactly when those matter) — countering the false
    /// positive where a query keyword overlaps an unrelated tool-reference memory.
    pub async fn recall_semantic(
        &self,
        query: &RuntimeString,
        opts: RecallSemanticOptions<'_>,
    ) -> io::Result<Vec<MemoryRecord>> {
        let limit = opts.limit.unwrap_or(5);
        let all = self.list().await?;
        let excluded: HashSet<RuntimeString> = opts
            .exclude
            .unwrap_or(&[])
            .iter()
            .map(lowercase_runtime)
            .collect();
        let records: Vec<MemoryRecord> = if !excluded.is_empty() {
            all.into_iter()
                .filter(|r| !excluded.contains(&lowercase_runtime(&r.name)))
                .collect()
        } else {
            all
        };
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let catalog = records
            .iter()
            .map(|record| {
                RuntimeString::from("- ")
                    .concat(&record.name)
                    .concat(&RuntimeString::from(": "))
                    .concat(&record.description)
            })
            .collect::<Vec<_>>();
        let catalog = join_exact_runtime_strings(&catalog, "\n");
        let tools_section = match opts.recent_tools {
            Some(tools) if !tools.is_empty() => RuntimeString::from("\n\nRecently used tools: ")
                .concat(&join_exact_runtime_strings(tools, ", ")),
            _ => RuntimeString::default(),
        };
        let prompt = RuntimeString::from("Query: ")
            .concat(query)
            .concat(&RuntimeString::from("\n\nMemories:\n"))
            .concat(&catalog)
            .concat(&tools_section);
        let req = ModelRequest {
            supports_image_detail_original: false,
            model_verbosity: None,
            reasoning_summary: Default::default(),
            model: opts.model.clone(),
            image_limits: None,
            max_media_blocks: None,
            system: vec![SystemPromptPart {
                name: "recall".into(),
                text: "You select which stored memories are relevant to a query. Be selective and \
                       discerning — if a memory is not clearly useful, leave it out; an empty list is \
                       fine. If a list of recently-used tools is given, do NOT select usage-reference \
                       or API-documentation memories for those tools (the agent is already using them), \
                       but DO still select memories containing warnings, gotchas, or known issues about \
                       them. Reply with ONLY a comma-separated list of memory names (no prose), most \
                       relevant first."
                    .into(),
                cacheable: None,
                cache_scope: None,
            }],
            messages: vec![Message::User(UserMessage {
            question_answer: None,
                uuid: "recall".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: prompt,
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            })],
            tools: Vec::new(),
            native_deferred_tools: None,
            deferred_tools: Vec::new(),
            tool_choice: None,
            max_output_tokens: 256,
            task_budget: None,
            output_schema: None,
            web_search: None,
            thinking: None,
            effort: None,
            skip_cache_write: None,
            cache_ttl: None,
            cache_domain: None,
            cache_key: None,
            context_management: None,
            context_hints: None,
            thinking_context: None,
        };

        let mut out = crate::runtime_values::RuntimeString::default();
        // Distinguish a genuine FAILURE (stream error) from a successful empty/garbage
        // selection. The selector is told an empty list is acceptable, so a deliberate
        // "nothing relevant" must be RESPECTED — only a failure falls
        // back to keyword recall, never a clean empty answer.
        let mut failed = false;
        // The model call needs a signal; use the caller's turn signal when given so an
        // aborted turn cancels the side-query, else a dummy that is never aborted.
        let signal = opts.signal.clone().unwrap_or_default();
        let mut stream = (opts.call_model)(req, signal);
        while let Some(ev) = stream.next().await {
            match ev {
                ModelStreamEvent::TextDelta { text } => out = out.concat(&text),
                ModelStreamEvent::Error { .. } => {
                    failed = true;
                    break;
                }
                ModelStreamEvent::MessageStop { .. } => break,
                _ => {}
            }
        }
        if failed {
            let query = runtime_memory_key_to_host(query);
            return self.recall(&query, Some(limit)).await;
        }

        let wanted = parse_wanted_names(&out);
        let mut by_name: HashMap<RuntimeString, usize> = HashMap::new();
        for (i, r) in records.iter().enumerate() {
            by_name.insert(lowercase_runtime(&r.name), i);
        }
        let mut picked: Vec<usize> = Vec::new();
        for w in &wanted {
            if let Some(&i) = by_name.get(w) {
                if !picked.contains(&i) {
                    picked.push(i);
                }
            }
            if picked.len() >= limit {
                break;
            }
        }
        // A successful selection is authoritative, including an empty one (the model
        // judged nothing relevant). Keyword recall is reserved for the failure path above.
        Ok(picked.into_iter().map(|i| records[i].clone()).collect())
    }

    /// Project relevant memories into a single [`SystemPromptPart`], each tagged with
    /// its FRESHNESS: a memory older than a day carries the staleness
    /// caveat before its heading; a fresh one just notes "(saved today/yesterday)". When a
    /// `query` is supplied the records come from [`TypedMemoryStore::recall`]; otherwise
    /// all memories are included via [`TypedMemoryStore::list`]. `now_ms` is the reference
    /// time for age (the engine's `now()` seam). Returns an empty vec when there is
    /// nothing to inject so callers can splice the result unconditionally.
    pub async fn to_system_prompt_parts(
        &self,
        query: Option<&str>,
        now_ms: i64,
    ) -> io::Result<Vec<SystemPromptPart>> {
        let records = match query {
            Some(q) => self.recall(q, None).await?,
            None => self.list().await?,
        };
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let sections: Vec<RuntimeString> = records
            .iter()
            .map(|r| {
                memory_section(
                    &MemorySectionInput {
                        name: r.name.clone(),
                        description: Some(r.description.clone()),
                        body: r.body.clone(),
                        mtime_ms: r.mtime_ms,
                    },
                    now_ms,
                )
            })
            .collect();
        let text = RuntimeString::from("Relevant memories:\n\n")
            .concat(&join_exact_runtime_strings(&sections, "\n\n"));
        Ok(vec![SystemPromptPart {
            name: "memory".into(),
            text,
            cacheable: None,
            cache_scope: None,
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        dir: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let base = std::env::temp_dir();
            let unique = format!(
                "mem-store-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
            );
            let dir = base.join(unique);
            std::fs::create_dir_all(&dir).unwrap();
            TempDir { dir }
        }

        fn path(&self) -> String {
            self.dir.to_string_lossy().into_owned()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    // TypedMemoryStore secret redaction on write
    #[tokio::test]
    async fn redacts_secrets_from_the_persisted_body_by_default() {
        let tmp = TempDir::new();
        let store = TypedMemoryStore::new(TypedMemoryStoreOptions {
            dir: Some(tmp.path().into()),
            redact_secrets: None,
        });
        store
            .write(&NewMemoryRecord {
                name: "creds".into(),
                description: "a fact".into(),
                memory_type: MemoryType::Reference,
                body: "the api_key=supersecret123 and token: \"abcdef123456\"".into(),
            })
            .await
            .unwrap();
        let raw = std::fs::read_to_string(tmp.dir.join("creds.md")).unwrap();
        assert!(raw.contains("[REDACTED]"));
        assert!(!raw.contains("supersecret123"));
        assert!(!raw.contains("abcdef123456"));
    }

    #[tokio::test]
    async fn leaves_the_body_verbatim_when_redact_secrets_is_off() {
        let tmp = TempDir::new();
        let store = TypedMemoryStore::new(TypedMemoryStoreOptions {
            dir: Some(tmp.path().into()),
            redact_secrets: Some(false),
        });
        store
            .write(&NewMemoryRecord {
                name: "creds".into(),
                description: "a fact".into(),
                memory_type: MemoryType::Reference,
                body: "api_key=supersecret123".into(),
            })
            .await
            .unwrap();
        let raw = std::fs::read_to_string(tmp.dir.join("creds.md")).unwrap();
        assert!(raw.contains("supersecret123"));
    }

    #[test]
    fn new_memory_record_codec_preserves_order_and_non_scalar_utf16() {
        let record = NewMemoryRecord {
            name: RuntimeString::from_utf16([b'n' as u16, 0xd800]),
            description: "description".into(),
            memory_type: MemoryType::Project,
            body: RuntimeString::from_utf16([0xdc00]),
        };
        let encoded = runtime_codec::encode_text(&record).expect("encode exact new memory");
        assert_eq!(
            encoded,
            RuntimeString::from(
                r#"{"name":"n\ud800","description":"description","type":"project","body":"\udc00"}"#,
            ),
        );
        assert_eq!(
            runtime_codec::decode_text::<NewMemoryRecord>(&encoded)
                .expect("decode exact new memory"),
            record,
        );
    }
}
