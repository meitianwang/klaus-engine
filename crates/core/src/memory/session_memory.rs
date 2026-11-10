//! Per-session automatic running notes (`summary.md`) and session-memory-backed
//! context compaction.
//!
//! As a long conversation progresses and accumulates tool calls, a background
//! maintenance fork periodically updates a structured markdown notes file for
//! the current session. When context compaction triggers, the compactor can
//! reuse these structured session notes directly alongside recent unsummarized
//! messages instead of issuing a cold full-history summarization call.

use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use crate::context::tokens::estimate_messages_tokens;
use crate::fs::paths::config_home;
use crate::runtime_values::RuntimeString;
use crate::schemas::content::ContentBlock;
use crate::schemas::message::Message;

/// Maximum tokens per section in session memory before condensation reminders
/// or compaction-time truncation apply.
pub const MAX_SECTION_LENGTH: usize = 2_000;

/// Maximum total token budget for the session memory file before a critical
/// condensation reminder is appended to the update prompt.
pub const MAX_TOTAL_SESSION_MEMORY_TOKENS: usize = 12_000;

/// Maximum time compaction waits for an in-flight session-memory update.
pub const EXTRACTION_WAIT_TIMEOUT_MS: u64 = 15_000;

/// Threshold beyond which an in-flight extraction is considered stale and not
/// waited on during compaction.
pub const EXTRACTION_STALE_THRESHOLD_MS: i64 = 60_000;

/// Default structured template for per-session running notes.
pub const DEFAULT_SESSION_MEMORY_TEMPLATE: &str = "\n\
# Session Title\n\
_A short and distinctive 5-10 word descriptive title for the session. Super info dense, no filler_\n\
\n\
# Current State\n\
_What is actively being worked on right now? Pending tasks not yet completed. Immediate next steps._\n\
\n\
# Task specification\n\
_What did the user ask to build? Any design decisions or other explanatory context_\n\
\n\
# Files and Functions\n\
_What are the important files? In short, what do they contain and why are they relevant?_\n\
\n\
# Workflow\n\
_What bash commands are usually run and in what order? How to interpret their output if not obvious?_\n\
\n\
# Errors & Corrections\n\
_Errors encountered and how they were fixed. What did the user correct? What approaches failed and should not be tried again?_\n\
\n\
# Codebase and System Documentation\n\
_What are the important system components? How do they work/fit together?_\n\
\n\
# Learnings\n\
_What has worked well? What has not? What to avoid? Do not duplicate items from other sections_\n\
\n\
# Key results\n\
_If the user asked a specific output such as an answer to a question, a table, or other document, repeat the exact result here_\n\
\n\
# Worklog\n\
_Step by step, what was attempted, done? Very terse summary for each step_\n";

/// Threshold configuration for triggering background session-memory updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionMemoryConfig {
    /// Minimum context window tokens before initializing session memory.
    pub minimum_message_tokens_to_init: u64,
    /// Minimum context window growth (in tokens) between session memory updates.
    pub minimum_tokens_between_update: u64,
    /// Number of tool calls between session memory updates.
    pub tool_calls_between_updates: usize,
}

impl Default for SessionMemoryConfig {
    fn default() -> Self {
        Self {
            minimum_message_tokens_to_init: 10_000,
            minimum_tokens_between_update: 5_000,
            tool_calls_between_updates: 3,
        }
    }
}

/// Threshold configuration for preserving recent messages during
/// session-memory-backed compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionMemoryCompactConfig {
    /// Minimum tokens of recent messages to preserve after compaction.
    pub min_tokens: u64,
    /// Minimum number of messages containing text blocks to preserve.
    pub min_text_block_messages: usize,
    /// Maximum tokens of recent messages to preserve (hard cap).
    pub max_tokens: u64,
}

impl Default for SessionMemoryCompactConfig {
    fn default() -> Self {
        Self {
            min_tokens: 10_000,
            min_text_block_messages: 5,
            max_tokens: 40_000,
        }
    }
}

/// Resolve the directory holding session-memory artifacts for `session_id`
/// inside `main_memory_dir`.
///
/// Stored under a hidden `.session-memory/<session_id>` subdirectory of the
/// session's pinned memory root so the restricted maintenance fork can edit it
/// under the existing pinned-root capability while keeping it invisible to
/// top-level `*.md` topic scans and cross-session consolidation.
pub fn session_memory_dir(main_memory_dir: &str, session_id: &str) -> PathBuf {
    let safe_id: String = session_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let folder = if safe_id.is_empty() {
        "default".to_string()
    } else {
        safe_id
    };
    Path::new(main_memory_dir)
        .join(".session-memory")
        .join(folder)
}

/// Resolve the path to `summary.md` for `session_id` inside `main_memory_dir`.
pub fn session_memory_path(main_memory_dir: &str, session_id: &str) -> PathBuf {
    session_memory_dir(main_memory_dir, session_id).join("summary.md")
}

/// Load a custom session-memory template from `<config_home>/session-memory/config/template.md`
/// if present, falling back to [`DEFAULT_SESSION_MEMORY_TEMPLATE`].
pub async fn load_session_memory_template() -> String {
    let custom_path = Path::new(&config_home())
        .join("session-memory")
        .join("config")
        .join("template.md");
    match tokio::fs::read_to_string(&custom_path).await {
        Ok(content) if !content.trim().is_empty() => content,
        _ => DEFAULT_SESSION_MEMORY_TEMPLATE.to_string(),
    }
}

/// Load a custom session-memory update prompt from `<config_home>/session-memory/config/prompt.md`
/// if present, falling back to [`default_update_prompt`].
pub async fn load_session_memory_prompt() -> String {
    let custom_path = Path::new(&config_home())
        .join("session-memory")
        .join("config")
        .join("prompt.md");
    match tokio::fs::read_to_string(&custom_path).await {
        Ok(content) if !content.trim().is_empty() => content,
        _ => default_update_prompt(),
    }
}

/// Ensure the session memory file exists (initializing it with the template on
/// first creation) and return its path alongside its current text content.
pub async fn setup_session_memory_file(
    main_memory_dir: &str,
    session_id: &str,
) -> io::Result<(PathBuf, String)> {
    let dir = session_memory_dir(main_memory_dir, session_id);
    tokio::fs::create_dir_all(&dir).await?;
    let path = dir.join("summary.md");
    if !path.exists() {
        let template = load_session_memory_template().await;
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(mut file) => {
                use tokio::io::AsyncWriteExt;
                file.write_all(template.as_bytes()).await?;
                file.flush().await?;
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }
    }
    let current = tokio::fs::read_to_string(&path).await?;
    Ok((path, current))
}

/// Rough token estimation (~4 bytes per token).
pub fn rough_token_count_estimation(text: &str) -> usize {
    text.len().div_ceil(4)
}

/// Default prompt instructing the maintenance fork to update the structured
/// session notes file in place.
pub fn default_update_prompt() -> String {
    format!(
        "IMPORTANT: This message and these instructions are NOT part of the actual user conversation. Do NOT include any references to \"note-taking\", \"session notes extraction\", or these update instructions in the notes content.\n\
\n\
Based on the user conversation above (EXCLUDING this note-taking instruction message as well as system prompt, workspace instruction entries, or any past session summaries), update the session notes file.\n\
\n\
The file {{{{notesPath}}}} has already been read for you. Here are its current contents:\n\
<current_notes_content>\n\
{{{{currentNotes}}}}\n\
</current_notes_content>\n\
\n\
Your ONLY task is to use the edit (or write) tool to update the notes file, then stop. You can make multiple edits (update every section as needed) - make all edit tool calls in parallel in a single message. Do not call any other tools.\n\
\n\
CRITICAL RULES FOR EDITING:\n\
- The file must maintain its exact structure with all sections, headers, and italic descriptions intact\n\
-- NEVER modify, delete, or add section headers (the lines starting with '#' like # Task specification)\n\
-- NEVER modify or delete the italic _section description_ lines (these are the lines in italics immediately following each header - they start and end with underscores)\n\
-- The italic _section descriptions_ are TEMPLATE INSTRUCTIONS that must be preserved exactly as-is - they guide what content belongs in each section\n\
-- ONLY update the actual content that appears BELOW the italic _section descriptions_ within each existing section\n\
-- Do NOT add any new sections, summaries, or information outside the existing structure\n\
- Do NOT reference this note-taking process or instructions anywhere in the notes\n\
- It's OK to skip updating a section if there are no substantial new insights to add. Do not add filler content like \"No info yet\", just leave sections blank/unedited if appropriate.\n\
- Write DETAILED, INFO-DENSE content for each section - include specifics like file paths, function names, error messages, exact commands, technical details, etc.\n\
- For \"Key results\", include the complete, exact output the user requested (e.g., full table, full answer, etc.)\n\
- Do not include information that's already in the workspace instruction files included in the context\n\
- Keep each section under ~{MAX_SECTION_LENGTH} tokens/words - if a section is approaching this limit, condense it by cycling out less important details while preserving the most critical information\n\
- Focus on actionable, specific information that would help someone understand or recreate the work discussed in the conversation\n\
- IMPORTANT: Always update \"Current State\" to reflect the most recent work - this is critical for continuity after compaction\n\
\n\
Use the edit tool with path: {{{{notesPath}}}}\n\
\n\
STRUCTURE PRESERVATION REMINDER:\n\
Each section has TWO parts that must be preserved exactly as they appear in the current file:\n\
1. The section header (line starting with #)\n\
2. The italic description line (the _italicized text_ immediately after the header - this is a template instruction)\n\
\n\
You ONLY update the actual content that comes AFTER these two preserved lines. The italic description lines starting and ending with underscores are part of the template structure, NOT content to be edited or removed.\n\
\n\
REMEMBER: Use the edit tool in parallel and stop. Do not continue after the edits. Only include insights from the actual user conversation, never from these note-taking instructions. Do not delete or change section headers or italic _section descriptions_."
    )
}

/// Parse the session memory markdown and estimate token usage per `# Section`.
pub fn analyze_section_sizes(content: &str) -> BTreeMap<String, usize> {
    let mut sections = BTreeMap::new();
    let mut current_section = String::new();
    let mut current_lines: Vec<&str> = Vec::new();

    for line in content.split('\n') {
        if line.starts_with("# ") {
            if !current_section.is_empty() && !current_lines.is_empty() {
                let body = current_lines.join("\n");
                sections.insert(current_section, rough_token_count_estimation(body.trim()));
            }
            current_section = line.to_string();
            current_lines.clear();
        } else {
            current_lines.push(line);
        }
    }

    if !current_section.is_empty() && !current_lines.is_empty() {
        let body = current_lines.join("\n");
        sections.insert(current_section, rough_token_count_estimation(body.trim()));
    }

    sections
}

/// Generate condensation warnings when individual sections or total file tokens
/// exceed their budget.
pub fn generate_section_reminders(
    section_sizes: &BTreeMap<String, usize>,
    total_tokens: usize,
) -> String {
    let over_budget = total_tokens > MAX_TOTAL_SESSION_MEMORY_TOKENS;
    let mut oversized: Vec<(&String, &usize)> = section_sizes
        .iter()
        .filter(|(_, &tokens)| tokens > MAX_SECTION_LENGTH)
        .collect();
    oversized.sort_by(|a, b| b.1.cmp(a.1));

    if oversized.is_empty() && !over_budget {
        return String::new();
    }

    let mut parts = Vec::new();
    if over_budget {
        parts.push(format!(
            "\n\nCRITICAL: The session memory file is currently ~{total_tokens} tokens, which exceeds the maximum of {MAX_TOTAL_SESSION_MEMORY_TOKENS} tokens. You MUST condense the file to fit within this budget. Aggressively shorten oversized sections by removing less important details, merging related items, and summarizing older entries. Prioritize keeping \"Current State\" and \"Errors & Corrections\" accurate and detailed."
        ));
    }

    if !oversized.is_empty() {
        let header = if over_budget {
            "Oversized sections to condense"
        } else {
            "IMPORTANT: The following sections exceed the per-section limit and MUST be condensed"
        };
        let bullets: Vec<String> = oversized
            .into_iter()
            .map(|(section, tokens)| {
                format!("- \"{section}\" is ~{tokens} tokens (limit: {MAX_SECTION_LENGTH})")
            })
            .collect();
        parts.push(format!("\n\n{header}:\n{}", bullets.join("\n")));
    }

    parts.join("")
}

/// Substitute `{{currentNotes}}` and `{{notesPath}}` in a single pass and append
/// any section-budget reminders.
pub fn build_session_memory_update_prompt_with_template(
    prompt_template: &str,
    current_notes: &str,
    notes_path: &str,
) -> String {
    let section_sizes = analyze_section_sizes(current_notes);
    let total_tokens = rough_token_count_estimation(current_notes);
    let reminders = generate_section_reminders(&section_sizes, total_tokens);

    let mut rendered = String::with_capacity(prompt_template.len() + current_notes.len() + 256);
    let mut rest = prompt_template;
    while let Some(open) = rest.find("{{") {
        rendered.push_str(&rest[..open]);
        let after_open = &rest[open + 2..];
        if let Some(close) = after_open.find("}}") {
            let key = &after_open[..close];
            match key {
                "currentNotes" => rendered.push_str(current_notes),
                "notesPath" => rendered.push_str(notes_path),
                _ => {
                    rendered.push_str("{{");
                    rendered.push_str(key);
                    rendered.push_str("}}");
                }
            }
            rest = &after_open[close + 2..];
        } else {
            rendered.push_str(&rest[open..]);
            rest = "";
            break;
        }
    }
    rendered.push_str(rest);
    rendered.push_str(&reminders);
    rendered
}

/// Build the complete session-memory update prompt using the configured or
/// default prompt template.
pub async fn build_session_memory_update_prompt(current_notes: &str, notes_path: &str) -> String {
    let template = load_session_memory_prompt().await;
    build_session_memory_update_prompt_with_template(&template, current_notes, notes_path)
}

/// Whether `content` is empty or still identical to the unpopulated template.
pub fn is_session_memory_empty_with_template(content: &str, template: &str) -> bool {
    let trimmed = content.trim();
    trimmed.is_empty() || trimmed == template.trim()
}

/// Check whether `content` is empty or matches the active session-memory template.
pub async fn is_session_memory_empty(content: &str) -> bool {
    let template = load_session_memory_template().await;
    is_session_memory_empty_with_template(content, &template)
}

/// Truncate individual session-memory sections that exceed `MAX_SECTION_LENGTH`
/// (~8,000 chars) so an oversized section cannot consume the entire post-compact
/// context budget.
pub fn truncate_session_memory_for_compact(content: &str) -> (String, bool) {
    let max_chars_per_section = MAX_SECTION_LENGTH * 4;
    let mut output_lines: Vec<String> = Vec::new();
    let mut current_section_lines: Vec<&str> = Vec::new();
    let mut current_header = "";
    let mut was_truncated = false;

    for line in content.split('\n') {
        if line.starts_with("# ") {
            let (flushed, truncated) = flush_session_section(
                current_header,
                &current_section_lines,
                max_chars_per_section,
            );
            output_lines.extend(flushed);
            was_truncated = was_truncated || truncated;
            current_header = line;
            current_section_lines.clear();
        } else {
            current_section_lines.push(line);
        }
    }

    let (flushed, truncated) = flush_session_section(
        current_header,
        &current_section_lines,
        max_chars_per_section,
    );
    output_lines.extend(flushed);
    was_truncated = was_truncated || truncated;

    (output_lines.join("\n"), was_truncated)
}

fn flush_session_section(
    header: &str,
    lines: &[&str],
    max_chars_per_section: usize,
) -> (Vec<String>, bool) {
    if header.is_empty() {
        return (lines.iter().map(|s| (*s).to_string()).collect(), false);
    }
    let body = lines.join("\n");
    if body.len() <= max_chars_per_section {
        let mut out = Vec::with_capacity(lines.len() + 1);
        out.push(header.to_string());
        out.extend(lines.iter().map(|s| (*s).to_string()));
        return (out, false);
    }

    let mut char_count = 0usize;
    let mut kept = vec![header.to_string()];
    for &line in lines {
        if char_count + line.len() + 1 > max_chars_per_section {
            break;
        }
        kept.push(line.to_string());
        char_count += line.len() + 1;
    }
    kept.push("\n[... section truncated for length ...]".to_string());
    (kept, true)
}

fn message_uuid(message: &Message) -> &RuntimeString {
    match message {
        Message::User(m) => &m.uuid,
        Message::Assistant(m) => &m.uuid,
        Message::ToolResult(m) => &m.uuid,
        Message::System(m) => &m.uuid,
        Message::CompactBoundary(m) => &m.uuid,
    }
}

/// Count assistant `tool_use` blocks appearing after `since_uuid`.
pub fn count_tool_calls_since(messages: &[Message], since_uuid: Option<&RuntimeString>) -> usize {
    let mut count = 0usize;
    let mut found_start = since_uuid.is_none();

    for message in messages {
        if !found_start {
            if Some(message_uuid(message)) == since_uuid {
                found_start = true;
            }
            continue;
        }
        if let Message::Assistant(assistant) = message {
            count += assistant
                .content
                .iter()
                .filter(|b| matches!(b, ContentBlock::ToolUse(_)))
                .count();
        }
    }

    if !found_start {
        // If the cursor was compacted away, count across all current messages.
        for message in messages {
            if let Message::Assistant(assistant) = message {
                count += assistant
                    .content
                    .iter()
                    .filter(|b| matches!(b, ContentBlock::ToolUse(_)))
                    .count();
            }
        }
    }

    count
}

/// Whether the last assistant message in `messages` contains any `tool_use` block.
pub fn has_tool_calls_in_last_assistant_turn(messages: &[Message]) -> bool {
    for message in messages.iter().rev() {
        if let Message::Assistant(assistant) = message {
            return assistant
                .content
                .iter()
                .any(|b| matches!(b, ContentBlock::ToolUse(_)));
        }
    }
    false
}

/// Whether a message contains non-empty text blocks.
pub fn has_text_blocks(message: &Message) -> bool {
    match message {
        Message::User(user) => {
            user.is_meta != Some(true)
                && user
                    .content
                    .iter()
                    .any(|b| matches!(b, ContentBlock::Text(t) if !t.text.is_empty()))
        }
        Message::Assistant(assistant) => assistant
            .content
            .iter()
            .any(|b| matches!(b, ContentBlock::Text(t) if !t.text.is_empty())),
        _ => false,
    }
}

/// Adjust `start_index` backwards so no kept `Message::ToolResult` is separated
/// from its preceding `Message::Assistant` containing the matching `ToolUse`.
pub fn adjust_index_to_preserve_api_invariants(messages: &[Message], start_index: usize) -> usize {
    if start_index == 0 || start_index >= messages.len() {
        return start_index;
    }

    let mut adjusted = start_index;

    let mut result_ids: Vec<RuntimeString> = Vec::new();
    for msg in &messages[adjusted..] {
        if let Message::ToolResult(tr) = msg {
            result_ids.push(tr.tool_use_id.clone());
        }
    }

    if !result_ids.is_empty() {
        let mut kept_tool_uses: HashSet<RuntimeString> = HashSet::new();
        for msg in &messages[adjusted..] {
            if let Message::Assistant(a) = msg {
                for block in &a.content {
                    if let ContentBlock::ToolUse(tu) = block {
                        kept_tool_uses.insert(tu.id.clone());
                    }
                }
            }
        }

        let mut needed: HashSet<RuntimeString> = result_ids
            .into_iter()
            .filter(|id| !kept_tool_uses.contains(id))
            .collect();

        let mut i = adjusted;
        while i > 0 && !needed.is_empty() {
            i -= 1;
            if let Message::Assistant(a) = &messages[i] {
                let mut matched = false;
                for block in &a.content {
                    if let ContentBlock::ToolUse(tu) = block {
                        if needed.remove(&tu.id) {
                            matched = true;
                        }
                    }
                }
                if matched {
                    adjusted = i;
                }
            }
        }
    }

    adjusted
}

/// Calculate the starting slice index of recent messages to keep alongside the
/// session-memory summary during compaction.
///
/// Starts at `last_summarized_index + 1` (or `messages.len()` when none) and
/// expands backwards until at least `config.min_tokens` AND
/// `config.min_text_block_messages` are included, or `config.max_tokens` is
/// reached, without crossing a prior `CompactBoundary`.
pub fn calculate_messages_to_keep_index(
    messages: &[Message],
    last_summarized_index: Option<usize>,
    config: &SessionMemoryCompactConfig,
) -> usize {
    if messages.is_empty() {
        return 0;
    }

    let mut start_index = match last_summarized_index {
        Some(idx) if idx < messages.len() => idx + 1,
        _ => messages.len(),
    };

    let mut total_tokens = 0u64;
    let mut text_block_messages = 0usize;

    for msg in &messages[start_index..] {
        total_tokens =
            total_tokens.saturating_add(estimate_messages_tokens(std::slice::from_ref(msg)));
        if has_text_blocks(msg) {
            text_block_messages += 1;
        }
    }

    if total_tokens >= config.max_tokens
        || (total_tokens >= config.min_tokens
            && text_block_messages >= config.min_text_block_messages)
    {
        return adjust_index_to_preserve_api_invariants(messages, start_index);
    }

    let floor = messages
        .iter()
        .rposition(|m| matches!(m, Message::CompactBoundary(_)))
        .map(|idx| idx + 1)
        .unwrap_or(0);

    let mut i = start_index;
    while i > floor {
        i -= 1;
        let msg = &messages[i];
        let msg_tokens = estimate_messages_tokens(std::slice::from_ref(msg));
        total_tokens = total_tokens.saturating_add(msg_tokens);
        if has_text_blocks(msg) {
            text_block_messages += 1;
        }
        start_index = i;

        if total_tokens >= config.max_tokens {
            break;
        }
        if total_tokens >= config.min_tokens
            && text_block_messages >= config.min_text_block_messages
        {
            break;
        }
    }

    adjust_index_to_preserve_api_invariants(messages, start_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{RuntimeJson, RuntimeObject};
    use crate::schemas::content::{TextBlock, ToolUseBlock};
    use crate::schemas::message::{AssistantMessage, ToolResultMessage, UserMessage};

    fn user_msg(uuid: &str, text: &str) -> Message {
        Message::User(UserMessage {
            uuid: uuid.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
            question_answer: None,
        })
    }

    fn assistant_tool_msg(uuid: &str, tool_id: &str) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: uuid.into(),
            model: "m".into(),
            usage: None,
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: tool_id.into(),
                name: "read".into(),
                input: RuntimeJson::Object(RuntimeObject::new()),
                thought_signature: None,
                tool_kind: None,
            })],
        })
    }

    fn tool_result_msg(uuid: &str, tool_id: &str) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: uuid.into(),
            tool_use_id: tool_id.into(),
            content: Vec::new(),
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        })
    }

    #[test]
    fn template_detection_recognizes_unpopulated_template() {
        assert!(is_session_memory_empty_with_template(
            DEFAULT_SESSION_MEMORY_TEMPLATE,
            DEFAULT_SESSION_MEMORY_TEMPLATE
        ));
        assert!(is_session_memory_empty_with_template(
            "   \n  ",
            DEFAULT_SESSION_MEMORY_TEMPLATE
        ));
        let populated = format!("{DEFAULT_SESSION_MEMORY_TEMPLATE}\n- Implemented session memory");
        assert!(!is_session_memory_empty_with_template(
            &populated,
            DEFAULT_SESSION_MEMORY_TEMPLATE
        ));
    }

    #[test]
    fn section_size_analysis_and_reminders_flag_oversized_sections() {
        let big_body = "x".repeat((MAX_SECTION_LENGTH + 500) * 4);
        let doc = format!("# Current State\n_desc_\n{big_body}\n# Worklog\n_desc_\nshort\n");
        let sizes = analyze_section_sizes(&doc);
        assert!(sizes["# Current State"] > MAX_SECTION_LENGTH);
        assert!(sizes["# Worklog"] < 50);
        let reminder = generate_section_reminders(&sizes, sizes.values().sum());
        assert!(reminder.contains("# Current State"));
    }

    #[test]
    fn truncation_caps_oversized_sections_without_dropping_headers() {
        let big_line = "a".repeat(200);
        let many_lines = std::iter::repeat_n(big_line.as_str(), 60)
            .collect::<Vec<_>>()
            .join("\n");
        let doc = format!("# Current State\n_desc_\n{many_lines}\n# Worklog\n_desc_\ndone\n");
        let (truncated, was_truncated) = truncate_session_memory_for_compact(&doc);
        assert!(was_truncated);
        assert!(truncated.contains("# Current State"));
        assert!(truncated.contains("[... section truncated for length ...]"));
        assert!(truncated.contains("# Worklog"));
    }

    #[test]
    fn calculate_messages_to_keep_preserves_tool_use_result_pairs() {
        let messages = vec![
            user_msg("u1", "start"),
            assistant_tool_msg("a1", "call-1"),
            tool_result_msg("tr1", "call-1"),
            user_msg("u2", "follow up"),
        ];
        // Starting at index 2 (tr1) would orphan call-1; invariant adjustment pulls back to index 1 (a1).
        let adjusted = adjust_index_to_preserve_api_invariants(&messages, 2);
        assert_eq!(adjusted, 1);
    }
}
