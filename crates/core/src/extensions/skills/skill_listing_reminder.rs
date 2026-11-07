//! A reminder provider surfacing the available skills (name + description) so the
//! model knows what it can call — the FIRST half of progressive disclosure. The
//! second half (loading a skill's body only when it's invoked) is handled by the
//! `skill` tool, which by default expands the body INLINE into the current turn
//! (or runs it as a sub-agent when the skill sets `context: fork`).
//!
//! A stable catalog is sent once. Changed content replaces the previous listing,
//! including an explicit removal notice when the last available skill disappears.
//! Budget overflow remains eligible for a later reminder in the same generation.

use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;

use super::loader::{model_invocable, Skill};
use crate::permission::bash::node_path::is_absolute;
use crate::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use crate::schemas::content::{ContentBlock, HarnessEnvelope};
use crate::schemas::message::Message;
use crate::types::reminder::{ReminderContext, ReminderProvider};
use sha2::{Digest, Sha256};

/// The usual ~4 chars/token approximation used to size the listing budget
/// against a token-denominated context window.
const CHARS_PER_TOKEN: f64 = 4.0;
/// The listing gets about 1% of the context window.
const SKILL_BUDGET_FRACTION: f64 = 0.01;
/// Window assumed when the caller supplies none — derived from the engine's own
/// default window so it tracks the config default.
const FALLBACK_CONTEXT_WINDOW: f64 = 200_000.0;
/// Cap on a single skill's rendered description.
const MAX_DESC_CHARS: usize = 200;
/// Floor below which a per-skill description share collapses the listing to names
/// only.
const MIN_DESC_CHARS: usize = 24;

/// Original preamble for the listing. It also states where these skills come
/// from: without that, a model that cannot find a capability among them starts
/// guessing at asset directories on disk — including other agent tools' config
/// directories, which are not its own.
const LISTING_PREAMBLE: &str = "Available skills can be loaded with the skill tool. This catalog lists their names, capabilities, and source paths; the environment block identifies the configured skill roots:";

/// The listing char budget derived from the context window (or the fallback).
fn char_budget(context_window: Option<i64>) -> usize {
    let window = match context_window {
        Some(w) if w > 0 => w as f64,
        _ => FALLBACK_CONTEXT_WINDOW,
    };
    (window * CHARS_PER_TOKEN * SKILL_BUDGET_FRACTION).floor() as usize
}

/// Length in UTF-16 code units — the measure a string `.length` uses.
fn u16_len(s: &RuntimeString) -> usize {
    s.len_utf16()
}

/// The exact prefix of `s` up to `end` UTF-16 code units.
fn u16_slice(s: &RuntimeString, end: usize) -> RuntimeString {
    s.slice_utf16(..end)
}

/// The description shown for a skill: `when_to_use` folded onto the description.
fn describe(s: &Skill) -> RuntimeString {
    match &s.when_to_use {
        Some(w) if !w.is_empty() => s.description.concat(&RuntimeString::from(" - ")).concat(w),
        _ => s.description.clone(),
    }
}

/// Truncate to `max` code units, replacing the last with `…` when it overflows.
fn truncate(s: &RuntimeString, max: usize) -> RuntimeString {
    if u16_len(s) <= max {
        return s.clone();
    }
    if max <= 1 {
        return u16_slice(s, max);
    }
    u16_slice(s, max - 1).concat(&RuntimeString::from("…"))
}

/// Per-line fixed cost: "- " + name + ": " + the joining newline.
fn name_overhead(name: &RuntimeString) -> usize {
    2 + u16_len(name) + 2 + 1
}

/// The skill's own `SKILL.md` location, shown so the model can read the skill's
/// supporting files without guessing where they live. Only a real absolute file
/// path qualifies: built-in skills carry a synthetic `<builtin>/…` path and a
/// programmatically registered skill may carry none, and pointing the model at
/// either would send it looking for a file that does not exist.
fn locator(s: &Skill) -> Option<RuntimeString> {
    let host = s.path.try_to_utf8().ok()?;
    is_absolute(&host).then(|| s.path.clone())
}

/// One `- name: description` line, with the description capped at `desc_max`
/// and the location appended when `with_path` and the skill has one.
fn render_line(s: &Skill, desc_max: usize, with_path: bool) -> RuntimeString {
    let mut line = RuntimeString::from("- ")
        .concat(&s.name)
        .concat(&RuntimeString::from(": "))
        .concat(&truncate(&describe(s), desc_max));
    if with_path {
        if let Some(path) = locator(s) {
            line = line
                .concat(&RuntimeString::from(" ("))
                .concat(&path)
                .concat(&RuntimeString::from(")"));
        }
    }
    line
}

/// The rendered width of a listing: the lines plus the newlines joining them.
fn listing_width(lines: &[RuntimeString]) -> usize {
    lines.iter().map(RuntimeString::len_utf16).sum::<usize>() + lines.len().saturating_sub(1)
}

/// Render `- name: description (path)` lines within `budget` chars, degrading in
/// four steps: full descriptions with locations → the same without locations →
/// descriptions shrunk to an even share of the budget → names only when even
/// that share is below the floor. Locations go first because a description is
/// what lets the model pick the right skill, so a location must never cost one a
/// single character. Returns the rendered text AND the skills actually NAMED in
/// it — so the caller can mark only those as "sent" (a skill collapsed into the
/// `… (+N more)` marker stays unsent and gets another chance to be named on a
/// later, roomier turn).
fn render_listing(skills: &[Skill], budget: usize) -> (RuntimeString, Vec<&Skill>) {
    for with_path in [true, false] {
        let lines: Vec<RuntimeString> = skills
            .iter()
            .map(|s| render_line(s, MAX_DESC_CHARS, with_path))
            .collect();
        if listing_width(&lines) <= budget {
            return (join_runtime_strings(&lines, "\n"), skills.iter().collect());
        }
    }

    let overhead: usize = skills.iter().map(|s| name_overhead(&s.name)).sum();
    // Signed division: `budget - overhead` may be negative; only a per_desc at or
    // above the floor (necessarily positive) is used as a truncate width, where
    // truncation toward zero and floor agree.
    let per_desc = (budget as i64 - overhead as i64) / (skills.len() as i64);
    if per_desc >= MIN_DESC_CHARS as i64 {
        let lines = skills
            .iter()
            .map(|s| render_line(s, per_desc as usize, false))
            .collect::<Vec<_>>();
        return (join_runtime_strings(&lines, "\n"), skills.iter().collect());
    }

    // Names only: even a description share is below the floor. Emit as many names
    // as fit the budget (always at least one), then a "+N more" marker — so the
    // listing still respects the budget instead of dumping every name unbounded.
    let mut lines: Vec<RuntimeString> = Vec::new();
    let mut named: Vec<&Skill> = Vec::new();
    let mut width: usize = 0;
    for (i, s) in skills.iter().enumerate() {
        let line = RuntimeString::from("- ").concat(&s.name);
        let cost = line.len_utf16() + if !lines.is_empty() { 1 } else { 0 };
        if !lines.is_empty() && width + cost > budget {
            lines.push(RuntimeString::from(format!(
                "… (+{} more)",
                skills.len() - i
            )));
            break;
        }
        lines.push(line);
        named.push(s);
        width += cost;
    }
    (join_runtime_strings(&lines, "\n"), named)
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

/// A reminder provider surfacing the available skills. Hold ONE instance per
/// session/sub-agent so each gets its own "already sent" set.
pub struct SkillListingReminderProvider {
    get_skills: Box<dyn Fn() -> Vec<Skill> + Send + Sync>,
    state: Mutex<SkillListingState>,
    budget: usize,
    context_window: Option<Box<dyn Fn() -> Option<i64> + Send + Sync>>,
}

#[derive(Default)]
struct SkillListingState {
    catalog: Option<Vec<Skill>>,
    sent: HashSet<RuntimeString>,
    retained_source: Option<String>,
    retained_pages: HashSet<String>,
    last_emission: Option<(Option<RuntimeString>, String)>,
}

struct CatalogPage {
    source: String,
    offset: usize,
    count: usize,
    marker: String,
}

fn catalog_page(text: &RuntimeString) -> Option<CatalogPage> {
    if !text.starts_with("<skill-catalog:") {
        return None;
    }
    let end = text
        .as_utf16()
        .iter()
        .take(160)
        .position(|unit| *unit == u16::from(b'>'))?;
    let marker = text.slice_utf16(..=end).try_to_utf8().ok()?;
    let mut parts = marker
        .strip_prefix("<skill-catalog:")?
        .strip_suffix('>')?
        .split(':');
    let source = parts.next()?.to_string();
    let offset = parts.next()?.parse().ok()?;
    let count = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(CatalogPage {
        source,
        offset,
        count,
        marker,
    })
}

impl SkillListingReminderProvider {
    /// Read a live model window without resetting which skills were already sent.
    pub fn with_context_window(
        mut self,
        get_window: impl Fn() -> Option<i64> + Send + Sync + 'static,
    ) -> Self {
        self.context_window = Some(Box::new(get_window));
        self
    }
    /// Compute this turn's listing, advancing the per-instance dedup set. Returns
    /// `None` when there is nothing new to surface.
    pub fn tick(&self) -> Option<RuntimeString> {
        self.tick_with_window(None)
    }

    fn retained_listing(
        &self,
        ctx: &ReminderContext,
        window: Option<i64>,
    ) -> Option<RuntimeString> {
        let skills = model_invocable((self.get_skills)());
        let budget = window
            .map(|window| char_budget(Some(window)))
            .unwrap_or_else(|| {
                self.context_window
                    .as_ref()
                    .map_or(self.budget, |get_window| char_budget(get_window()))
            });
        let mut digest = Sha256::new();
        let exact = runtime_json::stringify(
            &runtime_codec::encode_value(&skills)
                .expect("skill catalog runtime values encode exactly"),
        );
        for unit in exact.as_utf16() {
            digest.update(unit.to_le_bytes());
        }
        digest.update((budget as u64).to_le_bytes());
        let source = format!("{:x}", digest.finalize());
        let pages = ctx
            .messages
            .iter()
            .filter_map(|message| match message {
                Message::User(user) if user.is_meta == Some(true) => Some(user),
                _ => None,
            })
            .flat_map(|user| user.content.iter())
            .filter_map(|block| match block {
                ContentBlock::Text(text)
                    if text.harness_envelope == Some(HarnessEnvelope::SystemReminder) =>
                {
                    catalog_page(&text.text)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let prior_source = pages.last().map(|page| page.source.as_str());
        let retained = pages
            .iter()
            .rev()
            .take_while(|page| page.source == source)
            .collect::<Vec<_>>();
        let current_markers = retained
            .iter()
            .map(|page| page.marker.clone())
            .collect::<HashSet<_>>();
        let mut covered = vec![false; skills.len()];
        for page in &retained {
            let end = page.offset.saturating_add(page.count).min(covered.len());
            if page.offset < end {
                covered[page.offset..end].fill(true);
            }
        }
        let turn = ctx.messages.iter().rev().find_map(|message| match message {
            Message::User(user) if user.is_meta != Some(true) => Some(user.uuid.clone()),
            _ => None,
        });
        let mut state = self.state.lock().unwrap();
        let had_skills = state
            .catalog
            .as_ref()
            .is_some_and(|skills| !skills.is_empty());
        let lost_page = state.retained_source.as_ref() == Some(&source)
            && state
                .retained_pages
                .iter()
                .any(|marker| !current_markers.contains(marker));
        let same_turn =
            state
                .last_emission
                .as_ref()
                .is_some_and(|(previous_turn, previous_source)| {
                    previous_turn == &turn && previous_source == &source
                });
        state.catalog = Some(skills.clone());
        state.retained_source = Some(source.clone());
        state.retained_pages = current_markers;
        if same_turn && !lost_page && prior_source == Some(source.as_str()) {
            return None;
        }
        if skills.is_empty() {
            if !retained.is_empty() {
                return None;
            }
            if prior_source.is_none()
                && !had_skills
                && !ctx
                    .messages
                    .iter()
                    .any(|message| matches!(message, Message::CompactBoundary(_)))
            {
                return None;
            }
        } else if covered.iter().all(|covered| *covered) {
            return None;
        }
        let offset = covered.iter().position(|covered| !*covered).unwrap_or(0);
        let end = covered
            .iter()
            .enumerate()
            .skip(offset)
            .find_map(|(index, covered)| covered.then_some(index))
            .unwrap_or(skills.len());
        let (body, count) = if skills.is_empty() {
            (
                RuntimeString::from(
                    "No skills are currently available. Previous skill listings no longer apply.",
                ),
                0,
            )
        } else {
            let (body, named) = render_listing(&skills[offset..end], budget);
            (
                RuntimeString::from(LISTING_PREAMBLE)
                    .concat(&RuntimeString::from("\n"))
                    .concat(&body),
                named.len(),
            )
        };
        let marker = format!("<skill-catalog:{source}:{offset}:{count}>");
        state.retained_pages.insert(marker.clone());
        state.last_emission = Some((turn, source.clone()));
        let replacement = if prior_source.is_some_and(|prior| prior != source)
            || (prior_source.is_none() && had_skills)
        {
            "The available skill catalog has changed. This listing replaces all previous skill listings.\n"
        } else {
            ""
        };
        Some(
            RuntimeString::from(marker)
                .concat(&RuntimeString::from("\n"))
                .concat(&RuntimeString::from(replacement))
                .concat(&body),
        )
    }

    fn tick_with_window(&self, window: Option<i64>) -> Option<RuntimeString> {
        // A `disable-model-invocation` skill is not callable by the model, so it
        // never appears in the listing (it's surfaced to the user as a slash
        // command instead). Same predicate the call site uses.
        let skills: Vec<Skill> = model_invocable((self.get_skills)());
        let mut state = self.state.lock().unwrap();
        let changed = state
            .catalog
            .as_ref()
            .is_some_and(|previous| previous != &skills);
        if state.catalog.as_ref() != Some(&skills) {
            state.sent.clear();
            state.catalog = Some(skills.clone());
        }
        if skills.is_empty() {
            return changed.then(|| RuntimeString::from(
                "The available skill catalog has changed. No skills are currently available. Previous skill listings no longer apply.",
            ));
        }
        let fresh: Vec<Skill> = skills
            .into_iter()
            .filter(|s| !state.sent.contains(&s.name))
            .collect();
        if fresh.is_empty() {
            return None;
        }
        let budget = window
            .map(|window| char_budget(Some(window)))
            .unwrap_or_else(|| {
                self.context_window
                    .as_ref()
                    .map_or(self.budget, |get_window| char_budget(get_window()))
            });
        let (body, named) = render_listing(&fresh, budget);
        // Mark only the skills actually NAMED this turn as sent — a skill hidden
        // behind the `… (+N more)` marker stays fresh so a later turn can name it.
        for s in &named {
            state.sent.insert(s.name.clone());
        }
        let preamble = if changed {
            "The available skill catalog has changed. This listing replaces all previous skill listings.\n"
        } else {
            ""
        };
        let text = RuntimeString::from(preamble)
            .concat(&RuntimeString::from(LISTING_PREAMBLE))
            .concat(&RuntimeString::from("\n"))
            .concat(&body);
        if text.is_empty() {
            None
        } else {
            Some(text)
        }
    }
}

#[async_trait]
impl ReminderProvider for SkillListingReminderProvider {
    fn begin_turn(&self) {
        self.state.lock().unwrap().last_emission = None;
    }

    async fn reminder(&self, ctx: &ReminderContext) -> Option<RuntimeString> {
        self.retained_listing(ctx, None)
    }

    fn retained_context(&self) -> bool {
        true
    }

    async fn reminder_with_context_window(
        &self,
        ctx: &ReminderContext,
        window: u64,
    ) -> Option<RuntimeString> {
        self.retained_listing(ctx, Some(i64::try_from(window).unwrap_or(i64::MAX)))
    }
}

/// Build a skill-listing reminder provider. `context_window` sizes the listing
/// budget (a small fraction of the window); `None` uses the engine default. Hold
/// ONE instance for the conversation.
pub fn create_skill_listing_reminder_provider(
    get_skills: impl Fn() -> Vec<Skill> + Send + Sync + 'static,
    context_window: Option<i64>,
) -> SkillListingReminderProvider {
    SkillListingReminderProvider {
        get_skills: Box::new(get_skills),
        state: Mutex::new(SkillListingState::default()),
        budget: char_budget(context_window),
        context_window: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_json, RuntimeJson};
    use std::sync::Arc;

    fn skill(name: &str, description: &str) -> Skill {
        Skill {
            name: RuntimeString::from(name),
            description: RuntimeString::from(description),
            body: RuntimeString::default(),
            path: RuntimeString::from(format!("/skills/{name}/SKILL.md")),
            ..Default::default()
        }
    }

    fn skill_when(name: &str, description: &str, when_to_use: &str) -> Skill {
        Skill {
            when_to_use: Some(RuntimeString::from(when_to_use)),
            ..skill(name, description)
        }
    }

    fn provider(skills: Vec<Skill>) -> SkillListingReminderProvider {
        create_skill_listing_reminder_provider(move || skills.clone(), None)
    }

    #[test]
    fn lists_name_and_description_and_prompts_the_model_to_call_them() {
        let p = provider(vec![
            skill("pdf", "Extract text from PDFs"),
            skill_when("chart", "Render charts", "when asked for a graph"),
        ]);
        let text = p.tick().unwrap_or_default();
        assert!(text.contains("Available skills can be loaded with the skill tool."));
        assert!(text.contains("- pdf: Extract text from PDFs"));
        // when_to_use is folded into the description.
        assert!(text.contains("- chart: Render charts - when asked for a graph"));
    }

    #[test]
    fn returns_null_when_there_are_no_skills() {
        let p = provider(vec![]);
        assert_eq!(p.tick(), None);
    }

    /// A slash-only skill is never named to the model.
    ///
    /// This listing is the ONLY way the model learns a skill exists — the `skill`
    /// tool's schema takes a free-form name and enumerates nothing. So leaking a
    /// gated skill here is what would make the model reach for it, and `/canvas`
    /// stopping being the user's call starts with one name in this string.
    #[test]
    fn a_slash_only_skill_never_reaches_the_listing() {
        let gated = Skill {
            disable_model_invocation: Some(true),
            ..skill("canvas", "Create or edit .canvas.tsx visual artifacts")
        };
        let p = provider(vec![skill("pdf", "Extract text from PDFs"), gated.clone()]);
        let text = p.tick().unwrap_or_default();
        assert!(text.contains("- pdf: Extract text from PDFs"));
        assert!(!text.contains("canvas"), "{text:?}");

        // Gated ALONE reads as no skills at all, not as an empty listing header:
        // a preamble telling the model to use the `skill` tool with nothing under
        // it is an invitation to guess a name.
        let p = provider(vec![gated]);
        assert_eq!(p.tick(), None);
    }

    #[test]
    fn lists_each_skill_once_then_nothing_on_later_turns() {
        let p = provider(vec![skill("a", "first"), skill("b", "second")]);
        let first = p.tick().unwrap_or_default();
        assert!(first.contains("- a: first"));
        assert!(first.contains("- b: second"));
        // Second turn: nothing new → None (not re-listed every turn).
        assert_eq!(p.tick(), None);
    }

    #[test]
    fn live_model_window_updates_the_budget_without_repeating_sent_skills() {
        let window = Arc::new(std::sync::atomic::AtomicI64::new(1000));
        let get_window = window.clone();
        let p = create_skill_listing_reminder_provider(
            || {
                (0..10)
                    .map(|i| skill(&format!("skill{i}"), "a wordy description here"))
                    .collect()
            },
            None,
        )
        .with_context_window(move || Some(get_window.load(std::sync::atomic::Ordering::SeqCst)));
        let first = p.tick().unwrap();
        assert!(first.contains("- skill0"));
        assert!(first.contains("more)"));
        window.store(200_000, std::sync::atomic::Ordering::SeqCst);
        let second = p.tick().unwrap();
        assert!(!second.contains("- skill0"));
        assert!(second.contains("- skill9: a wordy description here"));
        assert!(!second.contains("more)"));
        assert_eq!(p.tick(), None);
    }

    #[test]
    fn replaces_the_listing_when_the_catalog_changes() {
        let skills = Arc::new(Mutex::new(vec![skill("a", "first")]));
        let sk = skills.clone();
        let p = create_skill_listing_reminder_provider(move || sk.lock().unwrap().clone(), None);
        p.tick();
        skills.lock().unwrap().push(skill("b", "second"));
        let next = p.tick().unwrap_or_default();
        assert!(next.contains("- b: second"));
        assert!(next.contains("- a: first"));
        assert!(next.contains("replaces all previous skill listings"));
    }

    #[test]
    fn body_edits_and_removal_replace_prior_catalog_instructions() {
        let skills = Arc::new(Mutex::new(vec![skill("draft", "Write a draft")]));
        let live = skills.clone();
        let provider =
            create_skill_listing_reminder_provider(move || live.lock().unwrap().clone(), None);
        assert!(provider.tick().is_some());
        assert_eq!(provider.tick(), None);
        skills.lock().unwrap()[0].body = "Updated implementation instructions".into();
        let edited = provider.tick().unwrap();
        assert!(edited.contains("replaces all previous skill listings"));
        skills.lock().unwrap().clear();
        assert!(provider
            .tick()
            .unwrap()
            .contains("No skills are currently available"));
        assert_eq!(provider.tick(), None);
        skills.lock().unwrap().push(skill("draft", "Write again"));
        assert!(provider.tick().unwrap().contains("- draft: Write again"));
    }

    fn context(messages: Vec<Message>) -> ReminderContext {
        ReminderContext {
            messages,
            cwd: "/workspace".into(),
            task_list_id: None,
            task_plan: None,
            signal: None,

            plan_file_path: None,
            plan_file_exists: None,
            has_task_tool: false,
        }
    }

    fn user(id: &str) -> Message {
        Message::User(crate::schemas::message::UserMessage {
            question_answer: None,
            uuid: id.into(),
            content: Vec::new(),
            is_meta: None,
        })
    }

    fn catalog_message(text: RuntimeString) -> Message {
        Message::User(crate::schemas::message::UserMessage {
            question_answer: None,
            uuid: "catalog".into(),
            is_meta: Some(true),
            content: vec![ContentBlock::Text(crate::schemas::content::TextBlock {
                text,
                citations: None,
                harness_envelope: Some(HarnessEnvelope::SystemReminder),
                message_id: None,
                phase: None,
            })],
        })
    }

    #[test]
    fn retained_catalog_pages_advance_on_later_turns_and_restore_dropped_coverage() {
        let p = provider(
            (0..10)
                .map(|index| skill(&format!("skill-{index}"), "Description"))
                .collect(),
        );
        let mut ctx = context(vec![user("first")]);
        let first = p.retained_listing(&ctx, Some(100)).unwrap();
        assert!(first.contains("- skill-0"));
        ctx.messages.push(catalog_message(first));
        assert_eq!(p.retained_listing(&ctx, Some(100)), None);
        ctx.messages.push(user("second"));
        let second = p.retained_listing(&ctx, Some(100)).unwrap();
        assert!(second.contains("- skill-1"));
        assert!(!second.contains("- skill-0"));
        ctx.messages.push(catalog_message(second));
        assert_eq!(p.retained_listing(&ctx, Some(100)), None);
        ctx.messages.remove(1);
        let restored = p.retained_listing(&ctx, Some(100)).unwrap();
        assert!(restored.contains("- skill-0"));
    }

    #[test]
    fn retained_catalog_dedup_uses_history_instead_of_provider_lifetime() {
        let p = provider(vec![skill("draft", "Draft text")]);
        let mut ctx = context(vec![user("first")]);
        let first = p.retained_listing(&ctx, None).unwrap();
        ctx.messages.push(catalog_message(first));
        assert_eq!(p.retained_listing(&ctx, None), None);
        let resumed = provider(vec![skill("draft", "Draft text")]);
        assert_eq!(resumed.retained_listing(&ctx, None), None);
        ctx.messages.retain(
            |message| !matches!(message, Message::User(user) if user.is_meta == Some(true)),
        );
        assert!(resumed.retained_listing(&ctx, None).is_some());
    }

    #[test]
    fn caps_a_very_long_description() {
        let long = "x".repeat(1000);
        let p = provider(vec![skill("big", &long)]);
        let text = p.tick().unwrap_or_default();
        assert!(text.contains("…"));
        assert!(text.len_utf16() < long.encode_utf16().count());
    }

    #[test]
    fn truncation_uses_javascript_utf16_slice_semantics() {
        assert_eq!(
            runtime_json::stringify(&RuntimeJson::String(u16_slice(
                &RuntimeString::from("😀x"),
                1,
            ))),
            r#""\ud83d""#
        );
        let parsed = runtime_json::parse(&RuntimeString::from(r#""\ud800x""#)).unwrap();
        let exact = parsed.as_str().unwrap();
        assert_eq!(exact.len_utf16(), 2);
        assert_eq!(
            runtime_json::stringify(&RuntimeJson::String(exact.slice_utf16(..1))),
            r#""\ud800""#
        );
    }

    #[test]
    fn listing_keeps_lone_surrogates_in_model_visible_fields_exact() {
        let exact_name = RuntimeString::from_utf16(vec![u16::from(b'n'), 0xd800]);
        let exact_description = RuntimeString::from_utf16(vec![u16::from(b'd'), 0xdc00]);
        let skill = Skill {
            name: exact_name.clone(),
            description: exact_description.clone(),
            body: RuntimeString::default(),
            path: RuntimeString::from("/x/SKILL.md"),
            ..Default::default()
        };
        let listing = provider(vec![skill]).tick().unwrap();
        let expected = RuntimeString::from(LISTING_PREAMBLE)
            .concat(&RuntimeString::from("\n- "))
            .concat(&exact_name)
            .concat(&RuntimeString::from(": "))
            .concat(&exact_description)
            .concat(&RuntimeString::from(" (/x/SKILL.md)"));

        assert_eq!(listing, expected);
    }

    #[test]
    fn lists_each_skill_with_its_own_absolute_location() {
        let p = provider(vec![skill("pdf", "Extract text from PDFs")]);
        let text = p.tick().unwrap_or_default();
        assert!(text.contains("- pdf: Extract text from PDFs (/skills/pdf/SKILL.md)"));
    }

    #[test]
    fn omits_a_location_that_is_not_a_real_file_path() {
        let p = provider(vec![
            Skill {
                path: RuntimeString::from("<builtin>/deep-research/SKILL.md"),
                ..skill("deep-research", "research deeply")
            },
            Skill {
                path: RuntimeString::default(),
                ..skill("gated", "registered in code")
            },
        ]);
        let text = p.tick().unwrap_or_default();
        assert!(text.contains("- deep-research: research deeply\n"));
        assert!(!text.contains("<builtin>"));
        assert!(text.ends_with("- gated: registered in code"));
    }

    #[test]
    fn drops_locations_before_shortening_descriptions() {
        let p = create_skill_listing_reminder_provider(
            || {
                vec![
                    skill("alpha", "a fairly wordy description here"),
                    skill("beta", "another fairly wordy description here"),
                ]
            },
            // 100-char budget: the two lines fit at 86 chars without their
            // locations but need 135 with them.
            Some(2500),
        );
        let text = p.tick().unwrap_or_default();
        assert!(text.contains("- alpha: a fairly wordy description here\n"));
        assert!(text.ends_with("- beta: another fairly wordy description here"));
        assert!(!text.contains("SKILL.md"));
    }

    #[test]
    fn degrades_to_names_only_under_a_tiny_budget() {
        let p = create_skill_listing_reminder_provider(
            || {
                vec![
                    skill("alpha", "a fairly wordy description here"),
                    skill("beta", "another fairly wordy description here"),
                    skill("gamma", "yet another wordy description here"),
                ]
            },
            Some(1000), // ~10 char budget → names only
        );
        let text = p.tick().unwrap_or_default();
        assert!(text.contains("- alpha"));
        assert!(!text.contains("- alpha:")); // no description
        assert!(!text.contains("wordy"));
    }

    #[test]
    fn falls_back_to_a_default_budget_when_no_context_window_is_given() {
        let p = provider(vec![skill("a", "desc")]);
        assert!(p.tick().unwrap_or_default().contains("- a: desc"));
    }

    #[test]
    fn caps_the_names_only_listing_to_the_budget_with_a_plus_n_more_marker() {
        let many: Vec<Skill> = (0..100)
            .map(|i| skill(&format!("skill{i}"), "a wordy description here"))
            .collect();
        // budget = 1000 * 4 * 0.01 = 40 chars → names only, must not dump all 100.
        let p = create_skill_listing_reminder_provider(move || many.clone(), Some(1000));
        let text = p.tick().unwrap_or_default();
        let listing = text.slice_utf16(LISTING_PREAMBLE.encode_utf16().count() + 1..);
        assert!(listing.contains("… (+"));
        assert!(listing.contains(" more)"));
        // bounded near budget, not the ~900-char unbounded dump
        assert!(listing.len_utf16() < 120);
        assert!(listing.contains("- skill0"));
    }

    #[test]
    fn does_not_mark_plus_n_more_skills_as_sent() {
        let many: Vec<Skill> = (0..100)
            .map(|i| skill(&format!("skill{i}"), "a wordy description here"))
            .collect();
        let p = create_skill_listing_reminder_provider(move || many.clone(), Some(1000));
        let turn1 = p.tick().unwrap_or_default();
        assert!(turn1.contains("- skill0"));
        // Turn 2 must NOT be None and must advance to skills that were behind the
        // previous "+N more" marker (skill0 was named → sent).
        let turn2 = p.tick();
        assert!(turn2.is_some());
        let turn2 = turn2.unwrap_or_default();
        assert!(!turn2.contains("- skill0\n"));
        assert!(!turn2.ends_with("- skill0"));
        assert!(turn2.contains("- skill"));
    }
}
