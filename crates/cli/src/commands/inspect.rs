//! Read-only views of what the session actually resolved.
//!
//! Skills, agents and hooks have no method of their own. The engine does no disk
//! discovery, so what it knows about them is whatever the host handed it at
//! `session/start` — and the authoritative answer to "what is loaded" is
//! therefore the effective extension graph, not the directory listing
//! [`crate::discovery`] produced on the way in. All three list commands read the
//! same `session/extensionRuntime` reply and project a different slice of it,
//! which is also why they cannot disagree with each other.
//!
//! Every extension line carries its source. That is the only thing that explains
//! a surprise: a skill the user never wrote came from a plugin, and a hook that
//! fires twice is two sources contributing the same event.

use klaus_agent_core::capabilities::{
    ResolvedRuntimeCapabilityRoot, RuntimeCapabilityResolution, SelectedRuntimeCapabilityRoot,
};
use klaus_agent_core::extensions::runtime_snapshot::{
    EffectiveExtension, EffectiveHook, ExtensionRuntimeSnapshot, ExtensionSource,
    SuppressedExtension,
};
use klaus_agent_server::app_server::methods::{
    SessionExtensionRuntime, SessionRuntimeCapabilities,
};

use crate::engine::EngineClient;
use crate::error::CliResult;
use crate::output::Printer;
use crate::render;
use crate::text;

use super::{missing, show};

pub async fn skills(engine: &EngineClient, printer: &Printer) -> CliResult<()> {
    extension_slice(engine, printer, "skills", |snapshot| &snapshot.skills).await
}

pub async fn agents(engine: &EngineClient, printer: &Printer) -> CliResult<()> {
    extension_slice(engine, printer, "agents", |snapshot| &snapshot.agents).await
}

pub async fn hooks(engine: &EngineClient, printer: &Printer) -> CliResult<()> {
    show::<SessionExtensionRuntime>(engine, printer, engine.scope(), |event| {
        match &event.extension_runtime {
            Some(snapshot) => hook_list(&snapshot.hooks),
            None => missing("extensionRuntime"),
        }
    })
    .await
}

/// The whole graph: every family, plus what was suppressed on the way in.
pub async fn extensions(engine: &EngineClient, printer: &Printer) -> CliResult<()> {
    show::<SessionExtensionRuntime>(engine, printer, engine.scope(), |event| {
        match &event.extension_runtime {
            Some(snapshot) => extension_runtime(snapshot),
            None => missing("extensionRuntime"),
        }
    })
    .await
}

pub async fn capabilities(engine: &EngineClient, printer: &Printer) -> CliResult<()> {
    show::<SessionRuntimeCapabilities>(engine, printer, engine.scope(), |event| {
        match &event.runtime_capabilities {
            Some(resolution) => capability_resolution(resolution),
            None => missing("runtimeCapabilities"),
        }
    })
    .await
}

/// One family out of the extension graph.
async fn extension_slice(
    engine: &EngineClient,
    printer: &Printer,
    family: &'static str,
    select: fn(&ExtensionRuntimeSnapshot) -> &Vec<EffectiveExtension>,
) -> CliResult<()> {
    show::<SessionExtensionRuntime>(engine, printer, engine.scope(), |event| {
        match &event.extension_runtime {
            Some(snapshot) => extension_list(family, select(snapshot)),
            None => missing("extensionRuntime"),
        }
    })
    .await
}

fn extension_list(family: &str, extensions: &[EffectiveExtension]) -> String {
    if extensions.is_empty() {
        return format!("(no {family})");
    }
    extensions
        .iter()
        .map(|extension| {
            format!(
                "{}  [{}]",
                text::text(&extension.name),
                source(&extension.source)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Hooks, grouped by nothing: the event is the first column precisely so that
/// sorting by line puts every handler for one event together, including the ones
/// contributed by different sources.
fn hook_list(hooks: &[EffectiveHook]) -> String {
    if hooks.is_empty() {
        return "(no hooks)".to_string();
    }
    let mut lines: Vec<String> = hooks.iter().map(hook).collect();
    lines.sort();
    lines.join("\n")
}

fn hook(hook: &EffectiveHook) -> String {
    let mut tags = vec![render::wire_tag(&hook.handler_kind)];
    if hook.is_async {
        tags.push("async".to_string());
    }
    if let Some(timeout) = hook.timeout_ms {
        tags.push(format!("{timeout}ms"));
    }
    tags.push(source(&hook.source));
    format!(
        "{}  {}  [{}]",
        render::wire_tag(&hook.event),
        render::label(hook.matcher.as_ref(), "*"),
        tags.join(", ")
    )
}

fn extension_runtime(snapshot: &ExtensionRuntimeSnapshot) -> String {
    let mut lines = vec![
        "skills:".to_string(),
        indent(&extension_list("skills", &snapshot.skills)),
        "agents:".to_string(),
        indent(&extension_list("agents", &snapshot.agents)),
        "output styles:".to_string(),
        indent(&extension_list("output styles", &snapshot.output_styles)),
    ];
    if let Some(selected) = &snapshot.selected_output_style {
        lines.push(format!("  selected: {}", text::text(selected)));
    }
    lines.push("hooks:".to_string());
    lines.push(indent(&hook_list(&snapshot.hooks)));
    lines.push(format!(
        "pricing: {} model(s) [{}]",
        snapshot.pricing.model_ids.len(),
        source(&snapshot.pricing.source)
    ));
    // Suppression is the one part of this snapshot that reports a decision rather
    // than a state: two sources declared the same name and one of them lost. A
    // caller wondering why its own skill does nothing finds the answer here.
    if !snapshot.suppressed.is_empty() {
        lines.push("suppressed:".to_string());
        for entry in &snapshot.suppressed {
            lines.push(indent(&suppressed(entry)));
        }
    }
    let plugins = &snapshot.plugin_runtime;
    lines.push(format!(
        "plugins: {} loaded, {} disabled, {} error(s), {} warning(s)",
        plugins.entries.len(),
        plugins.disabled.len(),
        plugins.errors.len(),
        plugins.warnings.len()
    ));
    lines.join("\n")
}

fn suppressed(entry: &SuppressedExtension) -> String {
    format!(
        "{} {} [{}] replaced by [{}]: {}",
        render::wire_tag(&entry.kind),
        text::text(&entry.name),
        source(&entry.source),
        source(&entry.replaced_by),
        text::text(&entry.reason)
    )
}

/// Where an extension came from. The id is appended when there is one, because
/// `plugin` alone does not identify which plugin.
fn source(source: &ExtensionSource) -> String {
    match &source.id {
        Some(id) => format!("{} {}", render::wire_tag(&source.kind), text::text(id)),
        None => render::wire_tag(&source.kind),
    }
}

/// Which runtime capabilities resolved, and which did not.
///
/// `missingRoots` is printed even when empty is not: a selected root the engine
/// could not resolve is the reason a capability silently does nothing, and it is
/// invisible in the effective list by definition.
fn capability_resolution(resolution: &RuntimeCapabilityResolution) -> String {
    let mut lines = Vec::new();
    if resolution.descriptors.is_empty() {
        lines.push("(no capabilities)".to_string());
    }
    for descriptor in &resolution.descriptors {
        let source = &descriptor.source;
        let mut tags = vec![render::wire_tag(&source.kind)];
        if !source.enabled {
            tags.push("disabled".to_string());
        }
        if let Some(version) = &source.version {
            tags.push(format!("v{}", text::text(version)));
        }
        lines.push(format!(
            "{}  {} ({})  [{}]",
            descriptor.name.as_str(),
            text::text(&source.name),
            text::text(&source.id),
            tags.join(", ")
        ));
    }
    if !resolution.selected_roots.is_empty() {
        lines.push(format!(
            "selected roots: {}",
            resolution
                .selected_roots
                .iter()
                .map(selected_root)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    for root in &resolution.effective_roots {
        lines.push(effective_root(root));
    }
    if !resolution.missing_roots.is_empty() {
        lines.push(format!(
            "[unresolved] {}",
            resolution
                .missing_roots
                .iter()
                .map(selected_root)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    lines.join("\n")
}

fn selected_root(root: &SelectedRuntimeCapabilityRoot) -> String {
    format!("{} {}", render::wire_tag(&root.kind), text::text(&root.id))
}

fn effective_root(root: &ResolvedRuntimeCapabilityRoot) -> String {
    let names = root
        .capabilities
        .iter()
        .map(|name| name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "  {} -> {}",
        selected_root(&root.selection),
        if names.is_empty() {
            "(nothing)".to_string()
        } else {
            names
        }
    )
}

fn indent(block: &str) -> String {
    block
        .lines()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}
