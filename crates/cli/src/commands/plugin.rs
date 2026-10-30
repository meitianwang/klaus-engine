//! Installed plugins: the ledger, the load graph, and the four mutations.
//!
//! Plugins belong to the engine, not to this client. The engine owns the ledger
//! file, decides what a package contributes, and loads it; the CLI names a
//! directory and reads back what happened. So there is no "plugin add" that
//! writes a config here — `install` hands the engine a resolved directory and the
//! engine records it.
//!
//! Fetching is the host's half of that split and the CLI does not do it: there is
//! no git or registry client in here on purpose. `install --source-dir` takes a
//! package already on disk, and `catalog --root` takes marketplace directories
//! already on disk, because a CLI that silently downloaded code would be a worse
//! thing than one that asks you to.

use klaus_agent_core::extensions::plugins::installed::{InstalledPluginRecord, InstalledPlugins};
use klaus_agent_core::extensions::plugins::runtime::PluginRuntimeSnapshot;
use klaus_agent_protocol::{
    InstalledPluginMutationResult, ProtocolPluginCatalog, ProtocolPluginCatalogEntry,
    ProtocolPluginContributions,
};
use klaus_agent_server::app_server::methods::{
    SessionInstallPlugin, SessionInstalledPlugins, SessionPluginCatalog, SessionPluginRuntime,
    SessionSetInstalledPluginEnabled, SessionUninstallPlugin,
};
use klaus_agent_server::app_server::wire;
use klaus_agent_server::Method;

use crate::cli::{Globals, PluginCommand};
use crate::engine::EngineClient;
use crate::error::{CliError, CliResult};
use crate::output::Printer;
use crate::render;
use crate::text;

use super::{missing, settle, show};

pub async fn run(
    engine: &EngineClient,
    printer: &Printer,
    globals: &Globals,
    command: &PluginCommand,
) -> CliResult<()> {
    // Every method below is answered by the endpoint's plugin ledger manager, and
    // an endpoint told not to READ the ledger declines to offer it: a process that
    // never loaded the installed set has no business rewriting it. So the engine
    // refuses all seven with "installed plugin administration is unavailable" —
    // true, but naming neither the cause nor the fix, when the cause is a flag on
    // this very command line.
    if globals.no_plugins {
        return Err(CliError::usage(
            "`plugin` administers the installed-plugin ledger, and --no-plugins told \
             the engine not to read it: drop --no-plugins",
        ));
    }
    match command {
        PluginCommand::List => {
            show::<SessionInstalledPlugins>(engine, printer, engine.scope(), |event| {
                match event.installed_plugins.as_deref() {
                    Some(ledger) => installed(ledger),
                    None => missing("installedPlugins"),
                }
            })
            .await
        }

        PluginCommand::Runtime => {
            show::<SessionPluginRuntime>(engine, printer, engine.scope(), |event| {
                match event.plugin_runtime.as_deref() {
                    Some(snapshot) => runtime(snapshot),
                    None => missing("pluginRuntime"),
                }
            })
            .await
        }

        PluginCommand::Enable { name } => enabled(engine, printer, name, true).await,
        PluginCommand::Disable { name } => enabled(engine, printer, name, false).await,

        PluginCommand::Install {
            marketplace,
            source_dir,
            version,
        } => {
            mutation::<SessionInstallPlugin>(
                engine,
                printer,
                wire::SessionPluginInstallParams {
                    session_id: Some(engine.session_id().to_string()),
                    marketplace: marketplace.clone(),
                    source_dir: source_dir.clone(),
                    version: version.clone(),
                    expected_installed_version: None,
                    submission_id: None,
                },
            )
            .await
        }

        PluginCommand::Uninstall { name, remove_files } => {
            mutation::<SessionUninstallPlugin>(
                engine,
                printer,
                wire::SessionPluginUninstallParams {
                    session_id: Some(engine.session_id().to_string()),
                    name: name.clone(),
                    remove_files: remove_files.then_some(true),
                    submission_id: None,
                },
            )
            .await
        }

        PluginCommand::Catalog { roots } => {
            let roots = roots
                .iter()
                .map(|root| catalog_root(root))
                .collect::<CliResult<Vec<_>>>()?;
            settle::<SessionPluginCatalog>(
                engine,
                printer,
                wire::SessionPluginCatalogParams {
                    session_id: Some(engine.session_id().to_string()),
                    roots,
                    submission_id: None,
                },
                |event| event.plugin_catalog.is_some(),
                |event| match event.plugin_catalog.as_deref() {
                    Some(catalog) => catalog_listing(catalog),
                    None => missing("pluginCatalog"),
                },
            )
            .await
        }
    }
}

async fn enabled(
    engine: &EngineClient,
    printer: &Printer,
    name: &str,
    enabled: bool,
) -> CliResult<()> {
    mutation::<SessionSetInstalledPluginEnabled>(
        engine,
        printer,
        wire::SessionPluginEnabledParams {
            session_id: Some(engine.session_id().to_string()),
            name: name.to_string(),
            enabled,
            submission_id: None,
        },
    )
    .await
}

/// The three mutations report the same field, so they settle and render the same
/// way and differ only in what they ask for.
async fn mutation<M>(engine: &EngineClient, printer: &Printer, params: M::Params) -> CliResult<()>
where
    M: Method<Result = wire::SubmissionAck>,
{
    settle::<M>(
        engine,
        printer,
        params,
        |event| event.plugin_mutation.is_some(),
        |event| match event.plugin_mutation.as_deref() {
            Some(result) => mutated(result),
            None => missing("pluginMutation"),
        },
    )
    .await
}

/// The ledger, one line per record.
fn installed(ledger: &InstalledPlugins) -> String {
    if ledger.plugins.is_empty() {
        return "(no installed plugins)".to_string();
    }
    ledger
        .plugins
        .values()
        .map(record)
        .collect::<Vec<_>>()
        .join("\n")
}

/// One ledger record. `install_path` is printed because it is the only thing that
/// distinguishes two records with the same name from different marketplaces, and
/// it is what `uninstall --remove-files` will delete.
fn record(plugin: &InstalledPluginRecord) -> String {
    let mut tags = vec![
        if plugin.enabled {
            "enabled"
        } else {
            "disabled"
        }
        .to_string(),
        text::text(&plugin.source),
    ];
    if let Some(marketplace) = &plugin.marketplace {
        tags.push(text::text(marketplace));
    }
    if let Some(scope) = &plugin.scope {
        tags.push(text::text(scope));
    }
    let mut line = format!(
        "{}{}  [{}]  {}",
        text::text(&plugin.name),
        plugin
            .version
            .as_ref()
            .map(|version| format!("@{}", text::text(version)))
            .unwrap_or_default(),
        tags.join(", "),
        text::text(&plugin.install_path)
    );
    if !plugin.dependencies.is_empty() {
        line.push_str(&format!(
            "\n  requires: {}",
            plugin
                .dependencies
                .iter()
                .map(text::text)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    line
}

/// The load graph: what loaded, what was refused, and what went wrong.
///
/// Errors and warnings print even when everything else looks fine, because that
/// is the whole reason to run this instead of `plugin list`: the ledger says a
/// plugin is enabled, and only the runtime says whether it actually loaded.
fn runtime(snapshot: &PluginRuntimeSnapshot) -> String {
    let mut lines = Vec::new();
    if snapshot.entries.is_empty() {
        lines.push("(no plugins loaded)".to_string());
    }
    for entry in &snapshot.entries {
        let mut line = format!(
            "{}{}  [{}]  {}",
            text::text(&entry.name),
            entry
                .version
                .as_ref()
                .map(|version| format!("@{}", text::text(version)))
                .unwrap_or_default(),
            if entry.enabled { "enabled" } else { "disabled" },
            text::text(&entry.dir)
        );
        // The namespace is what a plugin's skills and commands are addressed
        // under, so it is the one field a caller needs in order to USE what
        // loaded rather than just confirm that it did.
        line.push_str(&format!(
            "\n  namespace: {}",
            text::text(&entry.plugin_namespace)
        ));
        let mut contributes = Vec::new();
        if entry.has_skills {
            contributes.push("skills".to_string());
        }
        for (label, names) in [
            ("mcp", &entry.permission_scope.mcp_servers),
            ("lsp", &entry.permission_scope.lsp_servers),
            ("runtime", &entry.runtime_capabilities),
            ("interface", &entry.interface_capabilities),
            ("connectors", &entry.app_connectors),
        ] {
            if !names.is_empty() {
                contributes.push(format!(
                    "{label} {}",
                    names.iter().map(text::text).collect::<Vec<_>>().join(" ")
                ));
            }
        }
        if !contributes.is_empty() {
            lines.push(format!("{line}\n  {}", contributes.join(", ")));
        } else {
            lines.push(line);
        }
    }
    for disabled in &snapshot.disabled {
        lines.push(format!(
            "[not loaded] {}: {}  {}",
            render::label(disabled.name.as_ref(), "(unnamed)"),
            text::text(&disabled.reason),
            text::text(&disabled.dir)
        ));
    }
    for issue in &snapshot.errors {
        lines.push(format!(
            "[error {}] {}: {}",
            render::wire_tag(&issue.kind),
            text::text(&issue.dir),
            text::text(&issue.message)
        ));
    }
    for issue in &snapshot.warnings {
        lines.push(format!(
            "[warning {}] {}: {}",
            render::wire_tag(&issue.kind),
            text::text(&issue.dir),
            text::text(&issue.message)
        ));
    }
    // A pending refresh is reported because it changes what the rest of this
    // output means: the graph above describes the loaded set, not the ledger, and
    // they have diverged until the endpoint restarts.
    if snapshot.needs_refresh {
        lines.push(
            "[stale] the ledger changed since this graph was loaded; restart to apply".to_string(),
        );
    }
    lines.join("\n")
}

/// A mutation's outcome: what was written, and what it does not yet do.
///
/// `applied_to_current_session` is the field that matters and it is usually
/// false. Plugin activation is endpoint-scoped, so enabling a plugin persists
/// immediately and takes effect on the next run — reporting only "ok" would leave
/// a caller wondering why the skill it just enabled is still missing.
fn mutated(result: &InstalledPluginMutationResult) -> String {
    let mut lines = vec![record(&result.plugin)];
    let mut state = Vec::new();
    state.push(
        if result.persisted {
            "persisted"
        } else {
            "not persisted"
        }
        .to_string(),
    );
    state.push(if result.applied_to_current_session {
        "active in this session".to_string()
    } else {
        format!(
            "not active in this session (applies on {})",
            render::wire_tag(&result.activation_scope)
        )
    });
    if result.restart_required {
        state.push("restart required".to_string());
    }
    lines.push(format!("  {}", state.join(", ")));
    // Only the failures from the post-mutation graph, because the successes are
    // whatever `plugin runtime` would print and are not what this command was
    // asked about.
    for issue in result.runtime.errors.iter().chain(&result.runtime.warnings) {
        lines.push(format!(
            "  [{}] {}: {}",
            render::wire_tag(&issue.kind),
            text::text(&issue.dir),
            text::text(&issue.message)
        ));
    }
    lines.join("\n")
}

fn catalog_listing(catalog: &ProtocolPluginCatalog) -> String {
    if catalog.entries.is_empty() {
        return "(no plugin packages under those roots)".to_string();
    }
    catalog
        .entries
        .iter()
        .map(catalog_entry)
        .collect::<Vec<_>>()
        .join("\n")
}

/// One catalog entry, with what it would contribute if installed. That listing is
/// the point of the catalog: the runtime graph can only describe plugins that are
/// already loaded, so this is the only answer to "what do I get?".
fn catalog_entry(entry: &ProtocolPluginCatalogEntry) -> String {
    let state = match (entry.installed, entry.enabled) {
        (false, _) => "available",
        (true, true) => "installed, enabled",
        (true, false) => "installed, disabled",
    };
    let mut line = format!(
        "{}{}  [{state}]",
        text::text(&entry.id),
        entry
            .version
            .as_ref()
            .map(|version| format!("@{}", text::text(version)))
            .unwrap_or_default()
    );
    if let Some(description) = &entry.description {
        line.push_str(&format!("\n  {}", text::text(description)));
    }
    let contributions = contributions(&entry.contributions);
    if !contributions.is_empty() {
        line.push_str(&format!("\n  {contributions}"));
    }
    line
}

fn contributions(contributions: &ProtocolPluginContributions) -> String {
    [
        ("skills", &contributions.skills),
        ("agents", &contributions.agents),
        ("commands", &contributions.commands),
        ("output styles", &contributions.output_styles),
        ("hooks", &contributions.hooks),
        ("mcp", &contributions.mcp_servers),
        ("lsp", &contributions.lsp_servers),
    ]
    .into_iter()
    .filter(|(_, names)| !names.is_empty())
    .map(|(label, names)| {
        format!(
            "{label}: {}",
            names.iter().map(text::text).collect::<Vec<_>>().join(" ")
        )
    })
    .collect::<Vec<_>>()
    .join("; ")
}

/// `MARKETPLACE=DIR`, split on the first `=` so a Windows-style path keeps its
/// own separators intact.
fn catalog_root(raw: &str) -> CliResult<wire::PluginCatalogRootParam> {
    match raw.split_once('=') {
        Some((marketplace, dir)) if !marketplace.is_empty() && !dir.is_empty() => {
            Ok(wire::PluginCatalogRootParam {
                marketplace: marketplace.to_string(),
                dir: dir.to_string(),
            })
        }
        _ => Err(CliError::usage(format!(
            "--root takes MARKETPLACE=DIR, got `{raw}`"
        ))),
    }
}
