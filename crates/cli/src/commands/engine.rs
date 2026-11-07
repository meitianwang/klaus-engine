//! The engine's endpoint state: config, hooks, permission rules, credentials,
//! and the skill roster compiled into the binary.
//!
//! These commands exist because the facade insists on them. Its own declaration
//! of `config/*` says the file is "written ONLY here — a host that edits the file
//! itself puts the settings page and the running engine on two different sets of
//! bytes", and `hooks/*` says the engine validates the write because its dispatch
//! roster IS the file's schema. A CLI that told the user to open an editor instead
//! would be the escape hatch those methods were declared to close, and it would
//! be a worse one than a raw op: a hand-edited file is wrong on disk, not just on
//! the wire.
//!
//! No session is opened for any of it. Every method here takes no session id
//! because the state is the ENDPOINT's — it outlives every session — so binding
//! one would run discovery, load plugins and start MCP children in order to print
//! a config value. It would also make reading the config depend on a session the
//! config is what configures.
//!
//! Human output and `--json` diverge here in the same way they do everywhere
//! else: `--json` emits what the engine said, re-read through the engine's own
//! JSON writer, while the text below is this crate's to shape.

use klaus_agent_server::app_server::methods::{
    ConfigBatchWrite, ConfigRead, ConfigValueWrite, EngineAddPermissionRules, EngineBuiltinSkills,
    EngineMigrateRollouts, EnginePermissionRules, EngineRemovePermissionRule, HooksRead,
    HooksWrite, ModelCredentialDelete, ModelCredentialRead, ModelCredentialWrite,
};
use klaus_agent_server::app_server::wire;
use serde_json::Value;

use crate::cli::{
    ConfigCommand, CredentialCommand, EngineCommand, Globals, HooksCommand, PermissionCommand,
};
use crate::engine::EndpointClient;
use crate::error::{CliError, CliResult};
use crate::output::Printer;
use crate::session::{json_argument, CliEndpoint};
use crate::text;

pub async fn run(
    endpoint: &CliEndpoint,
    printer: &Printer,
    globals: &Globals,
    command: &EngineCommand,
) -> CliResult<()> {
    let engine = &endpoint.engine;
    match command {
        EngineCommand::Config { command } => config(engine, printer, globals, command).await,
        EngineCommand::Permissions { command } => permissions(engine, printer, command).await,
        EngineCommand::Hooks { command } => hooks(engine, printer, command).await,
        EngineCommand::Credential { command } => credential(engine, printer, command).await,
        EngineCommand::Skills => skills(engine, printer).await,
        EngineCommand::MigrateRollouts {
            apply,
            expected_revisions,
        } => {
            let expected_source_revisions = match expected_revisions {
                Some(raw) => Some(
                    klaus_agent_server::app_server::RawJson::from_json(text::json_line(
                        &json_argument(raw).await?,
                    ))
                    .map_err(|error| CliError::Codec(error.to_string()))?,
                ),
                None => None,
            };
            let result = engine
                .call::<EngineMigrateRollouts>(wire::EngineMigrateRolloutsParams {
                    mode: if *apply {
                        wire::RolloutMigrationMode::Apply
                    } else {
                        wire::RolloutMigrationMode::DryRun
                    },
                    expected_source_revisions,
                })
                .await?;
            printer.raw_json(&result)?;
            printer.line(result.as_str())
        }
    }
}

async fn config(
    engine: &EndpointClient,
    printer: &Printer,
    globals: &Globals,
    command: &ConfigCommand,
) -> CliResult<()> {
    let result = match command {
        // `cwd` is what folds the project file into the resolved view, so it is
        // sent only when asked for: a plain read must describe the user file
        // alone, or a value the repo overrides would look like the user's own.
        ConfigCommand::Show { file, project } => {
            engine
                .call::<ConfigRead>(wire::ConfigReadParams {
                    file_path: file.clone(),
                    cwd: match project {
                        true => Some(CliEndpoint::cwd(globals)?),
                        false => None,
                    },
                    submission_id: None,
                })
                .await?
        }
        ConfigCommand::Set {
            key_path,
            value,
            merge,
            expected_version,
            file,
        } => {
            engine
                .call::<ConfigValueWrite>(wire::ConfigValueWriteParams {
                    key_path: key_path.clone(),
                    value: json_literal(value)?,
                    merge_strategy: merge.map(|merge| merge.as_wire().to_string()),
                    expected_version: expected_version.clone(),
                    file_path: file.clone(),
                    submission_id: None,
                })
                .await?
        }
        ConfigCommand::Apply {
            edits,
            expected_version,
            file,
        } => {
            engine
                .call::<ConfigBatchWrite>(wire::ConfigBatchWriteParams {
                    edits: config_edits(edits).await?,
                    expected_version: expected_version.clone(),
                    file_path: file.clone(),
                    submission_id: None,
                })
                .await?
        }
    };
    printer.declared(&result, config_result)
}

async fn permissions(
    engine: &EndpointClient,
    printer: &Printer,
    command: &PermissionCommand,
) -> CliResult<()> {
    let result = match command {
        PermissionCommand::List => {
            engine
                .call::<EnginePermissionRules>(wire::PermissionRulesParams::default())
                .await?
        }
        PermissionCommand::Add { rules, behavior } => {
            engine
                .call::<EngineAddPermissionRules>(wire::PermissionRulesEditParams {
                    rules: json_document(rules).await?,
                    behavior: behavior.map(|behavior| behavior.as_wire().to_string()),
                    submission_id: None,
                })
                .await?
        }
        PermissionCommand::Remove { rule, behavior } => {
            engine
                .call::<EngineRemovePermissionRule>(wire::PermissionRuleRemoveParams {
                    rule: rule.clone(),
                    behavior: behavior.map(|behavior| behavior.as_wire().to_string()),
                    submission_id: None,
                })
                .await?
        }
    };
    printer.declared(&result, permission_rules)
}

async fn hooks(
    engine: &EndpointClient,
    printer: &Printer,
    command: &HooksCommand,
) -> CliResult<()> {
    let result = match command {
        HooksCommand::Show { file } => {
            engine
                .call::<HooksRead>(wire::HooksReadParams {
                    file_path: file.clone(),
                    submission_id: None,
                })
                .await?
        }
        HooksCommand::Write { hooks, file } => {
            engine
                .call::<HooksWrite>(wire::HooksWriteParams {
                    hooks: json_document(hooks).await?,
                    file_path: file.clone(),
                    submission_id: None,
                })
                .await?
        }
    };
    printer.declared(&result, hooks_result)
}

/// BYO provider keys.
///
/// `show` is the one command in this crate that prints a secret, and it exists
/// because the facade declares a read for exactly that: a host feature that has
/// to call the provider itself. Session auth never takes this path — the engine
/// reads the key out of its own store — so nothing here is on the path that
/// merely USES a key.
async fn credential(
    engine: &EndpointClient,
    printer: &Printer,
    command: &CredentialCommand,
) -> CliResult<()> {
    match command {
        CredentialCommand::Set { id, api_key } => {
            let api_key = match api_key {
                Some(key) => key.clone(),
                // Read rather than required: a key on the command line lands in
                // shell history and in every process listing on the machine.
                None => secret_from_stdin(printer)?,
            };
            let result = engine
                .call::<ModelCredentialWrite>(wire::ModelCredentialWriteParams {
                    id: id.clone(),
                    api_key,
                    submission_id: None,
                })
                .await?;
            printer.declared(&result, |result| acknowledged(result.ok, "stored", id))
        }
        CredentialCommand::Delete { id } => {
            let result = engine
                .call::<ModelCredentialDelete>(wire::ModelCredentialDeleteParams {
                    id: id.clone(),
                    submission_id: None,
                })
                .await?;
            printer.declared(&result, |result| acknowledged(result.ok, "removed", id))
        }
        CredentialCommand::Show { id } => {
            let result = engine
                .call::<ModelCredentialRead>(wire::ModelCredentialReadParams {
                    id: id.clone(),
                    submission_id: None,
                })
                .await?;
            // Printed bare so `$(klaus engine credential show openai)` is the
            // whole key and nothing else.
            printer.declared(&result, |result| match &result.api_key {
                Some(key) => key.clone(),
                None => format!("(no key stored for {id})"),
            })
        }
    }
}

/// The skills compiled into this binary.
///
/// Answerable with no session and no disk read at all, which is the point: it is
/// what this build of the engine can offer before anything configures it, so it
/// is also the baseline a surprising `klaus skill list` is read against.
async fn skills(engine: &EndpointClient, printer: &Printer) -> CliResult<()> {
    let result = engine
        .call::<EngineBuiltinSkills>(wire::EngineBuiltinSkillsParams::default())
        .await?;
    printer.declared(&result, builtin_skills)
}

// ---------------------------------------------------------------------------
// Arguments
// ---------------------------------------------------------------------------

/// A JSON DOCUMENT argument, inline or read from the file the caller named.
///
/// Goes through [`json_argument`] so a rule list, a hook set and a batch of
/// config edits all accept a path — a hook set inlined into one shell word is
/// unreadable — and lands as `serde_json` because that is what the facade's
/// params carry.
async fn json_document(raw: &str) -> CliResult<Value> {
    let parsed = json_argument(raw).await?;
    text::scalar_json(&parsed)
}

/// One config VALUE, which is inline JSON and never a path.
///
/// Deliberately not [`json_document`]: that reads anything not starting with `{`
/// or `[` as a file name, which is exactly wrong here, because a config value is
/// usually a scalar (`"sonnet"`, `true`, `null`).
///
/// Invalid JSON is refused rather than taken as a bare string. `sonnet` and
/// `"sonnet"` are different documents and the engine stores what it is given, so
/// guessing would write the string `"tru"` for a mistyped `true` and report
/// success. The message below is what a shell user needs instead.
fn json_literal(raw: &str) -> CliResult<Value> {
    let parsed = text::parse_json(raw).map_err(|_| {
        CliError::usage(format!(
            "the value must be JSON, so a string needs its quotes ('\"sonnet\"'), not {raw}"
        ))
    })?;
    text::scalar_json(&parsed)
}

/// The edits of a batch write, which must be an ARRAY of edit objects.
///
/// Decoded into the declared type rather than passed through as JSON, so a
/// mistyped key is reported here, naming the field, instead of arriving as an
/// engine-side rejection of the whole batch.
async fn config_edits(raw: &str) -> CliResult<Vec<wire::ConfigEditParam>> {
    let value = json_document(raw).await?;
    serde_json::from_value(value).map_err(|error| {
        CliError::usage(format!(
            "the edits must be a JSON array of {{keyPath, value, mergeStrategy?}}: {error}"
        ))
    })
}

/// One line from the terminal, without echoing a prompt into stdout.
fn secret_from_stdin(printer: &Printer) -> CliResult<String> {
    printer.prompt("api key: ")?;
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .map_err(|error| CliError::usage(format!("cannot read the key: {error}")))?;
    let key = line.trim().to_string();
    if key.is_empty() {
        return Err(CliError::usage("no key was given".to_string()));
    }
    Ok(key)
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// A config read or write. Both answer with the same shape, deliberately, so a
/// write shows what it landed on instead of the caller re-reading to find out.
fn config_result(result: &wire::ConfigResult) -> String {
    let mut lines = vec![
        format!("{} (version {})", result.path, result.version),
        "stored:".to_string(),
        indent(&pretty(&result.values)),
        "resolved:".to_string(),
        indent(&pretty(&result.resolved)),
    ];
    // Rendered as key/layer pairs rather than as JSON because this is the field
    // a caller reads to answer "why did my write do nothing" — the layer that
    // won is the answer, and one per line is how it is scanned.
    lines.push("sources:".to_string());
    lines.push(indent(&sources(&result.sources)));
    lines.join("\n")
}

fn sources(sources: &Value) -> String {
    match sources.as_object() {
        Some(map) if !map.is_empty() => map
            .iter()
            .map(|(key, layer)| format!("{key}: {}", scalar(layer)))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => "(none)".to_string(),
    }
}

fn permission_rules(result: &wire::PermissionRulesResult) -> String {
    [
        ("allow", &result.allow),
        ("deny", &result.deny),
        ("ask", &result.ask),
    ]
    .iter()
    .map(|(class, rules)| format!("{class}:\n{}", indent(&rule_list(rules))))
    .collect::<Vec<_>>()
    .join("\n")
}

fn rule_list(rules: &[String]) -> String {
    match rules.is_empty() {
        true => "(none)".to_string(),
        false => rules.join("\n"),
    }
}

fn hooks_result(result: &wire::HooksResult) -> String {
    // The event roster ships with every reply because it is the engine's own
    // dispatch list: it is what a write is validated against, so a caller
    // composing one reads the valid names off the same reply.
    format!(
        "{}\n{}\nevents the engine can fire:\n{}",
        result.path,
        indent(&pretty(&result.hooks)),
        indent(&match result.events.is_empty() {
            true => "(none)".to_string(),
            false => result.events.join("\n"),
        })
    )
}

fn builtin_skills(result: &wire::EngineBuiltinSkillsResult) -> String {
    if result.builtin_skills.is_empty() {
        return "(no built-in skills)".to_string();
    }
    result
        .builtin_skills
        .iter()
        .map(builtin_skill)
        .collect::<Vec<_>>()
        .join("\n")
}

fn builtin_skill(skill: &wire::BuiltinSkillRow) -> String {
    let mut line = format!("{}  {}", skill.name, skill.description);
    // Only the user-invocable ones are typeable as a command, so the flag is
    // shown where it is false rather than everywhere.
    if !skill.user_invocable {
        line.push_str("  [model only]");
    }
    if let Some(hint) = &skill.argument_hint {
        line.push_str(&format!("  <{hint}>"));
    }
    match &skill.when_to_use {
        Some(when) => format!("{line}\n{}", indent(when)),
        None => line,
    }
}

/// A bare `{ ok }` acknowledgement, as the sentence the caller was asking for.
/// A false `ok` is reported rather than dropped: the engine answered, so this is
/// not an error, but it did not do the thing either.
fn acknowledged(ok: bool, verb: &str, id: &str) -> String {
    match ok {
        true => format!("{verb} the key for {id}"),
        false => format!("the engine did not {verb} the key for {id}"),
    }
}

/// A JSON blob for reading. Written through the engine's own JSON writer rather
/// than serde's so the human view and `--json` cannot spell the same value two
/// different ways.
fn pretty(value: &Value) -> String {
    match text::from_wire(value) {
        Ok(json) => text::json_pretty(&json),
        Err(error) => format!("(cannot render: {error})"),
    }
}

/// A JSON scalar as one word, for a field that is a bare string in practice.
fn scalar(value: &Value) -> String {
    match value.as_str() {
        Some(text) => text.to_string(),
        None => value.to_string(),
    }
}

fn indent(block: &str) -> String {
    block
        .lines()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}
