//! The command surface.
//!
//! One binary, many subcommands, like every other engine CLI: a single `klaus`
//! reaches the whole protocol instead of a family of executables that each
//! re-derive endpoint setup. The shape of this file follows the protocol's own
//! shape — the noun groups (`session`, `mcp`, `plugin`, `queue`, `memory`) are the
//! op families, not invented product areas, so a new op lands in an obvious place
//! and nothing has to be renamed to make room.
//!
//! Global options are session-invariant: they configure the endpoint and the
//! session init, which is exactly what cannot be changed after a session is
//! bound. Per-command options are the ones the op itself carries.

use clap::{Args, Parser, Subcommand, ValueEnum};

use klaus_agent_core::engine::rewind::RewindScope;
use klaus_agent_core::hooks::setup::SetupTrigger;
use klaus_agent_core::memory::agent_memory::AgentMemoryMergeChoice;
use klaus_agent_core::schemas::rollout::SessionRole;
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_protocol::{AgentMemorySnapshotUnavailableReason, QueuePriority, SteerPriority};

#[derive(Parser, Debug)]
#[command(
    name = "klaus",
    version,
    about = "Terminal client for the Klaus agent engine",
    long_about = "Drives a Klaus engine over its own Op/Event protocol.\n\nBy default the engine runs in this process and the protocol travels over an \
in-memory duplex, so there is no daemon to start and no socket to secure. \
`--remote ws://…` points the same client at an already-served engine instead.",
    // The bare prompt is a REPL shorthand, so a token that names a subcommand is
    // read as that subcommand. NOT `args_conflicts_with_subcommands`: the global
    // options are flattened in as top-level args, and that setting would make
    // every one of them conflict with every subcommand — `klaus --json task list`
    // would be refused.
    subcommand_negates_reqs = true,
    args_override_self = true
)]
pub struct Cli {
    #[command(flatten)]
    pub globals: Globals,

    /// Prompt to open the REPL with. Shorthand for `klaus repl <PROMPT>`.
    #[arg(value_name = "PROMPT")]
    pub prompt: Option<String>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Options that configure the endpoint and the session init. Every one of these
/// is fixed for the life of a session, which is why they are global rather than
/// per-command: a command cannot change them after the session is bound.
#[derive(Args, Debug, Clone, Default)]
pub struct Globals {
    /// Working directory the session runs in. Defaults to the process cwd.
    #[arg(short = 'C', long = "cd", value_name = "DIR", global = true)]
    pub cwd: Option<String>,

    /// Model id for the session.
    #[arg(short = 'm', long, value_name = "MODEL", global = true)]
    pub model: Option<String>,

    /// Provider backend: anthropic (default), openai, gemini. Selecting the
    /// backend is the endpoint's decision, so a served engine owns its own.
    #[arg(long, value_name = "NAME", global = true, conflicts_with = "remote")]
    pub provider: Option<String>,

    /// Deprecated permission preset, translated to explicit policy fields.
    #[arg(
        short = 'p',
        long = "permission-mode",
        value_enum,
        global = true,
        hide = true
    )]
    pub permission_mode: Option<PermissionModeArg>,

    /// Filesystem access policy for the session.
    #[arg(long = "sandbox", value_enum, global = true)]
    pub sandbox_mode: Option<SandboxModeArg>,

    /// Whether operations outside policy may request approval.
    #[arg(long = "ask-for-approval", value_enum, global = true)]
    pub approval_policy: Option<ApprovalPolicyArg>,

    /// Who resolves eligible approval requests.
    #[arg(long = "approvals-reviewer", value_enum, global = true)]
    pub approvals_reviewer: Option<ApprovalsReviewerArg>,

    /// Engine config object, as JSON. Merged under the other flags.
    #[arg(long = "config-json", value_name = "JSON", global = true)]
    pub config_json: Option<String>,

    /// Enable goal mode: the goal tools, plus the automatic continuation of an
    /// active goal after every turn. Off by default in the engine because a
    /// continuation spends tokens without the user asking again, so it has to be
    /// asked for per session. Implied by the `goal` commands, which administer
    /// the runtime this builds.
    #[arg(long = "goal-mode", global = true)]
    pub goal_mode: bool,

    /// Directory the rollout store writes threads to.
    #[arg(
        long = "session-dir",
        value_name = "DIR",
        global = true,
        conflicts_with = "remote"
    )]
    pub session_dir: Option<String>,

    /// Installed-plugin home the engine reads its ledger from.
    #[arg(
        long = "plugin-home",
        value_name = "DIR",
        global = true,
        conflicts_with = "remote"
    )]
    pub plugin_home: Option<String>,

    /// Skip installed-plugin loading for this run.
    #[arg(long = "no-plugins", global = true, conflicts_with = "remote")]
    pub no_plugins: bool,

    /// Drive an already-served engine (`ws://host:port`) instead of running one
    /// in this process.
    #[arg(long, value_name = "URL", global = true)]
    pub remote: Option<String>,

    /// Bearer token for `--remote`.
    #[arg(long, value_name = "TOKEN", global = true, requires = "remote")]
    pub token: Option<String>,

    /// Bind to a persisted thread instead of starting a new one.
    #[arg(long, value_name = "THREAD_ID", global = true)]
    pub resume: Option<String>,

    /// Start a new thread seeded from a persisted thread's history.
    #[arg(
        long,
        value_name = "THREAD_ID",
        global = true,
        conflicts_with = "resume"
    )]
    pub fork: Option<String>,

    /// Emit the engine's wire JSON on stdout, one value per line, instead of
    /// human text.
    #[arg(long, global = true)]
    pub json: bool,

    /// Do not read skills, sub-agents, hooks or MCP servers from disk.
    #[arg(long = "no-discovery", global = true)]
    pub no_discovery: bool,

    /// Extra skill directory. Repeatable; searched after the standard roots.
    #[arg(long = "skills-dir", value_name = "DIR", global = true)]
    pub skills_dir: Vec<String>,

    /// Sub-agent definition directory. Repeatable.
    #[arg(long = "agents-dir", value_name = "DIR", global = true)]
    pub agents_dir: Vec<String>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Interactive session (the default when no subcommand is given).
    Repl {
        /// First prompt to submit.
        #[arg(value_name = "PROMPT")]
        prompt: Option<String>,
    },

    /// Run one turn and exit.
    Exec(ExecArgs),

    /// Ask a question that does not touch the conversation history.
    Ask {
        /// The question.
        #[arg(value_name = "QUESTION")]
        question: String,
    },

    /// Persisted threads.
    Session {
        #[command(subcommand)]
        command: SessionCommand,
    },

    /// MCP servers, tools, resources and prompts.
    Mcp {
        #[command(subcommand)]
        command: McpCommand,
    },

    /// Installed plugins and marketplace packages.
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },

    /// Skills visible to the session.
    Skill {
        #[command(subcommand)]
        command: ListOnly,
    },

    /// Sub-agent definitions visible to the session.
    Agent {
        #[command(subcommand)]
        command: ListOnly,
    },

    /// Command hooks visible to the session.
    Hook {
        #[command(subcommand)]
        command: ListOnly,
    },

    /// The thread's goal.
    Goal {
        #[command(subcommand)]
        command: GoalCommand,
    },

    /// The steering queue.
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },

    /// Agent memory and dreaming.
    Memory {
        #[command(subcommand)]
        command: MemoryCommand,
    },

    /// The engine's task list.
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },

    /// Setup hooks.
    Setup {
        #[command(subcommand)]
        command: SetupCommand,
    },

    /// Background sub-agents.
    Subagent {
        #[command(subcommand)]
        command: SubagentCommand,
    },

    /// Compact the session's conversation history.
    Compact {
        /// Extra instructions for the compaction turn.
        #[arg(long, value_name = "TEXT")]
        instructions: Option<String>,
    },

    /// Run an isolated code review against the session.
    Review(ReviewArgs),

    /// Undo user turns, their file edits, or both.
    Rewind {
        /// How many user turns to undo.
        #[arg(long, default_value_t = 1, value_name = "N")]
        turns: u32,
        #[arg(long, value_enum)]
        scope: Option<RewindScopeArg>,
        /// Report which files the undo would touch, and undo nothing.
        #[arg(long)]
        dry_run: bool,
    },

    /// Token and money spend for the session.
    Cost,

    /// Context-window occupancy for the session.
    Context,

    /// The session's effective runtime capability resolution.
    Capabilities,

    /// The session's effective extension graph (skills, agents, hooks, MCP).
    Extensions,

    /// The session's message history.
    History,

    /// The files the session changed, folded per file.
    ///
    /// The session's own record, not the working tree: it answers outside a git
    /// repository and reports nothing the session did not do itself.
    Changes,

    /// Endpoint state: what the engine keeps outside any session.
    ///
    /// Its own noun group because none of it is a session's. The config file, the
    /// hooks file, the durable permission rules and the credential store outlive
    /// every session and are read and written with no session id at all — so
    /// these commands never start one, which also means reading the config does
    /// not depend on a session the config is what configures.
    Engine {
        #[command(subcommand)]
        command: EngineCommand,
    },

    /// Sign in to a subscription, so `--model subscription:…` / `codex:…` /
    /// `qoder:…` works.
    ///
    /// The credential lands where every client on this machine reads it, so one
    /// sign-in serves the CLI and the desktop app rather than each keeping its
    /// own. A BYO API key is the other way to give a session a model, and it is
    /// set with `klaus engine credential set`.
    Login {
        /// Which subscription. Defaults to Claude.
        #[arg(value_enum, default_value_t = AuthSource::Claude)]
        source: AuthSource,
    },

    /// Forget a stored subscription sign-in.
    Logout {
        /// Which subscription. Defaults to Claude.
        #[arg(value_enum, default_value_t = AuthSource::Claude)]
        source: AuthSource,
    },

    /// Which subscriptions this machine is signed in to.
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },

    /// Speak the raw protocol: ops in on stdin, events out on stdout.
    Proto,

    /// Serve this endpoint over a loopback WebSocket for other clients.
    Serve(ServeArgs),

    /// Serve this endpoint over the JSON-RPC app-server facade on stdio.
    AppServer {
        /// Print the facade's request/notification schemas and exit.
        #[arg(long)]
        schema: bool,
    },

    /// Print a shell completion script.
    Completion {
        /// The shell to generate for.
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

#[derive(Args, Debug)]
pub struct ExecArgs {
    /// The prompt. Omit to read it from stdin.
    #[arg(value_name = "PROMPT")]
    pub prompt: Option<String>,

    /// Read the prompt from a file. `-` reads stdin.
    #[arg(short = 'i', long = "input-file", value_name = "FILE")]
    pub input_file: Option<String>,

    /// Write the turn's final assistant message to this file.
    #[arg(long = "output-last-message", value_name = "FILE")]
    pub output_last_message: Option<String>,

    /// Answer every approval request with allow. Required when stdin is not a
    /// terminal and the run may ask.
    #[arg(long = "auto-approve")]
    pub auto_approve: bool,

    /// Extra system prompt text for this turn only.
    #[arg(long = "append-system-prompt", value_name = "TEXT")]
    pub append_system_prompt: Option<String>,

    /// JSON Schema the turn's structured output must satisfy. A value starting
    /// with `{` is the schema; anything else is a path to it.
    #[arg(long = "output-schema", value_name = "JSON|FILE")]
    pub output_schema: Option<String>,
}

#[derive(Args, Debug)]
pub struct ReviewArgs {
    /// Review the working tree (default).
    #[arg(long, conflicts_with_all = ["base_branch", "commit", "files", "instructions"])]
    pub uncommitted: bool,

    /// Review this branch's changes against the given base.
    #[arg(long = "base-branch", value_name = "BRANCH")]
    pub base_branch: Option<String>,

    /// Review one commit.
    #[arg(long, value_name = "SHA")]
    pub commit: Option<String>,

    /// Review explicit paths. Repeatable.
    #[arg(long, value_name = "PATH")]
    pub files: Vec<String>,

    /// Review against free-form instructions.
    #[arg(long, value_name = "TEXT")]
    pub instructions: Option<String>,

    /// Label shown to the user for this review.
    #[arg(long, value_name = "TEXT")]
    pub hint: Option<String>,
}

#[derive(Args, Debug)]
pub struct ServeArgs {
    /// Bind host. Defaults to loopback; a non-loopback host requires `--token`.
    #[arg(long, value_name = "HOST")]
    pub host: Option<String>,

    /// Bind port. `0` picks an ephemeral one.
    #[arg(long, value_name = "PORT")]
    pub port: Option<u16>,

    /// Bearer token clients must present.
    #[arg(long, value_name = "TOKEN")]
    pub token: Option<String>,

    /// Web origin allowed to connect. Repeatable.
    #[arg(long = "allowed-origin", value_name = "ORIGIN")]
    pub allowed_origins: Vec<String>,
}

/// A noun group whose only op is a listing.
#[derive(Subcommand, Debug)]
pub enum ListOnly {
    /// Show the effective set.
    List,
}

#[cfg(test)]
mod rollout_maintenance_tests {
    use super::*;

    #[test]
    fn stored_history_migration_previews_unless_apply_is_explicit() {
        let preview = Cli::try_parse_from(["klaus", "engine", "migrate-rollouts"]).unwrap();
        assert!(matches!(
            preview.command,
            Some(Command::Engine {
                command: EngineCommand::MigrateRollouts {
                    apply: false,
                    expected_revisions: None
                }
            })
        ));
        let apply =
            Cli::try_parse_from(["klaus", "engine", "migrate-rollouts", "--apply"]).unwrap();
        assert!(matches!(
            apply.command,
            Some(Command::Engine {
                command: EngineCommand::MigrateRollouts { apply: true, .. }
            })
        ));
        assert!(Cli::try_parse_from([
            "klaus",
            "engine",
            "migrate-rollouts",
            "--expected-revisions",
            "{}"
        ])
        .is_err());
    }
}

#[derive(Subcommand, Debug)]
pub enum EngineCommand {
    /// Preview stored-history migration; only --apply changes files.
    MigrateRollouts {
        #[arg(long)]
        apply: bool,
        /// Per-path source revisions from a previous preview, as JSON or a JSON file.
        #[arg(long, value_name = "JSON_OR_PATH", requires = "apply")]
        expected_revisions: Option<String>,
    },

    /// The engine's config file: what it stores, and what a session resolves.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },

    /// The durable permission rules the engine evaluates tool calls against.
    Permissions {
        #[command(subcommand)]
        command: PermissionCommand,
    },

    /// The hooks file. Distinct from `klaus hook list`, which shows the hooks a
    /// SESSION resolved from every layer; this is the one file the engine owns.
    Hooks {
        #[command(subcommand)]
        command: HooksCommand,
    },

    /// Stored API keys for BYO model providers.
    Credential {
        #[command(subcommand)]
        command: CredentialCommand,
    },

    /// Skills compiled into this binary. Distinct from `klaus skill list`, which
    /// shows what a session resolved, including skills found on disk.
    Skills,
}

#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Print the file as stored, the view a session resolves, and which layer
    /// won each key.
    Show {
        /// Read a config file other than the user's.
        #[arg(long, value_name = "PATH")]
        file: Option<String>,
        /// Fold the working directory's project config into the resolved view,
        /// so the output marks the keys a repo takes over.
        #[arg(long)]
        project: bool,
    },

    /// Write one key.
    Set {
        /// Dotted key path. Quote a segment that itself contains dots.
        #[arg(value_name = "KEY_PATH")]
        key_path: String,
        /// The value, as JSON: a string needs its quotes (`'"sonnet"'`).
        /// `null` deletes the key — TOML has no null.
        #[arg(value_name = "JSON")]
        value: String,
        /// How the value combines with what is already there.
        #[arg(long = "merge", value_enum)]
        merge: Option<ConfigMergeArg>,
        /// The `version` the last read reported. A mismatch refuses the write
        /// instead of overwriting whatever landed in between.
        #[arg(long = "expected-version", value_name = "VERSION")]
        expected_version: Option<String>,
        /// Write a config file other than the user's.
        #[arg(long, value_name = "PATH")]
        file: Option<String>,
    },

    /// Apply several edits at once, all or nothing.
    Apply {
        /// The edits as JSON, or a path to a JSON file:
        /// `[{"keyPath": "model", "value": "sonnet"}]`.
        #[arg(value_name = "JSON")]
        edits: String,
        #[arg(long = "expected-version", value_name = "VERSION")]
        expected_version: Option<String>,
        #[arg(long, value_name = "PATH")]
        file: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum PermissionCommand {
    /// Every stored rule, by the class it belongs to.
    List,

    /// Append rules to one class.
    Add {
        /// The rules, as JSON or a path to a JSON file:
        /// `[{"toolName": "Bash", "argumentPrefix": "git status"}]`.
        #[arg(value_name = "JSON")]
        rules: String,
        #[arg(long, value_enum)]
        behavior: Option<PermissionBehaviorArg>,
    },

    /// Drop one rule, named as the engine stores it (`Read`, `Bash(git status)`).
    Remove {
        /// The rule string to drop.
        #[arg(value_name = "RULE")]
        rule: String,
        #[arg(long, value_enum)]
        behavior: Option<PermissionBehaviorArg>,
    },
}

#[derive(Subcommand, Debug)]
pub enum HooksCommand {
    /// The file as stored, plus every event the engine can fire.
    Show {
        /// Read a hooks file other than the user's.
        #[arg(long, value_name = "PATH")]
        file: Option<String>,
    },

    /// Replace the whole file. `{}` clears it, so a removed event really goes.
    Write {
        /// The hook set as JSON, or a path to a JSON file.
        #[arg(value_name = "JSON")]
        hooks: String,
        #[arg(long, value_name = "PATH")]
        file: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum CredentialCommand {
    /// Store a provider's API key.
    Set {
        /// The provider id, as `[modelProviders]` names it.
        #[arg(value_name = "ID")]
        id: String,
        /// The key. Omit to read it from stdin, which keeps it out of shell
        /// history and out of the process table.
        #[arg(long = "api-key", value_name = "KEY")]
        api_key: Option<String>,
    },

    /// Drop a provider's stored key.
    Delete {
        /// The provider id whose key to forget.
        #[arg(value_name = "ID")]
        id: String,
    },

    /// Print a provider's stored key.
    Show {
        /// The provider id to read back.
        #[arg(value_name = "ID")]
        id: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum SessionCommand {
    /// Page through persisted threads.
    List {
        /// How many threads per page.
        #[arg(long, value_name = "N")]
        limit: Option<usize>,
        /// Continue from the cursor the previous page ended on.
        #[arg(long, value_name = "CURSOR")]
        cursor: Option<String>,
        /// Include archived threads.
        #[arg(long = "include-archived")]
        include_archived: bool,
        /// Thread role to list.
        #[arg(long, value_enum)]
        role: Option<SessionRoleArg>,
    },
    /// Bind a thread and print its history.
    Show {
        /// The thread to read.
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
    },
    /// Branch a new thread from a persisted one.
    Fork {
        /// The thread to branch from.
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
        /// Thread id the branch must adopt.
        #[arg(long = "new-thread-id", value_name = "THREAD_ID")]
        new_thread_id: Option<String>,
        /// Inherit only history strictly before this message.
        #[arg(long = "before-message", value_name = "UUID")]
        before_message: Option<String>,
        /// Copy the source goal and defer automatic work until an explicit turn.
        #[arg(long = "inherit-goal")]
        inherit_goal: bool,
    },
    /// Open a side task bound to a persisted thread.
    SideTask {
        /// The thread the side task belongs to.
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
        /// Thread id the side task must adopt.
        #[arg(long = "new-thread-id", value_name = "THREAD_ID")]
        new_thread_id: Option<String>,
    },
    /// Hide a thread from the default listing.
    Archive {
        /// The thread to archive.
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
    },
    /// Return an archived thread to the default listing.
    Unarchive {
        /// The thread to unarchive.
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
    },
    /// Erase a thread from the store.
    Delete {
        /// The thread to delete.
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
    },
    /// Change a thread's id.
    Rename {
        /// The thread to rename.
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
        /// The id it takes instead.
        #[arg(value_name = "NEW_THREAD_ID")]
        new_thread_id: String,
    },
    /// Set a thread's title, or have the engine write one.
    Title {
        /// The thread to title.
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
        /// The title to set.
        #[arg(value_name = "TITLE", conflicts_with = "generate")]
        title: Option<String>,
        /// Have the engine generate the title for the bound session.
        #[arg(long)]
        generate: bool,
        /// With `--generate`, persist the result.
        #[arg(long, requires = "generate")]
        persist: bool,
        /// With `--generate`, extra instructions for the title turn.
        #[arg(long, value_name = "TEXT", requires = "generate")]
        instructions: Option<String>,
    },
    /// Replace a thread's host metadata object.
    Metadata {
        /// The thread whose metadata is replaced.
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
        /// The metadata object as JSON. Omit to clear it.
        #[arg(value_name = "JSON")]
        metadata: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum McpCommand {
    /// The live MCP runtime: servers, their status and their tools.
    List,
    /// One server's runtime entry.
    Get {
        /// The server, by the name it was declared under.
        #[arg(value_name = "SERVER")]
        server: String,
    },
    /// Declare a server for this session (and connect it).
    Add(McpAddArgs),
    /// Drop a server from this session.
    Remove {
        /// The server to drop.
        #[arg(value_name = "SERVER")]
        server: String,
    },
    /// Reconnect one server.
    Reconnect {
        /// The server to reconnect.
        #[arg(value_name = "SERVER")]
        server: String,
    },
    /// Replace the session's whole server set with what is on disk.
    Reload,
    /// Call a tool.
    Call {
        /// The server offering the tool.
        #[arg(value_name = "SERVER")]
        server: String,
        /// The tool to call.
        #[arg(value_name = "TOOL")]
        tool: String,
        /// Tool arguments as a JSON object.
        #[arg(long, value_name = "JSON")]
        arguments: Option<String>,
    },
    /// Read a resource.
    Resource {
        /// The server holding the resource.
        #[arg(value_name = "SERVER")]
        server: String,
        /// The resource uri.
        #[arg(value_name = "URI")]
        uri: String,
    },
    /// List every server's prompts.
    Prompts,
    /// Render one prompt.
    Prompt {
        /// The server offering the prompt.
        #[arg(value_name = "SERVER")]
        server: String,
        /// The prompt to render.
        #[arg(value_name = "NAME")]
        name: String,
        /// Prompt arguments as a JSON object.
        #[arg(long, value_name = "JSON")]
        arguments: Option<String>,
    },
    /// OAuth state for a server.
    AuthStatus {
        /// The server to report on.
        #[arg(value_name = "SERVER")]
        server: String,
    },
    /// Forget a server's OAuth tokens.
    AuthRevoke {
        /// The server whose tokens are forgotten.
        #[arg(value_name = "SERVER")]
        server: String,
    },
}

#[derive(Args, Debug)]
pub struct McpAddArgs {
    /// Name the server is declared and referred to under.
    #[arg(value_name = "NAME")]
    pub name: String,

    /// How to reach the server.
    #[arg(long, value_enum, default_value_t = McpTransportArg::Stdio)]
    pub transport: McpTransportArg,

    /// Executable, for `--transport stdio`.
    #[arg(long, value_name = "COMMAND")]
    pub command: Option<String>,

    /// Argument for the executable. Repeatable, order preserved.
    #[arg(long = "arg", value_name = "ARG")]
    pub args: Vec<String>,

    /// `KEY=VALUE` added to the subprocess environment. Repeatable.
    #[arg(long = "env", value_name = "KEY=VALUE")]
    pub env: Vec<String>,

    /// Endpoint, for `--transport http` or `sse`.
    #[arg(long, value_name = "URL")]
    pub url: Option<String>,

    /// `KEY=VALUE` request header. Repeatable.
    #[arg(long = "header", value_name = "KEY=VALUE")]
    pub headers: Vec<String>,

    /// Advertise OAuth for this server.
    #[arg(long)]
    pub oauth: bool,

    /// OAuth scope to request. Repeatable; implies `--oauth`.
    #[arg(long = "scope", value_name = "SCOPE")]
    pub scopes: Vec<String>,

    /// Load this server's tools eagerly instead of on demand.
    #[arg(long = "always-load")]
    pub always_load: bool,

    /// Tool to hide from the model. Repeatable.
    #[arg(long = "disabled-tool", value_name = "TOOL")]
    pub disabled_tools: Vec<String>,
}

#[derive(Subcommand, Debug)]
pub enum PluginCommand {
    /// The persisted installed-plugin ledger.
    List,
    /// The live plugin graph and its load diagnostics.
    Runtime,
    /// Turn an installed plugin on.
    Enable {
        /// The plugin, by its ledger name.
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// Turn an installed plugin off, leaving it installed.
    Disable {
        /// The plugin, by its ledger name.
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// Install a package from a directory already on disk.
    Install {
        /// Marketplace the package is attributed to.
        #[arg(long, value_name = "NAME")]
        marketplace: String,
        /// Package directory.
        #[arg(long = "source-dir", value_name = "DIR")]
        source_dir: String,
        /// Version to record in the ledger.
        #[arg(long, value_name = "VERSION")]
        version: Option<String>,
    },
    /// Remove a plugin from the ledger.
    Uninstall {
        /// The plugin, by its ledger name.
        #[arg(value_name = "NAME")]
        name: String,
        /// Also delete the installed files.
        #[arg(long = "remove-files")]
        remove_files: bool,
    },
    /// Read marketplace roots and join them with the ledger.
    Catalog {
        /// `MARKETPLACE=DIR` root to read. Repeatable.
        #[arg(long = "root", value_name = "MARKETPLACE=DIR", required = true)]
        roots: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum GoalCommand {
    /// The thread's goal, if it has one.
    Get,
    /// Create the thread's goal, or edit the one it has.
    Set {
        /// The objective. Creating a goal requires it; editing one does not.
        #[arg(value_name = "OBJECTIVE")]
        objective: Option<String>,
        /// Goal status (`active`, `paused`, `complete`, `blocked`).
        #[arg(long, value_name = "STATUS")]
        status: Option<String>,
        /// Tokens the goal may spend before it stops.
        #[arg(long = "token-budget", value_name = "N")]
        token_budget: Option<i64>,
        /// Remove the token budget.
        #[arg(long = "clear-token-budget", conflicts_with = "token_budget")]
        clear_token_budget: bool,
    },
    /// Drop the thread's goal.
    Clear,
}

#[derive(Subcommand, Debug)]
pub enum QueueCommand {
    /// Queued steering messages.
    List,
    /// Add a message to the queue. A turn already running takes it up mid-flight;
    /// otherwise it waits for the next one. The queue is part of the thread, so a
    /// message queued now is still there on a later `--resume`.
    Add {
        /// The message text.
        #[arg(value_name = "TEXT")]
        text: String,
        /// Where in the queue it lands.
        #[arg(long, value_enum)]
        priority: Option<QueuePriorityArg>,
    },
    /// Drain the pending set (destructive: the engine hands them over once).
    Pending,
    /// The revision-stamped queue snapshot mutations are checked against.
    Snapshot,
    /// Change a queued message. Only the fields passed are touched.
    Edit {
        /// Which queued message, by the uuid `queue snapshot` reports.
        #[arg(value_name = "UUID")]
        uuid: String,
        /// Snapshot revision this edit was decided against.
        #[arg(long, value_name = "N")]
        revision: u64,
        /// Replacement text.
        #[arg(long, value_name = "TEXT")]
        text: Option<String>,
        /// Permission mode to run this message under.
        #[arg(long, value_name = "MODE")]
        mode: Option<String>,
        /// Move it to this priority.
        #[arg(long, value_enum)]
        priority: Option<QueuePriorityArg>,
    },
    /// Drop a queued message.
    Remove {
        /// Which queued message, by the uuid `queue snapshot` reports.
        #[arg(value_name = "UUID")]
        uuid: String,
        /// Snapshot revision this removal was decided against.
        #[arg(long, value_name = "N")]
        revision: u64,
    },
    /// Move a queued message within the queue.
    Reorder {
        /// Which queued message, by the uuid `queue snapshot` reports.
        #[arg(value_name = "UUID")]
        uuid: String,
        /// Snapshot revision this move was decided against.
        #[arg(long, value_name = "N")]
        revision: u64,
        /// Move it before this message. Omit to move it last.
        #[arg(long, value_name = "UUID")]
        before: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum MemoryCommand {
    /// The memory/dream maintenance audit snapshot.
    Status,
    /// Run a dream pass now.
    Dream {
        /// Wait for the pass to finish instead of returning once it started.
        #[arg(long)]
        wait: bool,
    },
    /// Interactive memory-snapshot decisions.
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum SnapshotCommand {
    /// Every recorded decision.
    List,
    /// One decision by request id.
    Get {
        /// The decision, by the request id the engine raised it under.
        #[arg(value_name = "REQUEST_ID")]
        request_id: String,
    },
    /// Answer a pending decision.
    Resolve {
        /// The decision to answer.
        #[arg(value_name = "REQUEST_ID")]
        request_id: String,
        /// How to merge the snapshot.
        #[arg(value_enum)]
        choice: MergeChoiceArg,
    },
    /// Settle a pending decision that cannot be answered.
    Settle {
        /// The decision to settle.
        #[arg(value_name = "REQUEST_ID")]
        request_id: String,
        /// Why no answer is coming.
        #[arg(value_enum)]
        reason: SnapshotUnavailableArg,
        /// Detail to record alongside the reason.
        #[arg(long, value_name = "TEXT")]
        error: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum TaskCommand {
    /// Page through the engine's task records.
    List {
        /// How many tasks per page.
        #[arg(long, value_name = "N")]
        limit: Option<u32>,
        /// Continue from the cursor the previous page ended on.
        #[arg(long, value_name = "CURSOR")]
        cursor: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum SetupCommand {
    /// Dispatch the setup hooks.
    Run {
        /// Which lifecycle point to dispatch for.
        #[arg(long, value_enum, default_value_t = SetupTriggerArg::Init)]
        trigger: SetupTriggerArg,
        /// Correlation id for this run. Defaults to a fresh uuid.
        #[arg(long = "request-id", value_name = "ID")]
        request_id: Option<String>,
    },
    /// The setup lifecycle snapshot.
    Status,
}

#[derive(Subcommand, Debug)]
pub enum SubagentCommand {
    /// Live background sub-agents.
    List,
    /// The core-owned sub-agent lifecycle snapshot.
    Lifecycle,
    /// One sub-agent's transcript.
    Transcript {
        #[arg(long = "thread-id", value_name = "THREAD_ID")]
        thread_id: String,
        #[arg(long = "agent-id", value_name = "AGENT_ID")]
        agent_id: String,
    },
}

// ---------------------------------------------------------------------------
// Value enums.
//
// Each spells its wire tag as the primary name, with the kebab-case spelling as
// an alias. The wire tag has to be accepted because it is what the engine, the
// docs and `--json` output all use; the alias exists because a shell user types
// kebab-case by reflex. Accepting both is cheaper than making either one wrong.
// ---------------------------------------------------------------------------

/// Which subscription a sign-in command acts on.
///
/// Named after the account a person has, not after the wire it ends up on: a user
/// signs in to Claude or to ChatGPT, and which provider kind that becomes is the
/// endpoint's business.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum AuthSource {
    /// A Claude subscription (Pro/Max), used by `--model subscription:…`.
    #[value(name = "claude")]
    Claude,
    /// A ChatGPT subscription through Codex, used by `--model codex:…`.
    #[value(name = "codex")]
    Codex,
    /// A Qoder subscription, used by `--model qoder:…`.
    #[value(name = "qoder")]
    Qoder,
}

#[derive(Subcommand, Debug)]
pub enum AuthCommand {
    /// Report each subscription as signed in, signed out or expired.
    Status,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum SandboxModeArg {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

impl From<SandboxModeArg> for klaus_agent_core::types::permission::SandboxMode {
    fn from(value: SandboxModeArg) -> Self {
        match value {
            SandboxModeArg::ReadOnly => Self::ReadOnly,
            SandboxModeArg::WorkspaceWrite => Self::WorkspaceWrite,
            SandboxModeArg::DangerFullAccess => Self::DangerFullAccess,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ApprovalPolicyArg {
    #[value(name = "untrusted")]
    UnlessTrusted,
    OnRequest,
    Never,
}

impl From<ApprovalPolicyArg> for klaus_agent_core::types::permission::ApprovalPolicy {
    fn from(value: ApprovalPolicyArg) -> Self {
        match value {
            ApprovalPolicyArg::UnlessTrusted => Self::UnlessTrusted,
            ApprovalPolicyArg::OnRequest => Self::OnRequest,
            ApprovalPolicyArg::Never => Self::Never,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ApprovalsReviewerArg {
    User,
    #[value(name = "auto_review")]
    Automatic,
}

impl From<ApprovalsReviewerArg> for klaus_agent_core::types::permission::ApprovalsReviewer {
    fn from(value: ApprovalsReviewerArg) -> Self {
        match value {
            ApprovalsReviewerArg::User => Self::User,
            ApprovalsReviewerArg::Automatic => Self::Automatic,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PermissionModeArg {
    #[value(name = "default")]
    Default,
    #[value(name = "plan")]
    Plan,
    #[value(name = "acceptEdits", alias = "accept-edits")]
    AcceptEdits,
    #[value(name = "auto")]
    Auto,
    #[value(name = "bypassPermissions", alias = "bypass-permissions")]
    BypassPermissions,
    #[value(name = "dontAsk", alias = "dont-ask")]
    DontAsk,
}

impl From<PermissionModeArg> for ExternalPermissionMode {
    fn from(value: PermissionModeArg) -> Self {
        match value {
            PermissionModeArg::Default => Self::Default,
            PermissionModeArg::Plan => Self::Plan,
            PermissionModeArg::AcceptEdits => Self::AcceptEdits,
            PermissionModeArg::Auto => Self::Auto,
            PermissionModeArg::BypassPermissions => Self::BypassPermissions,
            PermissionModeArg::DontAsk => Self::DontAsk,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum RewindScopeArg {
    Conversation,
    Files,
    Both,
}

impl From<RewindScopeArg> for RewindScope {
    fn from(value: RewindScopeArg) -> Self {
        match value {
            RewindScopeArg::Conversation => Self::Conversation,
            RewindScopeArg::Files => Self::Files,
            RewindScopeArg::Both => Self::Both,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum QueuePriorityArg {
    Now,
    Next,
    Later,
}

impl From<QueuePriorityArg> for QueuePriority {
    fn from(value: QueuePriorityArg) -> Self {
        match value {
            QueuePriorityArg::Now => Self::Now,
            QueuePriorityArg::Next => Self::Next,
            QueuePriorityArg::Later => Self::Later,
        }
    }
}

impl From<QueuePriorityArg> for SteerPriority {
    fn from(value: QueuePriorityArg) -> Self {
        match value {
            QueuePriorityArg::Now => Self::Now,
            QueuePriorityArg::Next => Self::Next,
            QueuePriorityArg::Later => Self::Later,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum McpTransportArg {
    Stdio,
    Http,
    Sse,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum SessionRoleArg {
    Main,
    #[value(name = "side_task", alias = "side-task")]
    SideTask,
}

impl From<SessionRoleArg> for SessionRole {
    fn from(value: SessionRoleArg) -> Self {
        match value {
            SessionRoleArg::Main => Self::Main,
            SessionRoleArg::SideTask => Self::SideTask,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum SetupTriggerArg {
    Init,
    Maintenance,
}

impl From<SetupTriggerArg> for SetupTrigger {
    fn from(value: SetupTriggerArg) -> Self {
        match value {
            SetupTriggerArg::Init => Self::Init,
            SetupTriggerArg::Maintenance => Self::Maintenance,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum MergeChoiceArg {
    Merge,
    Keep,
    Replace,
}

impl From<MergeChoiceArg> for AgentMemoryMergeChoice {
    fn from(value: MergeChoiceArg) -> Self {
        match value {
            MergeChoiceArg::Merge => Self::Merge,
            MergeChoiceArg::Keep => Self::Keep,
            MergeChoiceArg::Replace => Self::Replace,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum SnapshotUnavailableArg {
    #[value(name = "timed-out")]
    TimedOut,
    Disconnected,
    #[value(name = "no-handler")]
    NoHandler,
}

impl From<SnapshotUnavailableArg> for AgentMemorySnapshotUnavailableReason {
    fn from(value: SnapshotUnavailableArg) -> Self {
        match value {
            SnapshotUnavailableArg::TimedOut => Self::TimedOut,
            SnapshotUnavailableArg::Disconnected => Self::Disconnected,
            SnapshotUnavailableArg::NoHandler => Self::NoHandler,
        }
    }
}

// The two below carry a bare string on the wire rather than a modelled enum, so
// clap is what rejects a typo. That is not a nicety: the engine reads an
// unrecognised merge strategy or behavior as its default, so `--behavior dney`
// would quietly write an ALLOW rule. Enumerating them here means the shell user
// gets an error instead.

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ConfigMergeArg {
    /// Set the key, leaving its siblings alone (the default).
    Upsert,
    /// Replace whatever is at the key path outright.
    Replace,
    /// Remove the key.
    Delete,
}

impl ConfigMergeArg {
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::Upsert => "upsert",
            Self::Replace => "replace",
            Self::Delete => "delete",
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PermissionBehaviorArg {
    /// Run the tool without asking (the default, and what an approval grants).
    Allow,
    /// Refuse the tool outright.
    Deny,
    /// Ask every time.
    Ask,
}

impl PermissionBehaviorArg {
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
            Self::Ask => "ask",
        }
    }
}
