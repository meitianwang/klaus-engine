//! Slash-command definition + expansion.
//!
//! A slash command has two flavors:
//!  - a prompt command expands into model-facing conversation content via
//!    [`PromptCommand::get_content`] (used by skill-backed commands);
//!  - a local command runs host-side logic via [`LocalCommand::run`], returning
//!    either a message to surface or a no-op (a command that only had a side
//!    effect).
//!
//! [`SlashCommandRegistry`] keys commands by name and parses `/line` input.
//! [`skills_as_commands`] wraps each user-invocable skill as a prompt command
//! whose invocation runs the shared skill-body expansion pipeline (named/indexed/
//! positional `$ARGUMENTS`, `${VAR}`, and — when a shell runner is wired —
//! inline-bash directives), exactly as the model-facing skill tool does.

use crate::extensions::skills::expand_skill::{
    expand_skill_body, ExpandSkillOptions, SkillBashRunner,
};
use crate::extensions::skills::loader::Skill;
use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, TextBlock};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Metadata surfaced for a slash command.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommandMeta {
    pub description: Option<String>,
    pub argument_hint: Option<String>,
}

/// The future produced by a prompt command's content expander. Resolves to the
/// model-facing content blocks, or propagates an error (e.g. a runner denial
/// that blocked the expansion).
pub type GetContentFuture = Pin<Box<dyn Future<Output = Result<Vec<ContentBlock>, String>> + Send>>;
type ContentFn = Arc<dyn Fn(RuntimeString) -> GetContentFuture + Send + Sync>;

/// The future produced by a local command's runner.
pub type RunFuture = Pin<Box<dyn Future<Output = Result<LocalRunOutcome, String>> + Send>>;
type RunFn = Arc<dyn Fn(RuntimeString) -> RunFuture + Send + Sync>;

/// The outcome of a local command's `run`: either a message to surface to the
/// user, or a no-op (the command only had a side effect).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalRunOutcome {
    Message { text: String },
    Noop,
}

/// A prompt command: invoking it expands the command body into model-facing
/// conversation content.
#[derive(Clone)]
pub struct PromptCommand {
    pub name: String,
    pub meta: CommandMeta,
    content_fn: ContentFn,
}

impl PromptCommand {
    /// Build a prompt command from a content producer. The producer is invoked
    /// once per invocation with the raw argument string.
    pub fn new<F, Fut>(name: impl Into<String>, meta: CommandMeta, get_content: F) -> Self
    where
        F: Fn(RuntimeString) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<ContentBlock>, String>> + Send + 'static,
    {
        Self {
            name: name.into(),
            meta,
            content_fn: Arc::new(move |args: RuntimeString| -> GetContentFuture {
                Box::pin(get_content(args))
            }),
        }
    }

    /// Expand this command's body for the given invocation args.
    pub async fn get_content(&self, args: RuntimeString) -> Result<Vec<ContentBlock>, String> {
        (self.content_fn)(args).await
    }
}

/// A local command: invoking it runs host-side logic.
#[derive(Clone)]
pub struct LocalCommand {
    pub name: String,
    pub meta: CommandMeta,
    run_fn: RunFn,
}

impl LocalCommand {
    /// Build a local command from a runner. The runner is invoked once per
    /// invocation with the raw argument string.
    pub fn new<F, Fut>(name: impl Into<String>, meta: CommandMeta, run: F) -> Self
    where
        F: Fn(RuntimeString) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<LocalRunOutcome, String>> + Send + 'static,
    {
        Self {
            name: name.into(),
            meta,
            run_fn: Arc::new(move |args: RuntimeString| -> RunFuture { Box::pin(run(args)) }),
        }
    }

    /// Run this command's host-side logic for the given invocation args.
    pub async fn run(&self, args: RuntimeString) -> Result<LocalRunOutcome, String> {
        (self.run_fn)(args).await
    }
}

/// A slash command in one of its two flavors.
#[derive(Clone)]
pub enum Command {
    Prompt(PromptCommand),
    Local(LocalCommand),
}

impl Command {
    /// The command's registered name.
    pub fn name(&self) -> &str {
        match self {
            Command::Prompt(c) => &c.name,
            Command::Local(c) => &c.name,
        }
    }

    /// The command's metadata.
    pub fn meta(&self) -> &CommandMeta {
        match self {
            Command::Prompt(c) => &c.meta,
            Command::Local(c) => &c.meta,
        }
    }
}

/// A parsed slash-command input line: the command name and its raw argument
/// string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSlashCommand {
    pub name: String,
    pub args: RuntimeString,
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

/// A registry of slash commands keyed by name, with `/line` parsing. Insertion
/// order is preserved by [`SlashCommandRegistry::list`]; re-registering an
/// existing name replaces the command while keeping its original position.
#[derive(Default)]
pub struct SlashCommandRegistry {
    order: Vec<String>,
    commands: HashMap<String, Command>,
}

impl SlashCommandRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, cmd: Command) {
        let name = cmd.name().to_string();
        if !self.commands.contains_key(&name) {
            self.order.push(name.clone());
        }
        self.commands.insert(name, cmd);
    }

    pub fn get(&self, name: &str) -> Option<&Command> {
        self.commands.get(name)
    }

    pub fn list(&self) -> Vec<&Command> {
        self.order
            .iter()
            .filter_map(|n| self.commands.get(n))
            .collect()
    }

    /// Parse a slash-command input line. Returns `None` if the line does not
    /// start with `/` or has no command name. The leading `/` is stripped; the
    /// first whitespace-delimited token is the name and the remainder (trimmed)
    /// is the raw argument string.
    pub fn parse(&self, line: &RuntimeString) -> Option<ParsedSlashCommand> {
        let units = line.as_utf16();
        if units.first() != Some(&u16::from(b'/')) {
            return None;
        }
        let rest = &units[1..];
        let name_end = rest
            .iter()
            .position(|unit| is_ecmascript_whitespace(*unit))
            .unwrap_or(rest.len());
        if name_end == 0 {
            return None;
        }
        // Registered command identifiers remain scalar host keys. Only this
        // token is projected; the invocation arguments stay exact UTF-16.
        let name = RuntimeString::from_utf16(rest[..name_end].to_vec())
            .try_to_utf8()
            .ok()?;
        let args_start = rest[name_end..]
            .iter()
            .position(|unit| !is_ecmascript_whitespace(*unit))
            .map(|offset| name_end + offset)
            .unwrap_or(rest.len());
        let args_end = rest[args_start..]
            .iter()
            .rposition(|unit| !is_ecmascript_whitespace(*unit))
            .map(|offset| args_start + offset + 1)
            .unwrap_or(args_start);
        let args = RuntimeString::from_utf16(rest[args_start..args_end].to_vec());
        Some(ParsedSlashCommand { name, args })
    }
}

/// Receives only the native path captured from a successfully expanded skill.
pub type OnSkillInvoked = Arc<dyn Fn(&RuntimeString) + Send + Sync>;

/// Receives the already expanded skill body without rerunning its commands.
pub type OnSkillExpanded = Arc<dyn Fn(&RuntimeString, &RuntimeString) + Send + Sync>;

/// Options for [`skills_as_commands`].
#[derive(Default)]
pub struct SkillsAsCommandsOptions {
    pub on_skill_invoked: Option<OnSkillInvoked>,
    /// Receives the already expanded skill body without rerunning its commands.
    pub on_skill_expanded: Option<OnSkillExpanded>,
    /// `${KEY}` values supplied to every skill body (e.g. KLAUS_SESSION_ID).
    pub vars: Option<Vec<(String, String)>>,
    /// Host's permission-gated shell runner for inline-bash directives in a
    /// skill body.
    pub run_bash: Option<SkillBashRunner>,
}

/// Wrap each user-invocable skill as a prompt command. Invoking the command
/// expands the skill body through the full substitution pipeline (named/indexed/
/// positional `$ARGUMENTS`, `${VAR}`, and — when a `run_bash` is wired —
/// inline-bash directives), through the same shared route the model-facing skill
/// tool uses. A skill with `user-invocable: false` is omitted (model-only).
pub fn skills_as_commands(skills: &[Skill], opts: SkillsAsCommandsOptions) -> Vec<Command> {
    skills
        .iter()
        .filter(|skill| skill.user_invocable != Some(false))
        .filter_map(|skill| {
            // Slash-command registry keys and metadata are scalar host values.
            // Reject a non-scalar programmatic skill here instead of replacing
            // code units; the model-facing skill tool retains the exact carrier.
            let name = skill.name.try_to_utf8().ok()?;
            let description = skill.description.try_to_utf8().ok()?;
            let argument_hint = match &skill.argument_hint {
                Some(value) => Some(value.try_to_utf8().ok()?),
                None => None,
            };
            let meta = CommandMeta {
                description: Some(description),
                argument_hint,
            };
            let skill = skill.clone();
            let run_bash = opts.run_bash.clone();
            let vars = opts.vars.clone();
            let on_skill_invoked = opts.on_skill_invoked.clone();
            let on_skill_expanded = opts.on_skill_expanded.clone();
            let content_fn: ContentFn = Arc::new(move |args: RuntimeString| -> GetContentFuture {
                let skill = skill.clone();
                let run_bash = run_bash.clone();
                let vars = vars.clone();
                let on_skill_invoked = on_skill_invoked.clone();
                let on_skill_expanded = on_skill_expanded.clone();
                Box::pin(async move {
                    // One shared expansion route (the same one the model skill
                    // tool uses); the host supplies run_bash.
                    let text =
                        expand_skill_body(&skill, &args, ExpandSkillOptions { run_bash, vars })
                            .await?;
                    if let Some(on_skill_invoked) = &on_skill_invoked {
                        on_skill_invoked(&skill.path);
                    }
                    if let Some(record) = &on_skill_expanded {
                        record(&skill.path, &text);
                    }
                    Ok(vec![ContentBlock::Text(TextBlock {
                        text,
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })])
                })
            });
            Some(Command::Prompt(PromptCommand {
                name,
                meta,
                content_fn,
            }))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn prompt_cmd(name: &str) -> Command {
        Command::Prompt(PromptCommand::new(
            name,
            CommandMeta::default(),
            |args: RuntimeString| async move {
                Ok(vec![ContentBlock::Text(TextBlock {
                    text: RuntimeString::from("expanded:").concat(&args),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })])
            },
        ))
    }

    fn local_cmd(name: &str) -> Command {
        Command::Local(LocalCommand::new(
            name,
            CommandMeta::default(),
            |_args: RuntimeString| async move { Ok(LocalRunOutcome::Noop) },
        ))
    }

    // Construct a `Skill` for tests. All optional fields default to `None` so
    // the effort/context inner types never need to be named here.
    fn make_skill(name: &str, description: &str, body: &str) -> Skill {
        Skill {
            name: name.into(),
            description: description.into(),
            short_description: None,
            when_to_use: None,
            argument_hint: None,
            allowed_tools: None,
            model: None,
            effort: None,
            ultra_mode: None,
            context: None,
            agent: None,
            arguments: None,
            user_invocable: None,
            disable_model_invocation: None,
            paths: None,
            shell: None,
            body: body.into(),
            path: format!("/skills/{name}/SKILL.md").into(),
        }
    }

    #[test]
    fn parse_rejects_non_slash_and_empty() {
        let reg = SlashCommandRegistry::new();
        assert_eq!(reg.parse(&rs("hello")), None);
        assert_eq!(reg.parse(&rs("")), None);
        assert_eq!(reg.parse(&rs("/")), None);
        assert_eq!(reg.parse(&rs("/   ")), None);
    }

    #[test]
    fn parse_extracts_name_and_args() {
        let reg = SlashCommandRegistry::new();
        assert_eq!(
            reg.parse(&rs("/foo")),
            Some(ParsedSlashCommand {
                name: "foo".into(),
                args: RuntimeString::default()
            })
        );
        assert_eq!(
            reg.parse(&rs("/foo   ")),
            Some(ParsedSlashCommand {
                name: "foo".into(),
                args: RuntimeString::default()
            })
        );
        assert_eq!(
            reg.parse(&rs("/foo bar baz")),
            Some(ParsedSlashCommand {
                name: "foo".into(),
                args: "bar baz".into()
            })
        );
        assert_eq!(
            reg.parse(&rs("/foo    bar")),
            Some(ParsedSlashCommand {
                name: "foo".into(),
                args: "bar".into()
            })
        );
        assert_eq!(
            reg.parse(&rs("/deploy prod  ")),
            Some(ParsedSlashCommand {
                name: "deploy".into(),
                args: "prod".into()
            })
        );
    }

    #[test]
    fn parse_is_dotall_across_newlines() {
        let reg = SlashCommandRegistry::new();
        // The newline separating name and args is consumed as the delimiter.
        assert_eq!(
            reg.parse(&rs("/say\nhello world")),
            Some(ParsedSlashCommand {
                name: "say".into(),
                args: "hello world".into()
            })
        );
        // Internal newlines within the args survive (only the ends are trimmed).
        assert_eq!(
            reg.parse(&rs("/note first\nsecond")),
            Some(ParsedSlashCommand {
                name: "note".into(),
                args: "first\nsecond".into()
            })
        );
    }

    #[test]
    fn parse_preserves_exact_arguments_and_rejects_only_a_non_scalar_name() {
        let high = RuntimeString::from_utf16(vec![0xd800]);
        let line = rs("/foo ").concat(&high);
        assert_eq!(
            SlashCommandRegistry::new().parse(&line),
            Some(ParsedSlashCommand {
                name: "foo".into(),
                args: high,
            })
        );

        let non_scalar_name = rs("/")
            .concat(&RuntimeString::from_utf16(vec![0xdc00]))
            .concat(&rs(" arg"));
        assert_eq!(SlashCommandRegistry::new().parse(&non_scalar_name), None);
    }

    #[test]
    fn parse_uses_ecmascript_whitespace_not_rust_unicode_whitespace() {
        let high = RuntimeString::from_utf16(vec![0xd800]);
        let line = rs("/foo\u{feff}").concat(&high).concat(&rs("\u{3000}"));
        assert_eq!(
            SlashCommandRegistry::new().parse(&line),
            Some(ParsedSlashCommand {
                name: "foo".into(),
                args: high,
            })
        );

        // U+0085 is Unicode White_Space but is not ECMAScript `\s`.
        assert_eq!(
            SlashCommandRegistry::new().parse(&rs("/foo\u{85}bar")),
            Some(ParsedSlashCommand {
                name: "foo\u{85}bar".into(),
                args: RuntimeString::default(),
            })
        );
    }

    #[test]
    fn register_get_list_preserve_insertion_order() {
        let mut reg = SlashCommandRegistry::new();
        reg.register(prompt_cmd("a"));
        reg.register(local_cmd("b"));
        reg.register(prompt_cmd("c"));
        let names: Vec<&str> = reg.list().iter().map(|c| c.name()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
        assert!(reg.get("b").is_some());
        assert!(reg.get("missing").is_none());
    }

    #[test]
    fn reregister_replaces_value_keeping_position() {
        let mut reg = SlashCommandRegistry::new();
        reg.register(prompt_cmd("a"));
        reg.register(prompt_cmd("b"));
        reg.register(prompt_cmd("c"));
        reg.register(Command::Prompt(PromptCommand::new(
            "a",
            CommandMeta {
                description: Some("updated".into()),
                argument_hint: None,
            },
            |_args: RuntimeString| async move {
                Ok(vec![ContentBlock::Text(TextBlock {
                    text: Default::default(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })])
            },
        )));
        let names: Vec<&str> = reg.list().iter().map(|c| c.name()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
        assert_eq!(
            reg.get("a").unwrap().meta().description.as_deref(),
            Some("updated")
        );
    }

    #[tokio::test]
    async fn prompt_command_get_content_runs_producer() {
        let cmd = PromptCommand::new(
            "greet",
            CommandMeta::default(),
            |args: RuntimeString| async move {
                Ok(vec![ContentBlock::Text(TextBlock {
                    text: RuntimeString::from("hi ").concat(&args),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })])
            },
        );
        let out = cmd.get_content("there".into()).await.unwrap();
        assert_eq!(out.len(), 1);
        match &out[0] {
            ContentBlock::Text(t) => assert_eq!(t.text, "hi there"),
            _ => panic!("expected text block"),
        }
    }

    #[tokio::test]
    async fn local_command_run_message_and_noop() {
        let say = LocalCommand::new(
            "say",
            CommandMeta::default(),
            |args: RuntimeString| async move {
                Ok(LocalRunOutcome::Message {
                    text: format!(
                        "said:{}",
                        args.try_to_utf8().expect("test command args are scalar")
                    ),
                })
            },
        );
        assert_eq!(
            say.run("hi".into()).await.unwrap(),
            LocalRunOutcome::Message {
                text: "said:hi".into()
            }
        );

        let noop = LocalCommand::new(
            "noop",
            CommandMeta::default(),
            |_a: RuntimeString| async move { Ok(LocalRunOutcome::Noop) },
        );
        assert_eq!(
            noop.run(RuntimeString::default()).await.unwrap(),
            LocalRunOutcome::Noop
        );
    }

    #[test]
    fn skills_as_commands_filters_non_user_invocable() {
        let mut hidden = make_skill("hidden", "d", "b");
        hidden.user_invocable = Some(false);
        let shown = make_skill("shown", "desc", "body");
        let mut explicit = make_skill("explicit", "d2", "b2");
        explicit.user_invocable = Some(true);

        let cmds = skills_as_commands(
            &[hidden, shown, explicit],
            SkillsAsCommandsOptions::default(),
        );
        let names: Vec<&str> = cmds.iter().map(|c| c.name()).collect();
        assert_eq!(names, vec!["shown", "explicit"]);
    }

    #[test]
    fn skills_as_commands_maps_meta() {
        let mut with_hint = make_skill("deploy", "Deploy the app", "body");
        with_hint.argument_hint = Some("<env>".into());
        let without_hint = make_skill("status", "Show status", "body");

        let cmds = skills_as_commands(
            &[with_hint, without_hint],
            SkillsAsCommandsOptions::default(),
        );
        assert_eq!(
            cmds[0].meta().description.as_deref(),
            Some("Deploy the app")
        );
        assert_eq!(cmds[0].meta().argument_hint.as_deref(), Some("<env>"));
        assert_eq!(cmds[1].meta().description.as_deref(), Some("Show status"));
        assert_eq!(cmds[1].meta().argument_hint, None);
    }

    #[tokio::test]
    async fn skill_command_expands_body_with_args() {
        let skill = make_skill("run", "d", "run $ARGUMENTS now");
        let cmds = skills_as_commands(&[skill], SkillsAsCommandsOptions::default());
        let content = match &cmds[0] {
            Command::Prompt(p) => p.get_content("the task".into()).await.unwrap(),
            _ => panic!("expected prompt command"),
        };
        assert_eq!(content.len(), 1);
        match &content[0] {
            // The slash path shares the model tool's expansion route, so it also
            // carries the skill's base-directory prefix.
            ContentBlock::Text(t) => assert_eq!(
                t.text,
                "Base directory for this skill: /skills/run\n\nrun the task now"
            ),
            _ => panic!("expected text block"),
        }
    }

    #[tokio::test]
    async fn skill_command_applies_vars() {
        let skill = make_skill("cfg", "d", "session=${KLAUS_SESSION_ID}");
        let opts = SkillsAsCommandsOptions {
            on_skill_expanded: None,
            on_skill_invoked: None,
            vars: Some(vec![("KLAUS_SESSION_ID".into(), "abc123".into())]),
            run_bash: None,
        };
        let cmds = skills_as_commands(&[skill], opts);
        let content = match &cmds[0] {
            Command::Prompt(p) => p.get_content(RuntimeString::default()).await.unwrap(),
            _ => panic!("expected prompt command"),
        };
        match &content[0] {
            ContentBlock::Text(t) => assert_eq!(
                t.text,
                "Base directory for this skill: /skills/cfg\n\nsession=abc123"
            ),
            _ => panic!("expected text block"),
        }
    }
    #[tokio::test]
    async fn skill_receipts_follow_successful_expansion_and_use_the_resolved_path() {
        use std::sync::Mutex;
        let receipts = Arc::new(Mutex::new(Vec::new()));
        let recorded = receipts.clone();
        let on_skill_invoked: OnSkillInvoked = Arc::new(move |path| {
            recorded.lock().unwrap().push(path.clone());
        });
        let skill = make_skill("run", "d", "run $ARGUMENTS");
        let expected = skill.path.clone();
        let commands = skills_as_commands(
            &[skill],
            SkillsAsCommandsOptions {
                on_skill_expanded: None,
                on_skill_invoked: Some(on_skill_invoked.clone()),
                ..Default::default()
            },
        );
        assert!(receipts.lock().unwrap().is_empty());
        let Command::Prompt(command) = &commands[0] else {
            panic!("expected skill prompt")
        };
        command
            .get_content("/tmp/model-supplied-path/SKILL.md".into())
            .await
            .unwrap();
        assert_eq!(*receipts.lock().unwrap(), vec![expected]);

        let blocked = make_skill("blocked", "d", "!`echo blocked`");
        let commands = skills_as_commands(
            &[blocked],
            SkillsAsCommandsOptions {
                on_skill_expanded: None,
                on_skill_invoked: Some(on_skill_invoked),
                run_bash: Some(Arc::new(|_, _| {
                    Box::pin(async { Err("denied".to_owned()) })
                })),
                ..Default::default()
            },
        );
        let Command::Prompt(command) = &commands[0] else {
            panic!("expected skill prompt")
        };
        assert!(command.get_content(RuntimeString::default()).await.is_err());
        assert_eq!(receipts.lock().unwrap().len(), 1);
    }
}
