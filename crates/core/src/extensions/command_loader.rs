//! Load flat markdown slash commands from a `commands/` directory: each `*.md`
//! file is a `'prompt'` command named after its basename, whose body is expanded
//! through the full substitution pipeline on invocation. Mirrors the
//! skill→command path but for the flat-file command layout (no per-command
//! directory).

use crate::extensions::arg_substitution::{
    expand_command_body, parse_argument_names, ArgNamesInput, ExpandOptions, RunBash, RunBashFuture,
};
use crate::extensions::commands::{Command, CommandMeta, PromptCommand};
use crate::extensions::frontmatter::{
    coerce_description_to_string, extract_description_from_markdown, parse_frontmatter,
    parse_tool_list, Frontmatter, ToolListInput,
};
use crate::extensions::skills::expand_skill::{
    decode_expansion_transport, encode_expansion_transport, SkillBashOpts, SkillBashRunner,
};
use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, TextBlock};
use crate::types::json::JsonObject;
use serde_json::Value;
use std::sync::Arc;

/// Shared expansion knobs (host-provided shell runner / vars) threaded to every
/// command body.
#[derive(Default)]
pub struct CommandLoadOptions {
    /// The host's permission-gated inline-shell runner — the rich form that
    /// carries `allow_tools`, so a command's own `allowed-tools` are
    /// auto-approved for its inline bash.
    pub run_bash: Option<SkillBashRunner>,
    /// `${KEY}` values supplied to every command body.
    pub vars: Option<Vec<(String, String)>>,
}

/// The metadata a manifest may supply for an inline command.
#[derive(Debug, Clone, Default)]
pub struct InlineCommandMeta {
    pub description: Option<String>,
    pub argument_hint: Option<String>,
}

/// A manifest entry's overrides for a command: meta + an allowed-tools grant.
#[derive(Default)]
struct ManifestOverrides {
    description: Option<String>,
    argument_hint: Option<String>,
    /// Manifest-level allowed-tools. When present it REPLACES the body's own.
    allowed_tools: Option<Vec<String>>,
}

/// A frontmatter value that is either a delimited string or a list of strings.
enum StrOrList {
    Str(String),
    List(Vec<String>),
}

impl StrOrList {
    fn into_tool_list_input(self) -> ToolListInput {
        match self {
            StrOrList::Str(s) => ToolListInput::Str(RuntimeString::from(s)),
            StrOrList::List(l) => {
                ToolListInput::List(l.into_iter().map(RuntimeString::from).collect())
            }
        }
    }

    fn into_arg_names_input(self) -> ArgNamesInput {
        match self {
            StrOrList::Str(s) => ArgNamesInput::Str(s),
            StrOrList::List(l) => ArgNamesInput::List(l),
        }
    }
}

/// Validated command frontmatter fields.
struct CommandFm {
    description: Option<Value>,
    argument_hint: Option<String>,
    arguments: Option<StrOrList>,
    shell: Option<String>,
    allowed_tools: Option<StrOrList>,
}

/// Read an optional string field: absent → `None`, a string → the string,
/// anything else → schema error.
fn get_string_field(data: &JsonObject, key: &str) -> Result<Option<String>, String> {
    match data.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(format!("\"{key}\": expected string")),
    }
}

/// Read an optional string-or-string-list field.
fn get_str_or_list_field(data: &JsonObject, key: &str) -> Result<Option<StrOrList>, String> {
    match data.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(StrOrList::Str(s.clone()))),
        Some(Value::Array(arr)) => {
            let mut list = Vec::with_capacity(arr.len());
            for v in arr {
                match v {
                    Value::String(s) => list.push(s.clone()),
                    _ => return Err(format!("\"{key}\": expected string or string[]")),
                }
            }
            Ok(Some(StrOrList::List(list)))
        }
        Some(_) => Err(format!("\"{key}\": expected string or string[]")),
    }
}

/// Validate the command frontmatter shape. A bad typed field (e.g. an
/// `argument-hint` that is a list, or a non-string `model`) fails the whole
/// parse, so the command is skipped rather than partially loaded.
fn validate_command_frontmatter(data: &JsonObject) -> Result<CommandFm, String> {
    let argument_hint = get_string_field(data, "argument-hint")?;
    let arguments = get_str_or_list_field(data, "arguments")?;
    let shell = get_string_field(data, "shell")?;
    let allowed_tools = get_str_or_list_field(data, "allowed-tools")?;
    // `model` is validated for shape but not otherwise used here.
    let _model = get_string_field(data, "model")?;
    let description = data.get("description").cloned();
    Ok(CommandFm {
        description,
        argument_hint,
        arguments,
        shell,
        allowed_tools,
    })
}

/// Adapt a rich [`SkillBashRunner`] down to the bare runner the expander calls,
/// folding a command's own `allowed-tools` into EVERY inline command it runs.
/// Returns `None` when no runner is wired (inline-bash markers stay literal).
/// The bare runner forwards the per-directive `shell` through unchanged.
fn bind_allowed_tools(
    runner: Option<SkillBashRunner>,
    allowed_tools: Option<Vec<String>>,
) -> Option<RunBash> {
    let runner = runner?;
    Some(Arc::new(
        move |command: String, shell: Option<String>| -> RunBashFuture {
            let runner = runner.clone();
            let allowed_tools = allowed_tools.clone();
            Box::pin(async move {
                let command = decode_expansion_transport(&command)
                    .try_to_utf8()
                    .map_err(|error| format!("invalid command shell text: {error}"))?;
                let shell = shell
                    .map(|value| {
                        decode_expansion_transport(&value)
                            .try_to_utf8()
                            .map_err(|error| format!("invalid command shell identifier: {error}"))
                    })
                    .transpose()?;
                let opts = SkillBashOpts {
                    shell: shell.filter(|s| !s.is_empty()),
                    allow_tools: match &allowed_tools {
                        Some(t) if !t.is_empty() => Some(t.clone()),
                        _ => None,
                    },
                };
                let output = runner(command, opts).await?;
                Ok(encode_expansion_transport(&RuntimeString::from(output)))
            })
        },
    ))
}

/// Build a `'prompt'` command from raw markdown — a `*.md` file's contents OR a
/// manifest `content` string. Both forms parse frontmatter (so an embedded
/// `allowed-tools` block reaches the inline-bash gate identically to skills). A
/// manifest entry may override the description / argument-hint and, when it
/// carries its own `allowed-tools`, REPLACE the body's list (a tighter manifest
/// grant is honored and never widened). Returns `None` on schema failure.
fn build_command_from_markdown(
    name: &str,
    raw: &str,
    opts: &CommandLoadOptions,
    manifest: ManifestOverrides,
) -> Option<Command> {
    let Frontmatter { data, body } = parse_frontmatter(raw);
    let fm = match validate_command_frontmatter(&data) {
        Ok(fm) => fm,
        Err(reason) => {
            tracing::warn!(
                "[extensions] skipping invalid command \"{}\": {}",
                name,
                reason
            );
            return None;
        }
    };

    let argument_names = parse_argument_names(fm.arguments.map(StrOrList::into_arg_names_input))
        .into_iter()
        .map(|name| encode_expansion_transport(&RuntimeString::from(name)))
        .collect::<Vec<_>>();
    // A manifest `allowed-tools` (joined, then parsed parens-aware) REPLACES the
    // body's own when present; otherwise the body's frontmatter list is used.
    let allowed_tools = if let Some(mt) = &manifest.allowed_tools {
        parse_tool_list(Some(ToolListInput::Str(RuntimeString::from(mt.join(",")))))
    } else {
        parse_tool_list(fm.allowed_tools.map(StrOrList::into_tool_list_input))
    };
    let allowed_tools = allowed_tools
        .map(|tools| {
            tools
                .iter()
                .map(command_tool_identifier_to_host)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()
        .ok()?;
    // Manifest meta overrides the body's; else coerce the body's description,
    // else the markdown H1.
    let description = manifest
        .description
        .or_else(|| coerce_description_to_string(fm.description.as_ref()))
        .unwrap_or_else(|| extract_description_from_markdown(&body, "Custom command"));
    let argument_hint = manifest.argument_hint.or(fm.argument_hint);
    let meta = CommandMeta {
        description: Some(description),
        argument_hint,
    };

    // Capture everything the body expander needs; each invocation clones its own
    // copy and re-binds the allowed-tools into a fresh inline-bash gate.
    let body_owned = encode_expansion_transport(&RuntimeString::from(body));
    let vars = opts.vars.as_ref().map(|vars| {
        vars.iter()
            .map(|(key, value)| {
                (
                    encode_expansion_transport(&RuntimeString::from(key.clone())),
                    encode_expansion_transport(&RuntimeString::from(value.clone())),
                )
            })
            .collect::<Vec<_>>()
    });
    let run_bash = opts.run_bash.clone();
    let shell = fm
        .shell
        .filter(|s| !s.is_empty())
        .map(|shell| encode_expansion_transport(&RuntimeString::from(shell)));

    let content_fn = move |args: RuntimeString| {
        let body = body_owned.clone();
        let argument_names = argument_names.clone();
        let vars = vars.clone();
        let run_bash = run_bash.clone();
        let allowed_tools = allowed_tools.clone();
        let shell = shell.clone();
        async move {
            let args = encode_expansion_transport(&args);
            let bound = bind_allowed_tools(run_bash, allowed_tools);
            let expand_opts = ExpandOptions {
                argument_names,
                vars,
                run_bash: bound,
                shell,
                append_if_no_placeholder: None,
            };
            let text = expand_command_body(&body, Some(args.as_str()), expand_opts).await?;
            Ok(vec![ContentBlock::Text(TextBlock {
                text: decode_expansion_transport(&text),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })])
        }
    };

    Some(Command::Prompt(PromptCommand::new(
        name.to_string(),
        meta,
        content_fn,
    )))
}

fn command_tool_identifier_to_host(value: &RuntimeString) -> Result<String, String> {
    value
        .try_to_utf8()
        .map_err(|error| format!("invalid command tool identifier: {error}"))
}

/// Build a `'prompt'` command from a manifest inline `content` string. The
/// content's own frontmatter is parsed (so an embedded `allowed-tools` reaches
/// the gate); the manifest entry's metadata + allowed-tools override it. `None`
/// if the content frontmatter is invalid.
pub fn make_inline_command(
    name: &str,
    content: &str,
    meta: InlineCommandMeta,
    opts: &CommandLoadOptions,
    allowed_tools: Option<Vec<String>>,
) -> Option<Command> {
    build_command_from_markdown(
        name,
        content,
        opts,
        ManifestOverrides {
            description: meta.description,
            argument_hint: meta.argument_hint,
            allowed_tools,
        },
    )
}

async fn load_one_command(
    file_path: &str,
    opts: &CommandLoadOptions,
    extra_allowed_tools: Option<Vec<String>>,
) -> Option<Command> {
    let raw = match tokio::fs::read_to_string(file_path).await {
        Ok(r) => r,
        Err(_) => return None,
    };
    let name = strip_md_ext(basename(file_path));
    build_command_from_markdown(
        &name,
        &raw,
        opts,
        ManifestOverrides {
            allowed_tools: extra_allowed_tools,
            ..Default::default()
        },
    )
}

/// Load a single `*.md` slash command file. `None` on failure.
/// `extra_allowed_tools` is an optional manifest-level grant that REPLACES the
/// file's own frontmatter `allowed-tools` for the command's inline-bash gate.
pub async fn load_command_from_file(
    file_path: &str,
    opts: CommandLoadOptions,
    extra_allowed_tools: Option<Vec<String>>,
) -> Option<Command> {
    load_one_command(file_path, &opts, extra_allowed_tools).await
}

/// Load every `*.md` slash command in `dir`. Invalid commands are skipped; a
/// missing dir yields `[]`.
pub async fn load_commands_from_dir(dir: &str, opts: CommandLoadOptions) -> Vec<Command> {
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return Vec::new(),
    };
    let mut commands: Vec<Command> = Vec::new();
    loop {
        let entry = match rd.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(_) => break,
        };
        let file_type = match entry.file_type().await {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if !file_type.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if !is_md(&file_name) {
            continue;
        }
        let path = entry.path();
        let path = path.to_string_lossy().into_owned();
        if let Some(cmd) = load_one_command(&path, &opts, None).await {
            commands.push(cmd);
        }
    }
    commands
}

/// Strip a trailing `.md` (case-insensitive) extension from a file name.
fn strip_md_ext(name: &str) -> String {
    let b = name.as_bytes();
    let n = b.len();
    if n >= 3
        && b[n - 3] == b'.'
        && b[n - 2].eq_ignore_ascii_case(&b'm')
        && b[n - 1].eq_ignore_ascii_case(&b'd')
    {
        name[..n - 3].to_string()
    } else {
        name.to_string()
    }
}

/// The final path component (after the last `/`).
fn basename(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[i + 1..],
        None => path,
    }
}

/// Whether a file name ends with `.md` (case-insensitive).
fn is_md(name: &str) -> bool {
    let b = name.as_bytes();
    let n = b.len();
    n >= 3
        && b[n - 3] == b'.'
        && b[n - 2].eq_ignore_ascii_case(&b'm')
        && b[n - 1].eq_ignore_ascii_case(&b'd')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    type Seen = Arc<Mutex<Vec<(String, Option<Vec<String>>)>>>;

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-cmd-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_file(dir: &Path, rel: &str, content: &str) -> PathBuf {
        let full = dir.join(rel);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&full, content).unwrap();
        full
    }

    fn as_prompt(c: &Command) -> &PromptCommand {
        match c {
            Command::Prompt(p) => p,
            Command::Local(_) => panic!("flat markdown commands should load as prompt commands"),
        }
    }

    fn text_of(b: &ContentBlock) -> &crate::runtime_values::RuntimeString {
        match b {
            ContentBlock::Text(t) => &t.text,
            _ => panic!("expected text block"),
        }
    }

    fn bracket_runner() -> SkillBashRunner {
        Arc::new(move |cmd: String, _opts: SkillBashOpts| -> RunBashFuture {
            Box::pin(async move { Ok(format!("[{cmd}]")) })
        })
    }

    fn recording_runner(seen: Seen, ret: &'static str) -> SkillBashRunner {
        Arc::new(move |cmd: String, opts: SkillBashOpts| -> RunBashFuture {
            seen.lock().unwrap().push((cmd, opts.allow_tools.clone()));
            Box::pin(async move { Ok(ret.to_string()) })
        })
    }

    #[tokio::test]
    async fn exposes_description_and_argument_hint_as_meta_and_threads_shell() {
        let dir = temp_dir();
        write_file(
            &dir,
            "now.md",
            "---\ndescription: print time\nargument-hint: [tz]\nshell: bash\n---\nTime in $0: !`date`",
        );
        let opts = CommandLoadOptions {
            run_bash: Some(bracket_runner()),
            vars: None,
        };
        let cmds = load_commands_from_dir(dir.to_str().unwrap(), opts).await;
        assert_eq!(cmds.len(), 1);
        let cmd = as_prompt(&cmds[0]);
        assert_eq!(cmd.meta.description.as_deref(), Some("print time"));
        assert_eq!(cmd.meta.argument_hint.as_deref(), Some("[tz]"));
        let blocks = cmd.get_content("UTC".into()).await.unwrap();
        assert_eq!(text_of(&blocks[0]), "Time in UTC: [date]");
    }

    #[tokio::test]
    async fn injects_the_commands_own_frontmatter_allowed_tools_into_its_inline_bash_gate() {
        let dir = temp_dir();
        write_file(
            &dir,
            "run.md",
            "---\nallowed-tools: Bash(echo:*), Bash(ls:*)\n---\n!`echo hi`",
        );
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let opts = CommandLoadOptions {
            run_bash: Some(recording_runner(seen.clone(), "OK")),
            vars: None,
        };
        let cmds = load_commands_from_dir(dir.to_str().unwrap(), opts).await;
        let cmd = as_prompt(&cmds[0]);
        let blocks = cmd.get_content(RuntimeString::default()).await.unwrap();
        assert_eq!(text_of(&blocks[0]), "OK");
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, "echo hi");
        assert_eq!(
            seen[0].1,
            Some(vec!["Bash(echo:*)".to_string(), "Bash(ls:*)".to_string()])
        );
    }

    #[tokio::test]
    async fn keeps_a_scoped_allowed_tools_spec_with_internal_spaces_commas_as_one_rule() {
        let dir = temp_dir();
        write_file(
            &dir,
            "build.md",
            "---\nallowed-tools: Bash(npm run build), Bash(git diff, --stat)\n---\n!`npm run build`",
        );
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let opts = CommandLoadOptions {
            run_bash: Some(recording_runner(seen.clone(), "OK")),
            vars: None,
        };
        let cmds = load_commands_from_dir(dir.to_str().unwrap(), opts).await;
        let cmd = as_prompt(&cmds[0]);
        cmd.get_content(RuntimeString::default()).await.unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(
            seen[0].1,
            Some(vec![
                "Bash(npm run build)".to_string(),
                "Bash(git diff, --stat)".to_string()
            ])
        );
    }

    #[tokio::test]
    async fn make_inline_command_injects_its_allowed_tools() {
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let opts = CommandLoadOptions {
            run_bash: Some(recording_runner(seen.clone(), "OK")),
            vars: None,
        };
        let cmd = make_inline_command(
            "run",
            "!`echo hi`",
            InlineCommandMeta::default(),
            &opts,
            Some(vec!["Bash(echo:*)".to_string()]),
        )
        .unwrap();
        let cmd = as_prompt(&cmd);
        let blocks = cmd.get_content(RuntimeString::default()).await.unwrap();
        assert_eq!(text_of(&blocks[0]), "OK");
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].0, "echo hi");
        assert_eq!(seen[0].1, Some(vec!["Bash(echo:*)".to_string()]));
    }

    #[tokio::test]
    async fn make_inline_command_parses_frontmatter_embedded_in_the_content() {
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let opts = CommandLoadOptions {
            run_bash: Some(recording_runner(seen.clone(), "OK")),
            vars: None,
        };
        let cmd = make_inline_command(
            "run",
            "---\nallowed-tools: Bash(echo:*)\n---\n!`echo hi`",
            InlineCommandMeta::default(),
            &opts,
            None,
        )
        .unwrap();
        let cmd = as_prompt(&cmd);
        let blocks = cmd.get_content(RuntimeString::default()).await.unwrap();
        assert_eq!(text_of(&blocks[0]), "OK");
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].1, Some(vec!["Bash(echo:*)".to_string()]));
    }

    #[tokio::test]
    async fn a_manifest_level_grant_replaces_the_file_frontmatter_list() {
        let dir = temp_dir();
        let path = write_file(
            &dir,
            "run.md",
            "---\nallowed-tools: Bash(echo:*)\n---\n!`echo hi`",
        );
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let opts = CommandLoadOptions {
            run_bash: Some(recording_runner(seen.clone(), "OK")),
            vars: None,
        };
        let cmd = load_command_from_file(
            path.to_str().unwrap(),
            opts,
            Some(vec!["Bash(ls:*)".to_string()]),
        )
        .await
        .unwrap();
        let cmd = as_prompt(&cmd);
        cmd.get_content(RuntimeString::default()).await.unwrap();
        assert_eq!(
            seen.lock().unwrap()[0].1,
            Some(vec!["Bash(ls:*)".to_string()])
        );
    }

    #[tokio::test]
    async fn an_empty_manifest_grant_replaces_the_file_list_with_nothing() {
        let dir = temp_dir();
        let path = write_file(
            &dir,
            "run.md",
            "---\nallowed-tools: Bash(echo:*)\n---\n!`echo hi`",
        );
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let opts = CommandLoadOptions {
            run_bash: Some(recording_runner(seen.clone(), "OK")),
            vars: None,
        };
        let cmd = load_command_from_file(path.to_str().unwrap(), opts, Some(vec![]))
            .await
            .unwrap();
        let cmd = as_prompt(&cmd);
        cmd.get_content(RuntimeString::default()).await.unwrap();
        // The present-but-empty manifest grant wins → the body's grant is dropped,
        // no allow-tools sent.
        assert_eq!(seen.lock().unwrap()[0].1, None);
    }

    #[tokio::test]
    async fn inline_command_preserves_exact_runtime_arguments() {
        let command = make_inline_command(
            "exact",
            "value=$ARGUMENTS",
            InlineCommandMeta::default(),
            &CommandLoadOptions::default(),
            None,
        )
        .expect("valid inline command");
        let command = as_prompt(&command);
        let arguments = [
            RuntimeString::from_utf16(vec![0xd800]),
            RuntimeString::from_utf16(vec![0xdc00]),
            RuntimeString::from("\u{fffd}"),
        ];

        for argument in arguments {
            let blocks = command.get_content(argument.clone()).await.unwrap();
            assert_eq!(
                text_of(&blocks[0]),
                &RuntimeString::from("value=").concat(&argument)
            );
        }
    }
}
