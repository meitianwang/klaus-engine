//! End-to-end expansion of a skill body — the single route shared by both the
//! user-slash command path and the model `skill` tool: named/indexed/positional
//! `$ARGUMENTS` substitution, `${KLAUS_SKILL_DIR}` + host `${VAR}`s, and — when a
//! bash runner is wired — inline-shell directives. The only per-caller
//! difference is where the runner comes from (the host's runner on the slash
//! path; the permission-gated runner on the tool path).

use crate::extensions::arg_substitution::{
    expand_command_body, ExpandOptions, RunBash, RunBashFuture,
};
use crate::extensions::skills::loader::Skill;
use crate::permission::bash::node_path::{dirname, is_absolute};
use crate::runtime_values::RuntimeString;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Options a host passes to a rich skill bash runner: the per-directive shell
/// (frontmatter `shell:`) and the prompt's own `allowed-tools` (auto-approved for
/// ITS inline commands only). The permission/cwd/abort context is injected by
/// the host at the wiring site, not by the expander.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SkillBashOpts {
    /// Shell to request for this inline command; omitted means the host default.
    pub shell: Option<String>,
    /// Tools auto-approved for this inline command (the prompt's `allowed-tools`).
    pub allow_tools: Option<Vec<String>>,
}

/// A boxed future returned by a rich skill bash runner.
pub type SkillBashRunnerFuture = Pin<Box<dyn Future<Output = Result<String, String>> + Send>>;

/// The RICH inline-bash runner a host wires for a skill — like the bare bash
/// runner but also carrying the prompt's `allowed-tools`. Returns the command's
/// stdout, or an error (a permission denial).
pub type SkillBashRunner =
    Arc<dyn Fn(String, SkillBashOpts) -> SkillBashRunnerFuture + Send + Sync>;

/// Adapt a rich [`SkillBashRunner`] down to the bare bash runner that the body
/// expander calls, folding a prompt's own `allowed-tools` into EVERY inline
/// command it runs. This is the ONE place the allowed-tools injection happens, so
/// every expansion path injects identically and cannot drift. Returns `None` when
/// no runner is wired (inline-bash markers stay literal). The per-directive
/// `shell` is forwarded unchanged.
pub fn bind_allowed_tools(
    runner: Option<SkillBashRunner>,
    allowed_tools: Option<&[RuntimeString]>,
) -> Result<Option<RunBash>, String> {
    let Some(runner) = runner else {
        return Ok(None);
    };
    let allowed: Option<Vec<String>> = match allowed_tools {
        Some(tools) if !tools.is_empty() => Some(
            tools
                .iter()
                .map(skill_tool_identifier_to_host)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        _ => None,
    };
    Ok(Some(Arc::new(
        move |command: String, shell: Option<String>| -> RunBashFuture {
            let runner = runner.clone();
            let allowed = allowed.clone();
            Box::pin(async move {
                let command = skill_shell_command_to_host(&decode_expansion_transport(&command))?;
                let shell = shell
                    .map(|value| {
                        skill_shell_identifier_to_host(&decode_expansion_transport(&value))
                    })
                    .transpose()?;
                let opts = SkillBashOpts {
                    shell: shell.filter(|s| !s.is_empty()),
                    allow_tools: allowed,
                };
                let output = runner(command, opts).await?;
                Ok(encode_expansion_transport(&RuntimeString::from(output)))
            })
        },
    )))
}

/// Options for [`expand_skill_body`].
#[derive(Default)]
pub struct ExpandSkillOptions {
    /// Permission-gated inline-shell runner; omitted means inline-bash markers
    /// stay literal.
    pub run_bash: Option<SkillBashRunner>,
    /// Extra `${VAR}` values supplied by the host (e.g. a session id).
    pub vars: Option<Vec<(String, String)>>,
}

/// Expand `skill.body` with `args`, wiring `KLAUS_SKILL_DIR` + the skill's
/// argument names + shell + its own `allowed-tools`, and prefixing the result
/// with the skill's base directory.
pub async fn expand_skill_body(
    skill: &Skill,
    args: &RuntimeString,
    opts: ExpandSkillOptions,
) -> Result<RuntimeString, String> {
    // vars = { KLAUS_SKILL_DIR: dirname(skill.path), ...opts.vars }. Object-spread
    // semantics: KLAUS_SKILL_DIR keeps its (first) position while a later entry of
    // the same key overrides its value.
    let mut order: Vec<String> = vec!["KLAUS_SKILL_DIR".to_string()];
    let mut map: HashMap<String, String> = HashMap::new();
    let skill_path = skill_path_to_host(&skill.path)?;
    map.insert(
        "KLAUS_SKILL_DIR".to_string(),
        encode_expansion_transport(&RuntimeString::from(dirname(&skill_path))),
    );
    if let Some(extra) = opts.vars {
        for (k, v) in extra {
            if !map.contains_key(&k) {
                order.push(k.clone());
            }
            map.insert(k, encode_expansion_transport(&RuntimeString::from(v)));
        }
    }
    let vars: Vec<(String, String)> = order
        .into_iter()
        .map(|k| {
            let v = map.get(&k).cloned().unwrap_or_default();
            (k, v)
        })
        .collect();

    // Inject THIS skill's `allowed-tools` into its OWN inline-bash via the one
    // shared binder, so the model `skill` tool and the user-slash command behave
    // identically.
    let inline_bash = bind_allowed_tools(opts.run_bash, skill.allowed_tools.as_deref())?;

    let expand_opts = ExpandOptions {
        argument_names: skill
            .arguments
            .clone()
            .unwrap_or_default()
            .iter()
            .map(encode_expansion_transport)
            .collect(),
        vars: Some(vars),
        run_bash: inline_bash,
        shell: skill
            .shell
            .as_ref()
            .filter(|shell| !shell.is_empty())
            .map(encode_expansion_transport),
        append_if_no_placeholder: None,
    };
    let body = encode_expansion_transport(&skill.body);
    let args = encode_expansion_transport(args);
    let expanded = expand_command_body(&body, Some(&args), expand_opts)
        .await
        .map(|output| decode_expansion_transport(&output))?;
    Ok(prefix_base_directory(&skill_path, expanded))
}

/// Prefix the expanded body with the directory the skill was loaded from. Every
/// route that hands a skill to the model runs through here — the `skill` tool
/// inline and forked, and the user-slash command — so the model can always open
/// the skill's supporting files instead of hunting for them. `${KLAUS_SKILL_DIR}`
/// only helps a skill that thought to spell it out; this helps all of them.
///
/// Skipped when the skill has no real path on disk (a built-in's synthetic
/// `<builtin>/…`, or none at all for one registered in code): naming a directory
/// that does not exist is worse than naming none.
fn prefix_base_directory(skill_path: &str, expanded: RuntimeString) -> RuntimeString {
    if !is_absolute(skill_path) {
        return expanded;
    }
    RuntimeString::from(format!(
        "Base directory for this skill: {}\n\n",
        dirname(skill_path)
    ))
    .concat(&expanded)
}

/// Injectively encode every non-ASCII UTF-16 code unit as one private-plane
/// scalar while leaving the ASCII expansion grammar and ECMAScript whitespace
/// literal. Existing scalar regex/shell-token code then observes JavaScript
/// code-unit indexing without collapsing lone surrogates; the mapping is decoded
/// before any value returns to the runtime domain.
pub(crate) fn encode_expansion_transport(value: &RuntimeString) -> String {
    let mut output = String::with_capacity(value.len_utf16());
    for unit in value.as_utf16().iter().copied() {
        if unit <= 0x007f || is_ecmascript_whitespace(unit) {
            output.push(char::from_u32(u32::from(unit)).expect("literal expansion transport unit"));
        } else {
            output.push(
                char::from_u32(0x0f0000 + u32::from(unit))
                    .expect("one private scalar exists per UTF-16 code unit"),
            );
        }
    }
    output
}

pub(crate) fn decode_expansion_transport(value: &str) -> RuntimeString {
    let mut units = Vec::with_capacity(value.len());
    for scalar in value.chars().map(u32::from) {
        if (0x0f0080..=0x0fffff).contains(&scalar) {
            units.push((scalar - 0x0f0000) as u16);
        } else {
            let mut encoded = [0; 2];
            units.extend(
                char::from_u32(scalar)
                    .expect("Rust string contains Unicode scalars")
                    .encode_utf16(&mut encoded)
                    .iter()
                    .copied(),
            );
        }
    }
    RuntimeString::from_utf16(units)
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

fn strict_skill_scalar(value: &RuntimeString, label: &str) -> Result<String, String> {
    value
        .try_to_utf8()
        .map_err(|error| format!("invalid {label}: {error}"))
}

fn skill_path_to_host(value: &RuntimeString) -> Result<String, String> {
    strict_skill_scalar(value, "skill path")
}

fn skill_shell_command_to_host(value: &RuntimeString) -> Result<String, String> {
    strict_skill_scalar(value, "skill shell command")
}

fn skill_shell_identifier_to_host(value: &RuntimeString) -> Result<String, String> {
    strict_skill_scalar(value, "skill shell identifier")
}

fn skill_tool_identifier_to_host(value: &RuntimeString) -> Result<String, String> {
    strict_skill_scalar(value, "skill tool identifier")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::skills::loader::Skill;
    use std::sync::Mutex;

    fn skill_full(
        body: &str,
        path: &str,
        arguments: Option<Vec<RuntimeString>>,
        allowed_tools: Option<Vec<RuntimeString>>,
        shell: Option<RuntimeString>,
    ) -> Skill {
        Skill {
            name: RuntimeString::from("s"),
            description: RuntimeString::from("d"),
            when_to_use: None,
            argument_hint: None,
            allowed_tools,
            model: None,
            effort: None,
            ultra_mode: None,
            context: None,
            agent: None,
            arguments,
            user_invocable: Some(true),
            disable_model_invocation: Some(false),
            paths: None,
            shell,
            body: RuntimeString::from(body),
            path: RuntimeString::from(path),
        }
    }

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    /// Every expansion is prefixed with the skill's base directory. Assert that
    /// prefix here and hand back the rest, so each test below stays about the
    /// substitution it is actually testing.
    fn strip_base_directory(out: RuntimeString, dir: &str) -> RuntimeString {
        let header = format!("Base directory for this skill: {dir}\n\n");
        assert!(out.starts_with(&header), "missing base directory header");
        out.slice_utf16(header.encode_utf16().count()..)
    }

    #[tokio::test]
    async fn prefixes_the_expansion_with_the_skill_base_directory() {
        let skill = skill_full("body", "/tmp/foo/bar/SKILL.md", None, None, None);
        let out = expand_skill_body(
            &skill,
            &RuntimeString::default(),
            ExpandSkillOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(out, "Base directory for this skill: /tmp/foo/bar\n\nbody");
    }

    #[tokio::test]
    async fn omits_the_header_for_a_skill_with_no_real_path_on_disk() {
        for path in ["<builtin>/deep-research/SKILL.md", ""] {
            let skill = skill_full("body", path, None, None, None);
            let out = expand_skill_body(
                &skill,
                &RuntimeString::default(),
                ExpandSkillOptions::default(),
            )
            .await
            .unwrap();
            assert_eq!(out, "body");
        }
    }

    #[test]
    fn expansion_transport_is_collision_free_for_exact_utf16() {
        let exact = RuntimeString::from_utf16(vec![
            0x0000, 0x007f, 0x0080, 0xd800, 0xdc00, 0xd800, 0x0061, 0xffff,
        ])
        .concat(&RuntimeString::from("\u{f0080}"));
        assert_eq!(
            decode_expansion_transport(&encode_expansion_transport(&exact)),
            exact
        );
    }

    #[tokio::test]
    async fn expands_arguments_and_skill_dir() {
        let skill = skill_full(
            "Hi $ARGUMENTS in ${KLAUS_SKILL_DIR}",
            "/tmp/foo/bar/SKILL.md",
            None,
            None,
            None,
        );
        let out = expand_skill_body(&skill, &rs("world"), ExpandSkillOptions::default())
            .await
            .unwrap();
        assert_eq!(
            strip_base_directory(out, "/tmp/foo/bar"),
            "Hi world in /tmp/foo/bar"
        );
    }

    #[tokio::test]
    async fn expands_named_arguments() {
        let skill = skill_full(
            "$who says hi",
            "/x/SKILL.md",
            Some(vec![rs("who")]),
            None,
            None,
        );
        let out = expand_skill_body(&skill, &rs("Ada"), ExpandSkillOptions::default())
            .await
            .unwrap();
        assert_eq!(strip_base_directory(out, "/x"), "Ada says hi");
    }

    #[tokio::test]
    async fn binds_allowed_tools_into_inline_bash() {
        let skill = skill_full(
            "run !`cmd`",
            "/x/SKILL.md",
            None,
            Some(vec![rs("Read"), rs("Write")]),
            None,
        );
        let captured: Arc<Mutex<Option<(String, SkillBashOpts)>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();
        let runner: SkillBashRunner = Arc::new(
            move |cmd: String, opts: SkillBashOpts| -> SkillBashRunnerFuture {
                let cap = cap.clone();
                Box::pin(async move {
                    *cap.lock().unwrap() = Some((cmd, opts));
                    Ok("OUT".to_string())
                })
            },
        );
        let out = expand_skill_body(
            &skill,
            &RuntimeString::default(),
            ExpandSkillOptions {
                run_bash: Some(runner),
                vars: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(strip_base_directory(out, "/x"), "run OUT");
        let (cmd, opts) = captured.lock().unwrap().clone().unwrap();
        assert_eq!(cmd, "cmd");
        assert_eq!(
            opts.allow_tools,
            Some(vec!["Read".to_string(), "Write".to_string()])
        );
        assert_eq!(opts.shell, None);
    }

    #[tokio::test]
    async fn forwards_frontmatter_shell() {
        let skill = skill_full("!`cmd`", "/x/SKILL.md", None, None, Some(rs("zsh")));
        let captured: Arc<Mutex<Option<SkillBashOpts>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();
        let runner: SkillBashRunner = Arc::new(
            move |_cmd: String, opts: SkillBashOpts| -> SkillBashRunnerFuture {
                let cap = cap.clone();
                Box::pin(async move {
                    *cap.lock().unwrap() = Some(opts);
                    Ok("X".to_string())
                })
            },
        );
        let _ = expand_skill_body(
            &skill,
            &RuntimeString::default(),
            ExpandSkillOptions {
                run_bash: Some(runner),
                vars: None,
            },
        )
        .await
        .unwrap();
        let opts = captured.lock().unwrap().clone().unwrap();
        assert_eq!(opts.shell, Some("zsh".to_string()));
    }

    #[tokio::test]
    async fn without_runner_inline_bash_stays_literal() {
        let skill = skill_full("x !`cmd` y", "/x/SKILL.md", None, None, None);
        let out = expand_skill_body(
            &skill,
            &RuntimeString::default(),
            ExpandSkillOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(strip_base_directory(out, "/x"), "x !`cmd` y");
    }

    #[tokio::test]
    async fn merges_host_vars_after_skill_dir() {
        let skill = skill_full(
            "${FOO} ${KLAUS_SKILL_DIR}",
            "/a/b/SKILL.md",
            None,
            None,
            None,
        );
        let out = expand_skill_body(
            &skill,
            &RuntimeString::default(),
            ExpandSkillOptions {
                run_bash: None,
                vars: Some(vec![("FOO".to_string(), "bar".to_string())]),
            },
        )
        .await
        .unwrap();
        assert_eq!(strip_base_directory(out, "/a/b"), "bar /a/b");
    }

    #[tokio::test]
    async fn host_var_can_override_skill_dir() {
        let skill = skill_full("${KLAUS_SKILL_DIR}", "/a/b/SKILL.md", None, None, None);
        let out = expand_skill_body(
            &skill,
            &RuntimeString::default(),
            ExpandSkillOptions {
                run_bash: None,
                vars: Some(vec![(
                    "KLAUS_SKILL_DIR".to_string(),
                    "/override".to_string(),
                )]),
            },
        )
        .await
        .unwrap();
        assert_eq!(strip_base_directory(out, "/a/b"), "/override");
    }

    #[tokio::test]
    async fn expansion_preserves_lone_surrogates_in_body_and_arguments() {
        let body = RuntimeString::from("before $ARGUMENTS after ")
            .concat(&RuntimeString::from_utf16(vec![0xd800]));
        let arg = RuntimeString::from_utf16(vec![0xdc00]);
        let skill = Skill {
            name: rs("s"),
            description: rs("d"),
            body,
            path: rs("/x/SKILL.md"),
            ..Default::default()
        };

        let output = expand_skill_body(&skill, &arg, ExpandSkillOptions::default())
            .await
            .unwrap();
        assert_eq!(
            strip_base_directory(output, "/x"),
            RuntimeString::from("before ")
                .concat(&arg)
                .concat(&RuntimeString::from(" after "))
                .concat(&RuntimeString::from_utf16(vec![0xd800]))
        );
    }

    #[tokio::test]
    async fn non_scalar_tool_identifier_fails_closed_before_shell_execution() {
        let skill = Skill {
            name: rs("s"),
            description: rs("d"),
            allowed_tools: Some(vec![RuntimeString::from_utf16(vec![0xd800])]),
            body: rs("plain body"),
            path: rs("/x/SKILL.md"),
            ..Default::default()
        };
        let runner: SkillBashRunner = Arc::new(|_, _| Box::pin(async { Ok(String::new()) }));

        let error = expand_skill_body(
            &skill,
            &RuntimeString::default(),
            ExpandSkillOptions {
                run_bash: Some(runner),
                vars: None,
            },
        )
        .await
        .unwrap_err();
        assert!(error.contains("invalid skill tool identifier"));
    }
}
