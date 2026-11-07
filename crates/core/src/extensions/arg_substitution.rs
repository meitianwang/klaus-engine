//! Argument substitution + inline-bash expansion for skill/command bodies.
//! This layer specifies the exact contract: the
//! placeholder tokens, the ORDER they are applied, the digit→index mapping, and
//! the empty-arg / append semantics are all handled exactly. Inline shell
//! execution is delegated to a host-provided `run_bash`.

use crate::permission::shell_quote::{parse_with_env, ParseEntry};
use fancy_regex::Regex as FancyRegex;
use regex::Regex;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

/// Parse an argument string into individual tokens, quotes-aware. `$KEY` is
/// preserved literally. Falls back to a whitespace split if the shell parse
/// fails.
pub fn parse_arguments(args: &str) -> Vec<String> {
    if args.is_empty() || args.trim().is_empty() {
        return Vec::new();
    }
    match parse_with_env(args, &|key| Some(format!("${key}"))) {
        Ok(tokens) => tokens
            .into_iter()
            .filter_map(|t| match t {
                ParseEntry::Word(s) => Some(s),
                _ => None,
            })
            .collect(),
        Err(_) => args.split_whitespace().map(String::from).collect(),
    }
}

/// The `arguments` frontmatter value: a space-separated string or a list.
pub enum ArgNamesInput {
    Str(String),
    List(Vec<String>),
}

/// Parse the frontmatter `arguments` field into named-argument positions: empty
/// and numeric-only names are dropped.
pub fn parse_argument_names(input: Option<ArgNamesInput>) -> Vec<String> {
    let is_valid = |name: &str| -> bool {
        let t = name.trim();
        !t.is_empty() && !name.chars().all(|c| c.is_ascii_digit())
    };
    match input {
        None => Vec::new(),
        Some(ArgNamesInput::List(list)) => list.into_iter().filter(|n| is_valid(n)).collect(),
        Some(ArgNamesInput::Str(s)) => s
            .split_whitespace()
            .filter(|n| is_valid(n))
            .map(String::from)
            .collect(),
    }
}

static ARGS_INDEXED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$ARGUMENTS\[(\d+)\]").unwrap());
static ARGS_SHORTHAND: LazyLock<FancyRegex> =
    LazyLock::new(|| FancyRegex::new(r"\$(\d+)(?!\w)").unwrap());

/// Global replace with a function replacer, for a fancy-regex pattern.
fn frx_replace(
    re: &FancyRegex,
    text: &str,
    f: impl Fn(&fancy_regex::Captures) -> String,
) -> String {
    let mut out = String::new();
    let mut last = 0;
    for cap in re.captures_iter(text) {
        let cap = match cap {
            Ok(c) => c,
            Err(_) => break,
        };
        let m = cap.get(0).unwrap();
        out.push_str(&text[last..m.start()]);
        out.push_str(&f(&cap));
        last = m.end();
    }
    out.push_str(&text[last..]);
    out
}

/// Substitute `$ARGUMENTS` placeholders in `content`. Order: named `$name` →
/// `$ARGUMENTS[n]` → `$n` → `$ARGUMENTS`. A missing arg substitutes "".
/// `args == None` returns content unchanged.
pub fn substitute_arguments(
    content: &str,
    args: Option<&str>,
    append_if_no_placeholder: bool,
    argument_names: &[String],
) -> String {
    let args = match args {
        None => return content.to_string(),
        Some(a) => a,
    };

    let parsed_args = parse_arguments(args);
    let original = content.to_string();
    let mut result = content.to_string();

    for (i, name) in argument_names.iter().enumerate() {
        if name.is_empty() {
            continue;
        }
        // `$name` but not `$name[...]` or `$nameXxx`. Name is regex-escaped.
        let re = FancyRegex::new(&format!(r"\${}(?![\[\w])", regex::escape(name))).unwrap();
        let val = parsed_args.get(i).cloned().unwrap_or_default();
        result = frx_replace(&re, &result, |_| val.clone());
    }

    result = ARGS_INDEXED
        .replace_all(&result, |c: &regex::Captures| {
            let idx: usize = c[1].parse().unwrap_or(usize::MAX);
            parsed_args.get(idx).cloned().unwrap_or_default()
        })
        .into_owned();

    result = frx_replace(&ARGS_SHORTHAND, &result, |c| {
        let idx: usize = c[1].parse().unwrap_or(usize::MAX);
        parsed_args.get(idx).cloned().unwrap_or_default()
    });

    result = result.replace("$ARGUMENTS", args);

    if result == original && append_if_no_placeholder && !args.is_empty() {
        result = format!("{result}\n\nARGUMENTS: {args}");
    }
    result
}

/// A boxed future returned by the host shell runner.
pub type RunBashFuture = Pin<Box<dyn Future<Output = Result<String, String>> + Send>>;
/// A host-provided shell runner for inline-bash expansion; returns the command's
/// stdout, or an error (a permission denial).
pub type RunBash = Arc<dyn Fn(String, Option<String>) -> RunBashFuture + Send + Sync>;

/// Replace `${KEY}` occurrences from a vars map, inserting values literally.
fn replace_vars(content: &str, vars: &[(String, String)]) -> String {
    let mut out = content.to_string();
    for (key, value) in vars {
        out = out.replace(&format!("${{{key}}}"), value);
    }
    out
}

static CODE_BLOCK_BASH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"```!\s*\n?([\s\S]*?)\n?```").unwrap());
static INLINE_BASH: LazyLock<FancyRegex> =
    LazyLock::new(|| FancyRegex::new(r"(?m)(?<=^|\s)!`([^`]+)`").unwrap());

struct BashJob {
    start: usize,
    end: usize,
    command: String,
}

/// Execute inline-shell directives and inline their stdout. Matches are
/// collected up-front with byte ranges, run, then spliced by INDEX from right to
/// left. A `run_bash` error propagates (blocking expansion on denial).
pub async fn execute_inline_bash(
    content: &str,
    run_bash: &RunBash,
    shell: Option<&str>,
) -> Result<String, String> {
    let mut raw: Vec<BashJob> = Vec::new();
    for c in CODE_BLOCK_BASH.captures_iter(content) {
        let m = c.get(0).unwrap();
        raw.push(BashJob {
            start: m.start(),
            end: m.end(),
            command: c
                .get(1)
                .map(|g| g.as_str().trim().to_string())
                .unwrap_or_default(),
        });
    }
    for c in INLINE_BASH.captures_iter(content).flatten() {
        let m = c.get(0).unwrap();
        raw.push(BashJob {
            start: m.start(),
            end: m.end(),
            command: c
                .get(1)
                .map(|g| g.as_str().trim().to_string())
                .unwrap_or_default(),
        });
    }
    // Sort by start; drop any range that overlaps one already kept.
    raw.sort_by_key(|j| j.start);
    let mut jobs: Vec<BashJob> = Vec::new();
    let mut last_end: isize = -1;
    for j in raw {
        if j.start as isize >= last_end {
            last_end = j.end as isize;
            jobs.push(j);
        }
    }
    // Run all commands, then splice right-to-left so earlier ranges stay valid.
    let mut outputs: Vec<String> = Vec::with_capacity(jobs.len());
    for job in &jobs {
        let out = run_bash(job.command.clone(), shell.map(String::from)).await?;
        outputs.push(out);
    }
    let mut out = content.to_string();
    for (i, job) in jobs.iter().enumerate().rev() {
        out = format!("{}{}{}", &out[..job.start], &outputs[i], &out[job.end..]);
    }
    Ok(out)
}

/// Options for [`expand_command_body`].
#[derive(Default)]
pub struct ExpandOptions {
    pub argument_names: Vec<String>,
    pub vars: Option<Vec<(String, String)>>,
    pub run_bash: Option<RunBash>,
    pub shell: Option<String>,
    pub append_if_no_placeholder: Option<bool>,
}

/// Expand a skill/command body end-to-end: argument substitution → `${VAR}`
/// replacement → inline-bash execution.
pub async fn expand_command_body(
    body: &str,
    args: Option<&str>,
    opts: ExpandOptions,
) -> Result<String, String> {
    let mut out = substitute_arguments(
        body,
        args,
        opts.append_if_no_placeholder.unwrap_or(true),
        &opts.argument_names,
    );
    if let Some(vars) = &opts.vars {
        out = replace_vars(&out, vars);
    }
    if let Some(run_bash) = &opts.run_bash {
        out = execute_inline_bash(&out, run_bash, opts.shell.as_deref()).await?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runner(f: impl Fn(String) -> Result<String, String> + Send + Sync + 'static) -> RunBash {
        let f = Arc::new(f);
        Arc::new(move |cmd, _shell| {
            let f = f.clone();
            Box::pin(async move { f(cmd) })
        })
    }

    #[test]
    fn parse_arguments_cases() {
        assert_eq!(parse_arguments("foo bar baz"), vec!["foo", "bar", "baz"]);
        assert_eq!(
            parse_arguments("foo \"hello world\" baz"),
            vec!["foo", "hello world", "baz"]
        );
        assert_eq!(parse_arguments("a 'b c' d"), vec!["a", "b c", "d"]);
        assert_eq!(parse_arguments(""), Vec::<String>::new());
        assert_eq!(parse_arguments("   "), Vec::<String>::new());
        assert_eq!(parse_arguments("$FOO bar"), vec!["$FOO", "bar"]);
        assert_eq!(parse_arguments(";"), Vec::<String>::new());
        assert_eq!(parse_arguments("|"), Vec::<String>::new());
    }

    #[test]
    fn parse_argument_names_cases() {
        assert_eq!(
            parse_argument_names(Some(ArgNamesInput::Str("foo bar baz".into()))),
            vec!["foo", "bar", "baz"]
        );
        assert_eq!(
            parse_argument_names(Some(ArgNamesInput::List(vec![
                "a".into(),
                "".into(),
                "1".into(),
                "b".into()
            ]))),
            vec!["a", "b"]
        );
        assert_eq!(parse_argument_names(None), Vec::<String>::new());
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn substitute_arguments_cases() {
        assert_eq!(
            substitute_arguments(
                "Hello $who from $where",
                Some("Ada London"),
                true,
                &names(&["who", "where"])
            ),
            "Hello Ada from London"
        );
        assert_eq!(
            substitute_arguments("$ARGUMENTS[0]-$ARGUMENTS[1]", Some("a b"), true, &[]),
            "a-b"
        );
        assert_eq!(
            substitute_arguments("$0/$1/$2", Some("x y"), true, &[]),
            "x/y/"
        );
        assert_eq!(
            substitute_arguments("run: $ARGUMENTS", Some("a \"b c\""), true, &[]),
            "run: a \"b c\""
        );
        assert_eq!(substitute_arguments("[$1]", Some("only"), true, &[]), "[]");
        assert_eq!(
            substitute_arguments("do the thing", Some("now"), true, &[]),
            "do the thing\n\nARGUMENTS: now"
        );
        assert_eq!(substitute_arguments("do it", Some(""), true, &[]), "do it");
        assert_eq!(
            substitute_arguments("keep $ARGUMENTS", None, true, &[]),
            "keep $ARGUMENTS"
        );
        assert_eq!(
            substitute_arguments("$foo $foobar $foo[0]", Some("V"), true, &names(&["foo"])),
            "V $foobar $foo[0]"
        );
    }

    #[test]
    fn named_argument_metacharacters_match_literally() {
        assert_eq!(
            substitute_arguments(
                "$a.b|$a[b|$a.bx|$a[b[0]",
                Some("ONE TWO"),
                true,
                &names(&["a.b", "a[b"]),
            ),
            "ONE|TWO|$a.bx|$a[b[0]"
        );
    }

    #[tokio::test]
    async fn expand_vars_and_inline_bash() {
        assert_eq!(
            expand_command_body(
                "dir=${KLAUS_SKILL_DIR}",
                Some(""),
                ExpandOptions {
                    vars: Some(vec![("KLAUS_SKILL_DIR".into(), "/x".into())]),
                    ..Default::default()
                }
            )
            .await
            .unwrap(),
            "dir=/x"
        );

        let out = expand_command_body(
            "branch is !`git branch`",
            Some(""),
            ExpandOptions {
                run_bash: Some(runner(|cmd| Ok(format!("[ran:{cmd}]")))),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(out, "branch is [ran:git branch]");

        let out = expand_command_body(
            "before\n```!\necho hi\n```\nafter",
            Some(""),
            ExpandOptions {
                run_bash: Some(runner(|cmd| Ok(format!("OUT({cmd})")))),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(out, "before\nOUT(echo hi)\nafter");

        assert_eq!(
            expand_command_body("x !`echo hi` y", Some(""), ExpandOptions::default())
                .await
                .unwrap(),
            "x !`echo hi` y"
        );

        let out = expand_command_body(
            "!`echo $ARGUMENTS`",
            Some("hello"),
            ExpandOptions {
                run_bash: Some(runner(|cmd| Ok(format!("ran<{cmd}>")))),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(out, "ran<echo hello>");
    }

    #[tokio::test]
    async fn inline_bash_stdout_is_literal() {
        let out = expand_command_body(
            "x !`cmd` y",
            Some(""),
            ExpandOptions {
                run_bash: Some(runner(|_| Ok("A$&B$`C$$D".to_string()))),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(out, "x A$&B$`C$$D y");

        assert_eq!(
            expand_command_body(
                "p=${DIR}",
                Some(""),
                ExpandOptions {
                    vars: Some(vec![("DIR".into(), "/a$&b$`c".into())]),
                    ..Default::default()
                }
            )
            .await
            .unwrap(),
            "p=/a$&b$`c"
        );
    }

    #[tokio::test]
    async fn index_splice_preserves_injected_marker() {
        let out = expand_command_body(
            "!`a` !`b`",
            Some(""),
            ExpandOptions {
                run_bash: Some(runner(|cmd| {
                    Ok(if cmd == "a" {
                        "INJ:!`b`".to_string()
                    } else {
                        "RAN".to_string()
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(out, "INJ:!`b` RAN");
    }

    #[tokio::test]
    async fn propagates_run_bash_error() {
        let rb = runner(|_| Err("denied".to_string()));
        let err = execute_inline_bash("!`rm -rf /`", &rb, None)
            .await
            .unwrap_err();
        assert!(err.contains("denied"));
    }
}
