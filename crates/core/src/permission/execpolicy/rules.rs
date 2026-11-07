//! Exec-policy rule model. A regex-free classifier that maps a shell command to
//! one of three decisions. It is a *static* heuristic layer that runs before the
//! interactive permission flow — it never executes anything itself.
//!
//! The model is a compact, structured rule set. Each rule
//! is keyed by `program` (the command's first token) and carries an ordered list
//! of structured [`ArgMatcher`]s describing the arguments it accepts (positional
//! patterns, flags, options-with-values, and "rest" wildcards). The engine
//! aggregates decisions across every matching rule with `forbidden > prompt >
//! allow`, and the MOST SPECIFIC matching rule wins ties.

use crate::permission::execpolicy::matchers::{
    flag, literal, option, rest, rest_at_least_one, ArgMatcher, OptionOpts, PatternToken,
};

/// The three possible static decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecDecision {
    Allow,
    Prompt,
    Forbidden,
}

impl ExecDecision {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecDecision::Allow => "allow",
            ExecDecision::Prompt => "prompt",
            ExecDecision::Forbidden => "forbidden",
        }
    }
}

/// A single rule.
///
///  - `program` must equal the command's first token.
///  - `match_`, if present, is the structured argument matcher (preferred). A
///    rule with no `match_` accepts any arguments for its program.
///  - `argv`, if present, is the legacy flat-prefix form: each entry must equal
///    the corresponding argv token in order (the command may have MORE trailing
///    tokens). `match_` takes precedence if both are given.
///  - `justification` optionally documents WHY the rule exists.
///  - `examples` / `not_examples` are self-tests consumed by `ExecPolicy.validate()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecRule {
    pub program: String,
    /// Structured argument matcher (preferred).
    pub match_: Option<Vec<ArgMatcher>>,
    /// Legacy flat-prefix tokens (back-compat). Normalized to literal matchers.
    pub argv: Option<Vec<String>>,
    pub decision: ExecDecision,
    /// When true, option-looking tokens NOT enumerated by this rule's matchers
    /// are treated as opaque positional operands instead of failing the match.
    pub ignore_unknown_flags: bool,
    /// Optional human-readable rationale for the rule.
    pub justification: Option<String>,
    /// Commands that SHOULD aggregate to `decision`.
    pub examples: Vec<String>,
    /// Commands that should NOT aggregate to `decision`.
    pub not_examples: Vec<String>,
}

impl ExecRule {
    /// Minimal builder helpers to keep the (large) default rule set readable.
    fn new(program: &str, decision: ExecDecision) -> ExecRule {
        ExecRule {
            program: program.to_string(),
            match_: None,
            argv: None,
            decision,
            ignore_unknown_flags: false,
            justification: None,
            examples: Vec::new(),
            not_examples: Vec::new(),
        }
    }

    fn with_match(mut self, m: Vec<ArgMatcher>) -> ExecRule {
        self.match_ = Some(m);
        self
    }

    fn ignore_unknown(mut self) -> ExecRule {
        self.ignore_unknown_flags = true;
        self
    }

    fn just(mut self, j: &str) -> ExecRule {
        self.justification = Some(j.to_string());
        self
    }

    fn ex(mut self, examples: &[&str]) -> ExecRule {
        self.examples = examples.iter().map(|s| s.to_string()).collect();
        self
    }

    fn notex(mut self, not_examples: &[&str]) -> ExecRule {
        self.not_examples = not_examples.iter().map(|s| s.to_string()).collect();
        self
    }
}

// ── Local matcher shorthands (construct the ArgMatcher values) ───────────────
fn s(v: &str) -> PatternToken {
    PatternToken::single(v)
}
fn a(v: &[&str]) -> PatternToken {
    PatternToken::alts(v)
}
fn f(names: PatternToken) -> ArgMatcher {
    flag(names, false)
}
fn freq(names: PatternToken) -> ArgMatcher {
    flag(names, true)
}
fn opt(names: PatternToken) -> ArgMatcher {
    option(names, OptionOpts::default())
}
fn opt_int(names: PatternToken) -> ArgMatcher {
    option(
        names,
        OptionOpts {
            required: false,
            value_is_pos_int: true,
        },
    )
}
fn lit(token: PatternToken) -> ArgMatcher {
    literal(token)
}

/// Zero-or-more trailing operands (paths / patterns / opaque).
fn args_or_none() -> Vec<ArgMatcher> {
    vec![rest()]
}
/// One-or-more trailing operands.
fn args_at_least_one() -> Vec<ArgMatcher> {
    vec![rest_at_least_one()]
}

/// A comprehensive default rule set for an agent context. Three tiers:
/// allow / prompt / forbidden.
///
/// Rule ORDER does not matter: the engine applies max-aggregation across all
/// matching rules (forbidden > prompt > allow), and among equally-severe matches
/// prefers the MOST SPECIFIC rule. Any program with no matching rule defaults to
/// `prompt`.
pub fn default_exec_rules() -> Vec<ExecRule> {
    use ExecDecision::*;
    let mut rules: Vec<ExecRule> = vec![ExecRule::new("ls", Allow)
        .with_match(vec![
            f(a(&[
                "-1", "-a", "-A", "-l", "-h", "-R", "-t", "-r", "-S", "-d", "-F", "--color",
                "--all",
            ])),
            rest(),
        ])
        .just("Directory listing is read-only.")
        .ex(&["ls", "ls -la /tmp", "ls -1", "ls --color src"])];

    // ════════════════════════════════════════════════════════════════════════
    // ALLOW — read-only filesystem & text inspection
    // ════════════════════════════════════════════════════════════════════════
    rules.push(
        ExecRule::new("cat", Allow)
            .with_match(vec![
                f(a(&["-b", "-n", "-s", "-A", "-e", "-t", "-v"])),
                rest_at_least_one(),
            ])
            .just("Reading file contents is read-only.")
            .ex(&["cat package.json", "cat -n file", "cat a b c"])
            .notex(&["cat"]),
    );
    rules.push(
        ExecRule::new("head", Allow)
            .with_match(vec![
                opt_int(a(&["-c", "-n", "--lines", "--bytes"])),
                rest_at_least_one(),
            ])
            .just("Reading the head of a file is read-only.")
            .ex(&["head -n 20 file", "head file", "head -n=5 file"])
            .notex(&["head"]),
    );
    rules.push(
        ExecRule::new("tail", Allow)
            .with_match(vec![
                f(a(&["-f", "-F"])),
                opt_int(a(&["-c", "-n", "--lines", "--bytes"])),
                rest_at_least_one(),
            ])
            .just("Reading the tail of a file is read-only.")
            .ex(&["tail -f log", "tail -n 100 file", "tail file"])
            .notex(&["tail"]),
    );
    rules.push(
        ExecRule::new("pwd", Allow)
            .with_match(vec![f(a(&["-L", "-P"])), rest()])
            .ex(&["pwd", "pwd -P"]),
    );
    rules.push(
        ExecRule::new("echo", Allow)
            .with_match(args_or_none())
            .ex(&["echo hello", "echo"]),
    );
    rules.push(
        ExecRule::new("printf", Allow)
            .with_match(args_or_none())
            .ex(&["printf %s x"]),
    );
    rules.push(
        ExecRule::new("printenv", Allow)
            .with_match(args_or_none())
            .ex(&["printenv", "printenv PATH"]),
    );
    rules.push(
        ExecRule::new("grep", Allow)
            .with_match(vec![
                opt_int(a(&["-A", "-B", "-C", "-m", "--max-count"])),
                opt(a(&["-e", "--regexp", "-f", "--file"])),
                f(a(&[
                    "-r", "-R", "-n", "-i", "-l", "-L", "-v", "-w", "-x", "-c", "-o", "-E", "-F",
                    "-H", "-h", "--color",
                ])),
                rest_at_least_one(),
            ])
            .just("Searching file contents is read-only.")
            .ex(&["grep -rn foo src", "grep -i pattern file", "grep foo a b"])
            .notex(&["grep"]),
    );
    rules.push(
        ExecRule::new("rg", Allow)
            .with_match(vec![
                opt_int(a(&[
                    "-A",
                    "-B",
                    "-C",
                    "-d",
                    "--max-depth",
                    "-m",
                    "--max-count",
                ])),
                opt(a(&["-g", "--glob", "-t", "--type", "-e", "--regexp"])),
                f(a(&[
                    "-n",
                    "-i",
                    "-l",
                    "-w",
                    "-v",
                    "-c",
                    "-o",
                    "-S",
                    "-F",
                    "--files",
                    "--files-with-matches",
                    "--files-without-match",
                    "--hidden",
                    "--no-ignore",
                    "--json",
                ])),
                rest_at_least_one(),
            ])
            .just("ripgrep searches file contents read-only.")
            .ex(&[
                "rg --json pattern",
                "rg -n init .",
                "rg --files --max-depth 2 .",
                "rg -i foo src",
            ])
            .notex(&["rg"]),
    );
    rules.push(
        ExecRule::new("find", Allow)
            .with_match(vec![
                opt(a(&[
                    "-name",
                    "-iname",
                    "-path",
                    "-ipath",
                    "-type",
                    "-maxdepth",
                    "-mindepth",
                    "-newer",
                    "-size",
                    "-mtime",
                    "-regex",
                    "-prune",
                ])),
                f(a(&[
                    "-L", "-H", "-P", "-print", "-print0", "-empty", "-not", "-a", "-o",
                ])),
                rest(),
            ])
            .just("Filesystem traversal without -delete/-exec is read-only.")
            .ex(&[
                "find . -name \"*.ts\"",
                "find . -type f",
                "find src -maxdepth 2",
            ])
            .notex(&["find . -delete", "find . -exec rm {} ;"]),
    );
    rules.push(
        ExecRule::new("wc", Allow)
            .with_match(vec![f(a(&["-l", "-w", "-c", "-m", "-L"])), rest()])
            .ex(&["wc -l file", "wc file"]),
    );
    rules.push(
        ExecRule::new("which", Allow)
            .with_match(vec![f(a(&["-a", "-s"])), rest_at_least_one()])
            .ex(&["which node", "which -a python3"])
            .notex(&["which"]),
    );
    rules.push(
        ExecRule::new("true", Allow)
            .with_match(args_or_none())
            .ex(&["true"]),
    );
    rules.push(
        ExecRule::new("false", Allow)
            .with_match(args_or_none())
            .ex(&["false"]),
    );
    rules.push(
        ExecRule::new("date", Allow)
            .with_match(args_or_none())
            .ex(&["date", "date +%s"]),
    );
    rules.push(
        ExecRule::new("whoami", Allow)
            .with_match(args_or_none())
            .ex(&["whoami"]),
    );
    rules.push(
        ExecRule::new("hostname", Allow)
            .with_match(args_or_none())
            .ex(&["hostname"]),
    );
    rules.push(
        ExecRule::new("uname", Allow)
            .with_match(args_or_none())
            .ex(&["uname -a"]),
    );
    rules.push(
        ExecRule::new("id", Allow)
            .with_match(args_or_none())
            .ex(&["id", "id -u"]),
    );
    rules.push(
        ExecRule::new("env", Prompt)
            .with_match(vec![f(a(&["-i", "-0"])), rest()])
            .just("env can launch an arbitrary program; surface for approval.")
            .ex(&["env FOO=bar node", "env node"]),
    );
    rules.push(
        ExecRule::new("sort", Allow)
            .with_match(args_or_none())
            .ex(&["sort file", "sort -u f"]),
    );
    rules.push(
        ExecRule::new("uniq", Allow)
            .with_match(args_or_none())
            .ex(&["uniq file"]),
    );
    rules.push(
        ExecRule::new("diff", Allow)
            .with_match(args_at_least_one())
            .ex(&["diff a b", "diff -u a b"])
            .notex(&["diff"]),
    );
    rules.push(
        ExecRule::new("cut", Allow)
            .with_match(args_or_none())
            .ex(&["cut -d, -f1 file"]),
    );
    rules.push(
        ExecRule::new("tr", Allow)
            .with_match(args_or_none())
            .ex(&["tr a b"]),
    );
    rules.push(
        ExecRule::new("realpath", Allow)
            .with_match(args_or_none())
            .ex(&["realpath ."]),
    );
    rules.push(
        ExecRule::new("readlink", Allow)
            .with_match(args_or_none())
            .ex(&["readlink -f x"]),
    );
    rules.push(
        ExecRule::new("basename", Allow)
            .with_match(args_or_none())
            .ex(&["basename /a/b"]),
    );
    rules.push(
        ExecRule::new("dirname", Allow)
            .with_match(args_or_none())
            .ex(&["dirname /a/b"]),
    );
    rules.push(
        ExecRule::new("stat", Allow)
            .with_match(args_or_none())
            .ex(&["stat file"]),
    );
    rules.push(
        ExecRule::new("file", Allow)
            .with_match(args_or_none())
            .ex(&["file bin"]),
    );
    rules.push(
        ExecRule::new("tree", Allow)
            .with_match(args_or_none())
            .ex(&["tree -L 2"]),
    );

    // ── ALLOW — read-only subcommands of otherwise-mutating tools ──────────────
    rules.push(
        ExecRule::new("git", Allow)
            .with_match(vec![lit(s("status")), rest()])
            .ex(&["git status", "git status --short"]),
    );
    rules.push(
        ExecRule::new("git", Allow)
            .with_match(vec![
                lit(a(&["diff", "show", "log", "blame", "reflog", "shortlog"])),
                rest(),
            ])
            .ex(&[
                "git diff HEAD",
                "git log --oneline",
                "git show HEAD",
                "git blame file",
            ]),
    );
    rules.push(
        ExecRule::new("git", Allow)
            .with_match(vec![lit(s("branch")), rest()])
            .ex(&["git branch", "git branch -a"])
            .notex(&["git branch -D feature"]),
    );
    rules.push(
        ExecRule::new("git", Allow)
            .with_match(vec![
                lit(a(&[
                    "describe",
                    "rev-parse",
                    "remote",
                    "config",
                    "ls-files",
                    "tag",
                ])),
                rest(),
            ])
            .ex(&[
                "git rev-parse HEAD",
                "git remote -v",
                "git config --get user.name",
                "git tag",
            ]),
    );
    rules.push(
        ExecRule::new("node", Allow)
            .with_match(vec![lit(a(&["--version", "-v", "--help"])), rest()])
            .ex(&["node --version", "node -v"]),
    );
    rules.push(
        ExecRule::new("npm", Allow)
            .with_match(vec![
                lit(a(&[
                    "--version",
                    "-v",
                    "ls",
                    "list",
                    "view",
                    "outdated",
                    "why",
                ])),
                rest(),
            ])
            .ex(&["npm --version", "npm ls", "npm outdated"]),
    );
    rules.push(
        ExecRule::new("python", Allow)
            .with_match(vec![lit(a(&["--version", "-V"])), rest()])
            .ex(&["python --version", "python -V"]),
    );
    rules.push(
        ExecRule::new("python3", Allow)
            .with_match(vec![lit(a(&["--version", "-V"])), rest()])
            .ex(&["python3 --version"]),
    );
    rules.push(
        ExecRule::new("cargo", Allow)
            .with_match(vec![
                lit(a(&["--version", "-V", "check", "tree", "metadata"])),
                rest(),
            ])
            .ex(&["cargo --version", "cargo check", "cargo tree"]),
    );

    // ════════════════════════════════════════════════════════════════════════
    // PROMPT — mutating / network / process-control
    // ════════════════════════════════════════════════════════════════════════
    rules.push(
        ExecRule::new("rm", Prompt)
            .with_match(args_at_least_one())
            .just("Deleting files is destructive; surface for approval.")
            .ex(&["rm file", "rm -r dir", "rm -rf ./build"])
            .notex(&["rm -rf /", "rm -fr /"]),
    );
    rules.push(
        ExecRule::new("mv", Prompt)
            .with_match(args_at_least_one())
            .ex(&["mv a b"]),
    );
    rules.push(
        ExecRule::new("cp", Prompt)
            .with_match(args_at_least_one())
            .ex(&["cp a b", "cp -r src dst"]),
    );
    rules.push(
        ExecRule::new("dd", Prompt)
            .with_match(args_or_none())
            .ex(&["dd if=a of=b"]),
    );
    rules.push(
        ExecRule::new("chmod", Prompt)
            .with_match(args_at_least_one())
            .ex(&["chmod +x script"]),
    );
    rules.push(
        ExecRule::new("chown", Prompt)
            .with_match(args_at_least_one())
            .ex(&["chown user file"]),
    );
    rules.push(
        ExecRule::new("ln", Prompt)
            .with_match(args_at_least_one())
            .ex(&["ln -s a b"]),
    );
    rules.push(
        ExecRule::new("mkdir", Prompt)
            .with_match(args_at_least_one())
            .ex(&["mkdir newdir"]),
    );
    rules.push(
        ExecRule::new("rmdir", Prompt)
            .with_match(args_at_least_one())
            .ex(&["rmdir olddir"]),
    );
    rules.push(
        ExecRule::new("touch", Prompt)
            .with_match(args_at_least_one())
            .ex(&["touch file"]),
    );
    rules.push(
        ExecRule::new("truncate", Prompt)
            .with_match(args_or_none())
            .ex(&["truncate -s 0 file"]),
    );
    rules.push(
        ExecRule::new("tee", Prompt)
            .with_match(args_or_none())
            .just("tee writes its stdin to files.")
            .ex(&["tee out.txt"]),
    );
    rules.push(
        ExecRule::new("find", Prompt)
            .with_match(vec![freq(s("-delete")), rest()])
            .ignore_unknown()
            .just("find -delete removes matched files.")
            .ex(&["find . -name \"*.tmp\" -delete", "find . -delete"]),
    );
    rules.push(
        ExecRule::new("find", Prompt)
            .with_match(vec![
                freq(a(&["-exec", "-execdir", "-ok", "-okdir"])),
                rest(),
            ])
            .ignore_unknown()
            .just("find -exec runs an arbitrary program per match.")
            .ex(&["find . -name x -execdir cat foo ;"]),
    );
    rules.push(
        ExecRule::new("kill", Prompt)
            .with_match(args_or_none())
            .ex(&["kill -9 123"]),
    );
    rules.push(
        ExecRule::new("killall", Prompt)
            .with_match(args_or_none())
            .ex(&["killall node"]),
    );
    rules.push(
        ExecRule::new("pkill", Prompt)
            .with_match(args_or_none())
            .ex(&["pkill node"]),
    );
    rules.push(
        ExecRule::new("curl", Prompt)
            .with_match(args_or_none())
            .ex(&["curl https://x.test"]),
    );
    rules.push(
        ExecRule::new("wget", Prompt)
            .with_match(args_or_none())
            .ex(&["wget https://x.test"]),
    );
    rules.push(
        ExecRule::new("ssh", Prompt)
            .with_match(args_at_least_one())
            .ex(&["ssh host"]),
    );
    rules.push(
        ExecRule::new("scp", Prompt)
            .with_match(args_at_least_one())
            .ex(&["scp a host:b"]),
    );
    rules.push(
        ExecRule::new("rsync", Prompt)
            .with_match(args_at_least_one())
            .ex(&["rsync a b"]),
    );
    rules.push(
        ExecRule::new("nc", Prompt)
            .with_match(args_or_none())
            .ex(&["nc -l 8080"]),
    );
    rules.push(
        ExecRule::new("telnet", Prompt)
            .with_match(args_or_none())
            .ex(&["telnet host 23"]),
    );
    rules.push(
        ExecRule::new("git", Prompt)
            .with_match(vec![lit(a(&["push", "pull", "fetch", "clone"])), rest()])
            .just("git network operations touch a remote.")
            .ex(&[
                "git push",
                "git push origin main",
                "git pull",
                "git clone url",
            ]),
    );
    rules.push(
        ExecRule::new("git", Prompt)
            .with_match(vec![
                lit(a(&[
                    "commit",
                    "merge",
                    "rebase",
                    "reset",
                    "checkout",
                    "restore",
                    "stash",
                    "cherry-pick",
                    "revert",
                    "am",
                    "apply",
                ])),
                rest(),
            ])
            .just("git history/worktree mutations.")
            .ex(&[
                "git commit -m x",
                "git merge feature",
                "git reset --hard",
                "git checkout main",
            ]),
    );
    rules.push(
        ExecRule::new("git", Prompt)
            .with_match(vec![
                lit(s("branch")),
                freq(a(&["-d", "-D", "-m", "-M", "--delete"])),
                rest(),
            ])
            .just("git branch -d/-D deletes or renames branches.")
            .ex(&["git branch -D feature", "git branch -d old"]),
    );
    rules.push(
        ExecRule::new("npm", Prompt)
            .with_match(vec![
                lit(a(&[
                    "install",
                    "i",
                    "ci",
                    "run",
                    "exec",
                    "publish",
                    "update",
                    "uninstall",
                    "rm",
                ])),
                rest(),
            ])
            .just("npm install/run executes arbitrary lifecycle scripts.")
            .ex(&[
                "npm install",
                "npm install left-pad",
                "npm run build",
                "npm ci",
            ]),
    );
    rules.push(
        ExecRule::new("pnpm", Prompt)
            .with_match(vec![
                lit(a(&[
                    "install", "i", "add", "run", "exec", "dlx", "publish", "update", "remove",
                ])),
                rest(),
            ])
            .ex(&["pnpm install", "pnpm add left-pad", "pnpm run build"]),
    );
    rules.push(
        ExecRule::new("yarn", Prompt)
            .with_match(vec![
                lit(a(&[
                    "install", "add", "run", "dlx", "publish", "upgrade", "remove",
                ])),
                rest(),
            ])
            .ex(&["yarn install", "yarn add left-pad", "yarn run build"]),
    );
    rules.push(
        ExecRule::new("pip", Prompt)
            .with_match(vec![lit(a(&["install", "uninstall", "download"])), rest()])
            .ex(&["pip install requests", "pip uninstall requests"]),
    );
    rules.push(
        ExecRule::new("pip3", Prompt)
            .with_match(vec![lit(a(&["install", "uninstall", "download"])), rest()])
            .ex(&["pip3 install requests"]),
    );
    rules.push(
        ExecRule::new("cargo", Prompt)
            .with_match(vec![
                lit(a(&["install", "publish", "run", "build", "test", "update"])),
                rest(),
            ])
            .ex(&["cargo install ripgrep", "cargo run", "cargo build"]),
    );
    rules.push(
        ExecRule::new("brew", Prompt)
            .with_match(vec![
                lit(a(&[
                    "install",
                    "uninstall",
                    "upgrade",
                    "remove",
                    "reinstall",
                ])),
                rest(),
            ])
            .ex(&["brew install jq"]),
    );
    rules.push(
        ExecRule::new("apt", Prompt)
            .with_match(vec![
                lit(a(&["install", "remove", "purge", "upgrade"])),
                rest(),
            ])
            .ex(&["apt install curl"]),
    );
    rules.push(
        ExecRule::new("sudo", Prompt)
            .with_match(args_or_none())
            .ex(&["sudo ls"]),
    );
    rules.push(
        ExecRule::new("doas", Prompt)
            .with_match(args_or_none())
            .ex(&["doas ls"]),
    );
    rules.push(
        ExecRule::new("su", Prompt)
            .with_match(args_or_none())
            .ex(&["su root"]),
    );
    rules.push(
        ExecRule::new("sh", Prompt)
            .with_match(args_or_none())
            .just("Spawning a shell runs arbitrary commands.")
            .ex(&["sh script.sh", "sh -c \"echo hi\""]),
    );
    rules.push(
        ExecRule::new("bash", Prompt)
            .with_match(args_or_none())
            .ex(&["bash script.sh"]),
    );
    rules.push(
        ExecRule::new("zsh", Prompt)
            .with_match(args_or_none())
            .ex(&["zsh script.sh"]),
    );
    rules.push(
        ExecRule::new("eval", Prompt)
            .with_match(args_or_none())
            .ex(&["eval cmd"]),
    );
    rules.push(
        ExecRule::new("exec", Prompt)
            .with_match(args_or_none())
            .ex(&["exec node"]),
    );
    rules.push(
        ExecRule::new("xargs", Prompt)
            .with_match(args_or_none())
            .just("xargs runs an arbitrary program over its input.")
            .ex(&["xargs rm"]),
    );
    rules.push(
        ExecRule::new("python", Prompt)
            .with_match(vec![lit(a(&["-c", "-m"])), rest()])
            .just("python -c / -m executes arbitrary code.")
            .ex(&["python -c \"print(1)\"", "python -m http.server"]),
    );
    rules.push(
        ExecRule::new("node", Prompt)
            .with_match(vec![lit(a(&["-e", "--eval", "-p", "--print"])), rest()])
            .just("node -e executes arbitrary code.")
            .ex(&["node -e \"process.exit(0)\""]),
    );
    rules.push(
        ExecRule::new("sed", Prompt)
            .with_match(args_or_none())
            .just("sed can edit in place (-i) and GNU sed can shell out (s///e).")
            .ex(&["sed -i s/a/b/ file", "sed s/a/b/ file"]),
    );

    // ════════════════════════════════════════════════════════════════════════
    // FORBIDDEN — unambiguously catastrophic
    // ════════════════════════════════════════════════════════════════════════
    rules.push(
        ExecRule::new("rm", Forbidden)
            .with_match(vec![f(a(&["-rf", "-fr", "-Rf", "-fR"])), lit(s("/"))])
            .just("Recursive force-delete of the root filesystem.")
            .ex(&["rm -rf /", "rm -fr /"])
            .notex(&["rm -rf ./build", "rm -rf /tmp/x"]),
    );
    rules.push(
        ExecRule::new("rm", Forbidden)
            .with_match(vec![freq(s("-r")), freq(s("-f")), lit(s("/")), rest()])
            .just("Recursive force-delete of the root filesystem (split flags).")
            .ex(&["rm -r -f /", "rm -f -r /"])
            .notex(&["rm -r -f /tmp"]),
    );
    rules.push(
        ExecRule::new("rm", Forbidden)
            .with_match(vec![freq(s("--no-preserve-root")), rest()])
            .ignore_unknown()
            .just("--no-preserve-root removes the safety net against wiping /.")
            .ex(&["rm -rf --no-preserve-root /", "rm --no-preserve-root -rf /"])
            .notex(&["rm -rf ./build"]),
    );
    rules.push(
        ExecRule::new("mkfs", Forbidden)
            .with_match(args_or_none())
            .just("Formatting a filesystem destroys the target device.")
            .ex(&["mkfs /dev/sda1"]),
    );
    for program in &[
        "mkfs.ext2",
        "mkfs.ext3",
        "mkfs.ext4",
        "mkfs.xfs",
        "mkfs.btrfs",
        "mkfs.fat",
        "mkfs.vfat",
        "mkfs.ntfs",
    ] {
        rules.push(
            ExecRule::new(program, Forbidden)
                .with_match(args_or_none())
                .just("Formatting a filesystem destroys the target device.")
                .ex(&[&format!("{} /dev/sda", program)]),
        );
    }
    rules.push(
        ExecRule::new(":", Forbidden)
            .with_match(vec![lit(s("/never-matches-a-real-fork-bomb-literal"))])
            .just("Fork-bomb builtin; defense-in-depth marker."),
    );

    rules
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_rules_cover_all_three_tiers() {
        let rules = default_exec_rules();
        assert!(rules.iter().any(|r| r.decision == ExecDecision::Allow));
        assert!(rules.iter().any(|r| r.decision == ExecDecision::Prompt));
        assert!(rules.iter().any(|r| r.decision == ExecDecision::Forbidden));
    }
}
