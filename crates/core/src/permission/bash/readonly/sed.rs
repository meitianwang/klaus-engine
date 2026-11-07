//! Read-only classifier for `sed` invocations: [`sed_command_is_allowed_by_allowlist`]
//! decides whether a `sed` command is read-only — no in-place `-i`, no `w`/`W` write
//! commands, no `s///w`, no `e` execute, and no file arguments that could be written.
//!
//! SECURITY: getting a flag type or a dangerous-operation pattern wrong means a
//! writing `sed` gets auto-approved. Every regex, every flag in the allowlist, and
//! every dangerous-operation pattern is deliberately exact — do not summarize or
//! relax them.
//!
//! This is the per-subcommand classifier; compound-command concerns (splitting on
//! `;`/`|`/`&&`, redirects, cd gating) are handled upstream by the decision engine,
//! so this operates on a single already-split subcommand.

use crate::permission::shell_quote::{parse_with_env, ParseEntry};
use fancy_regex::Regex as FancyRegex;
use regex::Regex;
use std::fmt;
use std::sync::LazyLock;

/// The exact whitespace set the classifier recognizes around the `sed` word and
/// when trimming expressions: ASCII blanks and line breaks, NBSP, the Ogham space
/// mark, the general-punctuation spaces, line/paragraph separators, narrow NBSP,
/// medium mathematical space, ideographic space, and the zero-width no-break
/// space (BOM). Kept deliberately explicit (rather than `\s`) so the prefix regex
/// and expression trimming agree on exotic Unicode blanks; NEL (U+0085) is
/// intentionally NOT included.
const BLANK_CLASS: &str =
    "[\\t\\n\\x0B\\x0C\\r \u{00a0}\u{1680}\u{2000}-\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}\u{feff}]";

/// True for characters in the [`BLANK_CLASS`] set.
fn is_blank(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\x0B' | '\x0C' | '\r' | ' ' | '\u{00a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

/// Trim [`BLANK_CLASS`] characters from both ends of a string.
fn trim_blank(s: &str) -> &str {
    s.trim_matches(is_blank)
}

/// A character class matching any single character EXCEPT line terminators
/// (`\n`, `\r`, U+2028, U+2029) — used where a wildcard must not cross line
/// breaks.
const ANY_CHAR: &str = "[^\\n\\r\u{2028}\u{2029}]";

/// The leading `sed` word: optional blanks, `sed`, then at least one blank.
static SED_PREFIX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!("^{BLANK_CLASS}*sed{BLANK_CLASS}+")).unwrap());

/// An unparseable or dangerous-to-parse `sed` command line. Callers must treat
/// this as "not allowed" (fail closed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SedParseError {
    pub message: String,
}

impl fmt::Display for SedParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SedParseError {}

/// Options for [`sed_command_is_allowed_by_allowlist`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SedAllowlistOptions {
    /// When true, allows the `-i` flag and file arguments for substitution
    /// commands (in-place editing).
    pub allow_file_writes: bool,
}

/// Parse a command into tokens (shell lexer with `$VAR` preserved as literal).
fn try_parse_shell_command(command: &str) -> Option<Vec<ParseEntry>> {
    parse_with_env(command, &|name| Some(format!("${name}"))).ok()
}

/// Validate flags against an allowlist. Handles both single flags and combined
/// flags (e.g. `-nE`). Returns true only when every flag is allowed.
fn validate_flags_against_allowlist(flags: &[&str], allowed_flags: &[&str]) -> bool {
    for flag in flags {
        // Handle combined flags like -nE or -Er.
        if flag.starts_with('-') && !flag.starts_with("--") && flag.chars().count() > 2 {
            // Check each character in the combined flag.
            for c in flag.chars().skip(1) {
                let single_flag = format!("-{c}");
                if !allowed_flags.contains(&single_flag.as_str()) {
                    return false;
                }
            }
        } else {
            // Single flag or long flag.
            if !allowed_flags.contains(flag) {
                return false;
            }
        }
    }
    true
}

/// Pattern 1: a line-printing command with the `-n` flag.
/// Allows `sed -n 'N'` / `sed -n 'N,M'` with optional `-E`, `-r`, `-z` flags,
/// and semicolon-separated print commands like `sed -n '1p;2p;3p'`.
/// File arguments ARE allowed for this pattern.
///
/// Public for tests only.
pub fn is_line_printing_command(command: &str, expressions: &[String]) -> bool {
    let Some(sed_match) = SED_PREFIX.find(command) else {
        return false;
    };
    let without_sed = &command[sed_match.end()..];
    let Some(parsed) = try_parse_shell_command(without_sed) else {
        return false;
    };

    // Extract all flags.
    let mut flags: Vec<&str> = Vec::new();
    for arg in &parsed {
        let ParseEntry::Word(word) = arg else {
            continue;
        };
        if word.starts_with('-') && word.as_str() != "--" {
            flags.push(word.as_str());
        }
    }

    // Validate flags — only allow -n, -E, -r, -z and their long forms.
    let allowed_flags = [
        "-n",
        "--quiet",
        "--silent",
        "-E",
        "--regexp-extended",
        "-r",
        "-z",
        "--zero-terminated",
        "--posix",
    ];

    if !validate_flags_against_allowlist(&flags, &allowed_flags) {
        return false;
    }

    // Check whether the -n flag is present (required for Pattern 1).
    let mut has_n_flag = false;
    for flag in &flags {
        if *flag == "-n" || *flag == "--quiet" || *flag == "--silent" {
            has_n_flag = true;
            break;
        }
        // Check in combined flags.
        if flag.starts_with('-') && !flag.starts_with("--") && flag.contains('n') {
            has_n_flag = true;
            break;
        }
    }

    // Must have the -n flag for Pattern 1.
    if !has_n_flag {
        return false;
    }

    // Must have at least one expression.
    if expressions.is_empty() {
        return false;
    }

    // All expressions must be print commands (strict allowlist);
    // semicolon-separated commands are allowed.
    for expr in expressions {
        for cmd in expr.split(';') {
            if !is_print_command(trim_blank(cmd)) {
                return false;
            }
        }
    }

    true
}

/// Whether a single command is a valid print command.
/// STRICT ALLOWLIST — only these exact forms are allowed:
/// - `p` (print all)
/// - `Np` (print line N, where N is digits)
/// - `N,Mp` (print lines N through M)
///
/// Anything else (including `w`, `W`, `e`, `E` commands) is rejected.
///
/// Public for tests only.
pub fn is_print_command(cmd: &str) -> bool {
    if cmd.is_empty() {
        return false;
    }
    // A single strict regex that only matches the allowed print commands:
    // p, 1p, 123p, 1,5p, 10,200p.
    static PRINT_CMD: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^(?:[0-9]+|[0-9]+,[0-9]+)?p$").unwrap());
    PRINT_CMD.is_match(cmd)
}

/// Pattern 2: a substitution command.
/// Allows `sed 's/pattern/replacement/flags'` where the flags are only
/// g, p, i, I, m, M, 1-9. When `allow_file_writes` is set, also allows the `-i`
/// flag and file arguments for in-place editing; otherwise (the default)
/// requires stdout-only (no file arguments, no `-i` flag).
fn is_substitution_command(
    command: &str,
    expressions: &[String],
    has_file_arguments: bool,
    options: Option<SedAllowlistOptions>,
) -> bool {
    let allow_file_writes = options.map(|o| o.allow_file_writes).unwrap_or(false);

    // When not allowing file writes, must NOT have file arguments.
    if !allow_file_writes && has_file_arguments {
        return false;
    }

    let Some(sed_match) = SED_PREFIX.find(command) else {
        return false;
    };
    let without_sed = &command[sed_match.end()..];
    let Some(parsed) = try_parse_shell_command(without_sed) else {
        return false;
    };

    // Extract all flags.
    let mut flags: Vec<&str> = Vec::new();
    for arg in &parsed {
        let ParseEntry::Word(word) = arg else {
            continue;
        };
        if word.starts_with('-') && word.as_str() != "--" {
            flags.push(word.as_str());
        }
    }

    // Validate flags based on the mode: these are the base allowed flags for
    // both modes …
    let mut allowed_flags = vec!["-E", "--regexp-extended", "-r", "--posix"];

    // … and when allowing file writes, also permit -i and --in-place.
    if allow_file_writes {
        allowed_flags.push("-i");
        allowed_flags.push("--in-place");
    }

    if !validate_flags_against_allowlist(&flags, &allowed_flags) {
        return false;
    }

    // Must have exactly one expression.
    if expressions.len() != 1 {
        return false;
    }

    let expr = trim_blank(&expressions[0]);

    // STRICT ALLOWLIST: must be exactly a substitution command starting with
    // `s` — this rejects standalone commands like `e`, `w file`, etc.
    if !expr.starts_with('s') {
        return false;
    }

    // Parse the substitution: s/pattern/replacement/flags.
    // Only allow `/` as the delimiter (strict).
    static S_SLASH_REST: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(&format!("^s/({ANY_CHAR}*?)$")).unwrap());
    let Some(caps) = S_SLASH_REST.captures(expr) else {
        return false;
    };
    let rest: Vec<char> = caps.get(1).unwrap().as_str().chars().collect();

    // Find the positions of the `/` delimiters.
    let mut delimiter_count = 0usize;
    let mut last_delimiter_pos: Option<usize> = None;
    let mut i = 0usize;
    while i < rest.len() {
        if rest[i] == '\\' {
            // Skip the escaped character.
            i += 2;
            continue;
        }
        if rest[i] == '/' {
            delimiter_count += 1;
            last_delimiter_pos = Some(i);
        }
        i += 1;
    }

    // Must have found exactly 2 delimiters (pattern and replacement).
    if delimiter_count != 2 {
        return false;
    }

    // Extract the flags (everything after the last delimiter).
    let expr_flags: String = rest[last_delimiter_pos.unwrap() + 1..].iter().collect();

    // Validate the flags: only allow g, p, i, I, m, M, and optionally ONE digit 1-9.
    static ALLOWED_FLAG_CHARS: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^[gpimIM]*[1-9]?[gpimIM]*$").unwrap());
    ALLOWED_FLAG_CHARS.is_match(&expr_flags)
}

/// Whether a `sed` command is allowed by the allowlist. The allowlist patterns
/// themselves are strict enough to reject dangerous operations.
///
/// With `options.allow_file_writes`, the `-i` flag and file arguments are
/// permitted for substitution commands. Returns true when the command matches
/// the allowlist and passes the denylist check, false otherwise.
pub fn sed_command_is_allowed_by_allowlist(
    command: &str,
    options: Option<SedAllowlistOptions>,
) -> bool {
    let allow_file_writes = options.map(|o| o.allow_file_writes).unwrap_or(false);

    // Extract the sed expressions (the content inside quotes where the actual
    // sed commands live). If parsing failed, treat as not allowed.
    let expressions = match extract_sed_expressions(command) {
        Ok(expressions) => expressions,
        Err(_) => return false,
    };

    // Check whether the sed command has file arguments.
    let has_file_arguments = has_file_args(command);

    // Check whether the command matches the allowlist patterns.
    let (is_pattern1, is_pattern2) = if allow_file_writes {
        // When allowing file writes, only check substitution commands (the
        // Pattern 2 variant) — Pattern 1 (line printing) doesn't need file
        // writes.
        (
            false,
            is_substitution_command(
                command,
                &expressions,
                has_file_arguments,
                Some(SedAllowlistOptions {
                    allow_file_writes: true,
                }),
            ),
        )
    } else {
        // Standard read-only mode: check both patterns.
        (
            is_line_printing_command(command, &expressions),
            is_substitution_command(command, &expressions, has_file_arguments, None),
        )
    };

    if !is_pattern1 && !is_pattern2 {
        return false;
    }

    // Pattern 2 does not allow semicolons (command separators);
    // Pattern 1 allows semicolons for separating print commands.
    for expr in &expressions {
        if is_pattern2 && expr.contains(';') {
            return false;
        }
    }

    // Defense-in-depth: even when the allowlist matches, check the denylist.
    for expr in &expressions {
        if contains_dangerous_operations(expr) {
            return false;
        }
    }

    true
}

/// Whether a sed command has file arguments (not just stdin).
///
/// Public for tests only.
pub fn has_file_args(command: &str) -> bool {
    let Some(sed_match) = SED_PREFIX.find(command) else {
        return false;
    };
    let without_sed = &command[sed_match.end()..];
    let Some(parsed) = try_parse_shell_command(without_sed) else {
        return true; // assume dangerous when parsing fails
    };

    let mut arg_count = 0usize;
    let mut has_e_flag = false;

    let mut i = 0usize;
    while i < parsed.len() {
        match &parsed[i] {
            // A glob pattern (like *.log) counts as a file argument.
            ParseEntry::Glob(_) => return true,
            // Skip non-word arguments that aren't glob patterns.
            ParseEntry::Op(_) | ParseEntry::Comment(_) => {}
            ParseEntry::Word(word) => {
                let word = word.as_str();
                if (word == "-e" || word == "--expression") && i + 1 < parsed.len() {
                    // The -e flag followed by an expression: skip the next
                    // argument since it's the expression.
                    has_e_flag = true;
                    i += 2;
                    continue;
                } else if word.starts_with("--expression=") {
                    // The --expression=value format.
                    has_e_flag = true;
                } else if word.starts_with("-e=") {
                    // The -e=value format (non-standard but defense in depth).
                    has_e_flag = true;
                } else if word.starts_with('-') {
                    // Skip other flags.
                } else {
                    arg_count += 1;

                    // If -e flags were used, ALL non-flag arguments are file
                    // arguments.
                    if has_e_flag {
                        return true;
                    }

                    // Without -e flags, the first non-flag argument is the sed
                    // expression, so more than 1 non-flag argument is needed to
                    // have file arguments.
                    if arg_count > 1 {
                        return true;
                    }
                }
            }
        }
        i += 1;
    }

    false
}

/// Extract the sed expressions from a command, ignoring flags and filenames.
/// Returns the expressions to check for dangerous operations, or an error when
/// parsing fails (the caller must fail closed).
///
/// Public for tests only.
pub fn extract_sed_expressions(command: &str) -> Result<Vec<String>, SedParseError> {
    let mut expressions: Vec<String> = Vec::new();

    // Compute the remainder after trimming off the leading `sed ` word.
    let Some(sed_match) = SED_PREFIX.find(command) else {
        return Ok(expressions);
    };
    let without_sed = &command[sed_match.end()..];

    // Reject dangerous flag combinations like -ew, -eW, -ee, -we
    // (combined -e/-w with dangerous commands).
    static E_COMBO: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"-e[wWe]").unwrap());
    static W_COMBO: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"-w[eE]").unwrap());
    if E_COMBO.is_match(without_sed) || W_COMBO.is_match(without_sed) {
        return Err(SedParseError {
            message: "Dangerous flag combination detected".to_string(),
        });
    }

    // Parse the arguments properly with the shell lexer.
    let Some(parsed) = try_parse_shell_command(without_sed) else {
        // Malformed shell syntax — surfaced as an error to the caller.
        return Err(SedParseError {
            message: "Malformed shell syntax".to_string(),
        });
    };

    let mut found_e_flag = false;
    let mut found_expression = false;

    let mut i = 0usize;
    while i < parsed.len() {
        // Skip non-word arguments (like control operators).
        let ParseEntry::Word(word) = &parsed[i] else {
            i += 1;
            continue;
        };
        let word = word.as_str();

        // Handle the -e flag followed by an expression.
        if (word == "-e" || word == "--expression") && i + 1 < parsed.len() {
            found_e_flag = true;
            if let ParseEntry::Word(next_arg) = &parsed[i + 1] {
                expressions.push(next_arg.clone());
                i += 1; // skip the next argument since it was consumed
            }
            i += 1;
            continue;
        }

        // Handle the --expression=value format.
        if let Some(value) = word.strip_prefix("--expression=") {
            found_e_flag = true;
            expressions.push(value.to_string());
            i += 1;
            continue;
        }

        // Handle the -e=value format (non-standard but defense in depth).
        if let Some(value) = word.strip_prefix("-e=") {
            found_e_flag = true;
            expressions.push(value.to_string());
            i += 1;
            continue;
        }

        // Skip other flags.
        if word.starts_with('-') {
            i += 1;
            continue;
        }

        // If no -e flag has been seen, the first non-flag argument is the sed
        // expression.
        if !found_e_flag && !found_expression {
            expressions.push(word.to_string());
            found_expression = true;
            i += 1;
            continue;
        }

        // After -e flags or a standalone expression, the remaining non-flag
        // arguments are filenames.
        break;
    }

    Ok(expressions)
}

/// Whether a sed expression contains dangerous operations (denylist).
/// `expression` is a single sed expression (without quotes). Returns true if
/// dangerous, false if safe.
fn contains_dangerous_operations(expression: &str) -> bool {
    let cmd = trim_blank(expression);
    if cmd.is_empty() {
        return false;
    }

    // CONSERVATIVE REJECTIONS: broadly reject patterns that could be dangerous.
    // When in doubt, treat as unsafe.

    // Reject non-ASCII characters (Unicode homoglyphs, combining chars, etc.).
    // Examples: `ｗ` (fullwidth), `ᴡ` (small capital), `w̃` (combining tilde).
    // Checks for characters outside the ASCII range (0x01-0x7F, excluding the
    // null byte) — everything past this point therefore sees pure ASCII, so the
    // `\d`/`\s` classes below are exact.
    static NON_ASCII: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[^\x01-\x7F]").unwrap());
    if NON_ASCII.is_match(cmd) {
        return true;
    }

    // Reject curly braces (blocks) — too complex to parse.
    if cmd.contains('{') || cmd.contains('}') {
        return true;
    }

    // Reject newlines — multi-line commands are too complex.
    if cmd.contains('\n') {
        return true;
    }

    // Reject comments (`#` not immediately after an `s` command).
    // A comment looks like `#comment` or starts with `#`;
    // a delimiter use looks like `s#pattern#replacement#`.
    if let Some(hash_index) = cmd.find('#') {
        if !(hash_index > 0 && cmd[..hash_index].ends_with('s')) {
            return true;
        }
    }

    // Reject the negation operator. Negation can appear at the start
    // (`!/pattern/`) or after an address (`/pattern/!`, `1,10!`, `$!`).
    // A delimiter use looks like `s!pattern!replacement!` (has `s` before it).
    static NEGATION_AFTER_ADDRESS: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"[/\d$]!").unwrap());
    if cmd.starts_with('!') || NEGATION_AFTER_ADDRESS.is_match(cmd) {
        return true;
    }

    // Reject tilde in the GNU step address format (digit~digit, ,~digit, or
    // $~digit), allowing whitespace around the tilde.
    static TILDE_STEP: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\d\s*~\s*\d|,\s*~\s*\d|\$\s*~\s*\d").unwrap());
    if TILDE_STEP.is_match(cmd) {
        return true;
    }

    // Reject a comma at the start (a bare comma is shorthand for the 1,$
    // address range).
    if cmd.starts_with(',') {
        return true;
    }

    // Reject a comma followed by +/- (GNU offset addresses).
    static COMMA_OFFSET: LazyLock<Regex> = LazyLock::new(|| Regex::new(r",\s*[+-]").unwrap());
    if COMMA_OFFSET.is_match(cmd) {
        return true;
    }

    // Reject backslash tricks:
    // 1. `s\` (substitution with a backslash delimiter)
    // 2. `\X` where X could be an alternate delimiter (|, #, %, @, etc.) — not
    //    regex escapes.
    static S_BACKSLASH: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"s\\").unwrap());
    static BACKSLASH_ALT_DELIM: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\\[|#%@]").unwrap());
    if S_BACKSLASH.is_match(cmd) || BACKSLASH_ALT_DELIM.is_match(cmd) {
        return true;
    }

    // Reject escaped slashes followed by w/W (patterns like
    // `/\/path\/to\/file/w`).
    static ESCAPED_SLASH_THEN_WRITE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(&format!(r"\\/{ANY_CHAR}*[wW]")).unwrap());
    if ESCAPED_SLASH_THEN_WRITE.is_match(cmd) {
        return true;
    }

    // Reject malformed/suspicious patterns we don't understand: a slash
    // followed by non-slash chars, then whitespace, then dangerous commands.
    // Examples: `/pattern w file`, `/pattern e cmd`, `/foo X;w file`.
    static SLASH_TEXT_THEN_DANGER: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"/[^/]*\s+[wWeE]").unwrap());
    if SLASH_TEXT_THEN_DANGER.is_match(cmd) {
        return true;
    }

    // Reject malformed substitution commands that don't follow the normal
    // pattern. Examples: `s/foobareoutput.txt` (missing delimiters),
    // `s/foo/bar//w` (extra delimiter).
    static S_SLASH_START: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^s/").unwrap());
    static S_SLASH_WELL_FORMED: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^s/[^/]*/[^/]*/[^/]*$").unwrap());
    if S_SLASH_START.is_match(cmd) && !S_SLASH_WELL_FORMED.is_match(cmd) {
        return true;
    }

    // PARANOID: reject any command starting with `s` that ends with a dangerous
    // char (w, W, e, E) and doesn't match the known safe substitution pattern.
    // This catches malformed s commands with non-slash delimiters that might be
    // trying to use dangerous flags.
    static S_ANY_START: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(&format!("^s{ANY_CHAR}")).unwrap());
    static ENDS_WITH_DANGER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[wWeE]$").unwrap());
    if S_ANY_START.is_match(cmd) && ENDS_WITH_DANGER.is_match(cmd) {
        // Check whether it's a properly formed substitution (any delimiter, not
        // just /).
        static PROPER_SUBSTITUTION: LazyLock<FancyRegex> = LazyLock::new(|| {
            FancyRegex::new(&format!(
                r"^s([^\\\n]){ANY_CHAR}*?\1{ANY_CHAR}*?\1[^wWeE]*$"
            ))
            .unwrap()
        });
        // Fail closed if the matcher gives up.
        let proper_subst = PROPER_SUBSTITUTION.is_match(cmd).unwrap_or(false);
        if !proper_subst {
            return true;
        }
    }

    // Check for dangerous write commands, i.e. w/W in contexts where it would
    // be a command (with optional whitespace): `[address]w filename`,
    // `[address]W filename`, `/pattern/w filename`, `/pattern/W filename`.
    // Split into simple alternatives to avoid exponential backtracking.
    static WRITE_COMMAND_PATTERNS: LazyLock<[Regex; 7]> = LazyLock::new(|| {
        [
            // At the start: w file
            Regex::new(r"^[wW]\s*\S+").unwrap(),
            // After a line number: 1w file or 1 w file
            Regex::new(r"^\d+\s*[wW]\s*\S+").unwrap(),
            // After $: $w file or $ w file
            Regex::new(r"^\$\s*[wW]\s*\S+").unwrap(),
            // After a pattern: /pattern/w file
            Regex::new(r"^/[^/]*/[IMim]*\s*[wW]\s*\S+").unwrap(),
            // After a range: 1,10w file
            Regex::new(r"^\d+,\d+\s*[wW]\s*\S+").unwrap(),
            // After a range: 1,$w file
            Regex::new(r"^\d+,\$\s*[wW]\s*\S+").unwrap(),
            // After a pattern range: /s/,/e/w file
            Regex::new(r"^/[^/]*/[IMim]*,/[^/]*/[IMim]*\s*[wW]\s*\S+").unwrap(),
        ]
    });
    if WRITE_COMMAND_PATTERNS.iter().any(|re| re.is_match(cmd)) {
        return true;
    }

    // Check for dangerous execute commands, i.e. e in contexts where it would
    // be a command (with optional whitespace): `[address]e [command]`,
    // `/pattern/e [command]`, or commands starting with e.
    // Split into simple alternatives to avoid exponential backtracking.
    static EXECUTE_COMMAND_PATTERNS: LazyLock<[Regex; 7]> = LazyLock::new(|| {
        [
            // At the start: e cmd
            Regex::new(r"^e").unwrap(),
            // After a line number: 1e or 1 e
            Regex::new(r"^\d+\s*e").unwrap(),
            // After $: $e or $ e
            Regex::new(r"^\$\s*e").unwrap(),
            // After a pattern: /pattern/e
            Regex::new(r"^/[^/]*/[IMim]*\s*e").unwrap(),
            // After a range: 1,10e
            Regex::new(r"^\d+,\d+\s*e").unwrap(),
            // After a range: 1,$e
            Regex::new(r"^\d+,\$\s*e").unwrap(),
            // After a pattern range: /s/,/e/e
            Regex::new(r"^/[^/]*/[IMim]*,/[^/]*/[IMim]*\s*e").unwrap(),
        ]
    });
    if EXECUTE_COMMAND_PATTERNS.iter().any(|re| re.is_match(cmd)) {
        return true;
    }

    // Check for substitution commands with dangerous flags:
    // s<delim>pattern<delim>replacement<delim>flags where the flags contain w
    // or e. Per POSIX, sed allows any character except backslash and newline as
    // the delimiter.
    static SUBSTITUTION_WITH_FLAGS: LazyLock<FancyRegex> = LazyLock::new(|| {
        FancyRegex::new(&format!(
            r"s([^\\\n]){ANY_CHAR}*?\1{ANY_CHAR}*?\1({ANY_CHAR}*?)$"
        ))
        .unwrap()
    });
    match SUBSTITUTION_WITH_FLAGS.captures(cmd) {
        // Fail closed if the matcher gives up.
        Err(_) => return true,
        Ok(Some(caps)) => {
            let flags = caps.get(2).map(|m| m.as_str()).unwrap_or("");

            // The write flag: s/old/new/w filename or s/old/new/gw filename.
            if flags.contains('w') || flags.contains('W') {
                return true;
            }

            // The execute flag: s/old/new/e or s/old/new/ge.
            if flags.contains('e') || flags.contains('E') {
                return true;
            }
        }
        Ok(None) => {}
    }

    // Check for the y (transliterate) command followed by dangerous operations:
    // y<delim>source<delim>dest<delim> followed by anything (the y command uses
    // the same delimiter syntax as the s command).
    // PARANOID: reject any y command that has w/W/e/E anywhere in the whole
    // command — y commands are rare and w/e after y is suspicious.
    static Y_COMMAND: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"y([^\\\n])").unwrap());
    if Y_COMMAND.is_match(cmd) {
        static ANY_DANGER_CHAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[wWeE]").unwrap());
        if ANY_DANGER_CHAR.is_match(cmd) {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allowed(command: &str) -> bool {
        sed_command_is_allowed_by_allowlist(command, None)
    }

    fn allowed_with_writes(command: &str) -> bool {
        sed_command_is_allowed_by_allowlist(
            command,
            Some(SedAllowlistOptions {
                allow_file_writes: true,
            }),
        )
    }

    fn exprs(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn allows_line_printing_commands() {
        assert!(allowed("sed -n \"1,5p\" file"));
        assert!(allowed("sed -n \"1p;2p;3p\" file.txt"));
        assert!(allowed("sed -n \"p\" file"));
        assert!(allowed("sed -n \"5p\""));
        assert!(allowed("sed -n \"1p ; 2p\""));
        // Long-form and combinable flags.
        assert!(allowed("sed --quiet \"p\" f"));
        assert!(allowed("sed --silent \"p\" f"));
        assert!(allowed("sed -n --posix \"p\" f"));
        assert!(allowed("sed -nE \"1,5p\" file"));
        assert!(allowed("sed -rn \"p\" f"));
        assert!(allowed("sed -zn \"p\" f"));
        assert!(allowed("sed -Ez -n \"3p\" file"));
        // File arguments (and globs) are fine for line printing.
        assert!(allowed("sed -n \"1,5p\" a.txt b.txt"));
        assert!(allowed("sed -n \"1,5p\" *.log"));
        // Leading/extra whitespace around the sed word.
        assert!(allowed("  sed   -n \"p\""));
    }

    #[test]
    fn line_printing_requires_the_n_flag() {
        assert!(!allowed("sed \"1,5p\" file"));
        assert!(!allowed("sed \"1p\""));
    }

    #[test]
    fn rejects_non_print_expressions_in_line_printing_mode() {
        assert!(!allowed("sed -n \"1d\" file"));
        assert!(!allowed("sed -n \"1,5p;6d\" file"));
        assert!(!allowed("sed -n \"1~2p\" f"));
    }

    #[test]
    fn rejects_expression_flag_forms() {
        // `-e`/`--expression` count as flags, and no flag allowlist contains them.
        assert!(!allowed("sed -n -e \"1p\" -e \"2p\" f.txt"));
        assert!(!allowed("sed -n --expression=1p f.txt"));
        assert!(!allowed("sed --expression=s/a/b/ "));
        assert!(!allowed("sed -e \"s/a/b/\" -e \"s/c/d/\""));
    }

    #[test]
    fn allows_stdout_only_substitution_commands() {
        assert!(allowed("sed \"s/foo/bar/\""));
        assert!(allowed("sed \"s/foo/bar/\" "));
        assert!(allowed("sed \"s/foo/bar/g\""));
        assert!(allowed("sed -E \"s/foo/bar/gi\""));
        assert!(allowed("sed -E -r --posix \"s/a/b/\""));
        // Numeric occurrence flag (a single digit 1-9), also combined.
        assert!(allowed("sed \"s/foo/bar/2\""));
        assert!(allowed("sed \"s/foo/bar/g2p\""));
        // Empty pattern/replacement are still well-formed.
        assert!(allowed("sed \"s//x/\""));
        assert!(allowed("sed \"s///\""));
    }

    #[test]
    fn rejects_writing_or_malformed_substitution_commands() {
        // File argument means sed would write nothing to stdout only.
        assert!(!allowed("sed \"s/foo/bar/\" file.txt"));
        // In-place editing.
        assert!(!allowed("sed -i \"s/foo/bar/\" file.txt"));
        // Write / execute flags on s///.
        assert!(!allowed("sed \"s/foo/bar/w out.txt\""));
        assert!(!allowed("sed \"s/foo/bar/e\""));
        // Multi-digit occurrence flag.
        assert!(!allowed("sed \"s/foo/bar/10\""));
        // Only `/` is accepted as the delimiter.
        assert!(!allowed("sed \"s|foo|bar|\""));
        // Escaped slash in the pattern makes the strict form ambiguous.
        assert!(!allowed("sed \"s/a\\/b/c/\""));
        // Pattern 2 does not allow semicolons (command separators).
        assert!(!allowed("sed \"s/foo/bar/;s/baz/qux/\""));
        // The -n flag is not in the substitution allowlist.
        assert!(!allowed("sed -n \"s/foo/bar/p\" "));
    }

    #[test]
    fn rejects_standalone_dangerous_commands() {
        assert!(!allowed("sed \"w out.txt\""));
        assert!(!allowed("sed \"1e id\""));
        assert!(!allowed("sed \"y/abc/xyz/\""));
    }

    #[test]
    fn rejects_non_sed_commands() {
        assert!(!allowed("sed"));
        assert!(!allowed("grep foo"));
        assert!(!allowed("SED -n \"1p\""));
        assert!(!allowed("sed-n \"1p\""));
    }

    #[test]
    fn allow_file_writes_permits_in_place_substitution() {
        assert!(allowed_with_writes("sed -i \"s/foo/bar/\" file.txt"));
        assert!(allowed_with_writes("sed --in-place \"s/foo/bar/\" f"));
        // -i combined with other allowed short flags.
        assert!(allowed_with_writes("sed -iE \"s/a/b/\" f"));
        assert!(allowed_with_writes("sed -Ei \"s/a/b/\" f"));
    }

    #[test]
    fn allow_file_writes_still_rejects_dangerous_forms() {
        // -i with a backup suffix is a combined flag with disallowed chars.
        assert!(!allowed_with_writes("sed -i.bak \"s/foo/bar/\" file.txt"));
        // Pattern 1 (line printing) is not checked in write mode.
        assert!(!allowed_with_writes("sed -n \"1,5p\" file"));
        // Write/execute flags and non-substitution commands stay rejected.
        assert!(!allowed_with_writes("sed -i \"s/foo/bar/w out.txt\" f"));
        assert!(!allowed_with_writes("sed -i \"s/foo/bar/e\" f"));
        assert!(!allowed_with_writes("sed -i \"w out.txt\" f"));
    }

    #[test]
    fn compound_separators_are_a_concern_for_upstream_splitting() {
        // The per-subcommand classifier only inspects the sed expression and
        // flags; operators like `|`, `;`, and `>` are split upstream before a
        // command ever reaches this classifier whole.
        assert!(allowed("sed -n \"1p\" | grep x"));
        assert!(allowed("sed -n \"1p\" f; rm x"));
        assert!(allowed("sed -n \"1p\" > out.txt"));
        // `&&` makes `rm` and `x` look like extra non-flag arguments, so the
        // substitution pattern rejects the command for having file args.
        assert!(!allowed("sed \"s/a/b/\" && rm x"));
    }

    #[test]
    fn print_command_strict_allowlist() {
        assert!(is_print_command("p"));
        assert!(is_print_command("1p"));
        assert!(is_print_command("123p"));
        assert!(is_print_command("1,5p"));
        assert!(is_print_command("10,200p"));

        assert!(!is_print_command(""));
        assert!(!is_print_command("d"));
        assert!(!is_print_command("pp"));
        assert!(!is_print_command("1,p"));
        assert!(!is_print_command(",5p"));
        assert!(!is_print_command("1,5P"));
        assert!(!is_print_command("5 p"));
        assert!(!is_print_command(" p"));
    }

    #[test]
    fn detects_file_arguments() {
        assert!(!has_file_args("sed -n \"1p\""));
        assert!(has_file_args("sed -n \"1p\" f"));
        assert!(has_file_args("sed -n \"1p\" a b"));
        // With -e, ALL non-flag arguments are file arguments.
        assert!(has_file_args("sed -e \"1p\" f"));
        assert!(has_file_args("sed --expression=1p f"));
        assert!(has_file_args("sed -e=1p f"));
        assert!(!has_file_args("sed -e \"1p\""));
        // Globs count as file arguments; preserved `$VAR` tokens do too.
        assert!(has_file_args("sed \"1p\" *.log"));
        assert!(has_file_args("sed -n \"1p\" $FILE"));
        // An unterminated quote still lexes to a single expression token.
        assert!(!has_file_args("sed \"1p"));
        // Not a sed command at all.
        assert!(!has_file_args("ls x"));
    }

    #[test]
    fn extracts_expressions_from_all_flag_forms() {
        assert_eq!(
            extract_sed_expressions("sed -n \"1p\" f").unwrap(),
            exprs(&["1p"])
        );
        assert_eq!(
            extract_sed_expressions("sed -e \"1p\" -e \"2p\" f g").unwrap(),
            exprs(&["1p", "2p"])
        );
        assert_eq!(
            extract_sed_expressions("sed --expression=1p f").unwrap(),
            exprs(&["1p"])
        );
        assert_eq!(
            extract_sed_expressions("sed -e=1p f").unwrap(),
            exprs(&["1p"])
        );
        // Without -e, only the first non-flag argument is the expression; the
        // rest are filenames.
        assert_eq!(
            extract_sed_expressions("sed -n \"1p\" \"2p\"").unwrap(),
            exprs(&["1p"])
        );
        // A bare `--` is skipped like any other flag.
        assert_eq!(
            extract_sed_expressions("sed -- \"1p\" f").unwrap(),
            exprs(&["1p"])
        );
        // Unterminated quotes are tolerated by the lexer.
        assert_eq!(extract_sed_expressions("sed \"1p").unwrap(), exprs(&["1p"]));
        // Non-sed commands and flag-only commands yield no expressions.
        assert_eq!(extract_sed_expressions("grep x").unwrap(), exprs(&[]));
        assert_eq!(extract_sed_expressions("sed -e").unwrap(), exprs(&[]));
        assert_eq!(extract_sed_expressions("sed -n").unwrap(), exprs(&[]));
    }

    #[test]
    fn extraction_rejects_dangerous_flag_combinations() {
        for command in ["sed -ew foo", "sed -we foo", "sed -ee foo"] {
            let err = extract_sed_expressions(command).unwrap_err();
            assert_eq!(
                err.message, "Dangerous flag combination detected",
                "{command}"
            );
        }
    }

    #[test]
    fn extraction_rejects_malformed_shell_syntax() {
        let err = extract_sed_expressions("sed -n ${}p").unwrap_err();
        assert_eq!(err.message, "Malformed shell syntax");
        // And the top-level classifier fails closed on it.
        assert!(!allowed("sed -n ${}p"));
    }

    #[test]
    fn line_printing_flag_handling() {
        assert!(is_line_printing_command("sed -n \"1p\" f", &exprs(&["1p"])));
        // At least one expression is required.
        assert!(!is_line_printing_command("sed -n \"1p\" f", &exprs(&[])));
        assert!(is_line_printing_command(
            "sed --quiet \"p\" f",
            &exprs(&["p"])
        ));
        assert!(is_line_printing_command(
            "sed --silent \"p\" f",
            &exprs(&["p"])
        ));
        assert!(is_line_printing_command(
            "sed -n --posix \"p\" f",
            &exprs(&["p"])
        ));
        // Combined short flags containing n.
        assert!(is_line_printing_command("sed -rn \"p\" f", &exprs(&["p"])));
        assert!(is_line_printing_command("sed -zn \"p\" f", &exprs(&["p"])));
        // -i is never allowed here.
        assert!(!is_line_printing_command(
            "sed -n -i \"p\" f",
            &exprs(&["p"])
        ));
        // Whitespace around semicolon-separated print commands is trimmed.
        assert!(is_line_printing_command(
            "sed -n \"1p ; 2p\"",
            &exprs(&["1p ; 2p"])
        ));
        assert!(is_line_printing_command("  sed   -n \"p\"", &exprs(&["p"])));
    }

    #[test]
    fn denylist_accepts_safe_expressions() {
        for expr in [
            "",
            " ",
            "p",
            "1p",
            "1,5p",
            "2,4p",
            "s/foo/bar/",
            "s/foo/bar/g",
            "s/a/b/gI",
            "s/a/b/im",
            "s/a/b/M2",
            "1d",
            "d",
            "q",
            "/pattern/p",
            "/pattern/d",
            // Alternate delimiters without dangerous flags are not flagged by
            // the denylist (the allowlist rejects them separately).
            "s#a#b#",
            "s!a!b!",
            "s,a,b,",
            "sx",
            "y/ab/cd/",
            "y|ab|cd|",
        ] {
            assert!(!contains_dangerous_operations(expr), "{expr:?}");
        }
    }

    #[test]
    fn denylist_flags_write_commands() {
        for expr in [
            "w out.txt",
            "W out.txt",
            "1w out.txt",
            "1 w out.txt",
            "$w f",
            "$ w f",
            "/pat/w f",
            "/pat/Iw f",
            "1,10w f",
            "1,$w f",
            "/a/,/b/w f",
        ] {
            assert!(contains_dangerous_operations(expr), "{expr:?}");
        }
    }

    #[test]
    fn denylist_flags_execute_commands() {
        for expr in [
            "e", "e cmd", "1e cmd", "1 e", "$e", "/pat/e", "1,10e", "1,$e", "/a/,/b/e",
        ] {
            assert!(contains_dangerous_operations(expr), "{expr:?}");
        }
    }

    #[test]
    fn denylist_flags_substitution_and_transliterate_danger() {
        for expr in [
            // s/// with write/execute flags, in any delimiter.
            "s/a/b/w f",
            "s/a/b/gw f",
            "s/a/b/e",
            "s/a/b/ge",
            "s|a|b|w",
            "s|a|b|e",
            "s#a#b#w",
            "s,a,b,w",
            // Malformed s command ending in a dangerous char.
            "saabbw",
            // y command with w/e anywhere.
            "y/ab/cd/w",
            "y/ab/cd/;w f",
        ] {
            assert!(contains_dangerous_operations(expr), "{expr:?}");
        }
    }

    #[test]
    fn denylist_conservative_rejections() {
        for expr in [
            // Non-ASCII (homoglyphs / combining chars).
            "ｗ out",
            "ẃ",
            "w̃ f",
            // Blocks and multi-line commands.
            "{p}",
            "p;{d}",
            "a\nb",
            // Comments (# not immediately after s).
            "#comment",
            "a#b",
            // Negation.
            "!d",
            "/pat/!d",
            "1!d",
            "$!d",
            // GNU step addresses.
            "1~2p",
            "0~3p",
            "1 ~ 2p",
            ",~3p",
            "$~2p",
            // Bare-comma and offset addresses.
            ",p",
            ",+5p",
            ", -3p",
            // Backslash tricks.
            "s\\af",
            "\\|",
            "\\#",
            "\\%",
            "\\@",
            // Escaped slash followed by a write.
            "/a\\/b/w f",
            // Slash, text, whitespace, dangerous command.
            "/pat w f",
            "/pat e c",
            // Malformed s/ commands.
            "s/foobareoutput.txt",
            "s/foo/bar//w",
        ] {
            assert!(contains_dangerous_operations(expr), "{expr:?}");
        }
    }
}
