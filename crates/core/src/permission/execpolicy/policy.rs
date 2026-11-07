//! A parseable exec-policy language — the file-based authoring front-end for the
//! [`ExecPolicy`](crate::permission::execpolicy::engine::ExecPolicy) engine.
//!
//! The policy language is Starlark with three directives:
//!
//!   prefix_rule(pattern=[...], decision?, justification?, match?, not_match?)
//!   host_executable(name=..., paths=[...])
//!   network_rule(host=..., protocol=..., decision?, justification?)
//!
//! This module is a small EVALUATOR for a practical subset of that
//! language — variables, list comprehensions, f-strings and `+` concatenation.
//! Honest boundary: no function definitions, no indented `for`/`if` blocks, no
//! arithmetic / comparison operators, no slicing.

use std::collections::HashMap;

use regex::Regex;
use std::sync::LazyLock;

use crate::permission::execpolicy::matchers::{literal, rest, ArgMatcher, PatternToken};
use crate::permission::execpolicy::rules::{ExecDecision, ExecRule};

/// Network protocols a `network_rule` may target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkRuleProtocol {
    Http,
    Https,
    Socks5Tcp,
    Socks5Udp,
}

/// One parsed `network_rule` directive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkRule {
    pub host: String,
    pub protocol: NetworkRuleProtocol,
    pub decision: ExecDecision,
    pub justification: Option<String>,
}

/// A parsed policy: native rules + host-executable allowlists + network rules.
#[derive(Debug, Clone, Default)]
pub struct ParsedPolicy {
    pub rules: Vec<ExecRule>,
    /// basename → absolute paths permitted to resolve to that basename's rules.
    pub host_executables: HashMap<String, Vec<String>>,
    /// Parsed `network_rule` directives (for the sandbox network layer).
    pub network_rules: Vec<NetworkRule>,
    /// Per-`prefix_rule` example-validation units.
    pub validations: Vec<PolicyValidation>,
}

/// One directive's rules + its load-time example checks.
#[derive(Debug, Clone)]
pub struct PolicyValidation {
    pub rules: Vec<ExecRule>,
    /// Command strings that MUST match at least one of `rules`.
    pub matches: Vec<String>,
    /// Command strings that must match NONE of `rules`.
    pub not_matches: Vec<String>,
}

/// Raised on any syntactic or semantic problem, tagged with file:line:col.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyParseError {
    pub message: String,
    pub identifier: String,
    pub line: usize,
    pub col: usize,
}

impl std::fmt::Display for PolicyParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}: {}",
            self.identifier, self.line, self.col, self.message
        )
    }
}

impl std::error::Error for PolicyParseError {}

fn valid_decision(s: &str) -> bool {
    matches!(s, "allow" | "prompt" | "forbidden")
}

fn decision_from_str(s: &str) -> ExecDecision {
    match s {
        "allow" => ExecDecision::Allow,
        "prompt" => ExecDecision::Prompt,
        "forbidden" => ExecDecision::Forbidden,
        _ => ExecDecision::Allow,
    }
}

const DIRECTIVES: &[&str] = &["prefix_rule", "host_executable", "network_rule"];

fn is_directive(name: &str) -> bool {
    DIRECTIVES.contains(&name)
}

// ── Runtime values (the kinds an expression can evaluate to) ──────────────────
#[derive(Debug, Clone)]
enum PolicyValue {
    Str(String),
    Bool(bool),
    List(Vec<PolicyValue>),
    None,
}

impl PolicyValue {
    fn kind(&self) -> &'static str {
        match self {
            PolicyValue::Str(_) => "str",
            PolicyValue::Bool(_) => "bool",
            PolicyValue::List(_) => "list",
            PolicyValue::None => "none",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Loc {
    line: usize,
    col: usize,
}

/// Parse policy `text` into native rules + host-executable + network metadata.
/// Does NOT run example validation — that happens when an `ExecPolicy` is
/// constructed.
pub fn parse_exec_policy(
    text: &str,
    identifier: Option<&str>,
) -> Result<ParsedPolicy, PolicyParseError> {
    let ident = identifier.unwrap_or("<policy>").to_string();
    let mut parser = Parser::new(text, ident)?;
    parser.parse_program()
}

// ─────────────────────────────────────────────────────────────────────────────
// Tokenizer
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum TokKind {
    Ident,
    Str,
    Fstring,
    Punct,
    Eof,
}

#[derive(Debug, Clone)]
struct Token {
    kind: TokKind,
    value: String,
    line: usize,
    col: usize,
}

const PUNCT: &[char] = &['(', ')', '[', ']', ',', '=', '+'];

struct Lexer {
    src: Vec<char>,
    identifier: String,
    pos: usize,
    line: usize,
    col: usize,
}

impl Lexer {
    fn new(src: &str, identifier: String) -> Lexer {
        Lexer {
            src: src.chars().collect(),
            identifier,
            pos: 0,
            line: 1,
            col: 1,
        }
    }

    fn fail<T>(&self, message: String) -> Result<T, PolicyParseError> {
        Err(PolicyParseError {
            message,
            identifier: self.identifier.clone(),
            line: self.line,
            col: self.col,
        })
    }

    fn peek_ch(&self, offset: usize) -> Option<char> {
        self.src.get(self.pos + offset).copied()
    }

    fn advance(&mut self) -> char {
        let ch = self.src.get(self.pos).copied().unwrap_or('\0');
        self.pos += 1;
        if ch == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        ch
    }

    fn skip_trivia(&mut self) {
        loop {
            let ch = match self.peek_ch(0) {
                Some(c) => c,
                None => return,
            };
            if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
                self.advance();
                continue;
            }
            if ch == '#' {
                while self.pos < self.src.len() && self.peek_ch(0) != Some('\n') {
                    self.advance();
                }
                continue;
            }
            return;
        }
    }

    fn next(&mut self) -> Result<Token, PolicyParseError> {
        self.skip_trivia();
        let line = self.line;
        let col = self.col;
        let ch = match self.peek_ch(0) {
            Some(c) => c,
            None => {
                return Ok(Token {
                    kind: TokKind::Eof,
                    value: String::new(),
                    line,
                    col,
                })
            }
        };

        // f-string: `f"..."` / `f'...'`.
        if ch == 'f' {
            if let Some(n) = self.peek_ch(1) {
                if n == '"' || n == '\'' {
                    self.advance(); // consume `f`
                    return self.read_string(line, col, TokKind::Fstring);
                }
            }
        }

        if ch == '"' || ch == '\'' {
            return self.read_string(line, col, TokKind::Str);
        }

        if PUNCT.contains(&ch) {
            self.advance();
            return Ok(Token {
                kind: TokKind::Punct,
                value: ch.to_string(),
                line,
                col,
            });
        }

        if is_ident_start(ch) {
            let mut ident = String::new();
            while let Some(c) = self.peek_ch(0) {
                if is_ident_part(c) {
                    ident.push(self.advance());
                } else {
                    break;
                }
            }
            return Ok(Token {
                kind: TokKind::Ident,
                value: ident,
                line,
                col,
            });
        }

        self.fail(format!("unexpected character {:?}", ch.to_string()))
    }

    fn read_string(
        &mut self,
        line: usize,
        col: usize,
        kind: TokKind,
    ) -> Result<Token, PolicyParseError> {
        let quote = self.advance(); // opening quote
        let mut out = String::new();
        loop {
            let ch = self.peek_ch(0);
            match ch {
                None | Some('\n') => return self.fail("unterminated string literal".to_string()),
                Some(c) if c == quote => {
                    self.advance();
                    return Ok(Token {
                        kind,
                        value: out,
                        line,
                        col,
                    });
                }
                Some('\\') => {
                    self.advance();
                    let esc = self.advance();
                    match esc {
                        'n' => out.push('\n'),
                        't' => out.push('\t'),
                        'r' => out.push('\r'),
                        '\\' => out.push('\\'),
                        '"' => out.push('"'),
                        '\'' => out.push('\''),
                        other => return self.fail(format!("invalid escape \\{}", other)),
                    }
                }
                Some(_) => {
                    out.push(self.advance());
                }
            }
        }
    }
}

fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}
fn is_ident_part(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

// ─────────────────────────────────────────────────────────────────────────────
// Expression AST
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)] // loc: AST 位置元数据,诊断路径当前未消费
enum Expr {
    Str {
        value: String,
        loc: Loc,
    },
    Bool {
        value: bool,
        loc: Loc,
    },
    Fstring {
        raw: String,
        loc: Loc,
    },
    Ident {
        name: String,
        loc: Loc,
    },
    List {
        items: Vec<Expr>,
        loc: Loc,
    },
    Concat {
        left: Box<Expr>,
        right: Box<Expr>,
        loc: Loc,
    },
    Comprehension {
        body: Box<Expr>,
        var_name: String,
        iterable: Box<Expr>,
        cond: Option<Box<Expr>>,
        loc: Loc,
    },
    Call {
        name: String,
        positional: Vec<Expr>,
        keyword: Vec<(String, Expr)>,
        loc: Loc,
    },
}

/// Argument values after evaluation, as consumed by the directive builders.
struct CallArgs {
    positional: Vec<PolicyValue>,
    keyword: HashMap<String, PolicyValue>,
    loc: Loc,
}

// ─────────────────────────────────────────────────────────────────────────────
// Parser + evaluator
// ─────────────────────────────────────────────────────────────────────────────

struct Parser {
    lexer: Lexer,
    lookahead: Token,
    identifier: String,
    rules: Vec<ExecRule>,
    host_executables: HashMap<String, Vec<String>>,
    network_rules: Vec<NetworkRule>,
    validations: Vec<PolicyValidation>,
    env: HashMap<String, PolicyValue>,
}

impl Parser {
    fn new(src: &str, identifier: String) -> Result<Parser, PolicyParseError> {
        let mut lexer = Lexer::new(src, identifier.clone());
        let lookahead = lexer.next()?;
        Ok(Parser {
            lexer,
            lookahead,
            identifier,
            rules: Vec::new(),
            host_executables: HashMap::new(),
            network_rules: Vec::new(),
            validations: Vec::new(),
            env: HashMap::new(),
        })
    }

    fn fail<T>(&self, message: String, tok: &Token) -> Result<T, PolicyParseError> {
        Err(PolicyParseError {
            message,
            identifier: self.identifier.clone(),
            line: tok.line,
            col: tok.col,
        })
    }

    fn fail_here<T>(&self, message: String) -> Result<T, PolicyParseError> {
        let tok = self.lookahead.clone();
        self.fail(message, &tok)
    }

    fn err<T>(&self, message: String, loc: Loc) -> Result<T, PolicyParseError> {
        Err(PolicyParseError {
            message,
            identifier: self.identifier.clone(),
            line: loc.line,
            col: loc.col,
        })
    }

    fn peek(&self) -> &Token {
        &self.lookahead
    }

    fn take(&mut self) -> Result<Token, PolicyParseError> {
        let t = self.lookahead.clone();
        self.lookahead = self.lexer.next()?;
        Ok(t)
    }

    fn is_punct(&self, p: &str) -> bool {
        self.lookahead.kind == TokKind::Punct && self.lookahead.value == p
    }
    fn is_ident_val(&self, v: &str) -> bool {
        self.lookahead.kind == TokKind::Ident && self.lookahead.value == v
    }
    fn expect_punct(&mut self, p: &str) -> Result<Token, PolicyParseError> {
        if !self.is_punct(p) {
            return self.fail_here(format!("expected '{}'", p));
        }
        self.take()
    }
    fn expect_ident(&mut self) -> Result<Token, PolicyParseError> {
        if self.peek().kind != TokKind::Ident {
            return self.fail_here("expected an identifier".to_string());
        }
        self.take()
    }
    fn loc_of(t: &Token) -> Loc {
        Loc {
            line: t.line,
            col: t.col,
        }
    }

    fn parse_program(&mut self) -> Result<ParsedPolicy, PolicyParseError> {
        while self.peek().kind != TokKind::Eof {
            self.parse_statement()?;
        }
        Ok(ParsedPolicy {
            rules: std::mem::take(&mut self.rules),
            host_executables: std::mem::take(&mut self.host_executables),
            network_rules: std::mem::take(&mut self.network_rules),
            validations: std::mem::take(&mut self.validations),
        })
    }

    /// A top-level statement: an assignment `name = expr` or a directive call.
    fn parse_statement(&mut self) -> Result<(), PolicyParseError> {
        let t = self.peek().clone();
        if t.kind == TokKind::Ident && !is_bool_ident(&t.value) {
            self.take()?;
            if self.is_punct("=") {
                self.take()?;
                let expr = self.parse_expr()?;
                let value = self.eval_expr(&expr, &self.env.clone())?;
                self.env.insert(t.value.clone(), value);
                return Ok(());
            }
            let primary = self.primary_from_ident(&t)?;
            let tail = self.parse_expr_tail(primary)?;
            self.eval_expr(&tail, &self.env.clone())?;
            return Ok(());
        }
        let expr = self.parse_expr()?;
        self.eval_expr(&expr, &self.env.clone())?;
        Ok(())
    }

    // ── expression parsing ──────────────────────────────────────────────────

    fn parse_expr(&mut self) -> Result<Expr, PolicyParseError> {
        let primary = self.parse_primary()?;
        self.parse_expr_tail(primary)
    }

    /// Fold trailing `+ primary` (string/list concatenation) onto `left`.
    fn parse_expr_tail(&mut self, left: Expr) -> Result<Expr, PolicyParseError> {
        let mut acc = left;
        while self.is_punct("+") {
            let op = self.take()?;
            let right = self.parse_primary()?;
            acc = Expr::Concat {
                left: Box::new(acc),
                right: Box::new(right),
                loc: Parser::loc_of(&op),
            };
        }
        Ok(acc)
    }

    fn parse_primary(&mut self) -> Result<Expr, PolicyParseError> {
        let t = self.peek().clone();
        if t.kind == TokKind::Str {
            self.take()?;
            let loc = Parser::loc_of(&t);
            return Ok(Expr::Str {
                value: t.value,
                loc,
            });
        }
        if t.kind == TokKind::Fstring {
            self.take()?;
            let loc = Parser::loc_of(&t);
            return Ok(Expr::Fstring { raw: t.value, loc });
        }
        if t.kind == TokKind::Ident {
            self.take()?;
            return self.primary_from_ident(&t);
        }
        if self.is_punct("[") {
            return self.parse_list_or_comprehension();
        }
        self.fail_here("expected a string, list, bool, variable, or directive call".to_string())
    }

    /// Continue a primary after its leading identifier `name_tok` is consumed.
    fn primary_from_ident(&mut self, name_tok: &Token) -> Result<Expr, PolicyParseError> {
        if is_bool_ident(&name_tok.value) {
            return Ok(Expr::Bool {
                value: name_tok.value == "True",
                loc: Parser::loc_of(name_tok),
            });
        }
        if self.is_punct("(") {
            return self.parse_call_expr(name_tok);
        }
        Ok(Expr::Ident {
            name: name_tok.value.clone(),
            loc: Parser::loc_of(name_tok),
        })
    }

    fn parse_list_or_comprehension(&mut self) -> Result<Expr, PolicyParseError> {
        let open = self.expect_punct("[")?;
        let loc = Parser::loc_of(&open);
        if self.is_punct("]") {
            self.take()?;
            return Ok(Expr::List {
                items: Vec::new(),
                loc,
            });
        }
        let first = self.parse_expr()?;
        if self.is_ident_val("for") {
            self.take()?;
            let var_name = self.expect_ident()?.value;
            if !self.is_ident_val("in") {
                return self.fail_here("expected 'in' in comprehension".to_string());
            }
            self.take()?;
            let iterable = self.parse_expr()?;
            let mut cond: Option<Box<Expr>> = None;
            if self.is_ident_val("if") {
                self.take()?;
                cond = Some(Box::new(self.parse_expr()?));
            }
            self.expect_punct("]")?;
            return Ok(Expr::Comprehension {
                body: Box::new(first),
                var_name,
                iterable: Box::new(iterable),
                cond,
                loc,
            });
        }
        let mut items: Vec<Expr> = vec![first];
        while self.is_punct(",") {
            self.take()?;
            if self.is_punct("]") {
                break;
            }
            items.push(self.parse_expr()?);
        }
        self.expect_punct("]")?;
        Ok(Expr::List { items, loc })
    }

    fn parse_call_expr(&mut self, name_tok: &Token) -> Result<Expr, PolicyParseError> {
        if !is_directive(&name_tok.value) {
            return self.fail(
                format!(
                    "unknown policy directive '{}' (supported: prefix_rule, host_executable, network_rule)",
                    name_tok.value
                ),
                name_tok,
            );
        }
        self.expect_punct("(")?;
        let mut positional: Vec<Expr> = Vec::new();
        let mut keyword: Vec<(String, Expr)> = Vec::new();
        let loc = Parser::loc_of(name_tok);

        while !self.is_punct(")") {
            let t = self.peek().clone();
            if t.kind == TokKind::Ident && !is_bool_ident(&t.value) {
                self.take()?;
                if self.is_punct("=") {
                    self.take()?;
                    if keyword.iter().any(|(k, _)| k == &t.value) {
                        return self.fail(format!("duplicate keyword argument '{}'", t.value), &t);
                    }
                    let v = self.parse_expr()?;
                    keyword.push((t.value.clone(), v));
                } else {
                    let primary = self.primary_from_ident(&t)?;
                    positional.push(self.parse_expr_tail(primary)?);
                }
            } else {
                positional.push(self.parse_expr()?);
            }
            if self.is_punct(",") {
                self.take()?;
            } else {
                break;
            }
        }
        self.expect_punct(")")?;
        Ok(Expr::Call {
            name: name_tok.value.clone(),
            positional,
            keyword,
            loc,
        })
    }

    // ── evaluation ────────────────────────────────────────────────────────────

    fn eval_expr(
        &mut self,
        expr: &Expr,
        env: &HashMap<String, PolicyValue>,
    ) -> Result<PolicyValue, PolicyParseError> {
        match expr {
            Expr::Str { value, .. } => Ok(PolicyValue::Str(value.clone())),
            Expr::Bool { value, .. } => Ok(PolicyValue::Bool(*value)),
            Expr::Fstring { raw, loc } => self.eval_fstring(raw, env, *loc),
            Expr::Ident { name, loc } => match env.get(name) {
                Some(v) => Ok(v.clone()),
                None => self.err(format!("undefined variable '{}'", name), *loc),
            },
            Expr::List { items, .. } => {
                let mut out = Vec::with_capacity(items.len());
                for e in items {
                    out.push(self.eval_expr(e, env)?);
                }
                Ok(PolicyValue::List(out))
            }
            Expr::Concat { left, right, loc } => {
                let a = self.eval_expr(left, env)?;
                let b = self.eval_expr(right, env)?;
                self.concat(a, b, *loc)
            }
            Expr::Comprehension { .. } => self.eval_comprehension(expr, env),
            Expr::Call { .. } => self.eval_call(expr, env),
        }
    }

    fn concat(
        &self,
        a: PolicyValue,
        b: PolicyValue,
        loc: Loc,
    ) -> Result<PolicyValue, PolicyParseError> {
        match (&a, &b) {
            (PolicyValue::Str(x), PolicyValue::Str(y)) => {
                Ok(PolicyValue::Str(format!("{}{}", x, y)))
            }
            (PolicyValue::List(x), PolicyValue::List(y)) => {
                let mut items = x.clone();
                items.extend(y.clone());
                Ok(PolicyValue::List(items))
            }
            _ => self.err(
                format!(
                    "'+' needs two strings or two lists (got {} + {})",
                    a.kind(),
                    b.kind()
                ),
                loc,
            ),
        }
    }

    fn eval_comprehension(
        &mut self,
        expr: &Expr,
        env: &HashMap<String, PolicyValue>,
    ) -> Result<PolicyValue, PolicyParseError> {
        if let Expr::Comprehension {
            body,
            var_name,
            iterable,
            cond,
            loc,
        } = expr
        {
            let iter = self.eval_expr(iterable, env)?;
            let items = match iter {
                PolicyValue::List(items) => items,
                _ => return self.err("comprehension iterable must be a list".to_string(), *loc),
            };
            let mut out: Vec<PolicyValue> = Vec::new();
            for item in items {
                let mut inner = env.clone();
                inner.insert(var_name.clone(), item);
                if let Some(c) = cond {
                    let cv = self.eval_expr(c, &inner)?;
                    if !truthy(&cv) {
                        continue;
                    }
                }
                out.push(self.eval_expr(body, &inner)?);
            }
            Ok(PolicyValue::List(out))
        } else {
            unreachable!()
        }
    }

    fn eval_fstring(
        &self,
        raw: &str,
        env: &HashMap<String, PolicyValue>,
        loc: Loc,
    ) -> Result<PolicyValue, PolicyParseError> {
        static FSTRING_RE: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"\{\{|\}\}|\{([A-Za-z_]\w*)\}").unwrap());
        let mut result = String::new();
        let mut last = 0usize;
        let mut error: Option<PolicyParseError> = None;
        for caps in FSTRING_RE.captures_iter(raw) {
            let whole = caps.get(0).unwrap();
            result.push_str(&raw[last..whole.start()]);
            last = whole.end();
            let m = whole.as_str();
            if m == "{{" {
                result.push('{');
                continue;
            }
            if m == "}}" {
                result.push('}');
                continue;
            }
            let name = caps.get(1).unwrap().as_str();
            match env.get(name) {
                None => {
                    error = Some(self.make_err(
                        format!("f-string references undefined variable '{}'", name),
                        loc,
                    ));
                    break;
                }
                Some(PolicyValue::Str(v)) => result.push_str(v),
                Some(other) => {
                    error = Some(self.make_err(
                        format!(
                            "f-string can only interpolate strings (got {} for '{}')",
                            other.kind(),
                            name
                        ),
                        loc,
                    ));
                    break;
                }
            }
        }
        if let Some(e) = error {
            return Err(e);
        }
        result.push_str(&raw[last..]);
        Ok(PolicyValue::Str(result))
    }

    fn make_err(&self, message: String, loc: Loc) -> PolicyParseError {
        PolicyParseError {
            message,
            identifier: self.identifier.clone(),
            line: loc.line,
            col: loc.col,
        }
    }

    /// Evaluate a directive call: run its side effect, return None.
    fn eval_call(
        &mut self,
        call: &Expr,
        env: &HashMap<String, PolicyValue>,
    ) -> Result<PolicyValue, PolicyParseError> {
        if let Expr::Call {
            name,
            positional,
            keyword,
            loc,
        } = call
        {
            if !is_directive(name) {
                return self.err(
                    format!(
                        "unknown policy directive '{}' (supported: prefix_rule, host_executable, network_rule)",
                        name
                    ),
                    *loc,
                );
            }
            let mut pos_vals = Vec::with_capacity(positional.len());
            for e in positional {
                pos_vals.push(self.eval_expr(e, env)?);
            }
            let mut kw_vals: HashMap<String, PolicyValue> = HashMap::new();
            for (k, e) in keyword {
                let v = self.eval_expr(e, env)?;
                kw_vals.insert(k.clone(), v);
            }
            let resolved = CallArgs {
                positional: pos_vals,
                keyword: kw_vals,
                loc: *loc,
            };
            match name.as_str() {
                "prefix_rule" => {
                    let unit = build_prefix_rules(self, &resolved)?;
                    self.rules.extend(unit.rules.clone());
                    if !unit.matches.is_empty() || !unit.not_matches.is_empty() {
                        self.validations.push(unit);
                    }
                }
                "host_executable" => {
                    build_host_executable(self, &resolved)?;
                }
                "network_rule" => {
                    let nr = build_network_rule(self, &resolved)?;
                    self.network_rules.push(nr);
                }
                _ => {}
            }
            Ok(PolicyValue::None)
        } else {
            unreachable!()
        }
    }
}

fn is_bool_ident(s: &str) -> bool {
    s == "True" || s == "False"
}

fn truthy(v: &PolicyValue) -> bool {
    match v {
        PolicyValue::Str(s) => !s.is_empty(),
        PolicyValue::Bool(b) => *b,
        PolicyValue::List(items) => !items.is_empty(),
        PolicyValue::None => false,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Directive builders
// ─────────────────────────────────────────────────────────────────────────────

/// Bind args to the `prefix_rule(pattern, decision?, match?, not_match?, justification?)` shape.
fn build_prefix_rules(
    parser: &Parser,
    args: &CallArgs,
) -> Result<PolicyValidation, PolicyParseError> {
    let pattern = arg(parser, args, "pattern", 0)?;
    let pattern = match pattern {
        None => return parser.err("prefix_rule requires a `pattern`".to_string(), args.loc),
        Some(p) => p,
    };
    let pattern_items = match &pattern {
        PolicyValue::List(items) if !items.is_empty() => items.clone(),
        _ => {
            return parser.err(
                "prefix_rule `pattern` must be a non-empty list".to_string(),
                args.loc,
            )
        }
    };

    let decision_val = arg(parser, args, "decision", 1)?;
    let mut decision = ExecDecision::Allow;
    if let Some(dv) = decision_val {
        match &dv {
            PolicyValue::Str(s) if valid_decision(s) => decision = decision_from_str(s),
            _ => {
                return parser.err(
                    "`decision` must be one of \"allow\", \"prompt\", \"forbidden\"".to_string(),
                    args.loc,
                )
            }
        }
    }

    let justification_val = arg(parser, args, "justification", 4)?;
    let mut justification: Option<String> = None;
    if let Some(jv) = justification_val {
        match &jv {
            PolicyValue::Str(s) if !s.trim().is_empty() => justification = Some(s.clone()),
            _ => {
                return parser.err(
                    "`justification` must be a non-empty string".to_string(),
                    args.loc,
                )
            }
        }
    }

    let match_val = arg(parser, args, "match", 2)?;
    let not_match_val = arg(parser, args, "not_match", 3)?;
    let matches = match match_val {
        Some(v) => parse_examples(parser, &v, args.loc, "match")?,
        None => Vec::new(),
    };
    let not_matches = match not_match_val {
        Some(v) => parse_examples(parser, &v, args.loc, "not_match")?,
        None => Vec::new(),
    };

    // Decompose the pattern: first token keys by program; the rest become literal
    // matchers, followed by an implicit `rest()` so it is a PREFIX match.
    let first = &pattern_items[0];
    let programs = pattern_token_strings(parser, first, args.loc)?;
    let mut match_matchers: Vec<ArgMatcher> = Vec::new();
    for tok in &pattern_items[1..] {
        match_matchers.push(literal(pattern_token_literal(parser, tok, args.loc)?));
    }
    match_matchers.push(rest());

    let rules: Vec<ExecRule> = programs
        .into_iter()
        .map(|program| ExecRule {
            program,
            match_: Some(match_matchers.clone()),
            argv: None,
            decision,
            ignore_unknown_flags: false,
            justification: justification.clone(),
            examples: Vec::new(),
            not_examples: Vec::new(),
        })
        .collect();

    Ok(PolicyValidation {
        rules,
        matches,
        not_matches,
    })
}

/// Bind args to the `host_executable(name, paths)` shape and record the allowlist.
fn build_host_executable(parser: &mut Parser, args: &CallArgs) -> Result<(), PolicyParseError> {
    let name_val = arg(parser, args, "name", 0)?;
    let name = match name_val {
        Some(PolicyValue::Str(s)) if !s.is_empty() => s,
        _ => {
            return parser.err(
                "host_executable requires a non-empty `name`".to_string(),
                args.loc,
            )
        }
    };
    if name.contains('/') || name.contains('\\') || name == "." || name == ".." {
        return parser.err(
            format!(
                "host_executable name must be a bare executable name (got {})",
                name
            ),
            args.loc,
        );
    }
    let key = executable_lookup_key(&name, current_platform());

    let paths_val = arg(parser, args, "paths", 1)?;
    let paths_items = match paths_val {
        Some(PolicyValue::List(items)) => items,
        _ => {
            return parser.err(
                "host_executable requires a `paths` list".to_string(),
                args.loc,
            )
        }
    };
    let mut paths: Vec<String> = Vec::new();
    for item in &paths_items {
        let path = match item {
            PolicyValue::Str(s) => s.clone(),
            _ => {
                return parser.err(
                    "host_executable paths must be strings".to_string(),
                    args.loc,
                )
            }
        };
        if !path.starts_with('/') {
            return parser.err(
                format!("host_executable paths must be absolute (got {})", path),
                args.loc,
            );
        }
        if executable_lookup_key(&basename(&path), current_platform()) != key {
            return parser.err(
                format!("host_executable path {} must have basename {}", path, name),
                args.loc,
            );
        }
        if !paths.contains(&path) {
            paths.push(path);
        }
    }

    match parser.host_executables.get_mut(&key) {
        Some(existing) => {
            for p in paths {
                if !existing.contains(&p) {
                    existing.push(p);
                }
            }
        }
        None => {
            parser.host_executables.insert(key, paths);
        }
    }
    Ok(())
}

fn valid_protocol(s: &str) -> Option<NetworkRuleProtocol> {
    match s {
        "http" => Some(NetworkRuleProtocol::Http),
        "https" => Some(NetworkRuleProtocol::Https),
        "https_connect" => Some(NetworkRuleProtocol::Https),
        "http-connect" => Some(NetworkRuleProtocol::Https),
        "socks5_tcp" => Some(NetworkRuleProtocol::Socks5Tcp),
        "socks5_udp" => Some(NetworkRuleProtocol::Socks5Udp),
        _ => None,
    }
}

/// Bind args to the `network_rule(host, protocol, decision?, justification?)` shape.
fn build_network_rule(parser: &Parser, args: &CallArgs) -> Result<NetworkRule, PolicyParseError> {
    let host_val = arg(parser, args, "host", 0)?;
    let host_raw = match host_val {
        Some(PolicyValue::Str(s)) => s,
        _ => {
            return parser.err(
                "network_rule requires a string `host`".to_string(),
                args.loc,
            )
        }
    };
    let proto_val = arg(parser, args, "protocol", 1)?;
    let proto_str = match proto_val {
        Some(PolicyValue::Str(s)) => s,
        _ => {
            return parser.err(
                "network_rule requires a string `protocol`".to_string(),
                args.loc,
            )
        }
    };
    let protocol = match valid_protocol(&proto_str) {
        Some(p) => p,
        None => {
            return parser.err(
                format!(
                "network_rule protocol must be one of http, https, socks5_tcp, socks5_udp (got {})",
                proto_str
            ),
                args.loc,
            )
        }
    };

    let decision_val = arg(parser, args, "decision", 2)?;
    let mut decision = ExecDecision::Allow;
    if let Some(dv) = decision_val {
        match &dv {
            PolicyValue::Str(s) if valid_decision(s) => decision = decision_from_str(s),
            _ => {
                return parser.err(
                    "network_rule `decision` must be allow, prompt, or forbidden".to_string(),
                    args.loc,
                )
            }
        }
    }

    let just_val = arg(parser, args, "justification", 3)?;
    let mut justification: Option<String> = None;
    if let Some(jv) = just_val {
        match &jv {
            PolicyValue::Str(s) if !s.trim().is_empty() => justification = Some(s.clone()),
            _ => {
                return parser.err(
                    "`justification` must be a non-empty string".to_string(),
                    args.loc,
                )
            }
        }
    }

    let host = normalize_network_host(parser, &host_raw, args.loc)?;
    Ok(NetworkRule {
        host,
        protocol,
        decision,
        justification,
    })
}

/// Normalize a network_rule host.
fn normalize_network_host(
    parser: &Parser,
    raw: &str,
    loc: Loc,
) -> Result<String, PolicyParseError> {
    let mut host = raw.trim().to_string();
    if host.is_empty() {
        return parser.err("network_rule host cannot be empty".to_string(), loc);
    }
    if host.contains("://") || host.contains('/') || host.contains('?') || host.contains('#') {
        return parser.err(
            "network_rule host must be a hostname or IP literal (no scheme or path)".to_string(),
            loc,
        );
    }
    if host.starts_with('[') {
        let end = match host.find(']') {
            Some(e) => e,
            None => {
                return parser.err(
                    "network_rule host has an invalid bracketed IPv6 literal".to_string(),
                    loc,
                )
            }
        };
        let inside = host[1..end].to_string();
        let after = &host[end + 1..];
        if !after.is_empty() && !is_colon_port(after) {
            return parser.err(
                format!("network_rule host contains an unsupported suffix: {}", raw),
                loc,
            );
        }
        host = inside;
    } else if host.matches(':').count() == 1 {
        let idx = host.rfind(':').unwrap();
        let cand = host[..idx].to_string();
        let port = &host[idx + 1..];
        if !cand.is_empty() && !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) {
            host = cand;
        }
    }
    let normalized = host.trim_end_matches('.').trim().to_lowercase();
    if normalized.is_empty() {
        return parser.err("network_rule host cannot be empty".to_string(), loc);
    }
    if normalized.contains('*') {
        return parser.err(
            "network_rule host must be a specific host; wildcards are not allowed".to_string(),
            loc,
        );
    }
    if normalized.chars().any(|c| c.is_whitespace()) {
        return parser.err(
            "network_rule host cannot contain whitespace".to_string(),
            loc,
        );
    }
    Ok(normalized)
}

/// True for a `:<digits>` suffix (a colon followed by one or more digits).
fn is_colon_port(s: &str) -> bool {
    if !s.starts_with(':') {
        return false;
    }
    let rest = &s[1..];
    !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit())
}

/// Resolve an argument by keyword name, falling back to its positional slot.
fn arg(
    parser: &Parser,
    args: &CallArgs,
    name: &str,
    positional_index: usize,
) -> Result<Option<PolicyValue>, PolicyParseError> {
    let kw = args.keyword.get(name);
    let pos = args.positional.get(positional_index);
    if kw.is_some() && pos.is_some() {
        return parser.err(
            format!("argument '{}' given both positionally and by keyword", name),
            args.loc,
        );
    }
    Ok(kw.cloned().or_else(|| pos.cloned()))
}

/// A pattern token's allowed program strings (Single → [s]; Alts → [...]).
fn pattern_token_strings(
    parser: &Parser,
    v: &PolicyValue,
    loc: Loc,
) -> Result<Vec<String>, PolicyParseError> {
    match v {
        PolicyValue::Str(s) => Ok(vec![s.clone()]),
        PolicyValue::List(items) => {
            if items.is_empty() {
                return parser.err("pattern alternatives cannot be empty".to_string(), loc);
            }
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                match it {
                    PolicyValue::Str(s) => out.push(s.clone()),
                    _ => {
                        return parser.err("pattern alternative must be a string".to_string(), loc)
                    }
                }
            }
            Ok(out)
        }
        _ => parser.err(
            "pattern element must be a string or list of strings".to_string(),
            loc,
        ),
    }
}

/// A pattern token as a matcher literal: Single or Alts.
fn pattern_token_literal(
    parser: &Parser,
    v: &PolicyValue,
    loc: Loc,
) -> Result<PatternToken, PolicyParseError> {
    let strings = pattern_token_strings(parser, v, loc)?;
    if strings.len() == 1 {
        Ok(PatternToken::Single(strings.into_iter().next().unwrap()))
    } else {
        Ok(PatternToken::Alts(strings))
    }
}

/// Convert a `match` / `not_match` value into the engine's string examples.
fn parse_examples(
    parser: &Parser,
    v: &PolicyValue,
    loc: Loc,
    field: &str,
) -> Result<Vec<String>, PolicyParseError> {
    let items = match v {
        PolicyValue::List(items) => items,
        _ => return parser.err(format!("`{}` must be a list of examples", field), loc),
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            PolicyValue::Str(s) => out.push(s.clone()),
            PolicyValue::List(toks) => {
                let mut tokens = Vec::with_capacity(toks.len());
                for tok in toks {
                    match tok {
                        PolicyValue::Str(s) => tokens.push(s.clone()),
                        _ => {
                            return parser.err(
                                format!("`{}` token arrays must contain strings", field),
                                loc,
                            )
                        }
                    }
                }
                out.push(shell_quote(&tokens));
            }
            _ => {
                return parser.err(
                    format!("`{}` examples must be strings or token arrays", field),
                    loc,
                )
            }
        }
    }
    Ok(out)
}

/// Join argv tokens into a command string, single-quoting any that need it.
fn shell_quote(tokens: &[String]) -> String {
    tokens
        .iter()
        .map(|t| quote_token(t))
        .collect::<Vec<_>>()
        .join(" ")
}

fn quote_token(tok: &str) -> String {
    static SAFE_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^[A-Za-z0-9_./:=@%+-]+$").unwrap());
    if !tok.is_empty() && SAFE_RE.is_match(tok) {
        return tok.to_string();
    }
    format!("'{}'", tok.replace('\'', "'\\''"))
}

/// POSIX-style basename (the policy language addresses POSIX program paths).
pub fn basename(p: &str) -> String {
    let slash = p.rfind('/').map(|i| i as isize).unwrap_or(-1);
    let backslash = p.rfind('\\').map(|i| i as isize).unwrap_or(-1);
    let idx = slash.max(backslash);
    if idx == -1 {
        p.to_string()
    } else {
        p[(idx as usize) + 1..].to_string()
    }
}

/// Executable suffixes stripped on Windows when keying host executables.
const WINDOWS_EXE_SUFFIXES: &[&str] = &[".exe", ".cmd", ".bat", ".com"];

/// The runtime platform string: `"win32"` on Windows, otherwise a
/// non-win32 value (`"darwin"` or `"linux"`).
pub fn current_platform() -> &'static str {
    if cfg!(windows) {
        "win32"
    } else if cfg!(target_os = "macos") {
        "darwin"
    } else {
        "linux"
    }
}

/// Normalize an executable name to its lookup key: on Windows,
/// lowercase and strip a trailing
/// `.exe`/`.cmd`/`.bat`/`.com`; on POSIX the name is returned verbatim.
pub fn executable_lookup_key(name: &str, platform: &str) -> String {
    if platform != "win32" {
        return name.to_string();
    }
    let lower = name.to_lowercase();
    for suffix in WINDOWS_EXE_SUFFIXES {
        if lower.ends_with(suffix) {
            return lower[..lower.len() - suffix.len()].to_string();
        }
    }
    lower
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executable_lookup_key_windows_strips_and_lowercases() {
        assert_eq!(executable_lookup_key("GIT.EXE", "win32"), "git");
        assert_eq!(executable_lookup_key("git.cmd", "win32"), "git");
        assert_eq!(executable_lookup_key("Node.BAT", "win32"), "node");
        assert_eq!(executable_lookup_key("git", "win32"), "git");
    }

    #[test]
    fn executable_lookup_key_posix_verbatim() {
        assert_eq!(executable_lookup_key("GIT.EXE", "linux"), "GIT.EXE");
        assert_eq!(executable_lookup_key("git", "darwin"), "git");
    }

    #[test]
    fn windows_suffixed_host_executable_path_matches_bare_name() {
        assert_eq!(
            executable_lookup_key("git.exe", "win32"),
            executable_lookup_key("git", "win32")
        );
    }

    #[test]
    fn network_rule_parses_with_host_normalization() {
        let parsed = parse_exec_policy(
            r#"
              network_rule(host = "API.Example.COM:443", protocol = "https", decision = "allow")
              network_rule(host = "[2001:db8::1]:80", protocol = "http", decision = "forbidden", justification = "no v6")
            "#,
            None,
        )
        .unwrap();
        assert_eq!(parsed.network_rules.len(), 2);
        assert_eq!(
            parsed.network_rules[0],
            NetworkRule {
                host: "api.example.com".to_string(),
                protocol: NetworkRuleProtocol::Https,
                decision: ExecDecision::Allow,
                justification: None,
            }
        );
        assert_eq!(parsed.network_rules[1].host, "2001:db8::1");
        assert_eq!(parsed.network_rules[1].protocol, NetworkRuleProtocol::Http);
        assert_eq!(parsed.network_rules[1].decision, ExecDecision::Forbidden);
        assert_eq!(
            parsed.network_rules[1].justification.as_deref(),
            Some("no v6")
        );
    }

    #[test]
    fn network_rule_rejects_invalid_protocol_and_wildcard() {
        assert!(
            parse_exec_policy(r#"network_rule(host="x.com", protocol="ftp")"#, None)
                .unwrap_err()
                .message
                .contains("protocol must be one of")
        );
        assert!(
            parse_exec_policy(r#"network_rule(host="*.x.com", protocol="https")"#, None)
                .unwrap_err()
                .message
                .contains("wildcards are not allowed")
        );
        assert!(parse_exec_policy(
            r#"network_rule(host="http://x.com/p", protocol="https")"#,
            None
        )
        .unwrap_err()
        .message
        .contains("no scheme or path"));
    }

    #[test]
    fn rejects_unknown_directive_with_positioned_error() {
        let e = parse_exec_policy(r#"bogus_rule(host = "x")"#, Some("p")).unwrap_err();
        assert!(e.message.contains("unknown policy directive 'bogus_rule'"));
    }

    #[test]
    fn rejects_invalid_decision_value() {
        let e =
            parse_exec_policy(r#"prefix_rule(pattern=["x"], decision="maybe")"#, None).unwrap_err();
        assert!(e.message.contains("decision") && e.message.contains("must be one of"));
    }

    #[test]
    fn rejects_empty_justification() {
        let e = parse_exec_policy(r#"prefix_rule(pattern=["x"], justification="   ")"#, None)
            .unwrap_err();
        assert!(e.message.contains("justification") && e.message.contains("non-empty"));
    }

    #[test]
    fn rejects_undefined_variable_reference() {
        let e = parse_exec_policy(r#"prefix_rule(pattern=foo)"#, None).unwrap_err();
        assert!(e.message.contains("undefined variable 'foo'"));
    }

    #[test]
    fn rejects_bad_host_executable_paths() {
        let e =
            parse_exec_policy(r#"host_executable(name="git", paths=["git"])"#, None).unwrap_err();
        assert!(e.message.contains("must be absolute"));
        let e = parse_exec_policy(
            r#"host_executable(name="git", paths=["/usr/bin/jj"])"#,
            None,
        )
        .unwrap_err();
        assert!(e.message.contains("must have basename git"));
    }

    #[test]
    fn reports_unterminated_strings_and_missing_parens() {
        assert!(parse_exec_policy(r#"prefix_rule(pattern=["x""#, None).is_err());
        let e = parse_exec_policy(r#"prefix_rule(pattern=["x]"#, None).unwrap_err();
        assert!(e.message.contains("unterminated string"));
    }

    #[test]
    fn tags_errors_with_identifier_line_col() {
        let e = parse_exec_policy("\n  bogus(", Some("my.rules")).unwrap_err();
        assert!(e.to_string().starts_with("my.rules:2:3:"));
    }

    #[test]
    fn host_executable_rejects_dot_names() {
        assert!(
            parse_exec_policy(r#"host_executable(name=".", paths=["/x"])"#, None)
                .unwrap_err()
                .message
                .contains("bare executable name")
        );
        assert!(
            parse_exec_policy(r#"host_executable(name="..", paths=["/x"])"#, None)
                .unwrap_err()
                .message
                .contains("bare executable name")
        );
    }

    #[test]
    fn rejects_comprehension_over_non_list() {
        let e = parse_exec_policy(r#"[prefix_rule(pattern=["x"]) for c in "notalist"]"#, None)
            .unwrap_err();
        assert!(e.message.contains("iterable must be a list"));
    }
}
