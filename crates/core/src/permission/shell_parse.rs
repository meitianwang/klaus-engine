//! Parse a shell command with the shared shell-quote lexer rather than a
//! hand-rolled parser (which is a ReDoS / bypass minefield). We only need enough
//! structure to decide "is this a plain command or does it chain/redirect?".

use crate::permission::shell_quote::{parse, ParseEntry};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCommand {
    pub first_token: Option<String>,
    pub tokens: Vec<String>,
    /// Any control operator / redirection (&&, ||, |, ;, >, <, $(), ...).
    pub has_operators: bool,
    /// Rough count of chained subcommands (for a sanity cap).
    pub subcommand_count: u32,
}

const CHAINING_OPS: &[&str] = &["&&", "||", "|", ";", "&"];

/// Parse a shell command into a coarse structure (see [`ParsedCommand`]). An
/// unparseable command is treated as operator-bearing so the caller stays
/// conservative.
pub fn parse_command(command: &str) -> ParsedCommand {
    let entries = match parse(command) {
        Ok(e) => e,
        Err(_) => {
            return ParsedCommand {
                first_token: None,
                tokens: Vec::new(),
                has_operators: true,
                subcommand_count: 1,
            }
        }
    };

    let mut tokens: Vec<String> = Vec::new();
    let mut has_operators = false;
    let mut subcommand_count = 1;

    for e in entries {
        match e {
            ParseEntry::Word(s) => tokens.push(s),
            ParseEntry::Op(op) => {
                has_operators = true;
                if CHAINING_OPS.contains(&op.as_str()) {
                    subcommand_count += 1;
                }
            }
            // A glob is not a control operator; a comment carries no `op`.
            ParseEntry::Glob(_) | ParseEntry::Comment(_) => {}
        }
    }

    ParsedCommand {
        first_token: tokens.first().cloned(),
        tokens,
        has_operators,
        subcommand_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_command_has_no_operators() {
        let p = parse_command("git status");
        assert_eq!(p.first_token.as_deref(), Some("git"));
        assert_eq!(p.tokens, vec!["git", "status"]);
        assert!(!p.has_operators);
        assert_eq!(p.subcommand_count, 1);
    }

    #[test]
    fn chaining_counts_subcommands() {
        let p = parse_command("a && b || c | d");
        assert!(p.has_operators);
        assert_eq!(p.subcommand_count, 4);
    }

    #[test]
    fn redirect_is_an_operator_not_a_chain() {
        let p = parse_command("echo hi > out.txt");
        assert!(p.has_operators);
        assert_eq!(p.subcommand_count, 1);
        assert_eq!(p.first_token.as_deref(), Some("echo"));
    }

    #[test]
    fn glob_is_not_an_operator() {
        let p = parse_command("ls *.rs");
        assert!(!p.has_operators);
        assert_eq!(p.tokens, vec!["ls"]);
    }

    #[test]
    fn unparseable_is_conservative() {
        let p = parse_command("${bad + sub}");
        assert!(p.has_operators);
        assert_eq!(p.first_token, None);
        assert!(p.tokens.is_empty());
    }
}
