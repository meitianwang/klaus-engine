//! A shell-command word splitter (`parse`) — the shell lexer
//! the bash permission subsystem is built on. It splits a command into a stream
//! of words, control operators, glob patterns, and a trailing comment, throwing
//! [`BadSubstitution`] on a malformed `${…}`, so
//! callers can fail closed.
//!
//! Variable expansion is driven by an `env` callback: the default [`parse`]
//! expands undefined variables to the empty string; [`parse_with_env`] lets a
//! caller preserve them (the bash splitter passes `|name| Some("$".+name)` so
//! `$HOME` survives as `$HOME`).

/// One parsed entry, one of:
/// a bare `string`, an `{op}`, an `{op:'glob', pattern}`, or a `{comment}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseEntry {
    Word(String),
    Op(String),
    Glob(String),
    Comment(String),
}

/// A malformed `${…}` substitution (raised as a `Bad substitution` error).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadSubstitution(pub String);

/// Parse with the empty environment: undefined variables expand to "".
pub fn parse(s: &str) -> Result<Vec<ParseEntry>, BadSubstitution> {
    parse_with_env(s, &|_| None)
}

/// Parse with a variable-resolution callback. `env(name)` returns the value, or
/// `None` when the variable is undefined.
pub fn parse_with_env(
    s: &str,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<Vec<ParseEntry>, BadSubstitution> {
    let ch: Vec<char> = s.chars().collect();
    let mut entries: Vec<ParseEntry> = Vec::new();
    let mut i = 0usize;
    let mut commented = false;

    while i < ch.len() {
        if ch[i].is_whitespace() {
            i += 1;
            continue;
        }
        if let Some(oplen) = control_at(&ch, i) {
            let op: String = ch[i..i + oplen].iter().collect();
            entries.push(ParseEntry::Op(op));
            i += oplen;
            continue;
        }
        // Consume one word chunk (barewords + quoted spans), then scan it.
        let start = i;
        while i < ch.len() {
            let c = ch[i];
            if c == '\\' {
                i += 1;
                if i < ch.len() {
                    i += 1;
                }
                continue;
            }
            if c == '\'' || c == '"' {
                let q = c;
                i += 1;
                while i < ch.len() && ch[i] != q {
                    if q == '"' && ch[i] == '\\' {
                        i += 1;
                        if i < ch.len() {
                            i += 1;
                        }
                    } else {
                        i += 1;
                    }
                }
                if i < ch.len() {
                    i += 1; // closing quote
                }
                continue;
            }
            if c.is_whitespace() || control_at(&ch, i).is_some() {
                break;
            }
            i += 1;
        }
        let chunk = &ch[start..i];
        let produced = scan_word(chunk, start, &ch, env, &mut commented)?;
        entries.extend(produced);
        if commented {
            break;
        }
    }

    Ok(entries)
}

/// If a control operator starts at `ch[i]`, return its length. The alternation
/// order is fixed so the leftmost matching alternative wins.
fn control_at(ch: &[char], i: usize) -> Option<usize> {
    let two = |a: char, b: char| ch.get(i) == Some(&a) && ch.get(i + 1) == Some(&b);
    if ch.get(i) == Some(&'<') && ch.get(i + 1) == Some(&'<') && ch.get(i + 2) == Some(&'<') {
        Some(3)
    } else if two('|', '|')
        || two('&', '&')
        || two(';', ';')
        || two('|', '&')
        || two('<', '(')
        || two('>', '>')
        || two('>', '&')
        || two('<', '&')
    {
        Some(2)
    } else if matches!(
        ch.get(i),
        Some('&') | Some(';') | Some('(') | Some(')') | Some('|') | Some('<') | Some('>')
    ) {
        Some(1)
    } else {
        None
    }
}

fn get_var(env: &dyn Fn(&str) -> Option<String>, key: &str) -> String {
    match env(key) {
        Some(v) => v,
        None if !key.is_empty() => String::new(),
        None => "$".to_string(),
    }
}

fn is_special_var_char(c: char) -> bool {
    matches!(c, '*' | '@' | '#' | '?' | '$' | '!' | '_' | '-')
}

/// Scan a word chunk into entries, handling quotes, escapes, `$` expansion,
/// globs, and a `#` comment (which ends the whole command).
fn scan_word(
    chunk: &[char],
    global_offset: usize,
    original: &[char],
    env: &dyn Fn(&str) -> Option<String>,
    commented: &mut bool,
) -> Result<Vec<ParseEntry>, BadSubstitution> {
    let mut out = String::new();
    let mut is_glob = false;
    let mut quote: Option<char> = None;
    let mut esc = false;
    let mut i = 0usize;

    while i < chunk.len() {
        let c = chunk[i];
        is_glob = is_glob || (quote.is_none() && (c == '*' || c == '?'));
        if esc {
            out.push(c);
            esc = false;
        } else if let Some(q) = quote {
            if c == q {
                quote = None;
            } else if q == '\'' {
                out.push(c);
            } else {
                // double quote
                if c == '\\' {
                    i += 1;
                    let c2 = chunk.get(i).copied();
                    match c2 {
                        Some('"') | Some('\\') | Some('$') => out.push(c2.unwrap()),
                        Some(other) => {
                            out.push('\\');
                            out.push(other);
                        }
                        None => out.push('\\'),
                    }
                } else if c == '$' {
                    out.push_str(&parse_env_var(chunk, &mut i, env)?);
                } else {
                    out.push(c);
                }
            }
        } else if c == '"' || c == '\'' {
            quote = Some(c);
        } else if c == '#' {
            *commented = true;
            let comment_start = global_offset + i + 1;
            let comment: String = original.iter().skip(comment_start).collect();
            let mut res = Vec::new();
            if !out.is_empty() {
                res.push(ParseEntry::Word(out));
            }
            res.push(ParseEntry::Comment(comment));
            return Ok(res);
        } else if c == '\\' {
            esc = true;
        } else if c == '$' {
            out.push_str(&parse_env_var(chunk, &mut i, env)?);
        } else {
            out.push(c);
        }
        i += 1;
    }

    if is_glob {
        Ok(vec![ParseEntry::Glob(out)])
    } else {
        Ok(vec![ParseEntry::Word(out)])
    }
}

/// Consume a `$…` variable reference, leaving `*i` at the last consumed char
/// (the caller's loop advances past it).
fn parse_env_var(
    s: &[char],
    i: &mut usize,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<String, BadSubstitution> {
    *i += 1;
    let char_at = s.get(*i).copied();
    match char_at {
        Some('{') => {
            *i += 1;
            if s.get(*i) == Some(&'}') {
                let lo = i.saturating_sub(2);
                return Err(BadSubstitution(
                    s[lo..(*i + 1).min(s.len())].iter().collect(),
                ));
            }
            let varend = (*i..s.len()).find(|&k| s[k] == '}');
            match varend {
                None => Err(BadSubstitution(s[*i..].iter().collect())),
                Some(ve) => {
                    let varname: String = s[*i..ve].iter().collect();
                    *i = ve;
                    Ok(get_var(env, &varname))
                }
            }
        }
        Some(c) if is_special_var_char(c) => {
            let varname = c.to_string();
            *i += 1;
            Ok(get_var(env, &varname))
        }
        _ => {
            let sliced: Vec<char> = s[(*i).min(s.len())..].to_vec();
            match sliced
                .iter()
                .position(|c| !(c.is_ascii_alphanumeric() || *c == '_'))
            {
                None => {
                    let varname: String = sliced.iter().collect();
                    *i = s.len();
                    Ok(get_var(env, &varname))
                }
                Some(k) => {
                    let varname: String = sliced[..k].iter().collect();
                    *i = *i + k - 1;
                    Ok(get_var(env, &varname))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dollar_env() -> impl Fn(&str) -> Option<String> {
        |name: &str| Some(format!("${name}"))
    }

    #[test]
    fn splits_words_and_operators() {
        assert_eq!(
            parse("git status && rm -rf /").unwrap(),
            vec![
                ParseEntry::Word("git".into()),
                ParseEntry::Word("status".into()),
                ParseEntry::Op("&&".into()),
                ParseEntry::Word("rm".into()),
                ParseEntry::Word("-rf".into()),
                ParseEntry::Word("/".into()),
            ]
        );
    }

    #[test]
    fn pipe_and_redirect_ops() {
        assert_eq!(
            parse("ls | grep x").unwrap(),
            vec![
                ParseEntry::Word("ls".into()),
                ParseEntry::Op("|".into()),
                ParseEntry::Word("grep".into()),
                ParseEntry::Word("x".into()),
            ]
        );
        // 2>&1 → "2", ">&", "1"
        assert_eq!(
            parse("cmd 2>&1").unwrap(),
            vec![
                ParseEntry::Word("cmd".into()),
                ParseEntry::Word("2".into()),
                ParseEntry::Op(">&".into()),
                ParseEntry::Word("1".into()),
            ]
        );
    }

    #[test]
    fn does_not_split_inside_quotes() {
        assert_eq!(
            parse("echo 'a && b'").unwrap(),
            vec![
                ParseEntry::Word("echo".into()),
                ParseEntry::Word("a && b".into())
            ]
        );
    }

    #[test]
    fn heredoc_becomes_two_lt_ops() {
        assert_eq!(
            parse("cat <<EOF").unwrap(),
            vec![
                ParseEntry::Word("cat".into()),
                ParseEntry::Op("<".into()),
                ParseEntry::Op("<".into()),
                ParseEntry::Word("EOF".into()),
            ]
        );
    }

    #[test]
    fn glob_and_comment_entries() {
        assert_eq!(
            parse("ls *.js").unwrap(),
            vec![
                ParseEntry::Word("ls".into()),
                ParseEntry::Glob("*.js".into())
            ]
        );
        assert_eq!(
            parse("git status # note").unwrap(),
            vec![
                ParseEntry::Word("git".into()),
                ParseEntry::Word("status".into()),
                ParseEntry::Comment(" note".into()),
            ]
        );
    }

    #[test]
    fn bad_substitution_throws() {
        assert!(parse("${var + expr}").is_err());
    }

    #[test]
    fn env_callback_preserves_variables() {
        let env = dollar_env();
        assert_eq!(
            parse_with_env("$HOME/f", &env).unwrap(),
            vec![ParseEntry::Word("$HOME/f".into())]
        );
    }

    #[test]
    fn subshell_ops() {
        assert_eq!(
            parse("( rm -rf / )").unwrap(),
            vec![
                ParseEntry::Op("(".into()),
                ParseEntry::Word("rm".into()),
                ParseEntry::Word("-rf".into()),
                ParseEntry::Word("/".into()),
                ParseEntry::Op(")".into()),
            ]
        );
    }
}
