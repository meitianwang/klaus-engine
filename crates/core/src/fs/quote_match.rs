//! Glyph-tolerant matching for Edit / MultiEdit, implemented over exact
//! JavaScript UTF-16 code units.

use crate::runtime_values::RuntimeString;
use std::collections::HashMap;

const LEFT_SINGLE: u16 = 0x2018;
const RIGHT_SINGLE: u16 = 0x2019;
const LEFT_DOUBLE: u16 = 0x201c;
const RIGHT_DOUBLE: u16 = 0x201d;

pub fn normalize_quotes(value: &RuntimeString) -> RuntimeString {
    RuntimeString::from_utf16(
        value
            .as_utf16()
            .iter()
            .copied()
            .map(|unit| match unit {
                LEFT_SINGLE | RIGHT_SINGLE => u16::from(b'\''),
                LEFT_DOUBLE | RIGHT_DOUBLE => u16::from(b'"'),
                _ => unit,
            })
            .collect::<Vec<_>>(),
    )
}

fn find_subslice(haystack: &[u16], needle: &[u16]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn has_unicode_escape(value: &RuntimeString) -> bool {
    let units = value.as_utf16();
    (0..units.len().saturating_sub(5)).any(|index| {
        units[index] == u16::from(b'\\')
            && units[index + 1] == u16::from(b'u')
            && units[index + 2..index + 6]
                .iter()
                .all(|unit| hex_value(*unit).is_some())
    })
}

fn has_non_ascii(value: &RuntimeString) -> bool {
    value.as_utf16().iter().any(|unit| *unit >= 0x80)
}

pub fn find_actual_string(
    file_content: &RuntimeString,
    search_string: &RuntimeString,
) -> Option<RuntimeString> {
    if find_subslice(file_content.as_utf16(), search_string.as_utf16()).is_some() {
        return Some(search_string.clone());
    }

    let normalized_file = normalize_quotes(file_content);
    let normalized_search = normalize_quotes(search_string);
    if let Some(index) = find_subslice(normalized_file.as_utf16(), normalized_search.as_utf16()) {
        return Some(file_content.slice_utf16(index..index + search_string.len_utf16()));
    }

    if has_unicode_escape(search_string) {
        let decoded = unescape_unicode(search_string);
        if decoded != *search_string
            && find_subslice(file_content.as_utf16(), decoded.as_utf16()).is_some()
        {
            return Some(decoded);
        }
    }

    has_non_ascii(search_string)
        .then(|| safe_escape_match(file_content, search_string))
        .flatten()
}

fn safe_escape_match(
    file_content: &RuntimeString,
    search_string: &RuntimeString,
) -> Option<RuntimeString> {
    let file = file_content.as_utf16();
    for start in 0..file.len() {
        let mut cursor = start;
        let mut matched = true;
        for unit in search_string.as_utf16().iter().copied() {
            if unit < 0x80 {
                if file.get(cursor) != Some(&unit) {
                    matched = false;
                    break;
                }
                cursor += 1;
            } else {
                if cursor + 6 > file.len()
                    || file[cursor] != u16::from(b'\\')
                    || file[cursor + 1] != u16::from(b'u')
                    || parse_hex4(&file[cursor + 2..cursor + 6]) != Some(unit)
                {
                    matched = false;
                    break;
                }
                cursor += 6;
            }
        }
        if matched {
            return Some(file_content.slice_utf16(start..cursor));
        }
    }
    None
}

pub fn escape_mismatch_hint(search_string: &RuntimeString) -> RuntimeString {
    if !has_unicode_escape(search_string) && !has_non_ascii(search_string) {
        RuntimeString::default()
    } else {
        RuntimeString::from(" (neither the \\uXXXX escapes nor their decoded characters were found; oldString must differ from the file somewhere else — open the file again and copy the target region byte-for-byte)")
    }
}

fn unescape_unicode(text: &RuntimeString) -> RuntimeString {
    let units = text.as_utf16();
    let mut output = Vec::with_capacity(units.len());
    let mut index = 0;
    while index < units.len() {
        if units[index] == u16::from(b'\\') {
            if units.get(index + 1) == Some(&u16::from(b'\\')) {
                output.extend_from_slice(&units[index..index + 2]);
                index += 2;
                continue;
            }
            if units.get(index + 1) == Some(&u16::from(b'u')) && index + 6 <= units.len() {
                if let Some(decoded) = parse_hex4(&units[index + 2..index + 6]) {
                    output.push(decoded);
                    index += 6;
                    continue;
                }
            }
        }
        output.push(units[index]);
        index += 1;
    }
    RuntimeString::from_utf16(output)
}

fn matches_escape_form(text: &RuntimeString, actual: &RuntimeString) -> bool {
    safe_escape_match(actual, text).as_ref() == Some(actual)
}

pub fn preserve_unicode_escape(
    old_string: &RuntimeString,
    actual_old_string: &RuntimeString,
    new_string: &RuntimeString,
) -> RuntimeString {
    if old_string == actual_old_string {
        return new_string.clone();
    }

    if has_non_ascii(old_string) && matches_escape_form(old_string, actual_old_string) {
        let actual = actual_old_string.as_utf16();
        let mut known: HashMap<u16, [u16; 4]> = HashMap::new();
        let mut upper = 0;
        let mut lower = 0;
        let mut position = 0;
        for unit in old_string.as_utf16().iter().copied() {
            if unit >= 0x80 {
                let digits: [u16; 4] = actual[position + 2..position + 6]
                    .try_into()
                    .expect("escape form has four hexadecimal digits");
                for digit in digits {
                    if (u16::from(b'a')..=u16::from(b'f')).contains(&digit) {
                        lower += 1;
                    } else if (u16::from(b'A')..=u16::from(b'F')).contains(&digit) {
                        upper += 1;
                    }
                }
                known.insert(unit, digits);
                position += 6;
            } else {
                position += 1;
            }
        }
        let uppercase = upper > lower;
        let mut output = Vec::new();
        for unit in new_string.as_utf16().iter().copied() {
            if unit >= 0x80 {
                output.extend_from_slice(&[u16::from(b'\\'), u16::from(b'u')]);
                if let Some(digits) = known.get(&unit) {
                    output.extend_from_slice(digits);
                } else {
                    output.extend_from_slice(&hex4(unit, uppercase));
                }
            } else {
                output.push(unit);
            }
        }
        return RuntimeString::from_utf16(output);
    }

    if has_unicode_escape(old_string) && unescape_unicode(old_string) == *actual_old_string {
        return unescape_unicode(new_string);
    }
    new_string.clone()
}

pub fn preserve_quote_style(
    old_string: &RuntimeString,
    actual_old_string: &RuntimeString,
    new_string: &RuntimeString,
) -> RuntimeString {
    if old_string == actual_old_string {
        return new_string.clone();
    }
    let actual = actual_old_string.as_utf16();
    let has_double = actual.contains(&LEFT_DOUBLE) || actual.contains(&RIGHT_DOUBLE);
    let has_single = actual.contains(&LEFT_SINGLE) || actual.contains(&RIGHT_SINGLE);
    let mut result = new_string.clone();
    if has_double {
        result = curl_double_quotes(&result);
    }
    if has_single {
        result = curl_single_quotes(&result);
    }
    result
}

fn opens_here(units: &[u16], index: usize) -> bool {
    index == 0
        || matches!(
            units[index - 1],
            0x0020 | 0x0009 | 0x000a | 0x000d | 0x0028 | 0x005b | 0x007b | 0x2014 | 0x2013
        )
}

fn curl_double_quotes(value: &RuntimeString) -> RuntimeString {
    let mut units = value.as_utf16().to_vec();
    for index in 0..units.len() {
        if units[index] == u16::from(b'"') {
            units[index] = if opens_here(&units, index) {
                LEFT_DOUBLE
            } else {
                RIGHT_DOUBLE
            };
        }
    }
    RuntimeString::from_utf16(units)
}

fn curl_single_quotes(value: &RuntimeString) -> RuntimeString {
    let mut units = value.as_utf16().to_vec();
    for index in 0..units.len() {
        if units[index] != u16::from(b'\'') {
            continue;
        }
        let contraction = index > 0
            && index + 1 < units.len()
            && is_alphabetic(units[index - 1])
            && is_alphabetic(units[index + 1]);
        units[index] = if contraction || !opens_here(&units, index) {
            RIGHT_SINGLE
        } else {
            LEFT_SINGLE
        };
    }
    RuntimeString::from_utf16(units)
}

fn is_alphabetic(unit: u16) -> bool {
    char::from_u32(u32::from(unit)).is_some_and(char::is_alphabetic)
}

fn hex_value(unit: u16) -> Option<u16> {
    match unit {
        value if (u16::from(b'0')..=u16::from(b'9')).contains(&value) => {
            Some(value - u16::from(b'0'))
        }
        value if (u16::from(b'a')..=u16::from(b'f')).contains(&value) => {
            Some(value - u16::from(b'a') + 10)
        }
        value if (u16::from(b'A')..=u16::from(b'F')).contains(&value) => {
            Some(value - u16::from(b'A') + 10)
        }
        _ => None,
    }
}

fn parse_hex4(digits: &[u16]) -> Option<u16> {
    if digits.len() != 4 {
        return None;
    }
    digits
        .iter()
        .try_fold(0_u16, |value, digit| Some(value * 16 + hex_value(*digit)?))
}

fn hex4(value: u16, uppercase: bool) -> [u16; 4] {
    let alphabet = if uppercase {
        b"0123456789ABCDEF"
    } else {
        b"0123456789abcdef"
    };
    [12, 8, 4, 0].map(|shift| u16::from(alphabet[usize::from((value >> shift) & 0x0f)]))
}

#[cfg(test)]
mod tests {
    use super::*;

    const E: char = '\u{00e9}';
    const LS: char = '\u{2018}';
    const RS: char = '\u{2019}';
    const LD: char = '\u{201c}';
    const RD: char = '\u{201d}';

    fn r(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn find(file: &str, search: &str) -> Option<RuntimeString> {
        find_actual_string(&r(file), &r(search))
    }

    #[test]
    fn normalize_quotes_folds_curly() {
        assert_eq!(
            normalize_quotes(&r(&format!("{LS}a{RS} {LD}b{RD}"))),
            r("'a' \"b\"")
        );
        assert_eq!(
            normalize_quotes(&r("it's a — test 'x' \"y\"")),
            r("it's a — test 'x' \"y\"")
        );
    }

    #[test]
    fn exact_match() {
        assert_eq!(find("hello world", "world"), Some(r("world")));
    }

    #[test]
    fn curly_file_straight_search() {
        let file = format!("it{RS}s here");
        let actual = find(&file, "it's here").unwrap();
        assert_eq!(actual, r(&file));
        assert!(actual.as_utf16().contains(&(RS as u16)));
        assert_eq!(actual.len_utf16(), r("it's here").len_utf16());
    }

    #[test]
    fn offsets_correct_after_earlier_curly() {
        let file = format!("{LD}hi{RD} and it{RS}s");
        assert_eq!(find(&file, "it's"), Some(r(&format!("it{RS}s"))));
    }

    #[test]
    fn straight_file_curly_search() {
        assert_eq!(
            find("it's here", &format!("it{RS}s here")),
            Some(r("it's here"))
        );
    }

    #[test]
    fn null_when_absent() {
        assert_eq!(find("alpha beta", "gamma"), None);
    }

    #[test]
    fn stage3_decode_escape() {
        assert_eq!(
            find(&format!("caf{E}"), "caf\\u00e9"),
            Some(r(&format!("caf{E}")))
        );
    }

    #[test]
    fn stage4_match_real_against_file_escape() {
        assert_eq!(
            find("caf\\u00e9", &format!("caf{E}")),
            Some(r("caf\\u00e9"))
        );
        assert_eq!(
            find("caf\\u00E9", &format!("caf{E}")),
            Some(r("caf\\u00E9"))
        );
    }

    #[test]
    fn does_not_misdecode_escaped_backslash() {
        assert_eq!(find("a\\\\u00e9b", "a\\\\u00e9b"), Some(r("a\\\\u00e9b")));
    }

    #[test]
    fn huge_non_ascii_degrades_to_miss() {
        let huge = format!("{}x", "中".repeat(7000));
        assert_eq!(find("unrelated file content", &huge), None);
    }

    #[test]
    fn preserve_unicode_escape_cases() {
        assert_eq!(
            preserve_unicode_escape(&r("abc"), &r("abc"), &r("xyz")),
            r("xyz")
        );
        assert_eq!(
            preserve_unicode_escape(&r("caf\\u00e9"), &r(&format!("caf{E}")), &r("caf\\u00e9!"),),
            r(&format!("caf{E}!"))
        );
        assert_eq!(
            preserve_unicode_escape(
                &r(&format!("caf{E}")),
                &r("caf\\u00e9"),
                &r(&format!("caf{E}!")),
            ),
            r("caf\\u00e9!")
        );
        assert_eq!(
            preserve_unicode_escape(
                &r(&format!("caf{E}")),
                &r("caf\\u00E9"),
                &r(&format!("{E}")),
            ),
            r("\\u00E9")
        );
        assert_eq!(
            preserve_unicode_escape(&r(&format!("a{E}")), &r(&format!("a{E}")), &r("plain"),),
            r("plain")
        );
    }

    #[test]
    fn escape_mismatch_hint_cases() {
        assert_eq!(escape_mismatch_hint(&r("plain text")), r(""));
        assert!(escape_mismatch_hint(&r("caf\\u00e9")).contains("\\uXXXX"));
        assert!(escape_mismatch_hint(&r(&format!("caf{E}"))).contains("open the file again"));
    }

    #[test]
    fn preserve_quote_style_cases() {
        assert_eq!(
            preserve_quote_style(&r("abc"), &r("abc"), &r("xyz")),
            r("xyz")
        );
        assert_eq!(
            preserve_quote_style(
                &r("it's fine"),
                &r(&format!("it{RS}s fine")),
                &r("it's great"),
            ),
            r(&format!("it{RS}s great"))
        );
        assert_eq!(
            preserve_quote_style(&r("\"hi\""), &r(&format!("{LD}hi{RD}")), &r("\"bye\""),),
            r(&format!("{LD}bye{RD}"))
        );
        assert_eq!(
            preserve_quote_style(&r(&format!("it{RS}s")), &r("it's"), &r("it's now"),),
            r("it's now")
        );
        assert_eq!(
            preserve_quote_style(&r("'x'"), &r(&format!("{LS}x{RS}")), &r("'q'"),),
            r(&format!("{LS}q{RS}"))
        );
    }

    #[test]
    fn lone_surrogate_escape_is_collision_free() {
        let surrogate = RuntimeString::from_utf16(vec![0xd800]);
        assert_eq!(
            find_actual_string(&r("\\ud800"), &surrogate),
            Some(r("\\ud800"))
        );
        assert_eq!(unescape_unicode(&r("\\ud800")), surrogate);
    }
}
