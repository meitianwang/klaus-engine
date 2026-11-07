//! Output styles: named personas whose body modifies the assistant's response
//! style.
//!
//! The host LOADS the available styles (from `output-styles/*.md` and/or
//! plugins) and supplies them to the session; the engine then SELECTS the one
//! named by the session config and APPLIES it — appending a
//! `# Output Style: <name>` section to the system prompt (see
//! [`resolve_output_style`] and the system-prompt assembly). The engine ships no
//! built-in styles.

use crate::extensions::frontmatter::{
    coerce_description_to_string, extract_description_from_markdown, parse_frontmatter, Frontmatter,
};
use crate::runtime_values::{
    runtime_codec, NodeBufferUtf8, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder,
    RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::types::json::JsonObject;
use serde_json::Value;

/// A named output style whose `body` modifies the assistant's response style.
#[derive(Debug, Clone, PartialEq)]
pub struct OutputStyle {
    pub name: RuntimeString,
    pub description: Option<RuntimeString>,
    pub body: RuntimeString,
}

impl RuntimeEncode for OutputStyle {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("description", &self.description)?;
        object.field("body", &self.body)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for OutputStyle {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                description: object.optional("description", &[])?,
                body: object.required("body", &[])?,
            })
        })
    }
}

/// Resolve the active output style by name. Returns the matching style, or
/// `None` when no style is active — i.e. the name is unset, the literal sentinel
/// `"default"`, or names no loaded style. Matching is exact and case-sensitive
/// on `name` (the key the style registry is indexed by).
pub fn resolve_output_style<'a>(
    name: Option<&str>,
    styles: Option<&'a [OutputStyle]>,
) -> Option<&'a OutputStyle> {
    let name = name?;
    if name.is_empty() || name == "default" {
        return None;
    }
    let styles = styles?;
    styles.iter().find(|s| s.name == name)
}

/// Validate an output-style frontmatter block: `name` must be a string when
/// present, `description` is unrestricted. Returns the extracted `name` and raw
/// `description` value, or `Err(())` when the shape is invalid.
fn validate_output_style_frontmatter(
    data: &JsonObject,
) -> Result<(Option<String>, Option<Value>), ()> {
    let name = match data.get("name") {
        None => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(()),
    };
    let description = data.get("description").cloned();
    Ok((name, description))
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

/// The final path component, in the host's dialect.
///
/// A `/`-only version returned the entire `C:\\dir\\name.md` string on Windows, so the
/// asset was named after its full path instead of its file.
use crate::permission::bash::node_path::basename;

/// Whether a file name ends with `.md` (case-insensitive).
fn is_md(name: &str) -> bool {
    let b = name.as_bytes();
    let n = b.len();
    n >= 3
        && b[n - 3] == b'.'
        && b[n - 2].eq_ignore_ascii_case(&b'm')
        && b[n - 1].eq_ignore_ascii_case(&b'd')
}

/// Load a single `*.md` output style file. `None` on read/parse failure.
pub async fn load_output_style_from_file(file_path: &str) -> Option<OutputStyle> {
    let bytes = match tokio::fs::read(file_path).await {
        Ok(bytes) => bytes,
        Err(_) => return None,
    };
    let raw = NodeBufferUtf8::decode(&bytes)
        .try_to_utf8()
        .expect("Node UTF-8 file decoding always yields Unicode scalar text");
    let Frontmatter { data, body } = parse_frontmatter(&raw);
    let (fm_name, fm_description) = match validate_output_style_frontmatter(&data) {
        Ok(v) => v,
        Err(()) => return None,
    };
    let name = fm_name.unwrap_or_else(|| strip_md_ext(&basename(file_path)));
    // Coerce the frontmatter description, else fall back to the markdown
    // H1/first line.
    let description = coerce_description_to_string(fm_description.as_ref())
        .unwrap_or_else(|| extract_description_from_markdown(&body, &name));
    let mut style = OutputStyle {
        name: name.into(),
        description: None,
        body: body.into(),
    };
    if !description.is_empty() {
        style.description = Some(description.into());
    }
    Some(style)
}

/// Load every `*.md` output style in `dir`. Invalid styles are skipped; a
/// missing dir yields `[]`.
pub async fn load_output_styles_from_dir(dir: &str) -> Vec<OutputStyle> {
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return Vec::new(),
    };
    let mut styles: Vec<OutputStyle> = Vec::new();
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
        if let Some(style) = load_output_style_from_file(&path).await {
            styles.push(style);
        }
    }
    styles
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-styles-{}-{}-{}",
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

    #[tokio::test]
    async fn uses_the_frontmatter_name_else_the_basename() {
        let dir = temp_dir();
        write_file(
            &dir,
            "styles/formal.md",
            "---\ndescription: formal\n---\nBe formal.",
        );
        write_file(&dir, "styles/named.md", "---\nname: custom\n---\nBody.");
        let styles_dir = dir.join("styles");
        let styles = load_output_styles_from_dir(styles_dir.to_str().unwrap()).await;
        let mut names: Vec<RuntimeString> = styles.iter().map(|s| s.name.clone()).collect();
        names.sort();
        assert_eq!(
            names,
            vec![RuntimeString::from("custom"), RuntimeString::from("formal")]
        );
        let formal = styles.iter().find(|s| s.name == "formal").unwrap();
        assert_eq!(formal.body, "Be formal.");
    }

    #[test]
    fn resolve_output_style_cases() {
        let styles = vec![OutputStyle {
            name: "formal".into(),
            description: None,
            body: "b".into(),
        }];
        assert!(resolve_output_style(None, Some(&styles)).is_none());
        assert!(resolve_output_style(Some(""), Some(&styles)).is_none());
        assert!(resolve_output_style(Some("default"), Some(&styles)).is_none());
        assert!(resolve_output_style(Some("nope"), Some(&styles)).is_none());
        assert!(resolve_output_style(Some("formal"), None).is_none());
        assert_eq!(
            resolve_output_style(Some("formal"), Some(&styles)).map(|s| s.name.clone()),
            Some(RuntimeString::from("formal"))
        );
    }

    #[test]
    fn output_style_runtime_codec_preserves_order_and_non_scalar_utf16() {
        let style = OutputStyle {
            name: RuntimeString::from_utf16(vec![u16::from(b'n'), 0xd800]),
            description: Some(RuntimeString::from_utf16(vec![0xdc00, u16::from(b'd')])),
            body: RuntimeString::from_utf16(vec![u16::from(b'b'), 0xd800, 0xdc00]),
        };
        let encoded = runtime_codec::encode_value(&style).unwrap();
        let RuntimeJson::Object(object) = &encoded else {
            panic!("output style encodes as an object");
        };
        assert_eq!(
            object.keys().collect::<Vec<_>>(),
            vec![
                RuntimeString::from("name"),
                RuntimeString::from("description"),
                RuntimeString::from("body")
            ]
        );
        let decoded: OutputStyle = runtime_codec::decode_value(&encoded).unwrap();
        assert_eq!(decoded, style);
    }

    #[tokio::test]
    async fn missing_dir_yields_empty() {
        let dir = temp_dir();
        let missing = dir.join("does-not-exist");
        assert!(load_output_styles_from_dir(missing.to_str().unwrap())
            .await
            .is_empty());
    }
}
