//! Decode a base64 blob from an MCP server to a temp file and return its path.
//!
//! Shared by the resource tools and the tool-result content mapper so a binary
//! payload is written to disk and referenced by path rather than dumped as
//! base64 into the model context. The filename is a random UUID plus a
//! mime-derived extension — never derived from server/uri input, so there is no
//! path-traversal surface.

use base64::Engine;
use rand::RngCore;

use crate::fs::node_path::runtime_path_from_native;
use crate::runtime_values::{RuntimePath, RuntimeString};

/// Decode `blob_base64` to a temp file and return its path. The filename is a
/// random UUID + a mime-derived extension.
pub async fn persist_mcp_blob(
    blob_base64: &RuntimeString,
    mime_type: Option<&RuntimeString>,
) -> std::io::Result<RuntimePath> {
    let file = std::env::temp_dir().join(format!(
        "klaus-agent-mcp-{}{}",
        random_uuid(),
        extension_for_mime(mime_type)
    ));
    let base64_bytes = blob_base64
        .as_utf16()
        .iter()
        .copied()
        .map(u8::try_from)
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_default();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(base64_bytes)
        .unwrap_or_default();
    tokio::fs::write(&file, bytes).await?;
    Ok(runtime_path_from_native(&file))
}

/// Map a mime type to a file extension for a persisted blob (defaults to `.bin`).
pub fn extension_for_mime(mime: Option<&RuntimeString>) -> String {
    let mime = match mime {
        Some(m) if !m.is_empty() => m,
        _ => return ".bin".to_string(),
    };
    let ext = if mime == "image/png" {
        ".png"
    } else if mime == "image/jpeg" {
        ".jpg"
    } else if mime == "image/gif" {
        ".gif"
    } else if mime == "image/webp" {
        ".webp"
    } else if mime == "application/pdf" {
        ".pdf"
    } else if mime == "application/json" {
        ".json"
    } else if mime == "text/plain" {
        ".txt"
    } else if mime == "audio/wav" {
        ".wav"
    } else if mime == "audio/mpeg" {
        ".mp3"
    } else if mime == "audio/ogg" {
        ".ogg"
    } else {
        ".bin"
    };
    ext.to_string()
}

/// A random v4 UUID rendered in the canonical 8-4-4-4-12 hex form.
fn random_uuid() -> String {
    let mut b = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14],
        b[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_mimes_and_defaults_to_bin() {
        assert_eq!(extension_for_mime(Some(&"image/png".into())), ".png");
        assert_eq!(extension_for_mime(Some(&"application/pdf".into())), ".pdf");
        assert_eq!(
            extension_for_mime(Some(&"application/octet-stream".into())),
            ".bin"
        );
        assert_eq!(extension_for_mime(None), ".bin");
        assert_eq!(extension_for_mime(Some(&RuntimeString::default())), ".bin");
    }

    #[test]
    fn uuid_has_canonical_shape() {
        let u = random_uuid();
        assert_eq!(u.len(), 36);
        let parts: Vec<&str> = u.split('-').collect();
        assert_eq!(
            parts.iter().map(|p| p.len()).collect::<Vec<_>>(),
            vec![8, 4, 4, 4, 12]
        );
        assert!(u.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
    }

    #[tokio::test]
    async fn writes_decoded_bytes_to_a_temp_file() {
        // base64 of "hi" is "aGk="
        let path = persist_mcp_blob(&"aGk=".into(), Some(&"text/plain".into()))
            .await
            .unwrap();
        assert!(path.as_runtime_string().ends_with(".txt"));
        let host_path = crate::fs::node_path::project_runtime_path(&path, "path").unwrap();
        let data = tokio::fs::read(&host_path).await.unwrap();
        assert_eq!(data, b"hi");
        let _ = tokio::fs::remove_file(&host_path).await;
    }

    #[tokio::test]
    async fn non_ascii_base64_input_follows_the_empty_decode_fallback_exactly() {
        let path = persist_mcp_blob(
            &RuntimeString::from_utf16([0xd800]),
            Some(&"application/octet-stream".into()),
        )
        .await
        .unwrap();
        let host_path = crate::fs::node_path::project_runtime_path(&path, "path").unwrap();
        assert!(tokio::fs::read(&host_path).await.unwrap().is_empty());
        let _ = tokio::fs::remove_file(host_path).await;
    }
}
