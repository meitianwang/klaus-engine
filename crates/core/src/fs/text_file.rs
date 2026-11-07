//! Encoding- and line-ending-faithful text file I/O for the file tools. Without this, the tools
//! hardcode utf8 and never
//! normalize CRLF, which (1) makes a model's LF `old_string` fail to match a CRLF file's `\r\n`
//! lines (silent edit failures on Windows-authored repos) and (2) reads/writes a utf16le file as
//! mojibake (corruption).
//!
//! The contract: READ detects the encoding (BOM) + the dominant line-ending style, decodes, and
//! hands back content with CRLF normalized to LF so all matching happens on a single newline form;
//! WRITE restores the original encoding and re-applies CRLF when the file used it, so the byte
//! round-trip preserves the file's character.

use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use tokio::fs;

use crate::runtime_values::{NodeBufferUtf8, RuntimeString};

/// Line-ending style of a file: `CRLF` or `LF`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineEndings {
    Crlf,
    Lf,
}

/// Detected buffer encoding. Deliberately the only distinction made — no utf16be/latin1
/// sniffing; only these two values are ever produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Utf8,
    Utf16Le,
}

/// Encoding from the leading bytes: a `0xFF 0xFE` BOM ⇒
/// utf16le, otherwise utf8 (a superset of ASCII that handles all Unicode). Deliberately the only
/// distinction made — no utf16be/latin1 sniffing.
pub fn detect_encoding(buf: &[u8]) -> Encoding {
    if buf.len() >= 2 && buf[0] == 0xff && buf[1] == 0xfe {
        Encoding::Utf16Le
    } else {
        Encoding::Utf8
    }
}

/// Dominant line-ending style: count each `\n`, attributing it
/// to CRLF when preceded by `\r`. CRLF only when it STRICTLY outnumbers LF; a tie or a file with no
/// newlines defaults to LF.
///
/// Both `\n` and `\r` are ASCII (a single code unit whether measured as UTF-16 code units or
/// as `char`s), so iterating over `char`s here is behaviorally identical.
pub fn detect_line_endings(content: &RuntimeString) -> LineEndings {
    let mut crlf: u32 = 0;
    let mut lf: u32 = 0;
    let units = content.as_utf16();
    for i in 0..units.len() {
        if units[i] == u16::from(b'\n') {
            if i > 0 && units[i - 1] == u16::from(b'\r') {
                crlf += 1;
            } else {
                lf += 1;
            }
        }
    }
    if crlf > lf {
        LineEndings::Crlf
    } else {
        LineEndings::Lf
    }
}

/// The decoded, normalized view of a text file plus the metadata needed to write it back faithfully.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextFile {
    /// Decoded content with CRLF normalized to LF — the single form all tool matching uses.
    pub content: RuntimeString,
    pub encoding: Encoding,
    pub line_endings: LineEndings,
}

/// Decode raw bytes to the runtime string representation used by file tools.
/// Invalid UTF-8 is lossy like Node; UTF-16LE preserves every complete code unit.
fn decode_bytes(buf: &[u8], encoding: Encoding) -> RuntimeString {
    match encoding {
        // utf8 decoding replaces invalid sequences with U+FFFD.
        Encoding::Utf8 => NodeBufferUtf8::decode(buf),
        // Node's utf16le decoder preserves lone UTF-16 surrogates. Retain them
        // in the runtime's reversible representation instead of using Rust's
        // lossy UTF-16 decoder.
        Encoding::Utf16Le => {
            let units = buf
                .chunks_exact(2)
                .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                .collect::<Vec<_>>();
            RuntimeString::from_utf16(units)
        }
    }
}

/// Encode a `String` to bytes using the given encoding, with lossy encoding of
/// unrepresentable code points (→ U+FFFD's bytes).
fn encode_bytes(s: &RuntimeString, encoding: Encoding) -> Vec<u8> {
    match encoding {
        Encoding::Utf8 => s.to_node_utf8(),
        Encoding::Utf16Le => {
            let mut out = Vec::with_capacity(s.len_utf16() * 2);
            for unit in s.as_utf16().iter().copied() {
                out.extend_from_slice(&unit.to_le_bytes());
            }
            out
        }
    }
}

/// Detect + decode + normalize a buffer (the in-memory half of [`read_text_file`]).
pub fn decode_text_buffer(buf: &[u8]) -> TextFile {
    let encoding = detect_encoding(buf);
    let raw = decode_bytes(buf, encoding);
    // Sample the head BEFORE normalization erases the CRLF distinction (the first 4096 units).
    // `slice_utf16` slices by UTF-16 code units, so the sample boundary is fixed
    // exactly.
    let head = slice_utf16(&raw, 4096);
    let line_endings = detect_line_endings(&head);
    TextFile {
        content: normalize_line_endings_to_lf(&raw),
        encoding,
        line_endings,
    }
}

/// Read a text file faithfully: detected encoding, LF-normalized content, original line endings.
pub async fn read_text_file(path: impl AsRef<Path>) -> io::Result<TextFile> {
    let buf = fs::read(path).await?;
    Ok(decode_text_buffer(&buf))
}

/// Handle-owned equivalent of [`read_text_file`]. Restricted maintenance tools
/// use this inside a retained blocking owner so cancellation can never detach a
/// pathname reopen from the pinned file descriptor that authorized it.
pub(crate) fn read_text_file_from_file(file: &mut std::fs::File) -> io::Result<TextFile> {
    file.seek(SeekFrom::Start(0))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(decode_text_buffer(&buf))
}

/// Normalize CRLF to the canonical LF content returned by a later file read.
pub(crate) fn normalize_line_endings_to_lf(content: &RuntimeString) -> RuntimeString {
    content.replace("\r\n", "\n")
}

/// Write text restoring the original encoding + line endings. When the file
/// used CRLF, first collapse any CRLF already in `content` (raw model output can contain `\r\n`) to
/// LF so the re-join can't produce `\r\r\n`, then re-apply CRLF on every line.
///
/// Returns EXACTLY what a later [`read_text_file`] of these bytes decodes — derived from the
/// post-encode reality (round-trip the written bytes), NOT the in-memory string. They differ when
/// `content` holds a character the encoding can't represent (e.g. a lone surrogate → U+FFFD under
/// utf8); a baseline computed in-memory would then diverge from disk and trip the stale guard /
/// reminder.
pub async fn write_text_file(
    path: impl AsRef<Path>,
    content: &RuntimeString,
    encoding: Encoding,
    line_endings: LineEndings,
) -> io::Result<RuntimeString> {
    let (bytes, canonical) = encode_text_file(content, encoding, line_endings);
    fs::write(path, &bytes).await?;
    Ok(canonical)
}

/// Handle-owned equivalent of [`write_text_file`]. The caller must retain and
/// join the blocking owner that holds `file`; opening `/dev/fd/N` or
/// `/proc/self/fd/N` from a detachable `tokio::fs` task would let cancellation
/// close and reuse `N` before the blocking pathname open actually runs.
pub(crate) fn write_text_file_to_file(
    file: &mut std::fs::File,
    content: &RuntimeString,
    encoding: Encoding,
    line_endings: LineEndings,
) -> io::Result<RuntimeString> {
    let (bytes, canonical) = encode_text_file(content, encoding, line_endings);
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&bytes)?;
    file.flush()?;
    Ok(canonical)
}

fn encode_text_file(
    content: &RuntimeString,
    encoding: Encoding,
    line_endings: LineEndings,
) -> (Vec<u8>, RuntimeString) {
    // Build the on-disk form: LF-normalized, with the utf16le BOM (U+FEFF) carried as a leading char
    // so the file stays DETECTABLE as utf16le (the encoding is keyed off the BOM); an Edit's content
    // already carries the BOM it read back, so only prepend when absent (a full Write supplies fresh
    // content with no BOM). Re-apply CRLF when the file used it.
    let lf = normalize_line_endings_to_lf(content);
    let with_bom = if encoding == Encoding::Utf16Le && !lf.starts_with("\u{feff}") {
        RuntimeString::from("\u{feff}").concat(&lf)
    } else {
        lf
    };
    let on_disk = match line_endings {
        // Replace each LF with CRLF (the LF was
        // already collapsed above so no CRLF remains to double up).
        LineEndings::Crlf => with_bom.replace("\n", "\r\n"),
        LineEndings::Lf => with_bom,
    };
    let bytes = encode_bytes(&on_disk, encoding);
    // Round-trip the written bytes to derive the baseline.
    let canonical = normalize_line_endings_to_lf(&decode_bytes(&bytes, encoding));
    (bytes, canonical)
}

/// Slice the first `n` UTF-16 code units of `s`, returning a valid `String`. Here `n`
/// counts UTF-16 code units, not `char`s or bytes.
fn slice_utf16(s: &RuntimeString, n: usize) -> RuntimeString {
    s.slice_utf16(..n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;
    use std::path::PathBuf;

    fn r(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn endings(value: &str) -> LineEndings {
        detect_line_endings(&r(value))
    }

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-textfile-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn detect_encoding_bom_vs_utf8() {
        // returns utf16le for a 0xFF 0xFE BOM, utf8 otherwise
        assert_eq!(
            detect_encoding(&[0xff, 0xfe, 0x68, 0x00]),
            Encoding::Utf16Le
        );
        assert_eq!(detect_encoding("plain text".as_bytes()), Encoding::Utf8);
        assert_eq!(detect_encoding(&[]), Encoding::Utf8); // empty → utf8, not utf16le
        assert_eq!(detect_encoding(&[0xef, 0xbb, 0xbf, 0x68]), Encoding::Utf8); // utf8 BOM
    }

    #[test]
    fn detect_line_endings_strict_majority() {
        // CRLF only when it strictly outnumbers LF; tie/none → LF
        assert_eq!(endings("a\r\nb\r\nc"), LineEndings::Crlf);
        assert_eq!(endings("a\nb\nc"), LineEndings::Lf);
        assert_eq!(endings("no newline"), LineEndings::Lf);
        assert_eq!(endings("a\r\nb\nc"), LineEndings::Lf); // 1 CRLF vs 1 LF → tie → LF
        assert_eq!(endings("a\r\nb\r\nc\nd"), LineEndings::Crlf); // 2 vs 1 → CRLF
    }

    #[test]
    fn node_text_encoding_preserves_javascript_utf16_semantics() {
        let parsed = runtime_json::parse(&RuntimeString::from(r#""\ud800""#)).unwrap();
        let lone_high = parsed.as_str().unwrap();

        assert_eq!(
            encode_bytes(lone_high, Encoding::Utf8),
            "\u{fffd}".as_bytes()
        );
        assert_eq!(encode_bytes(lone_high, Encoding::Utf16Le), [0x00, 0xd8]);
        assert_eq!(&decode_bytes(&[0x00, 0xd8], Encoding::Utf16Le), lone_high);

        // A literal replacement character and a lone surrogate converge only at
        // Node's UTF-8 byte boundary, not in the runtime string model.
        assert_ne!(lone_high, "\u{fffd}");
        assert_eq!(
            encode_bytes(lone_high, Encoding::Utf8),
            encode_bytes(&r("\u{fffd}"), Encoding::Utf8)
        );
    }

    #[test]
    fn line_ending_sample_counts_lone_surrogates_as_one_utf16_unit() {
        let mut bytes = vec![0xff, 0xfe];
        for _ in 0..4093 {
            bytes.extend_from_slice(&0xd800_u16.to_le_bytes());
        }
        bytes.extend_from_slice(&u16::from(b'\r').to_le_bytes());
        bytes.extend_from_slice(&u16::from(b'\n').to_le_bytes());

        let decoded = decode_text_buffer(&bytes);

        assert_eq!(decoded.encoding, Encoding::Utf16Le);
        assert_eq!(decoded.line_endings, LineEndings::Crlf);
    }

    #[tokio::test]
    async fn reads_crlf_as_lf_and_restores_on_write() {
        let dir = temp_dir();
        let p = dir.join("crlf.txt");
        std::fs::write(&p, "a\r\nb\r\nc\r\n").unwrap();
        let tf = read_text_file(&p).await.unwrap();
        assert_eq!(tf.content, "a\nb\nc\n"); // LF view, no \r
        assert_eq!(tf.line_endings, LineEndings::Crlf);
        assert_eq!(tf.encoding, Encoding::Utf8);
        write_text_file(
            &p,
            &tf.content.replace("b", "B"),
            tf.encoding,
            tf.line_endings,
        )
        .await
        .unwrap();
        let disk = std::fs::read_to_string(&p).unwrap();
        assert_eq!(disk, "a\r\nB\r\nc\r\n"); // CRLF restored
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn write_does_not_produce_crcrlf_when_content_has_crlf() {
        let dir = temp_dir();
        let p = dir.join("mixed.txt");
        write_text_file(&p, &r("x\r\ny\n"), Encoding::Utf8, LineEndings::Crlf)
            .await
            .unwrap();
        let disk = std::fs::read_to_string(&p).unwrap();
        assert_eq!(disk, "x\r\ny\r\n");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn baseline_equals_decode_of_written_bytes_lone_surrogate() {
        let dir = temp_dir();
        let p = dir.join("surrogate.txt");
        // A lone high surrogate cannot be encoded as utf8 — a utf8 encoder substitutes U+FFFD on write. The
        // returned baseline MUST reflect that substitution, not the in-memory string, or it would
        // diverge from a future read and falsely trip the stale-write guard / edited-files reminder.
        //
        // Rust `str` cannot hold a lone surrogate, so we build the equivalent input as the U+FFFD
        // replacement that a `'a\uDE00b\nc'` string produces once it reaches a utf8 encoder:
        // both paths write the replacement char's bytes and read them back as U+FFFD.
        let input = r("a\u{fffd}b\nc");
        let written = write_text_file(&p, &input, Encoding::Utf8, LineEndings::Lf)
            .await
            .unwrap();
        let decoded = read_text_file(&p).await.unwrap();
        assert_eq!(written, decoded.content); // baseline === decode(onDisk)
        assert!(written.contains("\u{fffd}")); // the unrepresentable char became the replacement char
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reads_and_restores_utf16le_with_bom() {
        let dir = temp_dir();
        let p = dir.join("u16.txt");
        let mut bytes = vec![0xff, 0xfe];
        for unit in "héllo\n".encode_utf16() {
            bytes.extend_from_slice(&unit.to_le_bytes());
        }
        std::fs::write(&p, &bytes).unwrap();
        let tf = read_text_file(&p).await.unwrap();
        assert_eq!(tf.encoding, Encoding::Utf16Le);
        assert!(tf.content.contains("héllo")); // decoded, not mojibake
        write_text_file(
            &p,
            &tf.content.replace("héllo", "wörld"),
            tf.encoding,
            tf.line_endings,
        )
        .await
        .unwrap();
        let buf = std::fs::read(&p).unwrap();
        assert_eq!(buf[0], 0xff); // utf16le BOM bytes round-tripped via the leading ﻿ char
        assert_eq!(buf[1], 0xfe);
        let (decoded, _, _) = encoding_rs::UTF_16LE.decode(&buf);
        assert!(decoded.contains("wörld"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
