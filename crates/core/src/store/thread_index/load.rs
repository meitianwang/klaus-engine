//! Bounded materialization of selected message records. Display image references
//! are resolved before the decoded page guard, without retaining inline bytes.
use super::{scan, MAX_ITEMS_PAGE_BYTES};
use std::io::{self, BufReader, Read, Seek, SeekFrom};

// A JSON Unicode escape can occupy six source bytes for one decoded ASCII byte.
// Whitespace is removed while copying, so encoded spelling cannot evade this
// finite materialization bound. The final guard measures the encoded display rows.
const MATERIALIZATION_BYTES: usize = MAX_ITEMS_PAGE_BYTES as usize * 6 + 64 * 1024;

pub(super) fn record(
    file: &mut std::fs::File,
    offset: u64,
    len: u64,
    budget: &mut usize,
) -> io::Result<Vec<u8>> {
    record_impl(file, offset, len, budget, true)
}

pub(super) fn record_raw(file: &mut std::fs::File, offset: u64, len: u64) -> io::Result<Vec<u8>> {
    record_impl(file, offset, len, &mut 0, false)
}

fn record_impl(
    file: &mut std::fs::File,
    offset: u64,
    len: u64,
    budget: &mut usize,
    substitute_images: bool,
) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(offset))?;
    let replacements = if substitute_images && len > MAX_ITEMS_PAGE_BYTES {
        let mut reader = BufReader::with_capacity(64 * 1024, Read::by_ref(file).take(len));
        scan::image_replacements(&mut reader)?
    } else {
        Vec::new()
    };
    let mut bytes = Vec::new();
    let mut position = 0;
    let mut quoted = false;
    let mut escaped = false;
    for replacement in replacements {
        copy(
            file,
            offset + position,
            replacement.offset - position,
            &mut bytes,
            *budget,
            &mut quoted,
            &mut escaped,
        )?;
        if bytes
            .len()
            .saturating_add(*budget)
            .saturating_add(replacement.bytes.len())
            > MATERIALIZATION_BYTES
        {
            return Err(super::oversized(MAX_ITEMS_PAGE_BYTES + 1));
        }
        bytes.extend_from_slice(&replacement.bytes);
        position = replacement.offset + replacement.len;
    }
    copy(
        file,
        offset + position,
        len - position,
        &mut bytes,
        *budget,
        &mut quoted,
        &mut escaped,
    )?;
    *budget += bytes.len();
    Ok(bytes)
}

#[allow(clippy::too_many_arguments)]
fn copy(
    file: &mut std::fs::File,
    offset: u64,
    len: u64,
    out: &mut Vec<u8>,
    previous: usize,
    quoted: &mut bool,
    escaped: &mut bool,
) -> io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    let mut input = Read::by_ref(file).take(len);
    let mut chunk = [0; 64 * 1024];
    let mut consumed = 0;
    loop {
        let count = input.read(&mut chunk)?;
        if count == 0 {
            break;
        }
        consumed += count as u64;
        for &byte in &chunk[..count] {
            if *quoted || !byte.is_ascii_whitespace() {
                out.push(byte);
            }
            if *escaped {
                *escaped = false;
            } else if *quoted && byte == b'\\' {
                *escaped = true;
            } else if byte == b'"' {
                *quoted = !*quoted;
            }
        }
        if out.len().saturating_add(previous) > MATERIALIZATION_BYTES {
            return Err(super::oversized(MAX_ITEMS_PAGE_BYTES + 1));
        }
    }
    if consumed != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "rollout changed while reading a message",
        ));
    }
    Ok(())
}
