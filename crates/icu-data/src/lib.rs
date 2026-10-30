//! Full ICU 77.1 data used by the embedded V8 runtime.

#[cfg(not(target_endian = "little"))]
compile_error!("the bundled icudt77l.dat ICU archive only supports little-endian targets");

use std::io::Read;
use std::sync::OnceLock;

use sha2::{Digest, Sha256};
use xz2::read::XzDecoder;

const ICU_DATA_XZ: &[u8] = include_bytes!("../data/icudt77l.dat.xz");
const ICU_DATA_LENGTH: usize = 31_895_376;
const ICU_DATA_SHA256: &str = "310d9b2cb42947fad7f388b09d2ae574e2adeb60ca76abf280438908f20b2d7b";
const ICU_DATA_XZ_SHA256: &str = "fa1f6481428576503e4e0a132f491a0f7e63a17d9a28dcbd81369318e3d5b76c";

#[repr(C, align(16))]
struct Aligned16([u8; 16]);

struct AlignedIcuData {
    chunks: Box<[Aligned16]>,
}

impl AlignedIcuData {
    fn from_bytes(data: &[u8]) -> Result<Self, String> {
        if !data.len().is_multiple_of(16) {
            return Err(format!(
                "bundled ICU data length {} is not divisible by 16",
                data.len()
            ));
        }
        let chunks = data
            .chunks_exact(16)
            .map(|chunk| {
                let mut aligned = [0_u8; 16];
                aligned.copy_from_slice(chunk);
                Aligned16(aligned)
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let data = Self { chunks };
        if !(data.as_bytes().as_ptr() as usize).is_multiple_of(16) {
            return Err("bundled ICU data allocation is not 16-byte aligned".to_string());
        }
        Ok(data)
    }

    fn as_bytes(&self) -> &[u8] {
        let Some(first) = self.chunks.first() else {
            return &[];
        };
        // SAFETY: `Aligned16` is `repr(C, align(16))` around exactly 16
        // initialized bytes. The boxed slice stores the chunks contiguously,
        // and the returned borrow cannot outlive the backing allocation.
        unsafe { std::slice::from_raw_parts(first.0.as_ptr(), self.chunks.len() * 16) }
    }
}

fn decompress_xz_bounded(compressed: &[u8], max_length: usize) -> Result<Vec<u8>, String> {
    let decoder = XzDecoder::new(compressed);
    let mut decoder = decoder.take((max_length as u64).saturating_add(1));
    let mut data = Vec::with_capacity(max_length);
    decoder
        .read_to_end(&mut data)
        .map_err(|error| format!("failed to decompress bundled ICU data: {error}"))?;
    if data.len() > max_length {
        return Err(format!(
            "bundled ICU data exceeds the {max_length}-byte decompression limit"
        ));
    }
    Ok(data)
}

/// Return the pinned, 16-byte-aligned ICU 77.1 common-data archive.
///
/// Decompression, length, and SHA-256 validation run once per process. A
/// corrupted or substituted archive fails closed and the error is cached.
pub fn common_data_77() -> Result<&'static [u8], String> {
    static ICU_DATA: OnceLock<Result<AlignedIcuData, String>> = OnceLock::new();
    match ICU_DATA.get_or_init(|| {
        let compressed_digest = format!("{:x}", Sha256::digest(ICU_DATA_XZ));
        if compressed_digest != ICU_DATA_XZ_SHA256 {
            return Err(format!(
                "bundled ICU XZ data has SHA-256 {compressed_digest}, expected {ICU_DATA_XZ_SHA256}"
            ));
        }
        let data = decompress_xz_bounded(ICU_DATA_XZ, ICU_DATA_LENGTH)?;
        if data.len() != ICU_DATA_LENGTH {
            return Err(format!(
                "bundled ICU data has length {}, expected {ICU_DATA_LENGTH}",
                data.len()
            ));
        }
        let data = AlignedIcuData::from_bytes(&data)?;
        let digest = format!("{:x}", Sha256::digest(data.as_bytes()));
        if digest != ICU_DATA_SHA256 {
            return Err(format!(
                "bundled ICU data has SHA-256 {digest}, expected {ICU_DATA_SHA256}"
            ));
        }
        Ok(data)
    }) {
        Ok(data) => Ok(data.as_bytes()),
        Err(error) => Err(error.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn common_data_matches_the_pinned_icu_77_1_identity() {
        let data = common_data_77().unwrap();
        assert_eq!(data.len(), 31_895_376);
        assert_eq!(data.as_ptr() as usize % 16, 0);
        assert_eq!(std::mem::size_of::<Aligned16>(), 16);
        assert_eq!(
            format!("{:x}", Sha256::digest(data)),
            "310d9b2cb42947fad7f388b09d2ae574e2adeb60ca76abf280438908f20b2d7b"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(ICU_DATA_XZ)),
            ICU_DATA_XZ_SHA256
        );
    }

    #[test]
    fn xz_decompression_rejects_data_beyond_the_configured_limit() {
        let mut encoder = xz2::write::XzEncoder::new(Vec::new(), 9);
        encoder.write_all(&[0_u8; 17]).unwrap();
        let compressed = encoder.finish().unwrap();

        let error = decompress_xz_bounded(&compressed, 16).unwrap_err();
        assert_eq!(
            error,
            "bundled ICU data exceeds the 16-byte decompression limit"
        );
    }
}
