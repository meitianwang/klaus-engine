//! Prompt-image preparation, final payload validation, and token estimation.
//!
//! API-sized images are prepared before entering context. A separate quality
//! search aims for a smaller raw-byte budget without claiming a hard ceiling.
//! Unprocessable images become explicit text markers; recoverable processing
//! failures retain a verified, API-compliant source. Sending a retained history
//! still validates its final image payloads before applying aggregate budgets.

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use image::GenericImageView as _;
use std::collections::HashMap;

use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, ImageBlock, OutputBlock, TextBlock};
use crate::schemas::message::{Message, ToolResultMessage, UserMessage};

/// Default maximum for each encoded pixel axis.
pub const IMAGE_MAX_DIMENSION: u32 = 2000;
/// Default final per-image base64 ceiling.
pub const IMAGE_MAX_BASE64_CHARS: usize = 5_242_880;
/// Raw-byte target corresponding to the default per-image API ceiling.
pub const IMAGE_TARGET_RAW_BYTES: usize = 3_932_160;
/// Best-effort raw-byte target after preparing an API-sized image.
pub const IMAGE_BEST_EFFORT_RAW_BYTES: usize = 512_000;

/// Model/source constraints. The raw target is independent from the final
/// base64 ceiling so a provider can reserve additional per-image headroom.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageLimits {
    pub max_width: u32,
    pub max_height: u32,
    pub max_base64_chars: usize,
    pub target_raw_bytes: usize,
}

impl Default for ImageLimits {
    fn default() -> Self {
        Self {
            max_width: IMAGE_MAX_DIMENSION,
            max_height: IMAGE_MAX_DIMENSION,
            max_base64_chars: IMAGE_MAX_BASE64_CHARS,
            target_raw_bytes: IMAGE_TARGET_RAW_BYTES,
        }
    }
}

impl ImageLimits {
    pub fn with_base64_cap(mut self, cap: usize) -> Self {
        self.max_base64_chars = cap;
        self.target_raw_bytes = cap / 4 * 3;
        self
    }

    /// Merge captured positive metadata without conflating it with concrete
    /// effective limits. A new base64 cap supplies its corresponding raw target
    /// unless the snapshot explicitly declares a raw target too.
    pub fn with_metadata(
        mut self,
        max_width: Option<u32>,
        max_height: Option<u32>,
        max_base64_chars: Option<usize>,
        target_raw_bytes: Option<usize>,
    ) -> Self {
        if let Some(width) = max_width.filter(|width| *width > 0) {
            self.max_width = width;
        }
        if let Some(height) = max_height.filter(|height| *height > 0) {
            self.max_height = height;
        }
        if let Some(cap) = max_base64_chars.filter(|cap| *cap > 0) {
            self = self.with_base64_cap(cap);
        }
        if let Some(target) = target_raw_bytes.filter(|target| *target > 0) {
            self.target_raw_bytes = target;
        }
        self
    }

    pub fn with_declaration(
        self,
        declaration: Option<&crate::types::provider::ImageLimits>,
    ) -> Self {
        let Some(declaration) = declaration else {
            return self;
        };
        self.with_metadata(
            declaration.max_width.map(std::num::NonZeroU32::get),
            declaration.max_height.map(std::num::NonZeroU32::get),
            declaration
                .max_base64_size
                .and_then(|value| usize::try_from(value.get()).ok()),
            declaration
                .target_raw_size
                .and_then(|value| usize::try_from(value.get()).ok()),
        )
    }

    fn contains(self, dimensions: ImageDimensions) -> bool {
        dimensions.width <= self.max_width && dimensions.height <= self.max_height
    }
}

/// Anthropic's image token formula: one token per ~750 pixels.
const PIXELS_PER_TOKEN: f64 = 750.0;

/// Bytes-per-token used when the encoded dimensions cannot be read.
const BYTES_PER_TOKEN: f64 = 4.0;

const API_JPEG_QUALITIES: [u8; 4] = [80, 60, 40, 20];

/// Pixel dimensions read from an encoded image header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageDimensions {
    pub width: u32,
    pub height: u32,
}

/// Decode the base64 payload of an image block.
fn decode_base64(base64: &RuntimeString) -> Option<Vec<u8>> {
    BASE64_STANDARD.decode(base64.try_to_utf8().ok()?).ok()
}

/// Read the pixel dimensions from the encoded header without decoding pixels.
/// Returns `None` for payloads that are not a decodable image; callers fall
/// back to a byte-based estimate or pass the block through untouched.
pub fn image_dimensions(block: &ImageBlock) -> Option<ImageDimensions> {
    let bytes = decode_base64(&block.base64)?;
    encoded_dimensions(&bytes)
}

fn encoded_dimensions(bytes: &[u8]) -> Option<ImageDimensions> {
    let reader = image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .ok()?;
    let (width, height) = reader.into_dimensions().ok()?;
    (width > 0 && height > 0).then_some(ImageDimensions { width, height })
}

/// Token estimate for one image block. Dimensioned images use the patch
/// formula; anything undecodable is charged by decoded byte size so an
/// oversized opaque payload can no longer hide behind a flat constant.
pub fn estimate_image_tokens(block: &ImageBlock) -> u64 {
    if let Some(dims) = image_dimensions(block) {
        let pixels = f64::from(dims.width) * f64::from(dims.height);
        return (pixels / PIXELS_PER_TOKEN).ceil().max(1.0) as u64;
    }
    let decoded_bytes = block.base64.len_utf16() as f64 * 0.75;
    (decoded_bytes / BYTES_PER_TOKEN).ceil().max(1.0) as u64
}

/// What the entry gate did with one image block.
#[derive(Debug, Clone, PartialEq)]
pub enum GatedImage {
    /// Small enough; passed through unchanged.
    Keep(ImageBlock),
    /// Resized or re-encoded. The MIME type follows the resulting bytes, and
    /// the transformed payload no longer represents the sender's source file.
    Downscaled(ImageBlock),
    /// Cannot be prepared or safely passed through; replaced by marker text.
    Rejected(RuntimeString),
}

fn rejection_marker(reason: &str) -> RuntimeString {
    RuntimeString::from_utf8(&format!(
        "[Image could not be processed (image omitted): {reason}]"
    ))
}

struct PreparedImage {
    bytes: Vec<u8>,
    format: image::ImageFormat,
}

fn jpeg_bytes(decoded: &image::DynamicImage, quality: u8) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    jpeg_rgb(decoded)
        .write_with_encoder(image::codecs::jpeg::JpegEncoder::new_with_quality(
            &mut bytes, quality,
        ))
        .map_err(|error| error.to_string())?;
    Ok(bytes)
}

fn jpeg_rgb(decoded: &image::DynamicImage) -> image::RgbImage {
    let mut rgb = decoded.to_rgb8();
    if decoded.color().has_alpha() {
        // Composite against black before discarding alpha. Merely dropping it
        // would expose hidden RGB in fully transparent source pixels.
        let rgba = decoded.to_rgba8();
        for (output, input) in rgb.pixels_mut().zip(rgba.pixels()) {
            for channel in &mut output.0 {
                *channel = ((u16::from(*channel) * u16::from(input.0[3]) + 127) / 255) as u8;
            }
        }
    }
    rgb
}

fn indexed_rgba(rgba: &image::RgbaImage) -> (Vec<u8>, Vec<u8>) {
    // An exact palette retains every RGBA value when the image already fits
    // within the format's 256-entry palette. Richer images use RGBA NeuQuant.
    let mut exact = HashMap::<[u8; 4], u8>::new();
    let mut colors = Vec::new();
    let mut indices = Vec::with_capacity(rgba.len() / 4);
    for pixel in rgba.pixels() {
        if let Some(index) = exact.get(&pixel.0) {
            indices.push(*index);
        } else {
            if exact.len() == 256 {
                break;
            }
            let index = exact.len() as u8;
            exact.insert(pixel.0, index);
            colors.extend_from_slice(&pixel.0);
            indices.push(index);
        }
    }
    if indices.len() == rgba.len() / 4 {
        return (colors, indices);
    }
    let quantizer = color_quant::NeuQuant::new(10, 256, rgba.as_raw());
    let mut colors = quantizer.color_map_rgba();
    // Quantization must not invent transparency for an image whose alpha is
    // constant. Mixed-alpha inputs are quantized in all four RGBA channels.
    let alpha = rgba.pixels().next().expect("decoded image has pixels").0[3];
    if rgba.pixels().all(|pixel| pixel.0[3] == alpha) {
        for color in colors.chunks_exact_mut(4) {
            color[3] = alpha;
        }
    }
    let indices = rgba
        .as_raw()
        .chunks_exact(4)
        .map(|pixel| quantizer.index_of(pixel) as u8)
        .collect();
    (colors, indices)
}

fn palette_png_bytes(decoded: &image::DynamicImage) -> Result<Vec<u8>, String> {
    let rgba = decoded.to_rgba8();
    let (colors, indices) = indexed_rgba(&rgba);
    let mut palette = Vec::with_capacity(256 * 3);
    let mut transparency = Vec::with_capacity(256);
    for color in colors.chunks_exact(4) {
        palette.extend_from_slice(&color[..3]);
        transparency.push(color[3]);
    }
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, rgba.width(), rgba.height());
        encoder.set_color(png::ColorType::Indexed);
        encoder.set_depth(png::BitDepth::Eight);
        encoder.set_palette(palette);
        encoder.set_trns(transparency);
        encoder.set_deflate_compression(png::DeflateCompression::Level(9));
        let mut writer = encoder.write_header().map_err(|error| error.to_string())?;
        writer
            .write_image_data(&indices)
            .map_err(|error| error.to_string())?;
    }
    Ok(bytes)
}

fn encode_in_source_format(
    decoded: &image::DynamicImage,
    format: image::ImageFormat,
) -> Result<PreparedImage, String> {
    let mut bytes = std::io::Cursor::new(Vec::new());
    decoded
        .write_to(&mut bytes, format)
        .map_err(|error| error.to_string())?;
    Ok(PreparedImage {
        bytes: bytes.into_inner(),
        format,
    })
}

fn fit_dimensions(dimensions: ImageDimensions, max_width: u32, max_height: u32) -> ImageDimensions {
    let mut width = dimensions.width;
    let mut height = dimensions.height;
    if width > max_width {
        height = (f64::from(height) * f64::from(max_width) / f64::from(width))
            .round()
            .max(1.0) as u32;
        width = max_width;
    }
    if height > max_height {
        width = (f64::from(width) * f64::from(max_height) / f64::from(height))
            .round()
            .max(1.0) as u32;
        height = max_height;
    }
    ImageDimensions { width, height }
}

fn try_api_encodings(
    decoded: &image::DynamicImage,
    source_format: image::ImageFormat,
    raw_target: usize,
) -> Result<Option<PreparedImage>, String> {
    if source_format == image::ImageFormat::Png {
        let bytes = palette_png_bytes(decoded)?;
        if bytes.len() <= raw_target {
            return Ok(Some(PreparedImage {
                bytes,
                format: image::ImageFormat::Png,
            }));
        }
    }
    for quality in API_JPEG_QUALITIES {
        let bytes = jpeg_bytes(decoded, quality)?;
        if bytes.len() <= raw_target {
            return Ok(Some(PreparedImage {
                bytes,
                format: image::ImageFormat::Jpeg,
            }));
        }
    }
    Ok(None)
}

fn prepare_api_image(bytes: &[u8], limits: ImageLimits) -> Result<PreparedImage, String> {
    let format = image::guess_format(bytes).map_err(|error| error.to_string())?;
    // load_from_memory reads encoded dimensions and does not auto-rotate EXIF.
    // Keep that existing library contract for both measurement and resizing.
    let decoded = image::load_from_memory(bytes).map_err(|error| error.to_string())?;
    let (width, height) = decoded.dimensions();
    let dimensions = ImageDimensions { width, height };
    if limits.contains(dimensions) && bytes.len() <= limits.target_raw_bytes {
        return Ok(PreparedImage {
            bytes: bytes.to_vec(),
            format,
        });
    }
    if limits.contains(dimensions) {
        if let Some(prepared) = try_api_encodings(&decoded, format, limits.target_raw_bytes)? {
            return Ok(prepared);
        }
    }
    let fitted = fit_dimensions(dimensions, limits.max_width, limits.max_height);
    let resized = decoded.resize_exact(
        fitted.width,
        fitted.height,
        image::imageops::FilterType::Triangle,
    );
    let prepared = encode_in_source_format(&resized, format)?;
    if prepared.bytes.len() <= limits.target_raw_bytes {
        return Ok(prepared);
    }
    if let Some(prepared) = try_api_encodings(&resized, format, limits.target_raw_bytes)? {
        return Ok(prepared);
    }
    let fallback = ImageDimensions {
        width: fitted.width.min(1000),
        height: (f64::from(fitted.height) * f64::from(fitted.width.min(1000))
            / f64::from(fitted.width.max(1)))
        .round()
        .max(1.0) as u32,
    };
    let resized = decoded.resize_exact(
        fallback.width,
        fallback.height,
        image::imageops::FilterType::Triangle,
    );
    Ok(PreparedImage {
        bytes: jpeg_bytes(&resized, 20)?,
        format: image::ImageFormat::Jpeg,
    })
}

/// At most five quality probes; return the highest-quality passing probe, or
/// the smallest candidate (possibly the original). This does not resize or
/// promise that an arbitrarily small target can be reached.
pub fn compress_image_to_raw_target(
    bytes: &[u8],
    target: usize,
    format: image::ImageFormat,
) -> Result<Vec<u8>, String> {
    let decoded = image::load_from_memory(bytes).map_err(|error| error.to_string())?;
    search_jpeg_quality(
        bytes,
        target,
        format == image::ImageFormat::Jpeg,
        |quality| jpeg_bytes(&decoded, quality),
    )
}

fn search_jpeg_quality<F>(
    original: &[u8],
    target: usize,
    is_jpeg: bool,
    mut encode: F,
) -> Result<Vec<u8>, String>
where
    F: FnMut(u8) -> Result<Vec<u8>, String>,
{
    let mut smallest = original.to_vec();
    let mut upper = 90u8;
    if !is_jpeg {
        let candidate = encode(90)?;
        if candidate.len() < smallest.len() {
            smallest = candidate.clone();
        }
        if candidate.len() <= target {
            return Ok(candidate);
        }
        upper = 89;
    }
    let mut lower = 1u8;
    let mut best = None;
    for _ in 0..5 {
        let quality = lower + (upper - lower) / 2;
        let candidate = encode(quality)?;
        if candidate.len() < smallest.len() {
            smallest = candidate.clone();
        }
        if candidate.len() <= target {
            best = Some(candidate);
            lower = quality + 1;
        } else {
            upper = quality - 1;
        }
        if lower > upper {
            break;
        }
    }
    Ok(best.unwrap_or(smallest))
}

/// Apply default source constraints to one image.
pub fn gate_image_block(block: &ImageBlock) -> GatedImage {
    gate_image_block_with_limits(block, ImageLimits::default())
}

/// Apply model/source constraints before admission. Oversized input is first
/// compressed and resized, not rejected solely for its original base64 size.
pub fn gate_image_block_with_limits(block: &ImageBlock, limits: ImageLimits) -> GatedImage {
    if limits.max_width == 0 || limits.max_height == 0 || limits.max_base64_chars == 0 {
        return GatedImage::Rejected(rejection_marker("Invalid image limits"));
    }
    let Some(bytes) = decode_base64(&block.base64) else {
        return GatedImage::Rejected(rejection_marker("Invalid base64 image data"));
    };
    if bytes.is_empty() {
        return GatedImage::Rejected(rejection_marker("Image file is empty (0 bytes)"));
    }
    let mut prepared = match prepare_api_image(&bytes, limits) {
        Ok(prepared) => prepared,
        Err(error) => {
            tracing::warn!(%error, "Image API-size preparation failed");
            let Some(dimensions) = encoded_dimensions(&bytes) else {
                return GatedImage::Rejected(rejection_marker(
                    "Image processing failed and dimensions could not be verified",
                ));
            };
            if !limits.contains(dimensions) || block.base64.len_utf16() > limits.max_base64_chars {
                return GatedImage::Rejected(rejection_marker(&format!(
                    "Image processing failed and the source exceeds the {}x{}px or {} base64-byte limit",
                    limits.max_width, limits.max_height, limits.max_base64_chars)));
            }
            let Ok(format) = image::guess_format(&bytes) else {
                return GatedImage::Rejected(rejection_marker("Unknown image format"));
            };
            PreparedImage {
                bytes: bytes.clone(),
                format,
            }
        }
    };
    if prepared.bytes.len() > IMAGE_BEST_EFFORT_RAW_BYTES {
        match compress_image_to_raw_target(
            &prepared.bytes,
            IMAGE_BEST_EFFORT_RAW_BYTES,
            prepared.format,
        ) {
            Ok(compressed) => {
                prepared.format = image::guess_format(&compressed).unwrap_or(prepared.format);
                prepared.bytes = compressed;
            }
            Err(error) => {
                tracing::warn!(%error, "Image byte-budget compression failed; retaining API-sized source")
            }
        }
    }
    let encoded = BASE64_STANDARD.encode(&prepared.bytes);
    if encoded.len() > limits.max_base64_chars {
        return GatedImage::Rejected(rejection_marker(&format!(
            "Final image payload has {} base64 bytes, exceeding the {} limit",
            encoded.len(),
            limits.max_base64_chars
        )));
    }
    let mime = prepared.format.to_mime_type();
    if prepared.bytes == bytes && block.mime_type == mime {
        return GatedImage::Keep(block.clone());
    }
    GatedImage::Downscaled(ImageBlock {
        mime_type: RuntimeString::from_utf8(mime),
        base64: RuntimeString::from_utf8(&encoded),
        detail: block.detail,
        source_path: None,
    })
}

fn plain_marker(text: RuntimeString) -> TextBlock {
    TextBlock {
        text,
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

/// Gate every image inside one content-block list (user messages and steers).
pub fn gate_content_blocks(blocks: Vec<ContentBlock>) -> Vec<ContentBlock> {
    gate_content_blocks_with_limits(blocks, ImageLimits::default())
}

pub fn gate_content_blocks_with_limits(
    blocks: Vec<ContentBlock>,
    limits: ImageLimits,
) -> Vec<ContentBlock> {
    blocks
        .into_iter()
        .map(|block| match block {
            ContentBlock::Image(image) => match gate_image_block_with_limits(&image, limits) {
                GatedImage::Keep(kept) => ContentBlock::Image(kept),
                GatedImage::Downscaled(resized) => ContentBlock::Image(resized),
                GatedImage::Rejected(marker) => ContentBlock::Text(plain_marker(marker)),
            },
            ContentBlock::ToolResult(mut result) => {
                result.content = gate_output_blocks_with_limits(result.content, limits);
                ContentBlock::ToolResult(result)
            }
            other => other,
        })
        .collect()
}

/// Gate every image inside one tool-result output-block list.
pub fn gate_output_blocks(blocks: Vec<OutputBlock>) -> Vec<OutputBlock> {
    gate_output_blocks_with_limits(blocks, ImageLimits::default())
}

pub fn gate_output_blocks_with_limits(
    blocks: Vec<OutputBlock>,
    limits: ImageLimits,
) -> Vec<OutputBlock> {
    blocks
        .into_iter()
        .map(|block| match block {
            OutputBlock::Image(image) => match gate_image_block_with_limits(&image, limits) {
                GatedImage::Keep(kept) => OutputBlock::Image(kept),
                GatedImage::Downscaled(resized) => OutputBlock::Image(resized),
                GatedImage::Rejected(marker) => OutputBlock::Text(plain_marker(marker)),
            },
            other => other,
        })
        .collect()
}

/// Cap the image tokens a compaction's retained tail may carry. Walks newest
/// first so the most recent screenshots survive; older images collapse to
/// markers once the budget is spent, which is what lets a compaction finally
/// shrink an image-heavy history instead of carrying it verbatim forever.
pub fn apply_tail_image_budget(messages: Vec<Message>, budget_tokens: u64) -> Vec<Message> {
    let mut spent: u64 = 0;
    let mut gated: Vec<Message> = Vec::with_capacity(messages.len());
    for message in messages.into_iter().rev() {
        gated.push(replace_images_over_budget(
            message,
            budget_tokens,
            &mut spent,
        ));
    }
    gated.reverse();
    gated
}

fn replace_images_over_budget(message: Message, budget_tokens: u64, spent: &mut u64) -> Message {
    match message {
        Message::User(user) => {
            let content = user
                .content
                .into_iter()
                .map(|block| match block {
                    ContentBlock::Image(image) => charge_image(image, budget_tokens, spent),
                    other => other,
                })
                .collect();
            Message::User(UserMessage { content, ..user })
        }
        Message::ToolResult(result) => {
            let content = result
                .content
                .into_iter()
                .map(|block| match block {
                    OutputBlock::Image(image) => match charge_image(image, budget_tokens, spent) {
                        ContentBlock::Image(kept) => OutputBlock::Image(kept),
                        ContentBlock::Text(marker) => OutputBlock::Text(marker),
                        _ => unreachable!("charge_image only yields image or text"),
                    },
                    other => other,
                })
                .collect();
            Message::ToolResult(ToolResultMessage { content, ..result })
        }
        other => other,
    }
}

fn charge_image(image: ImageBlock, budget_tokens: u64, spent: &mut u64) -> ContentBlock {
    let cost = estimate_image_tokens(&image);
    if *spent + cost <= budget_tokens {
        *spent += cost;
        return ContentBlock::Image(image);
    }
    ContentBlock::Text(plain_marker(RuntimeString::from_utf8(&format!(
        "[image omitted from retained history: ~{cost} tokens exceeds the tail image budget]"
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn png_block(width: u32, height: u32) -> ImageBlock {
        let img = image::RgbaImage::from_pixel(width, height, image::Rgba([200, 60, 40, 255]));
        let mut buf: Vec<u8> = Vec::new();
        img.write_with_encoder(image::codecs::png::PngEncoder::new(&mut buf))
            .unwrap();
        ImageBlock {
            mime_type: "image/png".into(),
            base64: RuntimeString::from_utf8(&BASE64_STANDARD.encode(&buf)),
            detail: None,

            source_path: None,
        }
    }

    #[test]
    fn quality_search_has_five_probes_and_keeps_the_highest_passing_quality() {
        let mut qualities = Vec::new();
        let best = search_jpeg_quality(&vec![0; 20_000], 5000, false, |quality| {
            qualities.push(quality);
            Ok(vec![quality; usize::from(quality) * 100])
        })
        .unwrap();
        assert_eq!(qualities, [90, 45, 67, 56, 50, 53]);
        assert_eq!(best, vec![50; 5000]);
    }

    #[test]
    fn jpeg_search_returns_the_smallest_candidate_when_no_quality_meets_the_target() {
        let mut qualities = Vec::new();
        let smallest = search_jpeg_quality(&vec![0; 201], 1, true, |quality| {
            qualities.push(quality);
            Ok(vec![quality; usize::from(quality) + 100])
        })
        .unwrap();
        assert_eq!(qualities, [45, 22, 11, 5, 2]);
        assert_eq!(smallest.len(), 102);
        assert!(smallest.len() > 1, "the target is not a hard guarantee");
    }

    #[test]
    fn unsuccessful_quality_search_can_retain_the_original_and_propagates_encoder_failure() {
        let original = vec![7; 200];
        assert_eq!(
            search_jpeg_quality(&original, 1, false, |_| Ok(vec![8; 250])).unwrap(),
            original
        );
        assert!(search_jpeg_quality(&original, 1, true, |_| Err("encoder failed".into())).is_err());
    }

    #[test]
    fn api_palette_stage_writes_a_real_indexed_png_before_jpeg() {
        let decoded = image::DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(
            96,
            64,
            image::Rgba([20, 70, 180, 255]),
        ));
        let prepared = try_api_encodings(&decoded, image::ImageFormat::Png, 20_000)
            .unwrap()
            .unwrap();
        assert_eq!(prepared.format, image::ImageFormat::Png);
        assert_eq!(prepared.bytes[25], 3, "IHDR color type must be indexed");
        assert_eq!(
            image::load_from_memory(&prepared.bytes)
                .unwrap()
                .dimensions(),
            (96, 64)
        );
    }

    #[test]
    fn api_fallback_caps_width_and_preserves_portrait_aspect_ratio_without_claiming_bytes() {
        let block = png_block(30, 3000);
        let bytes = decode_base64(&block.base64).unwrap();
        let prepared = prepare_api_image(
            &bytes,
            ImageLimits {
                target_raw_bytes: 1,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(prepared.format, image::ImageFormat::Jpeg);
        let dimensions = encoded_dimensions(&prepared.bytes).unwrap();
        assert_eq!(
            dimensions,
            ImageDimensions {
                width: 20,
                height: 2000
            }
        );
        assert!(prepared.bytes.len() > 1);
    }

    #[test]
    fn indexed_png_preserves_real_rgba_transparency_for_an_exact_palette() {
        let values = [
            [250, 40, 10, 0],
            [200, 100, 50, 128],
            [12, 34, 56, 255],
            [1, 2, 3, 1],
        ];
        let rgba =
            image::RgbaImage::from_fn(16, 8, |x, _| image::Rgba(values[x as usize % values.len()]));
        let bytes = palette_png_bytes(&image::DynamicImage::ImageRgba8(rgba.clone())).unwrap();
        assert_eq!(bytes[25], 3);
        assert!(bytes.windows(4).any(|part| part == b"tRNS"));
        assert_eq!(image::load_from_memory(&bytes).unwrap().to_rgba8(), rgba);
    }

    #[test]
    fn rgba_quantization_retains_constant_alpha_when_more_than_256_colors_are_present() {
        let rgba = image::RgbaImage::from_fn(32, 32, |x, y| {
            image::Rgba([(x * 7) as u8, (y * 7) as u8, (x * 3 + y * 5) as u8, 128])
        });
        let bytes = palette_png_bytes(&image::DynamicImage::ImageRgba8(rgba)).unwrap();
        assert!(image::load_from_memory(&bytes)
            .unwrap()
            .to_rgba8()
            .pixels()
            .all(|pixel| pixel.0[3] == 128));
    }

    #[test]
    fn jpeg_alpha_is_composited_on_black_not_discarded_as_hidden_rgb() {
        for (input, expected) in [
            ([250, 40, 10, 0], [0, 0, 0]),
            ([200, 100, 50, 128], [100, 50, 25]),
            ([12, 34, 56, 255], [12, 34, 56]),
        ] {
            let decoded = image::DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(
                32,
                32,
                image::Rgba(input),
            ));
            assert!(jpeg_rgb(&decoded).pixels().all(|pixel| pixel.0 == expected));
            let encoded = jpeg_bytes(&decoded, 90).unwrap();
            let result = image::load_from_memory(&encoded).unwrap().to_rgb8();
            for (actual, expected) in result.get_pixel(16, 16).0.into_iter().zip(expected) {
                assert!(
                    actual.abs_diff(expected) <= 3,
                    "JPEG rounding must not expose hidden RGB"
                );
            }
        }
    }

    #[test]
    fn optional_metadata_resolves_to_concrete_effective_limits() {
        let native = ImageLimits {
            max_width: 1800,
            max_height: 1200,
            max_base64_chars: 100,
            target_raw_bytes: 50,
        };
        assert_eq!(native.with_metadata(None, None, None, None), native);
        assert_eq!(
            native.with_metadata(Some(640), None, Some(80), None),
            ImageLimits {
                max_width: 640,
                max_height: 1200,
                max_base64_chars: 80,
                target_raw_bytes: 60,
            }
        );
        assert_eq!(
            native.with_metadata(None, Some(480), Some(80), Some(40)),
            ImageLimits {
                max_width: 1800,
                max_height: 480,
                max_base64_chars: 80,
                target_raw_bytes: 40,
            }
        );
    }

    #[test]
    fn verified_compliant_header_can_survive_a_pixel_processing_failure() {
        let mut bytes = decode_base64(&png_block(120, 80).base64).unwrap();
        let idat = bytes.windows(4).position(|part| part == b"IDAT").unwrap();
        bytes[idat + 4] = 0;
        assert_eq!(
            encoded_dimensions(&bytes),
            Some(ImageDimensions {
                width: 120,
                height: 80
            })
        );
        assert!(image::load_from_memory(&bytes).is_err());
        let block = ImageBlock {
            mime_type: "image/png".into(),
            base64: BASE64_STANDARD.encode(&bytes).into(),
            detail: None,
            source_path: Some("/synthetic/source".into()),
        };
        assert!(matches!(gate_image_block(&block), GatedImage::Keep(kept) if kept == block));
    }

    #[test]
    fn nested_tool_result_admission_uses_the_same_model_limits() {
        let blocks = gate_content_blocks_with_limits(
            vec![ContentBlock::ToolResult(
                crate::schemas::content::ToolResultBlock {
                    tool_use_id: "tool".into(),
                    is_error: Some(false),
                    content: vec![OutputBlock::Image(png_block(300, 100))],
                },
            )],
            ImageLimits {
                max_width: 64,
                max_height: 32,
                ..Default::default()
            },
        );
        let ContentBlock::ToolResult(result) = &blocks[0] else {
            panic!("tool result must remain a tool result");
        };
        let OutputBlock::Image(image) = &result.content[0] else {
            panic!("nested image must remain processable");
        };
        assert_eq!(
            image_dimensions(image),
            Some(ImageDimensions {
                width: 64,
                height: 21
            })
        );
        assert_eq!(result.tool_use_id, "tool");
        assert_eq!(result.is_error, Some(false));
    }

    #[test]
    fn invalid_or_empty_images_have_an_explicit_unprocessable_marker() {
        for base64 in ["", "not base64", "AAAA"] {
            let block = ImageBlock {
                mime_type: "image/png".into(),
                base64: base64.into(),
                detail: None,
                source_path: None,
            };
            assert!(
                matches!(gate_image_block(&block), GatedImage::Rejected(marker)
                if marker.contains("Image could not be processed"))
            );
        }
    }

    #[test]
    fn dimensions_come_from_the_encoded_header() {
        let block = png_block(120, 40);
        assert_eq!(
            image_dimensions(&block),
            Some(ImageDimensions {
                width: 120,
                height: 40
            })
        );
    }

    #[test]
    fn estimate_uses_pixels_not_a_flat_constant() {
        let small = png_block(100, 100);
        let tall = png_block(1000, 4000);
        let small_tokens = estimate_image_tokens(&small);
        let tall_tokens = estimate_image_tokens(&tall);
        // 100x100 / 750 ≈ 14; 1000x4000 / 750 ≈ 5334.
        assert_eq!(small_tokens, 14);
        assert_eq!(tall_tokens, 5334);
        assert!(tall_tokens > small_tokens * 100);
    }

    #[test]
    fn undecodable_payload_is_charged_by_bytes() {
        // A 400_000-char base64 payload decodes to 300_000 bytes; the fallback
        // must charge ~75_000 tokens, not the old flat 2000.
        let block = ImageBlock {
            mime_type: "image/png".into(),
            base64: RuntimeString::from_utf8(&"A".repeat(400_000)),
            detail: None,
            source_path: None,
        };
        assert_eq!(estimate_image_tokens(&block), 75_000);
    }

    #[test]
    fn small_image_passes_the_gate_unchanged() {
        let block = png_block(800, 600);
        match gate_image_block(&block) {
            GatedImage::Keep(kept) => assert_eq!(kept, block),
            other => panic!("expected Keep, got {other:?}"),
        }
    }

    #[test]
    fn oversized_image_is_downscaled_to_the_long_edge_limit() {
        let block = png_block(6000, 2000);
        match gate_image_block(&block) {
            GatedImage::Downscaled(resized) => {
                let dims = image_dimensions(&resized).expect("resized image must decode");
                assert_eq!(dims.width, 2000);
                assert_eq!(dims.height, 667);
                assert!(resized.base64.len_utf16() < block.base64.len_utf16());
            }
            other => panic!("expected Downscaled, got {other:?}"),
        }
    }

    #[test]
    fn payload_over_the_ceiling_becomes_a_marker() {
        let block = ImageBlock {
            mime_type: "image/png".into(),
            base64: RuntimeString::from_utf16(vec![b'A' as u16; IMAGE_MAX_BASE64_CHARS + 1]),
            detail: None,
            source_path: None,
        };
        match gate_image_block(&block) {
            GatedImage::Rejected(marker) => {
                let text = marker.try_to_utf8().unwrap();
                assert!(text.contains("image omitted"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn tail_budget_keeps_newest_images_and_collapses_older_ones() {
        let newest = Message::User(UserMessage {
            question_answer: None,
            uuid: "u2".into(),
            content: vec![ContentBlock::Image(png_block(1500, 1500))],
            is_meta: None,
        });
        let oldest = Message::User(UserMessage {
            question_answer: None,
            uuid: "u1".into(),
            content: vec![ContentBlock::Image(png_block(1500, 1500))],
            is_meta: None,
        });
        // One 1500x1500 image costs 3000 tokens; budget fits exactly one.
        let gated = apply_tail_image_budget(vec![oldest, newest], 3000);
        let collapsed = &gated[0];
        let kept = &gated[1];
        assert!(matches!(
            collapsed,
            Message::User(u) if matches!(&u.content[0], ContentBlock::Text(t) if t.text.contains("image omitted from retained history"))
        ));
        assert!(
            matches!(kept, Message::User(u) if matches!(&u.content[0], ContentBlock::Image(_)))
        );
    }
}
