// Copyright 2025 OpenAI
// Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified to prepare local inspection images under captured media constraints.

//! Image preparation for local inspection tools.

use crate::context::images::{gate_image_block_with_limits, GatedImage, ImageLimits};
use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ImageBlock, ImageDetail};
use crate::types::errors::ToolError;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use image::imageops::FilterType;
use image::{ColorType, DynamicImage, GenericImageView, ImageDecoder, ImageEncoder, ImageFormat};
use std::io::Cursor;

/// Bounds both accepted input representations before decoding image pixels.
pub(crate) const MAX_INSPECTION_IMAGE_INPUT_BYTES: usize = 1024 * 1024 * 1024;
const PATCH_SIZE: u32 = 32;

#[derive(Clone, Copy)]
struct ResizeLimits {
    max_dimension: u32,
    max_patches: usize,
}

fn resize_limits(detail: ImageDetail) -> ResizeLimits {
    match detail {
        ImageDetail::Original => ResizeLimits {
            max_dimension: 6000,
            max_patches: 10_000,
        },
        _ => ResizeLimits {
            max_dimension: 2048,
            max_patches: 2_500,
        },
    }
}

pub(crate) fn inspection_image_limits(
    detail: ImageDetail,
    declaration: Option<&crate::types::provider::ImageLimits>,
) -> ImageLimits {
    let mut limits = ImageLimits::default();
    let profile = resize_limits(detail);
    limits.max_width = profile.max_dimension;
    limits.max_height = profile.max_dimension;
    limits.max_base64_chars = MAX_INSPECTION_IMAGE_INPUT_BYTES;
    limits.target_raw_bytes = MAX_INSPECTION_IMAGE_INPUT_BYTES / 4 * 3;
    limits.with_declaration(declaration)
}

fn dimensions_fit(width: u32, height: u32, limits: ResizeLimits) -> bool {
    let patches = u64::from(width.div_ceil(PATCH_SIZE)) * u64::from(height.div_ceil(PATCH_SIZE));
    width <= limits.max_dimension
        && height <= limits.max_dimension
        && patches <= limits.max_patches as u64
}

fn output_dimensions(width: u32, height: u32, limits: ResizeLimits) -> (u32, u32) {
    let width = width.max(1);
    let height = height.max(1);
    if dimensions_fit(width, height, limits) {
        return (width, height);
    }
    let dimension_scale = (f64::from(limits.max_dimension) / f64::from(width.max(height))).min(1.0);
    let width = ((f64::from(width) * dimension_scale).round() as u32).max(1);
    let height = ((f64::from(height) * dimension_scale).round() as u32).max(1);
    if dimensions_fit(width, height, limits) {
        return (width, height);
    }
    let width_f64 = f64::from(width);
    let height_f64 = f64::from(height);
    let patch_size = f64::from(PATCH_SIZE);
    let mut scale =
        (patch_size * patch_size * limits.max_patches as f64 / width_f64 / height_f64).sqrt();
    let scaled_wide = width_f64 * scale / patch_size;
    let scaled_high = height_f64 * scale / patch_size;
    scale *= (scaled_wide.floor() / scaled_wide).min(scaled_high.floor() / scaled_high);
    (
        ((width_f64 * scale).floor() as u32).max(1),
        ((height_f64 * scale).floor() as u32).max(1),
    )
}

struct Metadata {
    icc_profile: Option<Vec<u8>>,
    exif: Option<Vec<u8>>,
}

fn apply_metadata(
    encoder: &mut impl ImageEncoder,
    metadata: Metadata,
) -> Result<(), image::ImageError> {
    if let Some(profile) = metadata.icc_profile {
        encoder
            .set_icc_profile(profile)
            .map_err(image::ImageError::Unsupported)?;
    }
    if let Some(exif) = metadata.exif {
        encoder
            .set_exif_metadata(exif)
            .map_err(image::ImageError::Unsupported)?;
    }
    Ok(())
}

fn encode_image(
    image: &DynamicImage,
    format: ImageFormat,
    metadata: Metadata,
) -> Result<(Vec<u8>, ImageFormat), image::ImageError> {
    let format = match format {
        ImageFormat::Jpeg | ImageFormat::WebP => format,
        _ => ImageFormat::Png,
    };
    let mut bytes = Vec::new();
    match format {
        ImageFormat::Jpeg => {
            let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut bytes, 85);
            apply_metadata(&mut encoder, metadata)?;
            encoder.encode_image(image)?;
        }
        ImageFormat::WebP => {
            let mut encoder = image::codecs::webp::WebPEncoder::new_lossless(&mut bytes);
            apply_metadata(&mut encoder, metadata)?;
            encoder.write_image(
                image.to_rgba8().as_raw(),
                image.width(),
                image.height(),
                ColorType::Rgba8.into(),
            )?;
        }
        _ => {
            let mut encoder = image::codecs::png::PngEncoder::new(&mut bytes);
            apply_metadata(&mut encoder, metadata)?;
            encoder.write_image(
                image.to_rgba8().as_raw(),
                image.width(),
                image.height(),
                ColorType::Rgba8.into(),
            )?;
        }
    }
    Ok((bytes, format))
}

pub(crate) fn prepare_inspection_image(
    bytes: Vec<u8>,
    detail: ImageDetail,
    declaration: Option<&crate::types::provider::ImageLimits>,
    source_path: Option<RuntimeString>,
) -> Result<ImageBlock, ToolError> {
    if bytes.len() > MAX_INSPECTION_IMAGE_INPUT_BYTES
        || bytes.len().div_ceil(3).saturating_mul(4) > MAX_INSPECTION_IMAGE_INPUT_BYTES
    {
        return Err(ToolError::new(
            "Image input exceeds the supported 1 GiB representation limit.",
        ));
    }
    let prepare = || -> Result<(Vec<u8>, ImageFormat), image::ImageError> {
        let format = image::guess_format(&bytes)?;
        let mut decoder =
            image::ImageReader::with_format(Cursor::new(&bytes), format).into_decoder()?;
        let metadata = Metadata {
            icc_profile: decoder
                .icc_profile()
                .ok()
                .flatten()
                .filter(|profile| profile.get(16..20) == Some(b"RGB ")),
            exif: decoder.exif_metadata().ok().flatten(),
        };
        let decoded = DynamicImage::from_decoder(decoder)?;
        let (width, height) = decoded.dimensions();
        let (target_width, target_height) = output_dimensions(width, height, resize_limits(detail));
        if (width, height) != (target_width, target_height) {
            let resized = decoded.resize_exact(target_width, target_height, FilterType::Triangle);
            encode_image(&resized, format, metadata)
        } else if matches!(
            format,
            ImageFormat::Png | ImageFormat::Jpeg | ImageFormat::WebP
        ) {
            Ok((bytes.clone(), format))
        } else {
            encode_image(&decoded, ImageFormat::Png, metadata)
        }
    };
    let (prepared, format) = prepare().map_err(|error| {
        ToolError::new(format!(
            "Unable to process image: invalid or unsupported image data ({error})."
        ))
    })?;
    let block = ImageBlock {
        mime_type: format.to_mime_type().into(),
        base64: BASE64_STANDARD.encode(&prepared).into(),
        detail: Some(detail),
        source_path: (prepared == bytes).then_some(source_path).flatten(),
    };
    let limits = inspection_image_limits(detail, declaration);
    let dimensions = crate::context::images::image_dimensions(&block)
        .ok_or_else(|| ToolError::new("Cannot verify prepared image dimensions."))?;
    if dimensions.width > limits.max_width
        || dimensions.height > limits.max_height
        || block.base64.len_utf16() > limits.max_base64_chars
    {
        return Err(ToolError::new(format!("Requested image detail exceeds this model's image limit ({}x{} pixels, {} base64 bytes). Use high detail or a smaller image.", limits.max_width, limits.max_height, limits.max_base64_chars)));
    }
    Ok(block)
}

pub(crate) fn request_uses_image_detail(request: &crate::types::provider::ModelRequest) -> bool {
    use crate::schemas::content::{ContentBlock, OutputBlock};
    use crate::schemas::message::Message;
    fn outputs(blocks: &[OutputBlock]) -> bool {
        blocks
            .iter()
            .any(|block| matches!(block, OutputBlock::Image(image) if image.detail.is_some()))
    }
    fn content(blocks: &[ContentBlock]) -> bool {
        blocks.iter().any(|block| match block {
            ContentBlock::Image(image) => image.detail.is_some(),
            ContentBlock::ToolResult(result) => outputs(&result.content),
            _ => false,
        })
    }
    request.messages.iter().any(|message| match message {
        Message::User(message) => content(&message.content),
        Message::Assistant(message) => content(&message.content),
        Message::ToolResult(message) => {
            outputs(&message.content)
                || message
                    .extra_user_blocks
                    .as_ref()
                    .is_some_and(|blocks| content(blocks))
        }
        _ => false,
    })
}

/// A detail hint never grants a model capability. Only the captured binding can
/// admit original detail; ordinary media without a hint keeps its existing gate.
pub(crate) fn gate_selected_image(
    block: &ImageBlock,
    supports_original: bool,
    declaration: Option<&crate::types::provider::ImageLimits>,
) -> GatedImage {
    let mut normalized = block.clone();
    if normalized.detail == Some(ImageDetail::Original) && !supports_original {
        normalized.detail = Some(ImageDetail::High);
    }
    let detail = match normalized.detail {
        None => {
            return gate_image_block_with_limits(
                &normalized,
                ImageLimits::default().with_declaration(declaration),
            )
        }
        Some(ImageDetail::Low) => {
            return GatedImage::Rejected(
                "[Image omitted: low detail is unsupported; use high or original.]".into(),
            )
        }
        Some(ImageDetail::Auto | ImageDetail::High) => ImageDetail::High,
        Some(ImageDetail::Original) => ImageDetail::Original,
    };
    let bytes = match BASE64_STANDARD.decode(normalized.base64.to_node_utf8()) {
        Ok(bytes) => bytes,
        Err(_) => {
            return GatedImage::Rejected("[Image omitted: invalid base64 image data.]".into())
        }
    };
    match prepare_inspection_image(bytes, detail, declaration, normalized.source_path.clone()) {
        Ok(prepared) if prepared == *block => GatedImage::Keep(prepared),
        Ok(prepared) => GatedImage::Downscaled(prepared),
        Err(error) => GatedImage::Rejected(
            RuntimeString::from("[Image omitted: ")
                .concat(&error.message)
                .concat(&RuntimeString::from("]")),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn high_and_original_use_separate_patch_budgets() {
        assert_eq!(
            output_dimensions(2048, 2048, resize_limits(ImageDetail::High)),
            (1600, 1600)
        );
        assert_eq!(
            output_dimensions(6401, 100, resize_limits(ImageDetail::Original)),
            (6000, 94)
        );
        assert_eq!(
            output_dimensions(3200, 3200, resize_limits(ImageDetail::Original)),
            (3200, 3200)
        );
    }

    #[test]
    fn imported_original_hint_cannot_supply_its_own_capability() {
        let mut bytes = Vec::new();
        image::RgbImage::from_pixel(2600, 16, image::Rgb([17, 29, 43]))
            .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
            .unwrap();
        let original = prepare_inspection_image(bytes, ImageDetail::Original, None, None).unwrap();
        let GatedImage::Downscaled(normalized) = gate_selected_image(&original, false, None) else {
            panic!("unsupported original must normalize through the high profile")
        };
        assert_eq!(normalized.detail, Some(ImageDetail::High));
        assert_eq!(
            crate::context::images::image_dimensions(&normalized)
                .unwrap()
                .width,
            2048
        );
        assert!(matches!(
            gate_selected_image(&original, true, None),
            GatedImage::Keep(_)
        ));
    }

    #[test]
    fn original_profile_preserves_resolution_and_explicit_limits_remain_binding() {
        let decoded = image::RgbImage::from_pixel(2600, 16, image::Rgb([13, 29, 47]));
        let mut bytes = Vec::new();
        decoded
            .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
            .unwrap();
        let original =
            prepare_inspection_image(bytes.clone(), ImageDetail::Original, None, None).unwrap();
        assert_eq!(
            crate::context::images::image_dimensions(&original)
                .unwrap()
                .width,
            2600
        );
        assert_eq!(
            BASE64_STANDARD
                .decode(original.base64.to_node_utf8())
                .unwrap(),
            bytes
        );
        let limits = crate::types::provider::ImageLimits {
            max_width: std::num::NonZeroU32::new(2000),
            ..Default::default()
        };
        assert!(
            prepare_inspection_image(bytes, ImageDetail::Original, Some(&limits), None)
                .unwrap_err()
                .message
                .contains("Use high detail")
        );
    }
}
