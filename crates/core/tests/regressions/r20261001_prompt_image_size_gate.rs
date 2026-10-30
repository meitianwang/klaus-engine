//! Defect: prompt images entered the context at source resolution while the
//! context estimate charged every image a flat 2000 tokens. A single
//! full-page browser screenshot (~2.1 MB base64) therefore occupied a large
//! share of the window invisibly: compaction stripped images from the summary
//! request but kept them verbatim in the retained tail, so every compaction
//! refilled to the limit within three turns and the rapid-refill breaker
//! aborted the turn ("History compaction is thrashing", observed 2026-10-01 in
//! a portal-redesign goal session after 11 compactions).
//! Root cause: no entry gate resized or rejected oversized images (Claude Code
//! caps client-side at 2000px/5MB base64, Codex resizes tool-output images to
//! 2048px), the estimate ignored dimensions, and the retained tail had no
//! image budget.
//! Fixed by: context::images — entry gate on user messages, steers and tool
//! results; pixel-based estimate (w*h/750, byte fallback); tail image budget
//! applied in full_compact's retained tail.

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use klaus_agent_core::context::images::{
    apply_tail_image_budget, estimate_image_tokens, gate_image_block, image_dimensions, GatedImage,
    IMAGE_MAX_DIMENSION,
};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, ImageBlock};
use klaus_agent_core::schemas::message::{Message, UserMessage};

fn png(width: u32, height: u32) -> ImageBlock {
    let img = image::RgbaImage::from_pixel(width, height, image::Rgba([12, 34, 56, 255]));
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
fn oversized_tool_screenshot_is_downscaled_at_the_entry_gate() {
    let block = png(6000, 4000);
    let gated = gate_image_block(&block);
    let GatedImage::Downscaled(resized) = gated else {
        panic!("expected the gate to downscale a 6000x4000 screenshot");
    };
    let dims = image_dimensions(&resized).expect("gated image must stay decodable");
    assert_eq!(dims.width, IMAGE_MAX_DIMENSION);
    assert!(resized.base64.len_utf16() < block.base64.len_utf16());
}

#[test]
fn estimate_tracks_pixels_instead_of_a_flat_constant() {
    let small = estimate_image_tokens(&png(200, 200));
    let large = estimate_image_tokens(&png(4000, 3000));
    assert_eq!(small, 54); // 200*200/750
    assert_eq!(large, 16_000); // 4000*3000/750
    assert!(large > small * 100);
}

#[test]
fn compaction_tail_image_budget_collapses_old_screenshots() {
    let image_message = |uuid: &str| {
        Message::User(UserMessage {
            question_answer: None,
            uuid: uuid.into(),
            content: vec![ContentBlock::Image(png(3000, 3000))],
            is_meta: None,
        })
    };
    // One 3000x3000 image costs 12_000 tokens; budget admits exactly one.
    let gated = apply_tail_image_budget(vec![image_message("old"), image_message("new")], 12_000);
    let Message::User(older) = &gated[0] else {
        panic!("expected a user message");
    };
    let Message::User(newer) = &gated[1] else {
        panic!("expected a user message");
    };
    assert!(
        matches!(&older.content[0], ContentBlock::Text(t) if t.text.contains("image omitted from retained history")),
        "the older screenshot must collapse to a marker once the budget is spent"
    );
    assert!(
        matches!(&newer.content[0], ContentBlock::Image(_)),
        "the newest screenshot must survive the retained tail"
    );
}
