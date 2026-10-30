// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified: integrated host-owned evidence and model adapters for this runtime.

//! Bounded image selection shared by Approval consumers.
//! Keeps source order and evicts oldest images using the existing count/byte caps.
//! Byte caps cover inline URLs and file ID strings, not referenced file contents.
//! Consumers still choose source visibility and model-specific image detail.

use crate::engine::approval_context::model::ContentItem;
use crate::engine::approval_context::model::FunctionCallOutputContentItem;
use crate::engine::approval_context::model::ImageDetail;
use crate::engine::approval_context::model::ImageReference;
use crate::engine::approval_context::model::ResponseItem;
use crate::engine::approval_context::ContextSection;
use crate::engine::approval_context::SectionContributor;
use crate::engine::approval_context::SectionError;
use crate::engine::approval_context::SectionInput;
use crate::engine::approval_context::SectionScope;
use std::collections::VecDeque;

const MAX_TRANSCRIPT_IMAGES: usize = 4;
const MAX_TRANSCRIPT_IMAGE_BYTES: usize = 8 * 1024 * 1024;

/// Request-local image policy and a frozen view of captured REPL screenshots.
#[derive(Clone, Copy)]
pub struct TranscriptImageInput<'a> {
    pub enabled: bool,
    pub include_tool_outputs: bool,
    pub node_repl_images: &'a [ContentItem],
}

/// Selected images and byte omissions for the consumer's existing telemetry.
#[derive(Clone, Default, PartialEq)]
pub struct TranscriptImages {
    pub images: Vec<ContentItem>,
    /// Omitted inline payload or file ID bytes, not the size of referenced files.
    pub omitted_bytes: usize,
}

impl std::fmt::Debug for TranscriptImages {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TranscriptImages")
            .field("count", &self.images.len())
            .field("omitted_bytes", &self.omitted_bytes)
            .finish_non_exhaustive()
    }
}

impl TranscriptImages {
    /// Selects images without changing their detail or allocating a second history.
    pub fn collect<'a>(
        items: impl IntoIterator<Item = &'a ResponseItem>,
        input: TranscriptImageInput<'_>,
    ) -> Self {
        if !input.enabled {
            return TranscriptImages {
                images: Vec::new(),
                omitted_bytes: 0,
            };
        }

        let mut images = VecDeque::new();
        let mut image_reference_bytes = 0usize;
        let mut omitted_bytes = 0usize;
        let mut include_image = |image: &ImageReference, detail: Option<ImageDetail>| {
            let reference_bytes = match image {
                ImageReference::Inline { image_url } => image_url.len(),
                ImageReference::File { file_id } => file_id.len(),
            };
            if reference_bytes > MAX_TRANSCRIPT_IMAGE_BYTES {
                omitted_bytes = omitted_bytes.saturating_add(reference_bytes);
                return;
            }
            while images.len() >= MAX_TRANSCRIPT_IMAGES
                || image_reference_bytes + reference_bytes > MAX_TRANSCRIPT_IMAGE_BYTES
            {
                let Some(ContentItem::InputImage { image, .. }) = images.pop_front() else {
                    break;
                };
                let evicted_bytes = match image {
                    ImageReference::Inline { image_url } => image_url.len(),
                    ImageReference::File { file_id } => file_id.len(),
                };
                image_reference_bytes = image_reference_bytes.saturating_sub(evicted_bytes);
                omitted_bytes = omitted_bytes.saturating_add(evicted_bytes);
            }
            image_reference_bytes = image_reference_bytes.saturating_add(reference_bytes);
            images.push_back(ContentItem::InputImage {
                image: image.clone(),
                detail,
            });
        };

        for item in items {
            match item {
                ResponseItem::Message { role, content, .. }
                    if matches!(role.as_str(), "user" | "assistant") =>
                {
                    for item in content {
                        match item {
                            ContentItem::InputImage { image, detail } => {
                                include_image(image, *detail)
                            }
                            ContentItem::InputText { .. }
                            | ContentItem::InputAudio { .. }
                            | ContentItem::OutputText { .. } => {}
                        }
                    }
                }
                ResponseItem::FunctionCallOutput { output, .. }
                | ResponseItem::CustomToolCallOutput { output, .. }
                    if input.include_tool_outputs =>
                {
                    if let Some(content) = output.content_items() {
                        for item in content {
                            match item {
                                FunctionCallOutputContentItem::InputImage { image, detail } => {
                                    include_image(image, *detail)
                                }
                                FunctionCallOutputContentItem::InputText { .. }
                                | FunctionCallOutputContentItem::InputAudio { .. }
                                | FunctionCallOutputContentItem::EncryptedContent { .. } => {}
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        if input.include_tool_outputs {
            for image in input.node_repl_images {
                match image {
                    ContentItem::InputImage {
                        image: image @ ImageReference::Inline { .. },
                        detail,
                    } => include_image(image, *detail),
                    ContentItem::InputImage {
                        image: ImageReference::File { .. },
                        ..
                    }
                    | ContentItem::InputText { .. }
                    | ContentItem::InputAudio { .. }
                    | ContentItem::OutputText { .. } => {}
                }
            }
        }

        TranscriptImages {
            images: images.into_iter().collect(),
            omitted_bytes,
        }
    }
}

pub(crate) struct TranscriptImagesSection;
impl SectionContributor for TranscriptImagesSection {
    fn scope(&self) -> SectionScope {
        SectionScope::Shared
    }
    fn contribute(&self, input: &SectionInput<'_>) -> Result<Option<ContextSection>, SectionError> {
        Ok(input.images.map(|images| {
            ContextSection::TranscriptImages(TranscriptImages::collect(
                input.history.items(),
                images,
            ))
        }))
    }
}
