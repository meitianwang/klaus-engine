use crate::engine::code_mode::protocol::{CodeModeToolKind, OutputContentItem};

use super::super::isolate;
use super::super::session_runtime::CreateCellRequest as CellRequest;
use super::super::session_runtime::ImageDetail as CellImageDetail;
use super::super::session_runtime::OutputItem as CellOutputItem;
use super::super::session_runtime::ToolKind as CellToolKind;

pub(super) fn runtime_request(request: CellRequest) -> isolate::ExecuteRequest {
    isolate::ExecuteRequest {
        tool_call_id: request.tool_call_id,
        enabled_tools: request
            .enabled_tools
            .into_iter()
            .map(
                |definition| crate::engine::code_mode::protocol::NestedToolMetadata {
                    tool_name: definition.tool_name,
                    global_name: definition.name,
                    description: definition.description,
                    kind: match definition.kind {
                        CellToolKind::Function => CodeModeToolKind::Function,
                        CellToolKind::Freeform => CodeModeToolKind::Freeform,
                    },
                },
            )
            .collect(),
        source: request.source,
    }
}

pub(super) fn cell_tool_kind(kind: CodeModeToolKind) -> CellToolKind {
    match kind {
        CodeModeToolKind::Function => CellToolKind::Function,
        CodeModeToolKind::Freeform => CellToolKind::Freeform,
    }
}

pub(super) fn output_item(item: OutputContentItem) -> CellOutputItem {
    match item {
        OutputContentItem::InputText { text } => CellOutputItem::Text { text },
        OutputContentItem::InputImage { image_url, detail } => CellOutputItem::Image {
            image_url,
            detail: detail.map(|detail| match detail {
                crate::engine::code_mode::protocol::ImageDetail::Auto => CellImageDetail::Auto,
                crate::engine::code_mode::protocol::ImageDetail::Low => CellImageDetail::Low,
                crate::engine::code_mode::protocol::ImageDetail::High => CellImageDetail::High,
                crate::engine::code_mode::protocol::ImageDetail::Original => {
                    CellImageDetail::Original
                }
            }),
        },
    }
}
