//! Engine-owned external history publication. No imported record asserts a
//! Klaus tool execution, billing request, or completed inference turn.

use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::schemas::message::Message;
use crate::schemas::rollout::{EventItem, RolloutItem, SessionMeta};

pub const IMPORT_EVENT: &str = "external_session_import";
pub const MESSAGE_EVENT: &str = "external_message";
pub const COMMIT_EVENT: &str = "external_session_import_complete";

#[derive(Debug, Clone)]
pub struct ExternalImportMessage {
    pub message: Message,
    /// Message source identity and time, never inference lifecycle metadata.
    pub origin: RuntimeJson,
}

#[derive(Debug, Clone)]
pub struct ExternalSessionImport {
    /// A facade may already have this ID registered as a lazy/live session.
    /// It can still verify an old import, but cannot create history beneath it.
    pub allow_create: bool,
    pub meta: SessionMeta,
    /// Canonical request digest and external file provenance, independent of
    /// the replaceable host metadata attachment.
    pub provenance: RuntimeJson,
    pub messages: Vec<ExternalImportMessage>,
    pub receipt: RuntimeJson,
}

#[derive(Debug, Clone, Copy)]
pub struct ExternalSessionImportResult {
    pub already_imported: bool,
    pub message_count: usize,
}

fn event(name: &str, payload: RuntimeJson) -> RolloutItem {
    RolloutItem::Event(EventItem {
        event: RuntimeString::from(name),
        turn_id: None,
        payload: Some(payload),
    })
}

impl ExternalSessionImport {
    /// The original import prefix is immutable even after later turns append,
    /// archive moves it, or the user changes the title/host attachment.
    pub(crate) fn prefix(&self) -> Vec<RolloutItem> {
        let mut items = Vec::with_capacity(self.messages.len() * 2 + 2);
        items.push(event(IMPORT_EVENT, self.provenance.clone()));
        for imported in &self.messages {
            items.push(event(MESSAGE_EVENT, imported.origin.clone()));
            items.push(RolloutItem::Message {
                message: imported.message.clone(),
            });
        }
        items.push(event(COMMIT_EVENT, self.receipt.clone()));
        items
    }
}
