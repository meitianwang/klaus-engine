//! Export the effective prompt-catalog resolution for a list of source models.
//!
//! The input is a JSON array of `{ "source": "...", "selection": "...",
//! "model": "...", "label": "..." }` rows. The output is JSON Lines with the
//! binding/profile provenance plus the base and commentary text digests. A
//! caller that needs the exact bytes can pass an output directory as the second
//! argument; the exporter writes one file per row under `prompts/`.

use std::fs;
use std::path::PathBuf;

use klaus_agent_core::prompt_catalog::ModelFragments;
use klaus_agent_core::provider::behavior::resolve_provider_behavior_profile;
use klaus_agent_core::runtime_values::RuntimeString;
use sha2::{Digest, Sha256};

#[derive(serde::Deserialize)]
struct Row {
    source: String,
    selection: String,
    model: String,
    #[serde(default)]
    label: String,
    #[serde(default)]
    menu: bool,
    #[serde(default)]
    note: String,
}

fn digest(text: &str) -> String {
    let bytes = Sha256::digest(text.as_bytes());
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn slot_provenance(fragments: &ModelFragments, slot: &str) -> Option<(String, String, usize)> {
    fragments
        .resolved()
        .describe()
        .into_iter()
        .find(|(id, _, _, _)| *id == slot)
        .map(|(_, layer, origin, size)| {
            (
                format!("{layer:?}"),
                origin.try_to_utf8().unwrap_or_default(),
                size,
            )
        })
}

fn safe_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let input_path = args
        .next()
        .expect("usage: prompt_catalog_matrix <input.json> [prompt-dir]");
    let prompt_dir = args.next().map(PathBuf::from);
    let input = fs::read_to_string(&input_path).expect("read matrix input");
    let rows: Vec<Row> = serde_json::from_str(&input).expect("parse matrix input");
    if let Some(dir) = &prompt_dir {
        fs::create_dir_all(dir.join("prompts")).expect("create prompt directory");
    }

    for (index, row) in rows.iter().enumerate() {
        let model = RuntimeString::from_utf8(&row.model);
        let profile = resolve_provider_behavior_profile(None, &model);
        let fragments = ModelFragments::for_route(&model, Some(&row.source), None);
        let base = fragments.base_instructions();
        let base_text = base.try_to_utf8().unwrap_or_default();
        let commentary = fragments
            .commentary()
            .and_then(|text| text.try_to_utf8().ok());
        let profile_ref = fragments
            .resolved()
            .profile_ref
            .as_ref()
            .and_then(|value| value.try_to_utf8().ok());
        let base_slot = slot_provenance(&fragments, "base.instructions");
        let commentary_slot = slot_provenance(&fragments, "response.commentary");
        let binding_kind = if profile_ref.is_some() {
            "model"
        } else if base_slot
            .as_ref()
            .is_some_and(|(layer, _, _)| layer == "FamilyProfile")
        {
            "family"
        } else {
            "generic"
        };
        let prompt_file = prompt_dir.as_ref().map(|dir| {
            let path = dir.join("prompts").join(format!(
                "{:03}-{}-{}.md",
                index + 1,
                safe_name(&row.source),
                safe_name(&row.selection)
            ));
            fs::write(&path, &base_text).expect("write prompt");
            path
        });
        let output = serde_json::json!({
            "source": row.source,
            "selection": row.selection,
            "model": row.model,
            "label": row.label,
            "menu": row.menu,
            "note": row.note,
            "family": profile.family,
            "profileRef": profile_ref,
            "bindingKind": binding_kind,
            "baseLayer": base_slot.as_ref().map(|(layer, _, _)| layer.clone()),
            "baseOrigin": base_slot.as_ref().map(|(_, origin, _)| origin.clone()),
            "baseUtf16": base_text.encode_utf16().count(),
            "baseSha256": digest(&base_text),
            "commentaryUtf16": commentary.as_ref().map(|text| text.encode_utf16().count()),
            "commentarySha256": commentary.as_deref().map(digest),
            "commentaryLayer": commentary_slot.as_ref().map(|(layer, _, _)| layer.clone()),
            "commentaryOrigin": commentary_slot.as_ref().map(|(_, origin, _)| origin.clone()),
            "promptFile": prompt_file.as_ref().map(|path| path.display().to_string()),
            "catalogRevision": fragments.resolved().catalog_revision.try_to_utf8().unwrap_or_default(),
        });
        println!("{output}");
    }
}
