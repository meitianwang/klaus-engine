//! Full objective reads from the current goal's durable file capability.

use std::io::{self, Read};
use std::path::Path;

use sha2::{Digest, Sha256};

use super::owned_resources::{
    GoalOwnedResourceFile, GoalOwnedResources, GoalOwnedResourcesManifest, MAX_GOAL_RESOURCE_BYTES,
};
use super::service::conflict;
use super::store;
use crate::fs::node_path::runtime_path_from_native;
use crate::runtime_values::RuntimeString;

const OBJECTIVE_FILE_REFERENCE: &str =
    "Read the complete goal objective from this Klaus-owned file: ";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GoalObjectivePreview {
    pub goal_id: RuntimeString,
    pub objective: RuntimeString,
}

struct ObjectiveBinding {
    goal_id: RuntimeString,
    objective: RuntimeString,
    intent_revision: i64,
    manifest: Option<GoalOwnedResourcesManifest>,
}

/// This read never constructs a session or requests a provider. Its only address
/// is the target thread and an expected current generation; paths and source
/// ownership come exclusively from that generation's saved capability.
pub async fn read_objective(
    thread_id: &str,
    expected_goal_id: &RuntimeString,
) -> io::Result<GoalObjectivePreview> {
    let binding = capture_binding(thread_id, expected_goal_id).await?;
    let config_root = crate::fs::paths::config_home();
    let objective = read_captured(&binding, Path::new(&config_root)).await?;
    finish_read(thread_id, binding, objective).await
}

async fn capture_binding(
    thread_id: &str,
    expected_goal_id: &RuntimeString,
) -> io::Result<ObjectiveBinding> {
    let record = store::get_record(thread_id).await?;
    let goal = record
        .goal
        .filter(|goal| &goal.goal_id == expected_goal_id)
        .ok_or_else(|| conflict("the goal changed before its objective could be read"))?;
    let manifest = store::get_goal_owned_resources_for_intent(
        thread_id,
        &goal.goal_id,
        record.intent_revision,
    )
    .await?;
    Ok(ObjectiveBinding {
        goal_id: goal.goal_id,
        objective: goal.objective,
        intent_revision: record.intent_revision,
        manifest,
    })
}

async fn read_captured(
    binding: &ObjectiveBinding,
    config_root: &Path,
) -> io::Result<RuntimeString> {
    if !binding.objective.starts_with(OBJECTIVE_FILE_REFERENCE) {
        return Ok(binding.objective.clone());
    }
    let Some(manifest) = binding.manifest.as_ref() else {
        return Ok(binding.objective.clone());
    };
    let Some(expected) = manifest
        .files
        .iter()
        .find(|file| file.name == "objective.md")
    else {
        return Ok(binding.objective.clone());
    };
    let suffix = Path::new("goal-objectives")
        .join(format!(
            "{:x}",
            Sha256::digest(manifest.source_thread_id.as_bytes())
        ))
        .join(&manifest.preparation_id)
        .join(&expected.name);
    let logical_path = config_root.join(&suffix);
    let canonical_path = crate::fs::node_path::canonicalize_host(config_root)?.join(&suffix);
    let matching_reference = [&logical_path, &canonical_path].iter().any(|path| {
        path.to_str()
            .is_some_and(|path| binding.objective == format!("{OBJECTIVE_FILE_REFERENCE}{path}"))
    });
    if !matching_reference {
        return Ok(binding.objective.clone());
    }
    let resources = GoalOwnedResources::restore(manifest, config_root).await?;
    let snapshot = resources
        .snapshot_read(&runtime_path_from_native(&canonical_path))
        .await?
        .ok_or_else(|| invalid("The goal preparation does not own this objective file"))?;
    let expected = expected.clone();
    tokio::task::spawn_blocking(move || {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let file = options.open(snapshot.path())?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() != expected.size_bytes {
            return Err(invalid("The private objective snapshot changed"));
        }
        let mut bytes = Vec::with_capacity(expected.size_bytes as usize);
        file.take(MAX_GOAL_RESOURCE_BYTES + 1)
            .read_to_end(&mut bytes)?;
        verify_snapshot_bytes(&bytes, &expected)?;
        String::from_utf8(bytes)
            .map(RuntimeString::from)
            .map_err(|_| invalid("The goal objective file is not valid UTF-8"))
    })
    .await
    .map_err(|error| io::Error::other(format!("Goal objective read worker failed: {error}")))?
}

fn verify_snapshot_bytes(bytes: &[u8], expected: &GoalOwnedResourceFile) -> io::Result<()> {
    if bytes.len() as u64 > MAX_GOAL_RESOURCE_BYTES
        || bytes.len() as u64 != expected.size_bytes
        || format!("{:x}", Sha256::digest(bytes)) != expected.sha256
    {
        return Err(invalid("The private objective snapshot content changed"));
    }
    Ok(())
}

async fn finish_read(
    thread_id: &str,
    binding: ObjectiveBinding,
    objective: RuntimeString,
) -> io::Result<GoalObjectivePreview> {
    let manifest = store::get_goal_owned_resources_for_intent(
        thread_id,
        &binding.goal_id,
        binding.intent_revision,
    )
    .await?;
    let current = store::get_record(thread_id).await?;
    if current.intent_revision != binding.intent_revision
        || current.goal.as_ref().is_none_or(|goal| {
            goal.goal_id != binding.goal_id || goal.objective != binding.objective
        })
        || manifest != binding.manifest
    {
        return Err(conflict(
            "the goal changed before its objective read could be acknowledged",
        ));
    }
    Ok(GoalObjectivePreview {
        goal_id: binding.goal_id,
        objective,
    })
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
#[path = "objective_preview_tests.rs"]
mod tests;
