use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

use super::SandboxWorkspaceWrite;
use crate::runtime_values::RuntimeString;

impl SandboxWorkspaceWrite {
    /// Resolve writable roots against an explicit configuration directory.
    /// Without a base, every input must already be native absolute.
    pub fn normalize_writable_roots(&mut self, base_dir: Option<&Path>) -> Result<(), String> {
        self.writable_roots = normalize_roots(&self.writable_roots, base_dir)?;
        Ok(())
    }
}

/// Normalize only the existing workspace root list, retaining the raw table's shape.
pub fn normalize_sandbox_workspace_roots(
    values: &mut Map<String, Value>,
    base_dir: Option<&Path>,
) -> Result<(), String> {
    let Some(roots) = values
        .get_mut("sandbox_workspace_write")
        .and_then(Value::as_object_mut)
        .and_then(|workspace| workspace.get_mut("writable_roots"))
    else {
        return Ok(());
    };
    let strings: Vec<String> = serde_json::from_value(roots.clone())
        .map_err(|error| format!("sandbox_workspace_write.writable_roots: {error}"))?;
    let roots_to_normalize = strings
        .into_iter()
        .map(RuntimeString::from)
        .collect::<Vec<_>>();
    let normalized = normalize_roots(&roots_to_normalize, base_dir)?;
    *roots = Value::Array(
        normalized
            .iter()
            .map(|root| Value::String(root.try_to_utf8().expect("normalized root is scalar text")))
            .collect(),
    );
    Ok(())
}

pub(super) fn deserialize_writable_roots<'de, D>(
    deserializer: D,
) -> Result<Vec<RuntimeString>, D::Error>
where
    D: Deserializer<'de>,
{
    let roots = Vec::<String>::deserialize(deserializer)?
        .into_iter()
        .map(RuntimeString::from)
        .collect::<Vec<_>>();
    normalize_roots(&roots, None).map_err(serde::de::Error::custom)
}

fn normalize_roots(
    roots: &[RuntimeString],
    base_dir: Option<&Path>,
) -> Result<Vec<RuntimeString>, String> {
    roots
        .iter()
        .enumerate()
        .map(|(index, root)| {
            let invalid =
                |message| format!("sandbox_workspace_write.writable_roots[{index}]: {message}");
            let text = root
                .try_to_utf8()
                .map_err(|error| invalid(error.to_string()))?;
            let input = Path::new(&text);
            if !input.is_absolute() && base_dir.is_none() {
                return Err(invalid(
                    "relative paths require an explicit configuration base directory".into(),
                ));
            }
            if !input.is_absolute() && base_dir.is_some_and(|base| !base.is_absolute()) {
                return Err(invalid(
                    "the configuration base directory must be absolute".into(),
                ));
            }
            let expanded = expand_home(input);
            let expanded = normalize_namespace(&expanded);
            let combined = match base_dir {
                Some(base) => combine_with_base(&expanded, &normalize_namespace(base)),
                None => expanded,
            };
            let normalized = lexical_normalize(&combined);
            let text = normalized
                .to_str()
                .ok_or_else(|| invalid("the resolved path is not scalar text".into()))?;
            Ok(RuntimeString::from(text))
        })
        .collect()
}

fn expand_home(path: &Path) -> PathBuf {
    if let Some(rest) = path.to_str().and_then(|path| path.strip_prefix('~')) {
        #[allow(deprecated)]
        if let Some(home) = std::env::home_dir() {
            if rest.is_empty() {
                return home;
            }
            if let Some(rest) = rest.strip_prefix('/') {
                return home.join(rest.trim_start_matches('/'));
            }
            #[cfg(windows)]
            if let Some(rest) = rest.strip_prefix('\\') {
                return home.join(rest.trim_start_matches('\\'));
            }
        }
    }
    path.to_path_buf()
}

fn normalize_namespace(path: &Path) -> PathBuf {
    #[cfg(windows)]
    if let Some(text) = path.to_str() {
        for prefix in [r"\\?\UNC\", r"\\.\UNC\"] {
            if let Some(rest) = text.strip_prefix(prefix) {
                return PathBuf::from(format!(r"\\{rest}"));
            }
        }
        for prefix in [r"\\?\", r"\\.\"] {
            if let Some(rest) = text.strip_prefix(prefix) {
                let bytes = rest.as_bytes();
                if bytes.len() >= 3
                    && bytes[0].is_ascii_alphabetic()
                    && bytes[1] == b':'
                    && matches!(bytes[2], b'/' | b'\\')
                {
                    return PathBuf::from(rest);
                }
            }
        }
    }
    path.to_path_buf()
}

fn combine_with_base(path: &Path, base: &Path) -> PathBuf {
    #[cfg(windows)]
    if !path.has_root() {
        let mut parts = path.components();
        if let Some(Component::Prefix(prefix)) = parts.next() {
            let mut combined = PathBuf::from(prefix.as_os_str());
            if parts.clone().next().is_none() {
                combined.push(std::path::MAIN_SEPARATOR_STR);
            } else {
                for component in base
                    .components()
                    .filter(|component| !matches!(component, Component::Prefix(_)))
                    .chain(parts)
                {
                    combined.push(component.as_os_str());
                }
            }
            return combined;
        }
    }
    base.join(path)
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}
