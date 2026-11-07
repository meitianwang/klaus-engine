//! Loader-injection environment hardening for both native startup maps and the
//! exact runtime environment carrier used by process plans.

use std::collections::HashMap;

use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::RuntimeString;

const STRIP_NAMES: [&str; 1] = ["NODE_OPTIONS"];
const STRIP_PREFIXES: [&str; 2] = ["DYLD_", "LD_"];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HardenResult<K = RuntimeString> {
    pub stripped: Vec<K>,
}

pub trait HardenEnvironment {
    type Key: Clone;

    fn keys_in_order(&self) -> Vec<Self::Key>;
    fn key_equals_ascii(key: &Self::Key, value: &str) -> bool;
    fn key_starts_with_ascii(key: &Self::Key, value: &str) -> bool;
    fn remove_key(&mut self, key: &Self::Key) -> bool;
    fn ascii_key(value: &str) -> Self::Key;
}

impl HardenEnvironment for RuntimeEnv {
    type Key = RuntimeString;

    fn keys_in_order(&self) -> Vec<Self::Key> {
        self.entries().map(|(key, _)| key).collect()
    }

    fn key_equals_ascii(key: &Self::Key, value: &str) -> bool {
        key.as_utf16()
            .iter()
            .copied()
            .eq(value.as_bytes().iter().copied().map(u16::from))
    }

    fn key_starts_with_ascii(key: &Self::Key, value: &str) -> bool {
        key.starts_with(value)
    }

    fn remove_key(&mut self, key: &Self::Key) -> bool {
        self.remove(key).is_some()
    }

    fn ascii_key(value: &str) -> Self::Key {
        RuntimeString::from_utf8(value)
    }
}

impl HardenEnvironment for HashMap<String, String> {
    type Key = String;

    fn keys_in_order(&self) -> Vec<Self::Key> {
        self.keys().cloned().collect()
    }

    fn key_equals_ascii(key: &Self::Key, value: &str) -> bool {
        key == value
    }

    fn key_starts_with_ascii(key: &Self::Key, value: &str) -> bool {
        key.starts_with(value)
    }

    fn remove_key(&mut self, key: &Self::Key) -> bool {
        self.remove(key).is_some()
    }

    fn ascii_key(value: &str) -> Self::Key {
        value.to_owned()
    }
}

pub fn harden_process<E: HardenEnvironment>(env: &mut E) -> HardenResult<E::Key> {
    let mut stripped = Vec::new();
    for name in STRIP_NAMES {
        let key = E::ascii_key(name);
        if E::remove_key(env, &key) {
            stripped.push(key);
        }
    }
    for key in E::keys_in_order(env) {
        if STRIP_PREFIXES
            .iter()
            .any(|prefix| E::key_starts_with_ascii(&key, prefix))
            && E::remove_key(env, &key)
        {
            stripped.push(key);
        }
    }
    HardenResult { stripped }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_environment_removal_preserves_surviving_property_order() {
        let mut env = RuntimeEnv::new();
        env.insert(RuntimeString::from("A"), RuntimeString::from("1"));
        env.insert(
            RuntimeString::from("LD_PRELOAD"),
            RuntimeString::from("evil"),
        );
        env.insert(RuntimeString::from("B"), RuntimeString::from("2"));
        let result = harden_process(&mut env);
        assert_eq!(result.stripped, vec![RuntimeString::from("LD_PRELOAD")]);
        assert_eq!(
            env.entries().map(|(key, _)| key).collect::<Vec<_>>(),
            vec![RuntimeString::from("A"), RuntimeString::from("B")]
        );
    }

    #[test]
    fn native_startup_map_remains_supported_without_runtime_projection() {
        let mut env = HashMap::from([
            ("NODE_OPTIONS".to_owned(), "--require evil".to_owned()),
            ("PATH".to_owned(), "/bin".to_owned()),
        ]);
        let result = harden_process(&mut env);
        assert_eq!(result.stripped, vec!["NODE_OPTIONS".to_owned()]);
        assert_eq!(env.get("PATH").map(String::as_str), Some("/bin"));
    }
}
