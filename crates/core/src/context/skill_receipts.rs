//! Exact instructions observed when a skill was invoked.

use crate::runtime_values::RuntimeString;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvokedSkillContent {
    pub path: RuntimeString,
    pub content: RuntimeString,
}
