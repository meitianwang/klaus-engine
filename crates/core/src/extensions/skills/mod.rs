//! Skills: `SKILL.md` discovery/loading, body expansion, and conditional
//! (path-gated) activation, plus the built-in skills, the model `skill` tool,
//! the skill-listing reminder, and remote skill-hub integration.

pub mod builtin_skills;
pub mod catalog;
pub mod conditional_skills;
pub mod expand_skill;
pub mod loader;
pub mod remote;
pub mod skill_listing_reminder;
pub mod skill_tool;
