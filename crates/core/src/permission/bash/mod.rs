//! Bash-specific permission analysis (command splitting, decisions, matching).

pub mod dangerous_removal;
pub mod decision;
pub mod match_rule;
pub mod node_path;
pub mod readonly;
pub mod split;
