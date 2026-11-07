//! Filesystem utilities (indexing, path safety, fuzzy matching, …).

mod bundled_ripgrep;
pub mod edit_input_rewrite;
pub(crate) mod exclusive_lock;
pub mod file_index;
pub mod flexible_edit_match;
pub mod fuzzy;
pub mod lockfile;
pub(crate) mod node_path;
pub mod path_safety;
pub mod paths;
pub mod pdf;
pub mod plan_files;
pub mod quote_match;
pub mod ripgrep;
pub mod sensitive_paths;
pub mod text_file;
pub mod watcher;
