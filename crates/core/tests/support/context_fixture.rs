//! Private temporary directories for context runtime regressions.

pub(super) struct TemporaryDirectory(std::path::PathBuf);

impl TemporaryDirectory {
    pub(super) fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) fn tempdir() -> std::io::Result<TemporaryDirectory> {
    let path = std::env::temp_dir().join(format!("context-runtime-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path)?;
    Ok(TemporaryDirectory(path))
}
