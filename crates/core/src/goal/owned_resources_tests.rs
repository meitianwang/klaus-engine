use super::*;
use crate::fs::node_path::runtime_path_from_native;

struct Preparation {
    config_root: PathBuf,
    thread_id: String,
    directory: PathBuf,
    input: GoalOwnedResourcesInput,
}

impl Preparation {
    fn new() -> Self {
        let config_root =
            std::env::temp_dir().join(format!("klaus-owned-goal-test-{}", uuid::Uuid::new_v4()));
        let thread_id = "owned-goal-thread".to_owned();
        let preparation_id = uuid::Uuid::new_v4().to_string();
        let directory = config_root
            .join("goal-objectives")
            .join(format!("{:x}", Sha256::digest(thread_id.as_bytes())))
            .join(&preparation_id);
        std::fs::create_dir_all(&directory).unwrap();
        Self {
            config_root,
            thread_id,
            directory,
            input: GoalOwnedResourcesInput {
                preparation_id,
                files: Vec::new(),
            },
        }
    }

    fn add(&mut self, name: &str, bytes: &[u8]) {
        std::fs::write(self.directory.join(name), bytes).unwrap();
        self.input.files.push(GoalOwnedResourceFile {
            name: name.to_owned(),
            size_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        });
    }

    async fn restore(&self) -> GoalOwnedResources {
        let manifest = validate(&self.thread_id, &self.input, &self.config_root)
            .await
            .unwrap();
        GoalOwnedResources::restore(&manifest, &self.config_root)
            .await
            .unwrap()
    }

    fn path(&self, name: &str) -> RuntimePath {
        runtime_path_from_native(&self.directory.join(name))
    }
}

impl Drop for Preparation {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.config_root);
    }
}

#[tokio::test]
async fn objective_paste_and_binary_image_survive_restore_as_exact_files() {
    let mut preparation = Preparation::new();
    preparation.add(
        "objective.md",
        "A complete objective: 世界\nSecond line".as_bytes(),
    );
    preparation.add("paste-1.txt", b"pasted evidence");
    preparation.add("image-2.png", &[137, 80, 78, 71, 0, 255]);
    let resources = preparation.restore().await;
    assert_eq!(resources.manifest.source_thread_id, preparation.thread_id);
    assert_eq!(
        resources
            .manifest
            .files
            .iter()
            .map(|file| file.name.as_str())
            .collect::<Vec<_>>(),
        ["image-2.png", "objective.md", "paste-1.txt"]
    );
    for file in &preparation.input.files {
        let path = preparation.path(&file.name);
        assert!(resources.allows_read(&path).await);
        let snapshot = resources.snapshot_read(&path).await.unwrap().unwrap();
        assert_eq!(
            snapshot.path().extension(),
            preparation.directory.join(&file.name).extension()
        );
        assert_eq!(
            std::fs::read(snapshot.path()).unwrap(),
            std::fs::read(preparation.directory.join(&file.name)).unwrap()
        );
        let snapshot_path = snapshot.path().to_owned();
        drop(snapshot);
        assert!(!snapshot_path.exists());
    }
    std::fs::write(preparation.directory.join("paste-2.txt"), b"unlisted").unwrap();
    assert!(
        !resources
            .allows_read(&preparation.path("paste-2.txt"))
            .await
    );
    assert!(resources
        .snapshot_read(&preparation.path("paste-2.txt"))
        .await
        .unwrap()
        .is_none());
    assert!(
        !resources
            .allows_read(&runtime_path_from_native(&preparation.config_root))
            .await
    );
}

#[tokio::test]
async fn validation_does_not_accept_another_threads_preparation() {
    let mut preparation = Preparation::new();
    preparation.add("objective.md", b"only this thread");
    assert!(validate(
        "different-thread",
        &preparation.input,
        &preparation.config_root
    )
    .await
    .is_err());
    let resources = preparation.restore().await;
    let captured = resources.manifest().clone();
    assert!(
        GoalOwnedResources::restore(&captured, &preparation.config_root)
            .await
            .is_ok()
    );
    // A fork restores this captured source owner, without deriving authority
    // from a target thread's text or requiring a source runtime to remain live.
    assert_eq!(captured.source_thread_id, preparation.thread_id);
    let mut forged = captured;
    forged.source_thread_id = "different-thread".into();
    assert!(
        GoalOwnedResources::restore(&forged, &preparation.config_root)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rejects_traversal_extra_names_noncanonical_ids_and_duplicate_files() {
    let mut preparation = Preparation::new();
    preparation.add("objective.md", b"objective");
    for name in [
        "../objective.md",
        "/objective.md",
        "nested/objective.md",
        "objective.md/child",
        "Objective.md",
        "paste-0.txt",
        "paste-01.txt",
        "paste--1.txt",
        "paste-1.json",
        "image-1.PNG",
        "image-1.png.exe",
        "image-0.svg",
        "image-1/2.png",
        "image-9007199254740992.png",
        "objective.md\0",
    ] {
        let mut input = preparation.input.clone();
        input.files[0].name = name.into();
        assert!(
            validate(&preparation.thread_id, &input, &preparation.config_root)
                .await
                .is_err(),
            "{name:?}"
        );
    }
    for id in [
        "../preparation",
        "",
        "00000000000000000000000000000000",
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
    ] {
        let mut input = preparation.input.clone();
        input.preparation_id = id.into();
        assert!(
            validate(&preparation.thread_id, &input, &preparation.config_root)
                .await
                .is_err()
        );
    }
    let mut duplicate = preparation.input.clone();
    duplicate.files.push(duplicate.files[0].clone());
    assert!(
        validate(&preparation.thread_id, &duplicate, &preparation.config_root)
            .await
            .is_err()
    );
}

#[test]
fn image_resource_names_accept_canonical_short_extensions_without_a_format_list() {
    for name in [
        "image-1.jpeg",
        "image-2.bmp",
        "image-3.tif",
        "image-4.tiff",
        "image-5.heic",
        "image-6.raw",
        "image-7.x",
        "image-8.0",
        "image-9007199254740991.a1234567",
    ] {
        assert!(
            valid_name(name),
            "valid owned filename was rejected: {name}"
        );
    }
    for name in [
        "image-1.",
        "image-1.abcdefghi",
        "image-1..png",
        "image-1.png.exe",
        "image-1.p/ng",
        "image-1.p\\ng",
        "image-1.é",
        "image-1.ＰＮＧ",
        "image-1.PNG",
        "image-1.png\0",
        "image-1.png ",
        "image-1.png-",
        "image-0.png",
        "image-01.png",
        "image-9007199254740992.png",
        "image--1.png",
        "image-1/2.png",
        "../image-1.png",
        ".image-1.png",
        "preparation.json",
        "admission.json",
    ] {
        assert!(
            !valid_name(name),
            "unsafe or noncanonical filename was accepted: {name}"
        );
    }
}

#[tokio::test]
async fn preserved_image_extensions_keep_exact_snapshot_bytes_without_decoding() {
    let mut preparation = Preparation::new();
    let bytes = [0, 255, 137, 80, 78, 71, 13, 10, 0, 1, 2, 3];
    let extensions = [
        "jpeg", "bmp", "tif", "tiff", "heic", "raw", "x", "0", "a1234567",
    ];
    for (index, extension) in extensions.iter().enumerate() {
        preparation.add(&format!("image-{}.{}", index + 1, extension), &bytes);
    }
    let resources = preparation.restore().await;
    for (index, extension) in extensions.iter().enumerate() {
        let name = format!("image-{}.{}", index + 1, extension);
        assert!(resources.allows_read(&preparation.path(&name)).await);
        let snapshot = resources
            .snapshot_read(&preparation.path(&name))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            snapshot.path().extension().and_then(|value| value.to_str()),
            Some(*extension)
        );
        assert_eq!(std::fs::read(snapshot.path()).unwrap(), bytes);
        assert_eq!(
            std::fs::read(preparation.directory.join(&name)).unwrap(),
            bytes
        );
    }
}

#[tokio::test]
async fn missing_changed_and_nonregular_files_never_restore_or_receive_capability() {
    let mut preparation = Preparation::new();
    preparation.add("objective.md", b"original");
    let resources = preparation.restore().await;
    let path = preparation.path("objective.md");
    std::fs::write(preparation.directory.join("objective.md"), b"tampered").unwrap();
    assert!(!resources.allows_read(&path).await);
    assert!(resources.snapshot_read(&path).await.is_err());
    assert!(
        GoalOwnedResources::restore(resources.manifest(), &preparation.config_root)
            .await
            .is_err()
    );
    std::fs::remove_file(preparation.directory.join("objective.md")).unwrap();
    assert!(!resources.allows_read(&path).await);
    assert!(resources.snapshot_read(&path).await.is_err());
    std::fs::create_dir(preparation.directory.join("objective.md")).unwrap();
    assert!(validate(
        &preparation.thread_id,
        &preparation.input,
        &preparation.config_root
    )
    .await
    .is_err());
}

#[tokio::test]
async fn a_snapshot_keeps_verified_bytes_when_the_original_changes_after_admission() {
    let mut preparation = Preparation::new();
    preparation.add("objective.md", b"original");
    let resources = preparation.restore().await;
    let snapshot = resources
        .snapshot_read(&preparation.path("objective.md"))
        .await
        .unwrap()
        .unwrap();
    std::fs::write(preparation.directory.join("objective.md"), b"tampered").unwrap();
    assert_eq!(std::fs::read(snapshot.path()).unwrap(), b"original");
    assert!(
        !resources
            .allows_read(&preparation.path("objective.md"))
            .await
    );
}

#[tokio::test]
async fn manifest_size_digest_and_file_count_are_bounded_without_truncation() {
    let mut preparation = Preparation::new();
    preparation.add("objective.md", b"objective");
    let mut input = preparation.input.clone();
    input.files[0].size_bytes = MAX_GOAL_RESOURCE_BYTES + 1;
    assert!(
        validate(&preparation.thread_id, &input, &preparation.config_root)
            .await
            .is_err()
    );
    for digest in [
        "a".repeat(63),
        "g".repeat(64),
        "A".repeat(64),
        "0".repeat(64),
    ] {
        let mut input = preparation.input.clone();
        input.files[0].sha256 = digest;
        assert!(
            validate(&preparation.thread_id, &input, &preparation.config_root)
                .await
                .is_err()
        );
    }
    let mut input = preparation.input.clone();
    input.files.clear();
    assert!(
        validate(&preparation.thread_id, &input, &preparation.config_root)
            .await
            .is_err()
    );
    for index in 1..=MAX_GOAL_RESOURCE_FILES {
        preparation.add(&format!("paste-{index}.txt"), b"");
    }
    assert_eq!(preparation.input.files.len(), MAX_GOAL_RESOURCE_FILES + 1);
    assert!(validate(
        &preparation.thread_id,
        &preparation.input,
        &preparation.config_root
    )
    .await
    .is_err());
    preparation.input.files.pop();
    assert!(validate(
        &preparation.thread_id,
        &preparation.input,
        &preparation.config_root
    )
    .await
    .is_ok());
    let mut at_limit = Preparation::new();
    at_limit.add(
        "objective.md",
        &vec![b'x'; MAX_GOAL_RESOURCE_BYTES as usize],
    );
    assert!(
        validate(&at_limit.thread_id, &at_limit.input, &at_limit.config_root)
            .await
            .is_ok()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_leaf_symlinks_hardlinks_and_replaced_preparation_directories() {
    use std::os::unix::fs::symlink;
    let mut preparation = Preparation::new();
    preparation.add("objective.md", b"objective");
    let resources = preparation.restore().await;
    let file = preparation.directory.join("objective.md");
    let outside = preparation.config_root.join("outside.txt");
    std::fs::write(&outside, b"objective").unwrap();
    std::fs::remove_file(&file).unwrap();
    symlink(&outside, &file).unwrap();
    assert!(
        !resources
            .allows_read(&preparation.path("objective.md"))
            .await
    );
    assert!(validate(
        &preparation.thread_id,
        &preparation.input,
        &preparation.config_root
    )
    .await
    .is_err());
    std::fs::remove_file(&file).unwrap();
    std::fs::hard_link(&outside, &file).unwrap();
    assert!(validate(
        &preparation.thread_id,
        &preparation.input,
        &preparation.config_root
    )
    .await
    .is_err());
    std::fs::remove_file(&file).unwrap();
    std::fs::write(&file, b"objective").unwrap();
    let moved = preparation.directory.with_extension("moved");
    std::fs::rename(&preparation.directory, &moved).unwrap();
    symlink(&moved, &preparation.directory).unwrap();
    assert!(
        !resources
            .allows_read(&preparation.path("objective.md"))
            .await
    );
    assert!(resources
        .snapshot_read(&preparation.path("objective.md"))
        .await
        .is_err());
    assert!(validate(
        &preparation.thread_id,
        &preparation.input,
        &preparation.config_root
    )
    .await
    .is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_symlinks_in_owned_directory_ancestors_and_alias_file_operands() {
    use std::os::unix::fs::symlink;
    let mut preparation = Preparation::new();
    preparation.add("objective.md", b"objective");
    let resources = preparation.restore().await;
    let alias = preparation.config_root.join("alias.md");
    symlink(preparation.directory.join("objective.md"), &alias).unwrap();
    assert!(
        !resources
            .allows_read(&runtime_path_from_native(&alias))
            .await
    );
    let original = preparation.config_root.join("goal-objectives");
    let moved = preparation.config_root.join("moved");
    std::fs::rename(&original, &moved).unwrap();
    symlink(&moved, &original).unwrap();
    assert!(
        !resources
            .allows_read(&preparation.path("objective.md"))
            .await
    );
    assert!(validate(
        &preparation.thread_id,
        &preparation.input,
        &preparation.config_root
    )
    .await
    .is_err());
}
