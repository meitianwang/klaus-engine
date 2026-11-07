fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=KLAUS_ENGINE_BUILD_COMMIT");

    // Embed the commit this engine binary was built from so a shipped binary
    // can name its own source. The manifest packaged beside the binary claims
    // an `engineCommit`, and the release script cross-checks this embedded
    // value against that claim; without it a binary built from any tree can be
    // labeled with any commit, and a stale build ships under a newer identity
    // with nothing on disk to contradict it.
    //
    // The release script sets the variable from the commit it verified before
    // invoking cargo, which makes the embedded value exact on that path. Any
    // other build (dev launches, ad-hoc cargo) falls back to asking git, and a
    // source tree without git metadata embeds `unknown` — which the release
    // cross-check refuses rather than shipping an unidentified binary.
    let commit = std::env::var("KLAUS_ENGINE_BUILD_COMMIT")
        .ok()
        .or_else(|| {
            std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|stdout| stdout.trim().to_string())
        })
        .filter(|commit| commit.len() == 40 && commit.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=KLAUS_ENGINE_BUILD_COMMIT={commit}");

    // The stdio server initializes the full engine on the main thread. Reserve
    // enough stack for that initialization on Windows without affecting other
    // binaries or non-Windows targets.
    #[cfg(all(windows, target_env = "msvc"))]
    println!("cargo:rustc-link-arg-bin=klaus-agent-stdio=/STACK:8388608");
}
