fn main() {
    #[cfg(target_os = "linux")]
    klaus_sandbox_linux_bwrap::helper::dispatch();
    eprintln!("This executable is the internal Linux sandbox helper.");
    std::process::exit(2);
}
