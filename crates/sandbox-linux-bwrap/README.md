# Linux native sandbox helper

The engine calls `klaus_agent_core::sandbox::linux::dispatch()` before starting
its runtime. Registered engine executables can re-enter their own native helper
modes.

Applications embedding the Rust SDK can either call that dispatch function before
starting threads or ship `klaus-sandbox-linux` beside their executable. The helper
is built from this package and contains the namespace launcher, filesystem setup,
syscall filter, descriptor authentication, and scoped proxy bridge. Resolution
uses the adjacent executable path; it does not search PATH or read backend settings.
A missing helper produces a sandbox initialization error for confined modes.

The workspace integration fixture uses the package's published helper executable,
which also makes the native helper available beside debug integration-test binaries.
