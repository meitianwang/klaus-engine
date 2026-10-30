# Exact runtime carrier migration

This change replaces runtime-domain `String`, `serde_json::Value`, path, and
process-environment carriers with exact JavaScript-compatible Rust types. It is
a breaking API change for callers that construct or inspect model-visible,
provider-visible, protocol, tool, hook, MCP, browser, rollout, or Eval values.

## Strings

Use `RuntimeString` for runtime-derived text:

```rust
use klaus_agent_core::runtime_values::RuntimeString;

let ordinary = RuntimeString::from("hello");
let exact = RuntimeString::from_utf16(vec![0xd800, b'x' as u16]);
assert_eq!(ordinary.len_utf16(), 5);
assert_eq!(exact.slice_utf16(..1).as_utf16(), &[0xd800]);
```

`RuntimeString` deliberately does not implement `Deref<Target = str>`,
`AsRef<str>`, `Display`, `Serialize`, or `Into<String>`. Choose the boundary
explicitly:

- `try_to_utf8()` is fallible and is appropriate only for a contract proven to
  contain Unicode scalar values.
- `to_node_utf8()` performs the Node/V8 host projection, replacing each
  unpaired surrogate exactly once. Use it only at a real byte-oriented host
  boundary such as HTTP, a file, or a child-process pipe.
- `as_utf16()`, `len_utf16()`, `slice_utf16()`, and `concat()` preserve exact
  JavaScript code units and must be used for runtime-domain operations.

Ordinary Unicode remains ergonomic through `RuntimeString::from(&str)`,
`RuntimeString::from(String)`, and comparisons with scalar `str` values. These
constructors cannot create an unpaired surrogate; use `from_utf16` only when an
exact JavaScript value requires one.

## JSON and typed payloads

Use `RuntimeJson`, `RuntimeObject`, and `RuntimeNumber` instead of
`serde_json::Value` for runtime payloads. Parse and write without a serde round
trip:

```rust
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};

let value = runtime_json::parse(&RuntimeString::from(r#"{"text":"\ud800"}"#))?;
let wire = runtime_json::stringify(&value);
assert_eq!(wire, r#"{"text":"\ud800"}"#);
# Ok::<(), Box<dyn std::error::Error>>(())
```

`runtime_json::stringify` returns `RuntimeString`, not `String` and not a
`Result`. Convert only at the terminal boundary described above.

Typed DTOs implement `RuntimeEncode` and `RuntimeDecode`. Use
`runtime_codec::{encode_value, decode_value}` for in-memory values and
`runtime_codec::{encode_text, decode_text, encode_bytes}` for exact wire
boundaries. Runtime maps use `RuntimeMap<V>` so keys stay `RuntimeString` and
values remain runtime-codec types.

Engine-authored schema, configuration, and provenance JSON may remain serde
JSON only through the validated `ScalarJson`, `ConfigJson`, or
`ProvenanceJson` boundary. These wrappers reject non-scalar strings and invalid
numbers before conversion. They are not a compatibility path for external or
model-visible payloads.

The former `js_json` compatibility module and its sentinel representation have
been removed. Callers must not reintroduce serde stringify/parse round trips or
U+FDD0 surrogate markers.

## Paths

Use `RuntimePath` for runtime-derived argv/cwd and filesystem identities. It
wraps a `RuntimeString` and intentionally cannot become `Path`, `PathBuf`,
`OsString`, or a native process atom implicitly. Use the exported Node path
operations (`path_resolve`, `path_join`, `path_relative`, `path_dirname`, and
related functions) while the value is logical. Filesystem and process modules
perform the named platform projection only at the actual host syscall.

For ordinary scalar paths, `RuntimePath::from(&str)` and
`RuntimePath::from(String)` remain available. Use `as_runtime_string()` or
`into_runtime_string()` when the exact logical identity is required.

## Environment and process launch

Replace `HashMap<String, String>` process environments with `RuntimeEnv`.
`insert`, `get`, `remove`, `overlay`, and `entries` all use exact
`RuntimeString` values. Enumeration follows JavaScript object order: array-index
keys first in ascending order, then ordinary properties in first-insertion
order. `normalize_child_env` implements the fixed Node coverage inheritance
rule.

Construct process inputs with `LogicalSpawnRequest`, `ProcessAtom`,
`RuntimePath`, and `RuntimeEnv`. Runtime-derived launches are owned by the
crate's unified `RuntimeSpawner` backend; downstream callers cannot select a
native projection, lifecycle policy, Windows verbatim mode, or raw platform
handle. This preserves one owner for admission, stdio, completion,
termination, shutdown, and reaping across foreground, background, PTY, MCP,
browser, hook, OAuth, Git, and sandbox surfaces.

`NativeOsString` and `NativeHostEnvOverlay` are opaque terminal carriers. Their
fields and projections are intentionally unavailable to downstream crates.

### Explicit process ownership

Hosts must initialize one `RuntimeProcessSupervisor` on the Tokio runtime that
owns process I/O, then create an explicit `RuntimeProcessContext` for each
engine or Eval namespace. Spawn-bearing public APIs require that context; there
is no lazy initializer, process-global production context getter, temporary
runtime, or `Default` fallback.

```rust
use klaus_agent_core::runtime_process::{
    RuntimeProcessContext, RuntimeProcessSupervisor,
};

let supervisor = RuntimeProcessSupervisor::initialize_current()?;
let process = RuntimeProcessContext::initialize(supervisor.clone())?;

// Pass `process.clone()` into AgentOptions, AgentEngineOptions, ToolContext,
// Eval runners, MCP transports, Git helpers, and other spawn-bearing owners.

process.shutdown().await?;
drop(process);
supervisor.shutdown().await?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`AgentEngineOptions` and `AgentOptions` now require `process_context` and do
not implement `Default`. `AgentEngine::new` is fallible because namespace and
browser-runtime binding are validated at construction. Call
`AgentEngine::shutdown` before shutting down its context, then shut down and
drop every context before the supervisor. Shutdown is fail-closed when a
namespace still owns a live process or an external spawn permit.

`PlaywrightBrowserRuntimeOptions::env` is now `PlaywrightEnvOverlay`, not a
generic map. Insert ordinary or exact strings with `insert`, or collect
`(String, String)` / `(RuntimeString, RuntimeString)` pairs. It preserves exact
UTF-16 values and Rust 1.93 string ordering before the target-specific
environment layer applies Unix key identity or Windows case-alias replacement.
`PlaywrightBrowserRuntime::{new,with_defaults}` both require the same explicit
process context used by `AgentEngineOptions::new`.

## Migration checklist

1. Change runtime-facing fields and function parameters to `RuntimeString`,
   `RuntimeJson`, `RuntimePath`, and `RuntimeEnv`.
2. Replace serde runtime serialization with `runtime_codec` or `runtime_json`.
3. Replace scalar length/slicing with UTF-16 operations.
4. Keep values exact until a named HTTP, filesystem, stdio, argv, cwd, or env
   terminal boundary.
5. Handle `try_to_utf8()` errors instead of adding a lossy or implicit
   conversion.
6. Launch runtime-derived processes through the unified engine surface; do not
   call `std::process::Command`, `tokio::process::Command`, `fork`, `exec`, or
   platform process APIs from a new callsite.
7. Thread the host-created `RuntimeProcessContext` through every spawn-bearing
   API and close engine, context, and supervisor ownership in that order.
