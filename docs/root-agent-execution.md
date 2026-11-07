# Named root agent execution

Clients use the existing named `session/start` facade. No raw Op payload or model-selected `task`/spawn call is needed.

```json
{
  "id": 2,
  "method": "session/start",
  "params": {
    "init": {
      "threadId": "host-owned-execution-thread-id",
      "cwd": "/absolute/workspace",
      "agents": [
        {
          "name": "reviewer",
          "description": "Read-only review specialist",
          "prompt": "Review the change and report actionable findings.",
          "tools": ["read", "grep", "glob"],
          "disallowedTools": ["write", "edit", "bash", "task"],
          "model": "high",
          "permissionMode": "dontAsk",
          "effort": "high",
          "maxTurns": 8
        }
      ],
      "agentDefinitionName": "reviewer"
    }
  }
}
```

The host also sends its ordinary provider/auth, model tier mapping, capability declarations and dynamic tools in this same init. The example omits those account-specific fields. Definition tool names are exact engine-internal canonical names, not provider aliases, wildcard permission rules, or UI labels. Use `model: "inherit"` or omit it when retaining the host's selected model. A definition model must be usable by the already selected provider; this selector does not switch credentials or providers.

For selected roots the response is `{ "sessionId": "...", "initialized": { "accepted": true, "agentDefinitionName": "reviewer" } }`. Hosts must verify this acknowledgment before admitting a turn; an older engine must never silently run an ordinary root. Unselected roots retain their original response shape. For a selected root, `session/start` now materializes the core before returning success, but does not send a model request. Unknown names and unsupported root definition settings return the existing `ENGINE_ERROR` (`-32000`) JSON-RPC error (`root agent selection failed: ...`); no ordinary-root fallback or successful handle is returned. A failed start releases the handle so the host can correct the definition and retry the same thread id. Wrong JSON types are rejected by the strict init codec.

Then execute the user directive through the existing named facade:

```json
{
  "id": 3,
  "method": "turn/start",
  "params": {
    "sessionId": "host-owned-execution-thread-id",
    "input": "Review the assigned change."
  }
}
```

## Resolution and precedence

- Resolve the selector by exact name from the same effective registry used for sub-agents. Session `init.agents` overrides endpoint definitions, which override plugins and built-ins. An SDK host owning the entire tool set retains the existing no-injected-builtins registry rule.
- Omitted/null selector preserves ordinary root behavior; registration alone still does not select a root.
- Append `prompt`, or its deprecated `systemPrompt` alias if `prompt` is absent, as a cacheable role part without replacing host base/developer instructions. Append `criticalSystemReminder` separately.
- Definition model and effort override the inherited session defaults. Model tiers use the existing session tier resolver; `inherit` retains the inherited model. Apply these before dependency/compactor construction. Explicit per-turn overrides and named model reconfiguration retain their existing behavior.
- Match the current child permission semantics: inherited Plan is a floor; inherited `acceptEdits`, `bypassPermissions`, and `auto` win; otherwise use the definition's permission mode. The selected mode initializes the actual session permission context, not only a prompt.
- `tools` absent inherits the available pool; `tools: []` admits no tools. `disallowedTools` subtracts after the allowlist. Filter both advertised schemas and the actual dispatch registry, including tool-to-tool invocation, automatic `tool_search` insertion, host refreshes, and lazily connected MCP tools. Root restrictions also constrain internal child queries. A denied `web_search` disables provider-native search as well as the client tool.
- `maxTurns` uses the existing per-Query agentic-round limit on each `turn/start`, not a lifetime session counter. Unselected roots remain unbounded.

## Deliberate minimal-scope limits

Root execution cannot honor spawn-only `memory`, `isolation`, `background: true`, per-definition MCP servers, or internal `permissionMode: "bubble"`. These are rejected explicitly, rather than ignored or approximated. Use a host-owned execution cwd and normal session MCP setup. `maxTurns: 0` and blank/surrounding-whitespace definition names are also rejected.

Tool isolation constrains model invocation, not network connection side effects of session-wide MCP discovery; existing MCP instruction loading and host-initiated MCP operations retain their lifecycle semantics. The host must not give a restricted specialist an unrestricted proxy tool under an allowed name.

The selector and definition are per-start configuration, not persisted root-role metadata. Resuming/restarting an execution requires the host to resend both `init.agents` and `init.agentDefinitionName`. The host owns execution/thread mapping and cancellation through the existing named facade.

## Focused validation (not yet executed)

Run only after the shared Cargo gate is available, from this engine worktree:

```sh
cargo nextest run --locked -p klaus-agent-core --all-features -E 'test(root_agent_definition)'
cargo nextest run --locked -p klaus-agent-protocol --all-features -E 'test(root_agent_definition)'
cargo nextest run --locked -p klaus-agent-server --all-features -E 'test(root_agent_definition)'
cargo nextest run --locked -p klaus-agent-server --all-features -E 'test(app_server::)'
cargo nextest run --locked -p klaus-agent-cli -E 'test(facade_coverage)'
cargo clippy --locked -p klaus-agent-core -p klaus-agent-protocol -p klaus-agent-server --all-targets --all-features -- -D warnings
```

The new tests cover real named-facade startup/turn execution, unknown-name startup rejection and handle reuse, strict selector shape, role prompt/alias preservation, model/effort/permission semantics, empty tools, round limits, direct/nested forbidden calls, refresh-time tool search reinsertion, native search, and a real in-process lazy MCP connection. No Cargo command was run during implementation, by request.
