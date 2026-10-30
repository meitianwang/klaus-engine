# Stored history maintenance

Both automatic jobs are opt-in endpoint features in the user config file:

```toml
[features]
cold_rollout_compression = false
background_rollout_migration = false
```

The endpoint reads these features in the background when its facade starts. Configuration and maintenance I/O do not delay initialization. Project configuration cannot enable endpoint-wide maintenance. A manual request remains available when its automatic feature is off.

## Cold compression

`engine/compressRollouts` accepts `{}` and immediately returns `{}` after scheduling the shared compression worker. Repeated kicks while a pass is pending or running are coalesced. The request does not force compression: the store still enforces the seven-day modification-time threshold and excludes active or changed files.

Completion reports and underlying errors are written to the engine log with the `[rollout-maintenance]` prefix. The report includes counters, byte totals, busy state, and per-path failures. The empty RPC receipt acknowledges the kick; it does not report filesystem completion.

## Migration preview and apply

The CLI opens an endpoint without creating a model session:

```sh
klaus --json engine migrate-rollouts > preview.json
klaus --json engine migrate-rollouts --apply
```

The default is a read-only preview. Only `--apply` changes stored files. To pin an application to the source revisions from a preview:

```sh
jq '[.files[] | select(.sourceRevision != null) | {key: .path, value: .sourceRevision}] | from_entries' preview.json > revisions.json
klaus --json engine migrate-rollouts --apply --expected-revisions revisions.json
```

The corresponding named method is `engine/migrateRollouts`. Its parameters are `mode` (`dryRun` by default, or `apply`) and optional `expectedSourceRevisions`, an object mapping source paths to their preview revision objects. It returns the store's complete `{busy, files}` report without flattening paths or dropping per-file source revisions, conflicts, active-file skips, cancellation, or failures. A caller must inspect each file's status before treating the operation as applied.

The automatic migration feature performs an apply pass; it remains disabled by default. When both automatic features are enabled, startup waits for the migration pass before kicking compression, so neither pass is lost to a busy gate. Compression and migration share endpoint admission and the store's cross-process maintenance leases. Shutdown signals cooperative cancellation and retains the worker until its temporary files, writer leases, and atomic publication have safely settled. Dropping an RPC waiter does not abandon a migration transaction.
