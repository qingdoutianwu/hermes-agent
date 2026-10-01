# Stable future-source startup consumer R1

This is an implementation candidate, **not a production installation**. Distribute
`future_source_startup.py` as a separately pinned, owner-controlled stable bootstrap
outside `authority/versions/`. Do not execute the copy inside a selected source tree.
The existing fixed-root production launcher is intentionally unchanged.

Future wiring, under separate authorization, invokes the already-approved runtime:

```text
approved-python -I -B stable/future_source_startup.py
  --authority-root /approved/private/source-authority --
  /approved/sealed-scheduler/gateway_bootstrap.py gateway run --external-supervisor
```

No Python patch version is pinned here. Interpreter/dependency provenance remains
the existing runtime authority's responsibility (supported range `>=3.11,<3.15`).
Consumer preserves the interpreter, inherited environment, cwd and exec-inheritable
file descriptors. It forwards scheduler/Gateway arguments unchanged to the selected
`gateway/startup_entry.py`; it does not change business, replay or notification code.

Selector contract is exactly the JSON emitted by transaction tool commit
`4b406455ec94c9b56ad9b07539bcf8c8fad7d617`:

```json
{"source_commit":"<40 lowercase hex>","source_tree":"<40 lowercase hex>","source_manifest_sha256":"<64 lowercase hex>"}
```

There is **no selector schema_version field** in that tool contract. The adjacent
`versions/<commit>.manifest.json` has `schema_version: 1` and the same commit/tree,
plus `files` with path, Git mode, blob SHA-1, SHA-256 and size. Consumer rejects unknown
fields, duplicate keys and unsupported manifest versions. It reads the selector once,
pins its record, checks the manifest hash, inventory, all source bytes/blobs, and
reconstructs the Git tree without executing Git. Commit-to-tree authority is the
approved selector/manifest issuer; consumer does not invent or fetch Git commits.

Seals are mode 0555 for source directories/executables and 0444 for ordinary files,
selector and manifest. Same-user ownership is required. Versions and manifest must
not be symlinks. Inventory traversal, symlinked directories, escaping symlinks and
nonregular entrypoints fail closed. Cooperative source writers must retain sealed
generations; permissions are not protection against a privileged/same-owner attacker.

Verification failure exits 78 before any selected source import/exec. There is no
fallback to working tree, Git HEAD, latest directory or the prior source. No watcher,
poller, journal write, repair, install, notification, migration or process control is
part of the consumer. A malformed journal is not a selector authority; a valid selected
generation remains usable even if publication's journal needs reconciliation.

Handoff is `os.execve` with `-I -B`: old working-root/PYTHONPATH entries are not carried
forward. Approved dependency site directories from the initialized interpreter are
retained; selected source is first on sys.path. The managed Hermes editable finder
and its namespace hook are removed at handoff so missing selected-source modules
cannot fall back to its prior working tree. Third-party dependency finders remain.
Deployment must still audit any other dependency `.pth`/custom source mappings.
Production readiness is not granted by fixture tests alone.

The checked-in test-only atomic tool fixture is byte-identical to the tool commit;
it is not a second production implementation and is never loaded by the consumer.
Tests exercise actual Git materialization and atomic publication on the host, exec
PID continuity, a running A after selector switch, fresh B, admission failures and
journal reconciliation. Run with `scripts/run_tests.sh tests/bootstrap/` using an
existing interpreter with pytest; no package installation is needed or authorized.

Production LaunchAgent wiring, authority namespace provisioning and any service
start/restart require a later controlled-binding task. Do not deploy or restart from
this implementation phase.
