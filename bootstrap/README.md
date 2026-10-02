# Stable real-CLI runtime adapter (inactive implementation)

`hermes` is a standalone stdlib consumer artifact, separately sealed outside
selected source versions. It is not a production installation or an edit to any
existing generated Hermes wrapper. Future child-command shape:

```text
stderr_timestamp --error-log existing-log --
  approved-python -I -B stable/hermes
  --authority-root approved-private-authority
  --runtime-python approved-generation/bin/python --
  gateway run --external-supervisor
```

The basename `hermes` and supervised CLI tail retain the established Gateway
identity matcher, without changing logging-wrapper logic. Real logs/signals/stdio
remain owned by unchanged stderr_timestamp. Consumer execs the real selected
`hermes_cli.main` module under the explicit approved interpreter; its physical
executable must equal the consumer's already approved runtime. Interpreter range
is the source project's supported `>=3.11,<3.15`, not a patch-version pin. Runtime
generation provenance and package approval remain deployment authority duties.

Selector schema is the existing exact three keys source_commit/source_tree/
source_manifest_sha256; adjacent manifest schema_version=1. The read-once pinned
record drives sealed full inventory/blob/tree/root verification. No fallback,
source/dependency maintenance, watcher, polling, selector write, notification,
permit or recovery code executes in the consumer. Selected CLI retains its own
existing initialization behavior; approved packages/generation must already be
available. Do not use a selected-source working tree to load the stable consumer.

Exec retains the child PID and inherited fd0/1/2 and any explicitly inheritable
descriptors, cwd and environment. Python -I/B prevents ambient source import and
bytecode writes; managed old Hermes editable-source finder is excluded before
selected CLI imports. No additional long-lived parent or stderr buffering exists.

Layered exit contract: selected Gateway 78 remains 78 at the exec child boundary;
unchanged logger recognizes the Hermes command, observes 78 and returns 0 to
launchd. Consumer does not normalize child exits. Source/CLI preflight failures
return POSIX sysexits EX_DATAERR=65; exec OS failures return EX_OSERR=71. Neither
impersonates Gateway's EX_CONFIG=78. These nonzero preflight failures may still
cause the existing supervisor's retry policy; consumer itself never retries or
requests restart. A later production binding must explicitly account for that
supervisor policy rather than claim invalid input cannot cause any launchd retry.

Signals TERM/INT/HUP/USR1/USR2 are forwarded by existing logger to the exec-replaced
child. Tests signal only inert isolated fixtures and reap them. This proves the
logger-child boundary, not an untested osascript or launchd process-group change.
Production startup/config/selector/source binding and any runtime transition
remain separately authorized future work.
