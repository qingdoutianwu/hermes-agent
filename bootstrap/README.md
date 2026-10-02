# Unified startup consumer and scoped defer candidate

The standalone `future_source_startup.py` is a stable control-plane artifact.
Install/seal it outside selected source versions only under separate deployment
authorization. No production launcher or selector is changed by this candidate.
Invocation uses the existing approved interpreter/dependency authority, not a
Python patch-version pin: `approved-python -I -B stable/future_source_startup.py
--authority-root /approved/private/authority -- sealed-scheduler-bootstrap.py
gateway run --external-supervisor`.

The tool selector has exactly `source_commit`, `source_tree` and
`source_manifest_sha256`; its manifest uses schema_version 1. Consumer reads the
selector once, verifies sealed inventory/blob identities, reconstructs the Git tree,
then execs the selected `gateway/startup_entry.py`. No policy, notification or defer
logic lives in the consumer. No fallback, polling, watcher, journal writes or Git
updates occur. Dependency paths/environment/cwd/exec-inheritable descriptors remain;
old managed Hermes editable-source fallback is removed from import resolution.

Selected Gateway defaults remain `startup_context: normal` and
`no_external_messages: false`. Scoped defer is explicit, through existing Core YAML:

```yaml
gateway:
  startup_context: silent_recovery
  no_external_messages: true
  startup_isolation: false
  recovery_enabled: true
  external_delivery_enabled: false
  delivery_state_write_enabled: false
```

This requires, rather than rewrites, existing delivery/state fences. Full startup
isolation still dominates. Internal recovery remains independently controlled.
Scoped context cannot relax in a live process; ordinary notification/replay rules
continue unchanged for normal startup. A later normal start needs independent
authorization and does not automatically replay or consume obligations.

Audit writes owner-controlled 0444 sidecars under the owning process home's
`runtime/gateway/silent-startup-deferrals`. Restart/update pending marker bytes and
delivery obligations are read only, never claimed, cancelled, completed or marked
sent. Each record includes obligation/event/reason/policy/time/context and zero
transport/send counters. Same-process repeat audit is idempotent. Sidecar is not a
business permit or a control-notification authorization. Existing Core transport,
pending-send and ledger-state admission remain authoritative and independent.

Source seals protect cooperative deployment, not a privileged same-owner attacker.
The current-lineage controlled one-shot permit has independent read-only validity
and transport-admission results. Silent recovery can validate an exact permit, but
cannot reserve/send it or invoke the dedicated provider directly. Normal recovery-only
controlled delivery remains one-shot and non-retryable. This gate lives in selected
runtime, never in the stable source consumer. No live permit is issued by this build.

Manifest/selector commit authority comes from the verified issuer; this consumer
does not fetch or manufacture Git commits. Incomplete audit/verification errors
fail startup closed. Production baseline/source binding, plist wiring, config
changes and process activation all require a subsequent authorized audit/deploy.
