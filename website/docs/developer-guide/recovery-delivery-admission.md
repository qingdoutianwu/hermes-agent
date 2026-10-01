# Core Recovery / Delivery admission contract

All keys are strict YAML booleans under `gateway`, resolved in Core before
sealed scheduler imports or recovery. Defaults preserve normal-mode code paths.

```yaml
gateway:
  startup_isolation: false
  recovery_enabled: true
  external_delivery_enabled: false
  delivery_state_write_enabled: false
```

`startup_isolation=true` still dominates every domain: full recovery, replay,
send, ledger and business-dispatch isolation is unchanged.

Recovery-only admits existing SessionStore interrupted-turn recovery and the
existing internal startup replay queue. It does **not** create external adapters,
connect transports, run plugin hooks/process commands, adopt pending outbound
messages, start notifications or launch a business Cron ticker. Scheduler bootstrap
and metadata imports remain available. Session marker promotion is Core recovery
state, not delivery-ledger advancement. Pending internal events with no available
internal intake remain queued; external events are not deleted. Recovery-only
does not promise every offline chat can execute a full agent turn without a
live intake: it records actual recovery and leaves unprocessable work pending.

Existing transport decorators and delivery-ledger `_connect()` share the Core
admission policy. Delivery OFF requires state writes OFF; inconsistent or
non-boolean config fails closed. No recovery is admitted until all required
delivery fences register and verify. Enabled delivery/state fences cannot relax
inside the same process. Notification and pending-send admission derive from
delivery admission; existing notification and ledger opt-outs remain usable in
normal mode, but cannot override hard denial.

`state/startup-isolation.json` retains the previous full-isolation fields and adds
requested/effective recovery, requested/effective external delivery, outbound
replay, state-write allowance, and actual recovery/replay method observations.
The normal-mode path is retained and tested with fake transports only.

This is a candidate contract, not production deployment authorization. No Skill,
AUTH3/V3, launcher logic or r1d bootstrap changes are needed. Non-positive restore
timeouts still mean infinite wait, zero freshness remains always fresh, and clean
shutdown evidence is not an isolation policy. Future production apply and
controlled de-isolation require separate authorization.
