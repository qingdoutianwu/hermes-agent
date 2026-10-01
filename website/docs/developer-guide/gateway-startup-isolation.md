# Gateway startup isolation

`gateway.startup_isolation` is a strict YAML boolean, defaulting to `false`.
When true, Core resolves and installs admission fences before the sealed scheduler
bootstrap imports its modules. Use the Core entry
`gateway/startup_entry.py <sealed-bootstrap.py> gateway run --external-supervisor`
with the normal managed dependency-generation launcher; the launcher remains
responsible only for dependency activation, not policy implementation.

Isolation lasts until that process exits. It does not automatically expire after
startup. Return to normal mode with a separately authorized restart configured
with `false`; changing configuration cannot unfence a running isolated process.

## Allowed state

Core platform schema, checkpoint, health, process bookkeeping and runtime
receipts may initialize. Isolation is not a read-only database mode.

## Denied admission

Core skips previous-run process/session recovery, startup message replay,
pending-work dispatch, pending-send adoption, notification initialization,
messaging adapter creation and background scheduler/housekeeping dispatch.
Existing persisted pending state is retained, not deleted or acknowledged.

Transport admission guards deny external sends before credentials, network
calls or media preprocessing. Base adapter inherited methods and subclass
overrides, standalone transport helpers and the send-message entry share this
Core policy. Delivery-ledger access is denied before opening its writable
connection. Existing `gateway.delivery_ledger` behavior remains unchanged in
normal mode; isolation dominates the partial opt-out.

## Evidence and failure

`state/startup-isolation.json` records requested/effective state, registered
admission points and explicit allowances. The initial receipt is ineffective
until all required Core admission owners have registered. Missing admission
owners, malformed configuration, policy disagreement or receipt failure
prevents startup; there is no partial-isolation continuation.

This contract is not a delivery simulator and does not use timeout, freshness,
clean-shutdown markers, package absence or disabled platform configuration as
proof. Non-positive restore timeouts still mean unlimited waiting, and a
non-positive freshness window still treats interruptions as fresh in normal mode.

## Isolated verification

Verification seeds a real pending session/active-turn marker, a pending Cron
task, a pending outbound Telegram ledger row and a restart notice. Before/after
row and file snapshots must remain equal while platform health and startup
receipts may change. Fake transport tests exercise denied and normal sends;
no real delivery is required. Network denial is an additional test safety net,
not a substitute for Core admission fences.

This candidate contract does not authorize Production apply, launcher deployment,
Gateway restart, Skill changes, BUILD1 or ACT1.
