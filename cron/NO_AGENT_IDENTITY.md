# POSIX no-agent native identity bridge candidate

This is an isolated development candidate, not a production installation. It adds
one sibling helper and the existing script-spawn call-site wiring. No execution
table/schema, retention, Job, provider, Gateway or scheduling policy is changed.

Authority is `cron.execution_identity.current_cron_execution()`, bound by
`_start_owned_run` after the claimed-to-running CAS. The matching existing profile
ledger row must be running and owned by this PID/process instance. Slot is the
row's scheduled_instant, never the current clock. For builtin runs the actual
dispatched Job's last_dispatch stamp supplies the matching occurrence and native
on_time/late/catch_up classification. Unknown providers or missing/inconsistent
stamps remain unproven. Direct stays direct even if stale scheduled metadata exists.

At `_run_job_script_with_claim_heartbeat` -> `_run_job_script`, the child environment
is still constructed by the standard secret-scrubbing factory. Ambient identity
keys (not HERMES_CRON_TIMEOUT) are removed. For a valid owned no-agent context, a
short anonymous pipe frame and POSIX pass_fds carry one child-bound identity.
HERMES_CRON_IDENTITY_FD is an ephemeral inherited capability reference, not a new
operator setting or long-lived secret. Other identity names match the existing
consumer contract. No stdin is taken over and unrelated scripts may ignore the
additive descriptor. Descriptors are closed on spawn/exit/error paths.

The child consumes the pipe once, checks child/parent PID, parent start fingerprint,
the running native row's process instance/source/slot, home, expected Job and
launcher, and equality of the identity environment to the authoritative frame.
The read is bounded; ledger access is mode=ro/query_only with bounded lock timeout.
No raw environment strings alone prove natural execution. Missing identity returns
None so the report can still be generated with UNPROVEN/null fields. One ordinary
log event records Job/execution/origin/slot/child PID; existing business Receipt and
Cron output supply the durable link before the global 1000-terminal-row pruning.

`on_time` -> HERMES_SCHEDULER_CRON; `late` -> HERMES_SCHEDULER_LATE;
`catch_up` -> HERMES_SCHEDULER_CATCH_UP; `direct` -> HERMES_DIRECT. Catch-up is a
builtin Scheduler-origin run, but never labeled on-time. Direct is not natural.
The HK client projects these into its existing fields without changing Artifact,
quality, publication or volume-history logic. Do not deploy only the environment
half: the consumer must verify the inherited capability and native row.

Threat scope follows SECURITY.md: this binds provenance under a trusted same-user
Scheduler, selected code and ledger. It is not cryptographic attestation, OS
containment, or protection from a compromised parent/ledger or same-user arbitrary
code execution. Cross-platform production support beyond POSIX is not claimed;
unsupported paths scrub untrusted identity and remain unproven.

Targeted validation uses fresh HOME/HERMES_HOME, temp native ledgers and real
isolated children; there is no real collection, report Producer, native tick or
delivery. Tests include different Jobs/home scopes, origin classes, spoofed/missing
identity, malformed/reused descriptor input and unchanged native ledger schema/rows.
The HK end-to-end tests use the existing wrapper finalize with synthetic Artifact
bytes and prove both successful and unproven identity preserve business success.

Deployment review must separately approve this Core helper/spawn delta, the HK
run_context consumer delta, import visibility in the selected child interpreter,
and a new matching source pin. No production write, restart, activation, real
delivery, push, historical ID reconstruction or extra execution ledger is authorized.
