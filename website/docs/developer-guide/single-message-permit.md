# Controlled single-message permit

The Core API `create_permit(ExactMessage)` and `deliver_once(...)` is a narrow,
explicit controlled-test path, not an ordinary sender, notification, or pending
replay exception. It is usable only in verified Recovery ON / Global Delivery OFF
/ Delivery state writes OFF mode. Full startup isolation always denies it.

Permit validity and transport admission are separate. `validate_permit(...)` reads
the exact permit row using SQLite read-only/query-only mode, validating destination,
message, purpose, TTL, one-attempt budget and ledger scope without consuming it.
It reports `permit_validation=PASS`, `execution_context`, `startup_context`,
`silent_recovery_state` and `external_send_allowed`. Validity alone never grants send.

Under explicit `gateway.startup_context: silent_recovery` and
`gateway.no_external_messages: true`, a valid request remains valid, but reservation
and transport admission reject with `TransportAdmissionDenied`. No provider call,
attempt count, consumption or unrelated obligation mutation occurs on that denial.
The exception carries the successful validation evidence when available. A request
with genuinely invalid scope, content or expiry still fails permit validation.
Normal context preserves the existing recovery-only controlled one-shot capability;
this does not globally enable or disable ordinary messaging.

The dedicated Core Telegram provider entry checks the same admission before invoking
the client. Calling it directly does not bypass silent context. An already durable
reservation remains consumed/uncertain if context becomes silent before transport;
it is not cancelled, rolled back, refreshed or retried. Existing post-transport
receipt/uncertainty handling is unchanged. Context fields are runtime evidence,
not a new SQL table schema or a second permit authority.

A permit binds UUID identity/nonce, purpose CONTROLLED_DELIVERY_TEST, one channel,
exact canonical destination (profile, account, effective chat ID and thread),
visible UTF-8 message bytes and SHA256, finite TTL (maximum 300s), attempts=1,
and its unique controlled-test ledger ID. No payload normalization, chunking or
Markdown conversion is permitted. The dedicated Telegram transport supplies
parse_mode=None, no media, one provider call, no retry/fallback; the normal helper
and its default attempts=3 remain byte-identical. A future iMessage adapter must
also satisfy the single-call transport interface; no real iMessage provider is
implemented or exercised in this candidate.

Core writes only its `controlled_delivery_permits` table in private state.db.
No ordinary delivery_obligations, queues or receipts are adopted or advanced.
SQLite BEGIN IMMEDIATE + synchronous=FULL commits consumed=true and
SENT_UNCERTAIN **before** transport entry. RESERVED is already terminal for
reuse; TRANSPORT_INVOKED spends the only attempt; terminal SENT requires provider
message ID. Only a transport's DefinitelyNotSent proof produces FAILED. Timeout,
disconnect, cancellation, missing response or process crash keeps SENT_UNCERTAIN,
with no automatic retry, refresh, replacement permit, restart sweep or resend.
`transport_entry_count` is an admitted entry count, not independent proof a
provider received bytes: crashing immediately after entry can leave count=1
without reaching the provider. The attempt budget remains consumed regardless.

Transport is trusted Core code, never a callback/route submitted by an external
request. The future owning route resolver must authenticate the client against
bound profile/account identity before selecting a transport. Fake clients in
tests have no credentials and no network. This candidate provides the tested
Core API and exact-route Telegram single-call adapter; it does not add a live
control-socket send endpoint or authorize issuance from untrusted callers.
Same-OS-owner arbitrary Python/SQL access is outside this capability's boundary:
the permit is not a defense against a malicious process already able to rewrite
Core state.db. Owner/private DB checks fail closed.

Evidence reports consumed/phase/outcome, attempt budget, exact hashes and
provider receipt. Tests independently compare unrelated pending ledger/permit
rows. No global/contextvar admission override exists; normal send_message and
ledger writer remain denied even inside the scoped transport.

Candidate only: no production change, restart, production permit, real send or
R9 execution. Future issuance/route wiring/deployment and one real send require
separate authorization.
