# Strict Telegram text contract — candidate only

This source candidate adds `hermes send --strict-topic` for explicit numeric
Telegram chat + non-General topic targets. It is not a parameter supported by
the currently installed baseline, not an activation receipt, and not a new client.
The option is forwarded to the existing native sender through its tool boundary.

The native formatter, text splitter, Bot factory and request helper are reused.
Strict mode rejects media, home/default targets, topic mapping changes and empty
text before external requests. Thread-not-found cannot drop the requested topic;
each native request uses one attempt. Definitive formatting rejection may retry
plain text in the same Topic only, with admission rechecked before that request.
Legacy mode keeps its existing compatibility behavior.

Every chunk needs a message ID, actual response chat ID and actual response
Topic matching the requested pair. Receipts expose requested/actual identities,
fallback_used=false, thread_fallback=false, route_confirmation and per-chunk
evidence. Missing or mismatched response proof and post-attempt exceptions are
UNKNOWN, confirmed=false and safe_to_auto_retry=false. Further chunks stop.

Admission delegates only to the existing canonical
`gateway.startup_isolation.require("external_delivery")` contract. This module
is absent from the current base. Absence, denial or verifier failure closes the
strict path before Bot creation. Checks repeat per chunk/request; no policy,
permit, signer, flag override or Recovery transition is supplied by this patch.
Mock admission success is conditional source proof, not real authorization.
Existing relay destination authorization remains in place.

21 strict behavioral tests use real module imports and mock only request,
admission and inactive Gateway runtime seams. The broader 4-file targeted suite
has 147 tests. No native lifecycle or real network is initialized. Canonical
`scripts/run_tests.sh` is run in a disposable source copy with isolated HOME,
offline guard and no-running-Gateway mock (the baseline's unrelated live-Gateway
import is incompatible with the managed Python 3.11 typing runtime).

US Premarket 45a7ef3 full regression: 209 PASS, including UNKNOWN ledger,
BLOCKED no-send and full synthetic report/render/Manifest/Receipt pipeline.
This patch does not update that service or its production shim/shared transport.
Consumer Adapter integration still requires candidate review; merely promoting
P0 would neither select this new flag nor upgrade the existing shared normalizer.

Current installed native strict-topic capability and admission are unqualified.
Native offline health is UNVERIFIED. No deployment, message experiment, Scheduler
resume, iMessage change or production pin promotion is included.
