# No-agent provenance R2-S1: one native execution chain

Isolated candidate based on Hermes 0.21.7 commit
`3b63f7b0a4c7beb9eadbce2f15b9da75d01f5065`. No install, commit, push,
Gateway restart, production invocation or real transport experiment is authorized.

## Before editing

The input worktree already contained 23 dirty source/test files. They were
archived byte-for-byte before refactoring; the closeout records archive hash and
location. No reset, stash, clean, old-worktree deletion, or automatic commit was
performed. R1 worktrees are separate historical/other-consumer candidates.

## Bounded inventory and decisions

| Module / surface | Decision | Reason |
| --- | --- | --- |
| `cron/scheduler_origin_auth.py` | KEEP / FIX | Sole ephemeral Authority; macOS LOCAL_PEERPID, native process-start fingerprint and native ledger one-use CAS retained; canonical UTC instant and no nonce in returned proof. |
| `cron/scheduler_script.py` | FIX | Pass existing Job context to the script spawn; configured authority is mandatory, failed authentication kills/reaps script tree, no insecure fallback. |
| `cron/scheduler.py` | KEEP / FIX | Existing native claim/adoption/worker paths; explicit Job opt-in crosses the real macOS child boundary; invalid Authority settles the execution and closes resources even on pre-ack failure. |
| `cron/executions.py` | KEEP | Use existing profile-local execution ledger; no second state database. |
| `cron/provenance_readonly_r2.py` | KEEP / FIX | Single authoritative read-only verifier, six named checks, completed native row, owner/profile/Job/instant/digest/manifest binding. |
| `cron/r3_provenance_contract_r2.py` | MERGE | SQL, JSON parsing and proof validation removed; existing `verify_r3` entry only preserves its result shape and delegates to the single verifier. |
| `cron/r2_receipt_consumer.py` | KEEP / FIX | One reusable socket request in the existing consumer; exclusive new Receipt creation; Consumer never grants trust. |
| `ScriptOriginAuthority.poll`, `close_child_in_parent` | REMOVE | Stdin-era compatibility-shaped entry/no-op; no live caller after direct `consume` wiring. Actual socket close and child cleanup remain. |
| `test_noagent_provenance_r2_e2e.py` old FD/pre-seeded-row fixture | REMOVE / FIX | Was not a Scheduler E2E; now tests real manual/missing-Authority refusal. The complete E2E lives in `test_r2_external_worker_full_chain.py`. |
| `cron/child_execution_proof.py`, `provenance_readonly.py`, `r3_provenance_contract.py` | DEFER / NOT PRESENT IN R2 | Not in this R2 worktree at entry. R1/other-consumer versions were not deleted and are not loaded by the R2 chain. |
| Existing CLI/tool/API/DTO opt-in wiring | KEEP / DEFER | Pre-existing user changes preserved; native `create_job` is exercised here. Not every management surface is certified by E2E. The inherited API-server file-size ratchet is separately disclosed. |

Discovery used the indexed R2 project and targeted direct-source/import checks.
The index reported changed/untracked paths; it was not treated as exhaustive
negative proof and was not force-rebuilt.

## Minimal chain and first real fault

`create_job` -> builtin Scheduler native attempt -> existing external-worker
adoption -> ephemeral Socket Authority -> no-agent script/Receipt Consumer ->
native ledger CAS -> one read-only verifier -> R3 six-field result.

The unmodified exact E2E failed before ready acknowledgment with
`CANONICAL_ENTRY_BINDING_REQUIRED`, not merely a missing output pathname.
Its Manifest lacked the actual entry path/hash. The next fixture issues were
duplicate newline encoding and profile-name env pins that disagreed with an
actual named-profile home. Fixed fixtures now contain the real isolated script
digest and use native `<root>/profiles/<name>` layout. No execution/proof used
state is pre-seeded in the complete E2E: Scheduler creates the row, external
worker adopts it, Authority consumes its CAS, and only then does Receipt exist.

## Opt-in and fail-closed behavior

Native `create_job` accepts `r2_authority_required=True` and an explicit
`r2_release_manifest`. Existing persisted Job fields carry those to the native
worker. Default legacy Jobs are not automatically enabled for Authority.
Authority-required Jobs cannot fall back to in-process/unproven script execution.
Manual/direct/native-source mismatches, missing scheduled identity, wrong profile,
incorrect entry bytes or missing Manifest reject before trusted work proceeds.

Socket environment fields are request/address hints, not attestations. Trust is
not derived from origin strings, inherited env, a copied ContextVar, stdout IDs,
or Receipt declarations. macOS obtains the peer PID from LOCAL_PEERPID and checks
it against the actual Popen child plus native start fingerprint. The native
execution row must belong to the running adopted worker. A second Authority for
the same native execution cannot reset its CAS. This is native operator-boundary
provenance, not protection from an authorized same-user actor rewriting Core,
the manifest or the execution ledger.

No long-lived token or signing key is introduced. The bounded request nonce is
not returned or stored in Receipt; Consumer rejects proof payloads containing it.
Receipt stores only correlation proof/digest, Job/execution/profile/canonical UTC
instant and release identity, with `trusted=false`. A Receipt's `NATURAL_CRON`
label is only a pending claim until the independent native verifier succeeds.

## Minimal schema rationale

Only the pre-existing R2 candidate additions are retained:

- `origin_proof_sha256`: binds the exact kernel-peer-authenticated proof to the
  native row; native fields alone cannot correlate a business Receipt to the
  actual script child.
- `origin_proof_used`: compare-and-swap prevents another child/Authority from
  claiming the same native execution, even across Authority objects.
- `profile`: captures profile name at native execution creation, independent of
  later Receipt/env claims. Profile-local DB path scopes storage but is not by
  itself a stored immutable name for read-only six-field reconciliation.

No additional schema/migration, profile table, proof database or service is added
by S1. Old schemas migrate through existing native `add_column_if_missing`; a
read-only verifier never migrates and rejects old/missing-proof schemas.

## Verification boundaries

The single verifier checks native builtin **completed** status, Job ID, profile,
canonical UTC instant, one-use proof digest + native owner identity, and Manifest
bytes. Reading the same Receipt repeatedly is not another consumption; using a
different receipt ID for the same execution is rejected when supplied to the
related-receipt reconciliation scope. Global duplicate discovery is not claimed.

Release identity is a native-authenticated manifest digest plus pinned script
entry digest, not a newly invented source-tree signing authority. Manifest
selection and expected ledger/profile are trusted operator configuration. This
candidate is not installed into the live HK release; its isolated consumer
integration must not be described as Production HK receipt migration.

Failure cleanup tests exercise real child sockets/processes and prove the child
is reaped and endpoint closed after request timeout; they do not assert that
arbitrary earlier business effects can be undone. A failure to drain/reap remains
an error, not successful provenance. macOS tests do not prove Linux/Windows peer
identity, mixed-version rollout, hostile same-user isolation or all crash timing.

## Handoff

Use only the canonical `scripts/run_tests.sh` runner with a byte-equivalent
`.git`-free temporary source mirror and temporary Homes/Profiles/Ledgers/Sockets.
The mirror avoids production-linked Git metadata bootstrap reads; test I/O guards
remain enabled. The closeout separates focused, native E2E and affected regression
results and lists the inherited API facade size ratchet. No passing test grants
deployment, production schedule changes, R3 historical Receipt upgrades, or
permission to re-create the deleted Codex R3 monitor. Stop for review; commit and
isolated-runtime acceptance each need subsequent explicit user scope.

## R2-S4 management API configuration boundary

The existing API key is the management-operator credential; authentication and
the update whitelist still run before mutation. The existing request-options
sibling owns one public-to-Core Authority field map: create takes public keyword
arguments, update uses `_r2_*` storage keys. Private request fields are refused;
omission never enables Authority, and the opt-in flag must be a JSON boolean.

HTTP Manifest selection is deliberately narrower than native operator/CLI
configuration: an existing absolute, non-symlink, profile-local file must match
the native release tuple rules and the actual contained script digest. On update
it must also bind the stored Job script. An Authority-enabled Job cannot lose its Manifest
binding unless Authority is explicitly disabled. These are configuration checks,
not proof issuance or independent verification of the declared source-tree hash.
The API response grants no provenance; worker ownership, peer identity, native
CAS and completed-ledger readback remain mandatory and unchanged. The create
route keeps its existing payload fields: configuring Authority is not a claim
that this API can create a runnable no-agent script Job without native setup.
