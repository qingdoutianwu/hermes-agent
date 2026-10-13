# Future-only Cron resume — R1 history and R2 partial candidate

**Status: CANDIDATE_PARTIAL_BLOCKED. Not installed, deployed, or approved upstream.**
R1 remains immutable at `e886f8235cc6bd9f30a0902383821d0715e1168f`.
The sections below describing R1 gaps are historical evidence; see the R2
contract at the end for the implemented changes and remaining limitations.
This is a source candidate based on installed Hermes 0.21.7 commit
`3b63f7b0a4c7beb9eadbce2f15b9da75d01f5065`, not a claim about latest upstream main.
Do not use the candidate to authorize an existing production Job's recovery.

## API and preserved behavior

The new state-layer entry is `cron.jobs.resume_future_only`. CLI:

```sh
hermes cron revision JOB_ID
hermes cron resume-future-only JOB_ID --expected-revision REVISION --request-id UNIQUE_ID
```

The existing `cronjob_manage` tool adds `revision` and `resume_future_only`
actions with `expected_revision` and `request_id`. Exact Job IDs are required;
names are intentionally not resolved. These are candidate extensions to existing
surfaces, not a new tool, Scheduler, daemon, or database.

Core signature additionally makes `expected_state="paused"`,
`expected_enabled=False`, and `resume_mode="future_only"` explicit. Other
values, stale revisions, malformed stores, ambiguous IDs, or unsupported
schedules fail closed. Revision is canonical SHA256 of the exact persisted
Job record (not display fields, timestamp, feature HEAD, or execution identity).

Ordinary `resume_job`, CLI `resume`, and tool `resume` retain their old elapsed
slot/catch-up policy. The legacy body lives in `cron/jobs_resume.py`, accessed
through the original entry. Existing public symbols and patch seams remain.

An admitted transaction changes only `enabled`, `state`, `paused_at`,
`paused_reason`, `next_run_at`, plus `dispatch_generation` and its durable
`future_resume` operation record. It preserves Job ID, schedule, prompt, script,
skills, profile binding, workdir, no_agent, delivery and repeat policies. No
execution is started by this API. Native schedule calculation uses Hermes'
configured IANA timezone and DST behavior; no business-day calculator is added.

## Locks and quiescence

Lock order: profile/Job fire fence → strict Job-store lock → existing native
execution-ledger transaction. State/revision check, original-schedule future
selection, lifecycle update, and atomic write occur while these locks are held.
New strict locking refuses missing backends/timeouts and degraded nested locks;
ordinary store operations retain their existing degraded-lock behavior.

Any unresolved `pending_slot`, `fire_claim`, `run_claim`, manual marker,
profile-local running registration, claimed/running/unknown execution or
pending handoff returns `BLOCKED_TARGET_JOB_NOT_QUIESCENT`. Nothing is aged out,
deleted, rewritten, or relabeled to manufacture quiescence. A historical
unknown row may therefore block indefinitely pending a separate owner contract.

Builtin submission and direct `run_one_job` registration now share the fire /
strict-store gate through execution registration. The durable ledger row exists
before releasing it. Queued/running attempts therefore block a subsequent
resume; an old captured snapshot instead loses on generation comparison.
Batch advance rejects stale generations and `_process_due_job` supplies the
captured generation to its native fire claim. Protected scheduled snapshots
must carry an instant at or after the first selected future slot. A missing,
removed, duplicate, or malformed protected record cannot fall back to legacy
admission. This generation is concurrency metadata, **not Cron provenance**.

## Time and persistence boundary

Request entry precedes lock acquisition. The persisted `boundary` is the UTC
state-check instant under locks; the selected slot must be strictly later.
Wall-clock progress is compared with elapsed monotonic time (one-second
tolerance). The five-second minimum lead is checked before staging and again
immediately before atomic replacement. Clock jumps and near-slot requests are
refused without publishing the staged state. This guard does not silently skip
to a different later slot.

**Remaining blocker:** the last clock check and actual filesystem rename are
not a single clock-aware primitive. An arbitrary process/OS stall between them
can publish a slot which is no longer future. The diagnostic test deliberately
reproduces that counterexample without dispatch or delivery. The check instant
must not be advertised as an actual commit-time bound. A complete protocol
needs a reviewed, dispatch-enforced durable commit/activation boundary or an
equivalent narrowly scoped contract. A five-second lead alone is not proof.

Existing fsynced staging + atomic replace + directory fsync provides all-or-none
Job fields. Exceptions after replacement are reported
`PERSISTENCE_OUTCOME_UNKNOWN`, not an assumed rollback. Inspect `revision` and
the durable `future_resume` record. Retry only the identical request ID/revision;
the latest matching operation returns its original outcome without recalculating.
Replay describes the operation's outcome, not the present Job lifecycle (it may
subsequently have been paused). An older superseded request cannot create a
second transition with its stale revision. No automatic retry/new request ID.

Abrupt process-exit tests cover before stage, after stage, after rename/before
response, and fresh-interpreter retry. They do **not** prove power-loss durability
or SQLite/JSON atomicity across two files; the ledger transaction here is used
for admission reads, not a distributed state commit.

## Remaining integration boundaries

- Mixed-version schedulers/writers and unfenced direct business entry paths are
  not certified. Existing legacy degraded writers can still bypass a held flock
  after their timeout. No candidate deployment/restart is authorized here.
- External providers and protected-job manual/run-now surfaces are not fully
  adapted. Without the captured generation/slot contract they are conservatively
  refused; this is a documented compatibility gap, not successful integration.
- Full detached-worker owner/handoff races, dead processes, terminal-record
  pruning, and direct `run_job` business entry are not exhaustively certified.
  No claim is made that earlier external effects can be cancelled or recalled.
- Isolated callback counters cannot establish a universal zero side-effect
  count for all runtime delivery paths. No real credentials/messages are used.

## Tests and source-only review summary

New tests cover revision/lifecycle and unchanged-other-Job assertions, elapsed
slots with either catch-up setting, strict lock refusal, pending/native ledger
refusal, identical retry, expiry/near-slot/time jumps, month/weekend/DST schedule
selection, CLI/tool roundtrip, pause-after-resume, real Event/Barrier races for
submit/tick/advance/fire claim/pause/two restores, same-ID profiles, queued and
running registration, first future-slot dedup, and child process exits.

`test_future_resume_proof_limits.py` contains **diagnostic** tests. Their passing
assertions record an unclosed commit-stall window and conservative manual
refusal; they are not safety acceptance tests. Final closeout distinguishes them.

Use `scripts/run_tests.sh` with the existing interpreter. A worktree linked to a
production Git directory triggers the test I/O guard during bootstrap; run a
byte-identical temporary source mirror excluding `.git`, not a weakened guard.
All Hermes homes, runtime paths and state are temporary. The source repository,
production install, HK Job, ESTOP and R3 snapshots must remain untouched.

Minimal candidate review scope: opt-in state transaction; existing write hook;
generation guards on native seams; CLI/tool extension; legacy body extraction;
tests and these documents. No upstream publication has occurred. Before HK
integration review: close the commit/dispatch boundary, reconcile all execution
surfaces, certify matching runtime owners, re-run affected tests, then request
separate deployment and target-Job restore authority. Native R2A's provenance
blocker is independent and unchanged. Never upgrade MANUAL_CLI receipts,
backfill old receipt gaps, change R3 baseline, or use source review as promotion.

## Rollback boundary

In isolation, native `pause_job` returns the Job to paused and blocks future
scan/admission; original IDs and unrelated Jobs remain unchanged. It cannot
revoke already admitted workers or retract Telegram/iMessage effects. No
production rollback was attempted and there is no production-ready rollback
claim while the forward recovery contract remains partial.

## R2: post-replace durable admission fence

The lifecycle commit point is the existing atomic replacement of `jobs.json`.
The candidate does not first resume and then rewrite `next_run_at`. Its single
JSON transition includes a `post_replace_v2` operation/generation. Until a
matching confirmation commits in the **existing profile execution ledger**, the
Job is non-runnable, even if its display fields say enabled/scheduled.

The confirmation's clock sample occurs after the JSON replace and directory
fsync return. It must precede the chosen slot, with the existing lead/clock
continuity checks. Thus a slot that expires during JSON persistence gets no
confirmation. Indefinite stalls retain the fire/store locks; process exit
releases locks but rolls back the SQLite transaction and leaves dispatch
blocked. No new database, daemon, global ESTOP or Scheduler is introduced.

SQLite confirmation commit is the admission publication point. The confirmation
sample is an upper bound on the earlier JSON commit, not a claim that filesystem
rename and wall clock are one primitive. A delay after the confirmation sample
can allow normal late execution of a slot which was genuinely future at the
JSON commit; this is not recovery of a pre-commit expired slot. Missing ledger
records never auto-confirm on restart/retry; identical requests read back either
the confirmed original outcome or `BLOCKED_RESUME_UNCONFIRMED`. JSON fields can
remain scheduled but non-runnable after failure. This is explicitly a safe
incomplete publication, not a claim of a two-database atomic transaction or
automatic rollback to paused.

Due scan, recurring advance, snapshot admission, fire claim and business
entry now enforce confirmation. `force` cannot override missing confirmation.
Native execution rows additionally bind generation and fire owner. Protected
raw `run_job` requires a running, matching native attempt before script/agent
preparation. Generation remains concurrency metadata, not provenance.

Explicit native `manual=True`/`force=True` claims remain occurrence-free and
preserve the pending scheduled slot. The native `trigger_job` marker survives
protected batch advance. Provider `claim_fire` accepts additive
`expected_generation` and `scheduled_instant`; a protected scheduled callback
without them is refused, not silently upgraded to a current scheduled event.
Provider manual claims carry native fire ownership plus execution binding.
The existing transport/tool authorization boundaries are unchanged; these
parameters do not grant business permission.

R1's two diagnostic scenarios were reproduced on the unmodified implementation,
then converted to safety assertions (both failed on R1). R2 closes those scoped
scenarios. New tests cover provider old/current/manual claims, long persistence
with simultaneous scan/manual claim, native trigger, unresolved confirmation,
profile-local confirmation and protected direct business refusal. Process exit
tests additionally cover before and after ledger confirmation commit.

### R2 remaining blockers — do not promote

- Raw **unprotected** legacy `run_job` retains its old locking contract, to
  avoid holding the fire fence across legacy registration/sweep interactions.
  Its race with the first protection transition is not certified. The native
  registered paths are guarded, but arbitrary raw/private entry paths cannot
  be declared covered by those tests.
- Third-party provider overrides, mixed-version/degraded legacy writers and
  the full protected detached-worker adoption/delivery failure matrix remain
  unverified. Unknown entry paths must not be used for protected execution.
- The complete Cron regression has baseline failures (incident/preflight and
  a managed-interpreter child codec failure); exact results live in R2 closeout.
  No test was removed or marked passed merely because it failed on R1 too.
- Production shared-file attribution remains separate. B4-R2 preserves the
  target record and whole-store hash, not all other Job preimages. A changed
  SHA alone cannot prove business configuration drift or its authorization.

Both scoped diagnostic fixes are not equivalent to all-execution-path acceptance.
Upstream/integration readiness stays false until these proof boundaries and
regression failures are resolved. R3 baseline, Production Job and messages are
untouched; no automatic deployment or recovery follows this candidate.
