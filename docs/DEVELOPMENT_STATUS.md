# HERMES-R10C-R3C development status

Updated: 2026-10-12 (Asia/Shanghai).

## Scope and source authority

- Project status: **IN_PROGRESS**.
- Development branch: `codex/r10c-r3c-recovery-fences`.
- Base commit: `908e4a4b44480912ae3571833492c1aeac2d0eda`.
- This document and the existing isolated candidate are frozen together by the
  local commit containing this file. Resolve its exact identity with
  `git rev-parse HEAD`; the base commit alone does not contain the candidate.
- This is a Hermes Core candidate, not Stock News application code. Stock News
  V3.3-P2A remains separately frozen on its own repository and branch.

No production installation, source switch, Gateway restart, Cron mutation,
ESTOP release, collection, replay, or external delivery is part of this work.

## Completed candidate implementation

The existing candidate contains nine runtime files and three focused test files.
This source-reconciliation step preserves their previously accepted bytes; it
adds this status document, not new runtime behavior.

| Existing owner | Candidate behavior |
| --- | --- |
| `gateway/startup_isolation.py` | Reuses the process-lifetime policy: strict booleans, monotonic delivery restrictions, and all nine required registrations before restricted activation. |
| `gateway/run.py` | Calls the policy's existing `install/verify` before subsystem startup. |
| `cron/jobs.py`, `cron/scheduler_tick.py` | Deny restricted claims and dispatch before claim mutation or the due scan. |
| `tools/cronjob_tools.py` | Manual execution cannot bypass scheduler admission. |
| `gateway/run_startup.py` | Guards adapter setup, pending tasks, business replay, pending sends, and startup recovery effects. |
| `gateway/run_notifications.py` | Denies restricted restart notification before consuming its marker. |
| `gateway/platforms/base.py` | Applies the existing outbound-admission contract to inherited and overridden adapter operations. |
| `gateway/delivery_ledger.py` | Checks delivery-state admission before opening the ledger and denies pending-send availability. |

The original nine registration requirements are not reduced. Missing owners fail
closed. Internal session recovery and external business replay remain distinct;
Recovery-Only does not claim OS-level containment or a new authorization model.

## History reconciliation

Original branches and commits are retained without merge, reset, or deletion:

- `codex/finance-daily-nosend-r2a` at
  `b3579f3172f48929b23698ba0d9fa9e9761e0e21`.
- `codex/r10c-r3a-contract-qualification` at
  `4434781b3762cd3d97b29e99f313b14f0a7bd4e4`.
- Their merge-base with the current development base is
  `cfdcea4f2226932adde690f6e53e0c8d259147ec`.

The original restoration branch has four unique commits relative to the current
base; the R3A branch adds a historical launcher-observation test commit. Reusable
policy and admission behavior is already represented in the reviewed candidate.
The old dependency launcher, private sealed startup entry, configuration edits,
and one-shot delivery permit are not silently imported. They belong to separate
binding or delivery contracts and are not missing requirements of this source
freeze. Existing historical qualification evidence is retained outside Git.

Upstream `main` was fetched and inspected at
`835c3dd038d903182a66ecc45e34bf5e0de65f00`, 581 commits ahead of the candidate
base, with no divergence at that base. The R3C remote branch was absent at the
readback. This candidate stays on its verified base; no upstream merge or rebase
is claimed. Integration with a future upstream baseline requires a separate
bounded impact review and affected tests, not an automatic production update.

## Current verification

On 2026-10-12, the existing official `scripts/run_tests.sh` was used in a
disposable HOME/HERMES_HOME, with the retained test interpreter, clean
environment, one worker, and zero retries:

| Test file | Passed |
| --- | ---: |
| `tests/gateway/test_r10c_policy_owners.py` | 12 |
| `tests/gateway/test_r10c_recovery_fence_candidate.py` | 12 |
| `tests/gateway/test_r10c_startup_registration.py` | 1 |
| `tests/cron/test_estop.py` | 19 |
| `tests/gateway/test_delivery.py` | 15 |
| `tests/gateway/test_delivery_ledger.py` | 43 |
| **Total** | **102** |

No failures or retries occurred. The strict Ruff configuration passes for the
twelve candidate Python files. Ruff reports four existing configuration warnings
for rules requiring preview mode; preview was not enabled and rules were not
changed. `git diff --check` passes. The full Hermes suite, Mypy, and CI were not
run in this source-freeze step; targeted results are not a full release result.

Historical isolated official startup evidence establishes nine real registrations
and post-startup denial at the four bounded Recovery-Only entrances. That evidence
is preserved and the candidate file identities match it. It is not a new startup
proof on the local commit, nor proof that production has installed this candidate.

## Scheduler and path compatibility

This extends the existing Hermes CLI/Gateway and Cron ownership rather than
creating another Scheduler, Launcher, Registry, Recovery Manager, or Skill.
Runtime policy resolves configuration through `get_process_hermes_home()`, and
tests use isolated owning-home paths. Existing CLI error behavior and state
semantics are preserved.

No production Job definition, enabled/state value, scheduler cadence, task policy,
dependency lock, database schema, or installed Skill is changed. The source
remains in the original isolated worktree; source centralization is not a Core
deployment procedure and is not performed while this project is IN_PROGRESS.

## Remaining blockers and next actions

1. **GitHub destination authority:** current origin is the public
   `NousResearch/hermes-agent` repository, and the current account has read but
   not push permission. No remote replacement, fork, tag, CI dispatch, or push is
   performed. Obtain applicable write authority or explicitly designate an
   existing authorized source destination before a later synchronization step.
2. **Sealed Chain qualification:** this local Git freeze is not a formal release
   manifest, trusted build receipt, committed candidate Runtime Generation, or
   actual sealed process binding. Historical sealed Cron files are not equivalent
   to the patched candidate Cron. Preserve
   `ISOLATED_SEALED_CHAIN_EQUIVALENCE=UNVERIFIED` and do not manufacture authority.
3. **Production authority:** the historical isolated startup authorization window
   expired on 2026-10-11. The current task authorizes source reconciliation,
   documentation, isolated unit/regression tests, and a local commit only; it does
   not renew full Gateway experiments or authorize production recovery.

Keep `READY_FOR_HERMES_MIGRATION=false`, `READY_FOR_GITHUB_PUSH=false`, and
`PRODUCTION_PROMOTION=NOT_AUTHORIZED`. Do not apply this candidate to a live Core
checkout, enable Cron, leave Recovery-Only, or send Telegram/iMessage messages.

Private historical evidence remains under
`$HERMES_HOME/release-audit/hermes-r10c-r3c-20261010/`. Its machine-specific
receipts, configuration, databases, logs, and authorization messages are not
copied into this source commit. Only this portable development status is added.
