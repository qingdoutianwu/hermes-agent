"""Opt-in, revision-checked future-only resume candidate (builtin Cron only).

No identity attestation, no dispatch, and no changes to ordinary resume. See the
developer contract for the remaining mixed-version/direct-entry proof boundary.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from contextlib import contextmanager
from datetime import UTC, datetime

MIN_COMMIT_LEAD_SECONDS = 5.0
MAX_CLOCK_SKEW_SECONDS = 1.0


@contextmanager
def _store_home():
    from cron import jobs
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    token = set_hermes_home_override(jobs._current_cron_store().cron_dir.parent)
    try:
        yield
    finally:
        reset_hermes_home_override(token)


class ResumeBlocked(ValueError):
    """An admission refusal, before jobs.json's commit point."""


def job_revision(job: dict) -> str:
    """Hash the exact stored record, not a display/normalized job or its mtime."""
    raw = json.dumps(job, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _read_target(job_id):
    from cron import jobs
    records = jobs._peek_jobs_unlocked()
    if records is None or any(not isinstance(row, dict) for row in records):
        raise ResumeBlocked("BLOCKED_STORE_INVALID")
    matches = [row for row in records if row.get("id") == job_id]
    if len(matches) != 1:
        raise ResumeBlocked("BLOCKED_JOB_ID_NOT_UNIQUE")
    return records, matches[0]


def inspect_job_revision(job_id: str) -> dict:
    """Repair-free official candidate readback; exact ID, current profile only."""
    from cron import jobs
    with jobs._jobs_lock(strict=True):
        _, job = _read_target(job_id)
        return {"job_id": job_id, "revision": job_revision(job),
                "state": job.get("state"), "enabled": job.get("enabled"),
                "future_resume": job.get("future_resume")}


def _quiescent(job, conn):
    from cron import jobs
    from cron.scheduler import is_job_running
    home = jobs._current_cron_store().cron_dir.parent
    if any(job.get(key) is not None for key in (
        "pending_slot", "fire_claim", "run_claim", "manual_run_at", "manual_run_prompt",
    )) or is_job_running(job["id"], home=home):
        raise ResumeBlocked("BLOCKED_TARGET_JOB_NOT_QUIESCENT")
    # Unknown is not proof that an external side effect stopped. Never age it out.
    active = conn.execute(
        "SELECT 1 FROM executions WHERE job_id=? AND "
        "(status IN ('claimed','running','unknown') OR handoff_pending=1) LIMIT 1",
        (job["id"],),
    ).fetchone()
    if active:
        raise ResumeBlocked("BLOCKED_TARGET_JOB_NOT_QUIESCENT")


def _validate_time(checked_at, checked_mono, next_run):
    from cron import jobs
    now = jobs._hermes_now().astimezone(UTC)
    elapsed = time.monotonic() - checked_mono
    wall_elapsed = (now - checked_at).total_seconds()
    if elapsed < 0 or wall_elapsed < 0 or abs(wall_elapsed - elapsed) > MAX_CLOCK_SKEW_SECONDS:
        raise ResumeBlocked("BLOCKED_CLOCK_DISCONTINUITY")
    if (next_run - now).total_seconds() <= MIN_COMMIT_LEAD_SECONDS:
        raise ResumeBlocked("BLOCKED_NEXT_SLOT_TOO_CLOSE")


def _prepare(job, expected_revision, request_id, checked_at):
    from cron import jobs
    if job_revision(job) != expected_revision:
        raise ResumeBlocked("BLOCKED_REVISION_CONFLICT")
    if job.get("state") != "paused" or job.get("enabled") is not False:
        raise ResumeBlocked("BLOCKED_EXPECTED_PAUSED_DISABLED")
    schedule = job.get("schedule", {})
    generation = job.get("dispatch_generation", 0)
    if type(generation) is not int or generation < 0:
        raise ResumeBlocked("BLOCKED_GENERATION_INVALID")
    if schedule.get("kind") not in {"cron", "interval", "once"}:
        raise ResumeBlocked("BLOCKED_SCHEDULE_UNSUPPORTED")
    selected = jobs.compute_next_run(schedule, checked_at.isoformat())
    instant = datetime.fromisoformat(selected).astimezone(UTC) if selected else None
    if instant is None or instant <= checked_at:
        raise ResumeBlocked("BLOCKED_NO_FUTURE_SLOT")
    operation = {"protocol": "post_replace_v2", "request_id": request_id, "expected_revision": expected_revision,
                 "boundary": checked_at.isoformat(), "next_run_at": selected,
                 "generation": generation + 1}
    job.update(enabled=True, state="scheduled", paused_at=None, paused_reason=None,
               next_run_at=selected, dispatch_generation=operation["generation"],
               future_resume=operation)
    return operation, instant


def _result(operation, *, replayed=False):
    return {"success": True, "resume_mode": "future_only", "atomic_persisted": True,
            "quiescence_checked": True, "replayed": replayed, "operation": dict(operation)}


def _resume_locked(job_id, expected_revision, request_id):
    from cron import executions, jobs
    records, job = _read_target(job_id)
    prior = job.get("future_resume") or {}
    if prior.get("request_id") == request_id:
        if prior.get("expected_revision") != expected_revision:
            raise ResumeBlocked("BLOCKED_REQUEST_ID_CONFLICT")
        # Return the original outcome, not a claim about the job's present lifecycle.
        from cron.future_admission import confirmed
        if not confirmed(job):
            return {"success": False, "error": "BLOCKED_RESUME_UNCONFIRMED", "atomic_persisted": True,
                    "dispatch_admitted": False, "operation": prior}
        return _result(prior, replayed=True)
    checked_at = jobs._hermes_now().astimezone(UTC)
    checked_mono = time.monotonic()
    # Same profile/store as the job, even when use_cron_store differs from launch home.
    with _store_home(), executions._transaction() as conn:
        _quiescent(job, conn)
        operation, next_run = _prepare(job, expected_revision, request_id, checked_at)
        validate = lambda: _validate_time(checked_at, checked_mono, next_run)
        validate()
        jobs._save_jobs_unlocked(records, before_commit=validate)
        from cron.future_admission import confirm_after_replace
        try:
            confirm_after_replace(job, conn, checked_at, checked_mono, next_run)
        except ResumeBlocked as exc:
            return {"success": False, "error": str(exc), "atomic_persisted": True,
                    "dispatch_admitted": False, "operation": operation}
    return _result(operation)


def resume_future_only(
    job_id: str, *, expected_revision: str, request_id: str,
    expected_state="paused", expected_enabled=False, resume_mode="future_only",
) -> dict:
    """CAS under fire -> strict jobs -> execution locks; never triggers a run.

    A persistence exception has an uncertain outcome: inspect the durable operation
    and retry ONLY this exact request ID/revision, never automatically mint a new ID.
    """
    from cron import jobs
    if expected_state != "paused" or expected_enabled is not False or resume_mode != "future_only":
        return {"success": False, "error": "BLOCKED_RESUME_CONTRACT"}
    if (any(not isinstance(value, str) or not value for value in (job_id, expected_revision, request_id))
            or len(request_id) > 128):
        return {"success": False, "error": "BLOCKED_RESUME_ARGUMENTS"}
    try:
        with jobs._fire_job_lock(job_id) as acquired:
            if not acquired:
                raise ResumeBlocked("BLOCKED_FIRE_FENCE_UNAVAILABLE")
            with jobs._jobs_lock(strict=True):
                return _resume_locked(job_id, expected_revision, request_id)
    except ResumeBlocked as exc:
        return {"success": False, "error": str(exc), "atomic_persisted": False}
    except Exception as exc:
        logging.getLogger(__name__).exception("Future-only resume outcome uncertain for %s", job_id)
        return {"success": False, "error": "PERSISTENCE_OUTCOME_UNKNOWN",
                "detail": type(exc).__name__, "retry_same_request_only": True}
