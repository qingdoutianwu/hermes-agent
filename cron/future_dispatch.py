"""Generation admission for opt-in future-only jobs at builtin dispatch seams."""
from contextlib import contextmanager
from functools import wraps


def _current_snapshot(snapshot):
    from cron import jobs
    records = jobs._peek_jobs_unlocked()
    if records is None or any(not isinstance(row, dict) for row in records):
        return False
    matches = [row for row in records if row.get("id") == snapshot["id"]]
    # Preserve legacy embedding callers without a persisted job. Protected snapshots
    # may never degrade to that legacy path after removal or store corruption.
    if not matches:
        return not snapshot.get("dispatch_generation")
    if len(matches) != 1:
        return False
    current = matches[0]
    if not current.get("dispatch_generation"):
        return not snapshot.get("dispatch_generation")
    if (current.get("dispatch_generation") != snapshot.get("dispatch_generation", 0)
            or not jobs.is_job_runnable(current)):
        return False
    instant = snapshot.get("_scheduled_instant")
    if (current.get("manual_run_at") is not None
            and snapshot.get("manual_run_at") == current.get("manual_run_at")
            and snapshot.get("next_run_at") == current.get("manual_run_at")):
        return True  # exact native trigger marker, NOT an environment assertion
    if instant is None:
        return (snapshot.get("_dispatch_kind") == "manual"
                and bool((current.get("fire_claim") or {}).get("by"))
                and (snapshot.get("fire_claim") or {}).get("by") == current["fire_claim"]["by"])
    from datetime import datetime
    first = current["future_resume"]["next_run_at"]
    return datetime.fromisoformat(instant).timestamp() >= datetime.fromisoformat(first).timestamp()


@contextmanager
def dispatch_registration(snapshot):
    """Hold both fences through execution registration, NOT the business body."""
    from cron import jobs
    from cron.future_resume import _store_home
    with jobs._fire_job_lock(snapshot["id"]) as acquired:
        if not acquired:
            yield False
            return
        with jobs._jobs_lock(strict=True), _store_home():
            yield _current_snapshot(snapshot)


def fence_submission(function):
    """The ledger row must exist before resume can next test quiescence."""
    @wraps(function)
    def fenced(job, *args, **kwargs):
        try:
            with dispatch_registration(job) as admitted:
                return function(job, *args, **kwargs) if admitted else None
        except (OSError, ValueError, KeyError, TypeError):
            from cron.scheduler import logger
            logger.warning("Cron generation admission failed for %s", job.get("id"), exc_info=True)
            return None
    return fenced


def prepare_direct_execution(scheduler, job):
    """Reject a stale worker before handoff/delivery; create direct rows under fence."""
    with dispatch_registration(job) as admitted:
        if not admitted:
            return False
        if job.get("dispatch_generation"):
            from cron import jobs
            owner = (job.get("fire_claim") or {}).get("by")
            records = jobs._peek_jobs_unlocked() or []
            matching = [row for row in records if row.get("id") == job["id"]]
            if not owner or len(matching) != 1 or owner != (matching[0].get("fire_claim") or {}).get("by"):
                return False
        if job.get("execution_id") and job.get("dispatch_generation"):
            record = scheduler.get_execution(job["execution_id"])
            if not execution_matches(job, record, ("claimed", "running")):
                return False
        if not job.get("execution_id"):
            execution = scheduler.create_execution(
                job["id"], source="direct", scheduled_instant=job.get("_scheduled_instant"))
            job["execution_id"] = execution["id"]
            if job.get("dispatch_generation"):
                from cron.future_admission import record_execution_admission
                record_execution_admission(execution["id"], job)
        return True


def execution_matches(job, record, statuses):
    from cron.occurrences import scheduled_instant
    return bool(record and record["job_id"] == job["id"] and record["status"] in statuses
                and record.get("dispatch_generation") == job.get("dispatch_generation")
                and record.get("scheduled_instant") == scheduled_instant(job.get("_scheduled_instant"))
                and record.get("fire_owner") == (job.get("fire_claim") or {}).get("by"))


def fence_business(function):
    """Last shared script/agent gate. Raw protected calls require a native running row."""
    @wraps(function)
    def fenced(job, *args, **kwargs):
        from cron import executions, jobs
        from cron.future_resume import _store_home
        with jobs._jobs_lock(strict=True):
            records = jobs._peek_jobs_unlocked()
            legacy = records is not None and not job.get("dispatch_generation") and all(
                row.get("id") != job["id"] or not row.get("dispatch_generation") for row in records)
        if legacy:
            # Existing unprotected run_job callers keep their original lock contract.
            # Raw legacy direct calls racing first protection are NOT certified.
            return function(job, *args, **kwargs)
        with jobs._fire_job_lock(job["id"]) as acquired:
            if not acquired:
                return False, "", "", "BLOCKED_EXECUTION_ADMISSION"
            with jobs._jobs_lock(strict=True), _store_home():
                admitted = _current_snapshot(job)
                if admitted and job.get("dispatch_generation"):
                    record = executions.get_execution(kwargs.get("execution_id") or job.get("execution_id"))
                    admitted = execution_matches(job, record, ("running",))
            if not admitted:
                return False, "", "", "BLOCKED_EXECUTION_ADMISSION"
            return function(job, *args, **kwargs)
    return fenced
