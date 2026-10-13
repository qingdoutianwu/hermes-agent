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
    if instant is None:
        return False  # protected builtin scheduled admission must carry its slot
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
        if not job.get("execution_id"):
            execution = scheduler.create_execution(
                job["id"], source="direct", scheduled_instant=job.get("_scheduled_instant"))
            job["execution_id"] = execution["id"]
        return True
