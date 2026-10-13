"""Legacy resume policy, intentionally separate from future-only admission."""
from typing import Any, Optional

def resume_job(job_id: str) -> Optional[dict[str, Any]]:
    """Resume a paused job. Accepts a job ID or name.

    A recurring job paused across one of its slots must not lose that slot silently: the stored
    ``next_run_at`` (already past) survives resume as the due instant, and the ordinary late /
    catch-up / ``cron.catch_up_missed`` policy in the due scan decides what happens to it — one
    fire or a logged skip, never a silent re-anchor past it (#113603). One-shots and future
    instants recompute from now as before.
    """
    from cron import jobs
    job = jobs.resolve_job_ref(job_id)
    if not job:
        return None
    stored_next = job.get("next_run_at")
    stored_dt = jobs._parse_aware(stored_next) if stored_next else None
    if (
        job["schedule"].get("kind") in {"cron", "interval"}
        and stored_dt is not None
        and jobs._instant_at_or_before(stored_dt, jobs._hermes_now())
    ):
        next_run_at = stored_next
        jobs.logger.info(
            "Job '%s' resumed with occurrence %s that elapsed while paused kept due; the next "
            "tick fires it (late/catch-up) or logs the skip.",
            job.get("name", job["id"]), stored_next)
    else:
        next_run_at = jobs.compute_next_run(job["schedule"])
    if next_run_at is None and job["schedule"].get("kind") == "once":
        run_at = job["schedule"].get("run_at", "unknown")
        raise ValueError(
            f"Cannot resume: one-shot time {run_at} is in the past "
            f"(grace window: {jobs.ONESHOT_GRACE_SECONDS}s) and will never fire.")
    return jobs.update_job(job["id"], {
        "enabled": True,
        "state": "scheduled",
        "paused_at": None,
        "paused_reason": None,
        "next_run_at": next_run_at,
    })
