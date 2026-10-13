"""Post-rename admission records in the EXISTING profile execution ledger.

The Job JSON replace is the lifecycle commit. A ledger confirmation sampled
AFTER its durable completion bounds that commit from above. No confirmation
means no execution, including after an interrupted or expired publication.
"""
import hashlib
import json
import sqlite3
from datetime import datetime


def initialize(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS resume_admissions (
        job_id TEXT NOT NULL, generation INTEGER NOT NULL, operation_sha256 TEXT NOT NULL,
        confirmed_after_replace TEXT NOT NULL, PRIMARY KEY(job_id, generation))""")


def operation_digest(operation):
    return hashlib.sha256(json.dumps(operation, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def confirmed(job, conn=None):
    operation = job.get("future_resume") or {}
    if operation.get("protocol") != "post_replace_v2":
        return False
    if conn is None:
        from cron.executions import _transaction
        from cron.future_resume import _store_home
        try:
            with _store_home(), _transaction() as connection:
                return confirmed(job, connection)
        except (OSError, sqlite3.Error, ValueError):
            return False
    row = conn.execute("SELECT * FROM resume_admissions WHERE job_id=? AND generation=?",
                       (job["id"], job.get("dispatch_generation"))).fetchone()
    return bool(row and row["operation_sha256"] == operation_digest(operation)
                and datetime.fromisoformat(row["confirmed_after_replace"]) <
                datetime.fromisoformat(operation["next_run_at"]))


def confirm_after_replace(job, conn, checked_at, checked_mono, next_run):
    from cron import jobs
    from cron.future_resume import _validate_time
    _validate_time(checked_at, checked_mono, next_run)
    # The sample is AFTER directory fsync / replace, never a pre-write estimate.
    after = jobs._hermes_now()
    if next_run <= after:
        from cron.future_resume import ResumeBlocked
        raise ResumeBlocked("BLOCKED_SLOT_EXPIRED_AT_COMMIT")
    conn.execute("INSERT INTO resume_admissions VALUES (?, ?, ?, ?)",
                 (job["id"], job["dispatch_generation"], operation_digest(job["future_resume"]),
                  after.isoformat()))


def record_execution_admission(execution_id, job):
    from cron.executions import _transaction
    with _transaction() as conn:
        conn.execute("UPDATE executions SET dispatch_generation=?, fire_owner=? WHERE id=? AND status='claimed'",
                     (job.get("dispatch_generation", 0), (job.get("fire_claim") or {}).get("by"), execution_id))


def valid_fire(job, expected_generation, *, manual, instant=None):
    if not job.get("dispatch_generation"):
        return not expected_generation
    if not confirmed(job):
        return False
    if manual:
        return expected_generation is None or expected_generation == job["dispatch_generation"]
    if expected_generation != job["dispatch_generation"] or instant is None:
        return False
    try:
        return datetime.fromisoformat(instant) >= datetime.fromisoformat(job["future_resume"]["next_run_at"])
    except (ValueError, TypeError):
        return False
