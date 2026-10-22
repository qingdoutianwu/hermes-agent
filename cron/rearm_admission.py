"""Durable rearm admission identities; marker absence is never a commit.

The Job Store publishes a generation before its approval transaction commits. A crashed or
interrupted publisher leaves no visible approval, even if marker cleanup already succeeded.
The SQLite COMMIT is the linearization point: a lost API acknowledgement after that point is
not an uncommitted job. Claim resolves the durable outcome, never an in-memory success flag.
Native tick/fire/jobs file fences still serialize scheduling; this is not a lock manager.
"""
from __future__ import annotations

import contextlib
import json
from pathlib import Path
import sqlite3


_SCHEMA = """CREATE TABLE IF NOT EXISTS admissions (
    job_id TEXT NOT NULL, generation TEXT NOT NULL, identity TEXT NOT NULL,
    PRIMARY KEY(job_id, generation))"""


def identity(job: dict) -> str:
    """Bind immutable rearm intent, not next_run_at which legitimate ticks advance."""
    return json.dumps({"job_id": job["id"], "generation": job["rearm_generation"],
                       "schedule": job["schedule"], "admission": job["rearm_admission"]},
                      sort_keys=True, separators=(",", ":"))


@contextlib.contextmanager
def publication(cron_dir: Path, job: dict):
    """Approval becomes visible only after every publication/readback/cleanup step succeeds."""
    from hermes_cli.sqlite_util import open_db, transaction

    conn = open_db(cron_dir / "rearm-admissions.db", db_label="cron/rearm-admissions.db",
                   wal=False, synchronous_full=True)
    try:
        conn.execute(_SCHEMA)
    except BaseException:
        conn.close()
        raise
    with transaction(conn, immediate=True):
        value = identity(job)
        conn.execute("INSERT INTO admissions(job_id,generation,identity) VALUES(?,?,?)",
                     (str(job["id"]), job["rearm_generation"], value))
        yield
        actual = conn.execute("SELECT identity FROM admissions WHERE job_id=? AND generation=?",
                              (str(job["id"]), job["rearm_generation"])).fetchone()
        if actual is None or actual[0] != value:
            raise RuntimeError("rearm durable admission readback mismatch")


def admitted(cron_dir: Path, job: dict) -> bool:
    """Read-only fresh connection: absent, malformed, locked or unreadable means deny."""
    if not job.get("rearm_generation"):
        return "rearm_admission" not in job  # legacy jobs have no rearm publication
    try:
        value = identity(job)
        path = cron_dir / "rearm-admissions.db"
        conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.1)
        try:
            row = conn.execute("SELECT identity FROM admissions WHERE job_id=? AND generation=?",
                               (str(job["id"]), job["rearm_generation"])).fetchone()
            return row is not None and row[0] == value
        finally:
            conn.close()
    except (OSError, sqlite3.Error, KeyError, TypeError, ValueError):
        return False
