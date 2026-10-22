"""Durable admission faults reopen the actual store in independent processes."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest
from cron import jobs

ROOT = Path(__file__).resolve().parents[2]


def reopen(home, job_id):
    script = """
import json, sys
from cron import jobs
from cron.rearm_admission import admitted
j = jobs.get_job(sys.argv[1])
print(json.dumps({'enabled': j['enabled'], 'marker': jobs._rearm_dispatch_is_blocked(j['id']),
                  'admitted': admitted(jobs._current_cron_store().cron_dir, j),
                  'claim': jobs.claim_job_for_fire(j['id'], return_job=True),
                  'force': jobs.claim_job_for_fire(j['id'], force=True, return_job=True),
                  'manual': jobs.claim_job_for_fire(j['id'], manual=True, return_job=True)}))
"""
    child = subprocess.run([sys.executable, "-c", script, job_id], cwd=ROOT,
                           env={**os.environ, "HERMES_HOME": str(home)},
                           capture_output=True, text=True, timeout=20, check=False)
    assert child.returncode == 0, child.stderr
    return json.loads(child.stdout)


@pytest.mark.parametrize("fault", ["unlink-interrupt", "readback-error", "readback-mismatch",
                                    "commit-denied", "approval-readback-mismatch"])
def test_uncommitted_active_record_never_admitted_after_reopen(tmp_path, monkeypatch, fault):
    from hermes_cli import sqlite_util
    home = tmp_path / "home"
    monkeypatch.setenv("HERMES_HOME", str(home))
    with jobs.use_cron_store(home):
        target = jobs.create_job(prompt="synthetic only", schedule="every 15m", deliver="local")
        job_id = target["id"]
        jobs.pause_job(job_id, "durable faults")
        real_save, real_clear, real_peek = jobs._save_jobs_unlocked, jobs._clear_rearm_dispatch_block, jobs._peek_jobs_unlocked
        real_open = sqlite_util.open_db
        published = False

        def save(rows, **kwargs):
            nonlocal published
            if published:
                raise OSError("all rollback writes unavailable")
            result = real_save(rows, **kwargs)
            published = True
            return result

        def clear(target):
            real_clear(target)
            if fault == "unlink-interrupt":
                raise KeyboardInterrupt("after marker unlink")

        def peek():
            if published and fault == "readback-error":
                raise OSError("unreadable publication")
            rows = real_peek()
            if published and fault == "readback-mismatch":
                rows = [dict(row, rearm_generation="different-generation") for row in rows]
            return rows

        def open_db(path, **kwargs):
            conn = real_open(path, **kwargs)
            if Path(path).name == "rearm-admissions.db" and fault == "commit-denied":
                conn.set_authorizer(lambda action, arg, *_args:
                                    sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_TRANSACTION and arg == "COMMIT"
                                    else sqlite3.SQLITE_OK)
            return conn

        with monkeypatch.context() as failures:
            failures.setattr(jobs, "_save_jobs_unlocked", save)
            failures.setattr(jobs, "_clear_rearm_dispatch_block", clear)
            failures.setattr(jobs, "_peek_jobs_unlocked", peek)
            failures.setattr(sqlite_util, "open_db", open_db)
            if fault == "approval-readback-mismatch":
                def corrupt_approval(target):
                    real_clear(target)
                    # The live publication connection is captured by open_db, not a second writer.
                    approval_conn.execute("UPDATE admissions SET identity='wrong'")
                def capture(path, **kwargs):
                    nonlocal approval_conn
                    conn = real_open(path, **kwargs)
                    if Path(path).name == "rearm-admissions.db":
                        approval_conn = conn
                    return conn
                approval_conn = None
                failures.setattr(sqlite_util, "open_db", capture)
                failures.setattr(jobs, "_clear_rearm_dispatch_block", corrupt_approval)
            revision = hashlib.sha256(jobs._current_cron_store().jobs_file.read_bytes()).hexdigest()
            with pytest.raises(RuntimeError, match="durable Job state is uncertain"):
                jobs.rearm_recurring_future_only(job_id, request_id="durable-fault", expected_revision=revision)
        actual = reopen(home, job_id)
        assert actual["enabled"] is True, "prove admission without relying on successful Pause"
        if fault in {"unlink-interrupt", "commit-denied", "approval-readback-mismatch"}:
            assert actual["marker"] is False, "prove denial without relying on a marker"
        assert actual["admitted"] is False
        assert actual["claim"] is False and actual["force"] is False and actual["manual"] is False


@pytest.mark.parametrize("broken", ["missing", "corrupt", "mismatched"])
def test_committed_job_denied_when_admission_store_unknown(tmp_path, monkeypatch, broken):
    home = tmp_path / "home"
    monkeypatch.setenv("HERMES_HOME", str(home))
    with jobs.use_cron_store(home):
        target = jobs.create_job(prompt="synthetic", schedule="every 15m", deliver="local")
        job_id = target["id"]
        jobs.pause_job(job_id)
        revision = hashlib.sha256(jobs._current_cron_store().jobs_file.read_bytes()).hexdigest()
        jobs.rearm_recurring_future_only(job_id, request_id="admission-loss", expected_revision=revision)
        db = home / "cron" / "rearm-admissions.db"
        if broken == "missing":
            db.rename(db.with_suffix(".saved"))
        elif broken == "corrupt":
            db.write_bytes(b"not sqlite")
        else:
            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE admissions SET identity='wrong'")
        actual = reopen(home, job_id)
        assert actual["enabled"] is True and actual["marker"] is False
        assert actual["admitted"] is False and actual["claim"] is False
