"""Strict-lock and repeated readback fault matrix for R2."""
from pathlib import Path
import hashlib
import os
import subprocess
import sys

import pytest
from cron import jobs


@pytest.fixture
def paused(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    with jobs.use_cron_store(home):
        target = jobs.create_job(prompt="synthetic", schedule="every 15m", deliver="local")
        jobs.pause_job(target["id"], "fault matrix")
        yield home, target["id"]


def rearm(job_id):
    revision = hashlib.sha256(jobs._current_cron_store().jobs_file.read_bytes()).hexdigest()
    return jobs.rearm_recurring_future_only(job_id, request_id="r2-matrix", expected_revision=revision)


@pytest.mark.parametrize("failure", [None, "io-error"])
def test_missing_backend_or_io_error_refuses(paused, monkeypatch, failure):
    _home, job_id = paused
    acquire = jobs._acquire_flock

    def fault(fd, timeout):
        if str(fd.name).endswith(".jobs.lock"):
            if failure == "io-error":
                raise OSError("lock I/O failed")
            return None
        return acquire(fd, timeout)

    before = jobs._current_cron_store().jobs_file.read_bytes()
    monkeypatch.setattr(jobs, "_acquire_flock", fault)
    with pytest.raises(RuntimeError, match="strict Job Store lock"):
        rearm(job_id)
    assert jobs._current_cron_store().jobs_file.read_bytes() == before


@pytest.mark.parametrize("read_failures", [1, 2])
def test_readback_failures_deny_after_reopen(paused, monkeypatch, read_failures):
    home, job_id = paused
    real_save, real_peek = jobs._save_jobs_unlocked, jobs._peek_jobs_unlocked
    published = False
    remaining = read_failures

    def peek():
        nonlocal remaining
        if published and remaining:
            remaining -= 1
            raise OSError("injected readback failure")
        return real_peek()

    def save(rows, **kwargs):
        nonlocal published
        result = real_save(rows, **kwargs)
        published = True
        return result

    with monkeypatch.context() as faults:
        faults.setattr(jobs, "_save_jobs_unlocked", save)
        faults.setattr(jobs, "_peek_jobs_unlocked", peek)
        with pytest.raises((RuntimeError, OSError)):
            rearm(job_id)
    child = subprocess.run(
        [sys.executable, "-c", "from cron.jobs import claim_job_for_fire; import sys; "
         "assert claim_job_for_fire(sys.argv[1], return_job=True) is False", job_id],
        cwd=Path(__file__).resolve().parents[2],
        env={**os.environ, "HERMES_HOME": str(home)}, capture_output=True, text=True, timeout=20, check=False)
    assert child.returncode == 0, child.stderr


def test_nested_strict_lock_refuses_degraded_lock(paused, monkeypatch):
    _home, job_id = paused
    with monkeypatch.context() as faults:
        faults.setattr(jobs, "_acquire_flock", lambda *_args: False)
        with jobs._jobs_lock():
            with pytest.raises(RuntimeError, match="strict Job Store lock"):
                with jobs._jobs_lock(strict=True):
                    pytest.fail("degraded lock certified strict")
    assert jobs.get_job(job_id)["enabled"] is False


def test_post_unlink_interrupt_with_unwritable_marker_stays_closed(paused, monkeypatch):
    _home, job_id = paused
    real_clear, real_set = jobs._clear_rearm_dispatch_block, jobs._set_rearm_dispatch_block
    cleared = False

    def clear_then_interrupt(target):
        nonlocal cleared
        real_clear(target)
        cleared = True
        raise KeyboardInterrupt("interrupt after committed unlink")

    def set_block(target):
        if cleared:
            raise OSError("marker store became unwritable")
        return real_set(target)

    with monkeypatch.context() as faults:
        faults.setattr(jobs, "_clear_rearm_dispatch_block", clear_then_interrupt)
        faults.setattr(jobs, "_set_rearm_dispatch_block", set_block)
        with pytest.raises(RuntimeError, match="durable Job state is uncertain"):
            rearm(job_id)
    assert jobs.claim_job_for_fire(job_id, return_job=True) is False
