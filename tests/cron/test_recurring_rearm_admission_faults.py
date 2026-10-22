"""Admission safety probes for the isolated recurring-rearm candidate."""
from datetime import UTC, datetime, timedelta
import hashlib

import pytest

from cron import jobs


@pytest.fixture
def paused_store(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    with jobs.use_cron_store(home):
        job = jobs.create_job(prompt="synthetic only", schedule="every 15m", deliver="local")
        jobs.pause_job(job["id"], "isolated fault test")
        yield home, job["id"]


def rearm(job_id):
    revision = hashlib.sha256(jobs._current_cron_store().jobs_file.read_bytes()).hexdigest()
    return jobs.rearm_recurring_future_only(
        job_id, request_id="admission-fault-probe", expected_revision=revision)


def test_result_error_and_failed_rollback_cannot_reopen_dispatch(paused_store, monkeypatch):
    _home, job_id = paused_store
    real_save = jobs._save_jobs_unlocked
    saves = 0

    def save(rows, **kwargs):
        nonlocal saves
        saves += 1
        if saves > 1:
            raise OSError("rollback unavailable")
        return real_save(rows, **kwargs)

    def normalize(_record):
        raise RuntimeError("result normalization failed")

    with monkeypatch.context() as faults:
        faults.setattr(jobs, "_save_jobs_unlocked", save)
        faults.setattr(jobs, "_normalize_job_record", normalize)
        with pytest.raises(RuntimeError, match="durable Job state is uncertain"):
            rearm(job_id)
    assert jobs.claim_job_for_fire(job_id, return_job=True) is False


@pytest.mark.parametrize("pending", ["foreign-live", "malformed"])
def test_unresolved_pending_ownership_refuses_rearm(paused_store, pending):
    _home, job_id = paused_store
    now = datetime.now(UTC)
    rows = jobs.load_jobs()
    rows[0]["pending_slot"] = (
        {"scheduled_at": (now - timedelta(minutes=1)).isoformat(),
         "at": now.isoformat(), "by": "remote-owner:12345"}
        if pending == "foreign-live" else {"scheduled_at": "unknown"})
    jobs.save_jobs(rows)
    before = jobs._current_cron_store().jobs_file.read_bytes()
    with pytest.raises((ValueError, RuntimeError), match="pending"):
        rearm(job_id)
    assert jobs._current_cron_store().jobs_file.read_bytes() == before


def test_mismatched_readback_cannot_be_certified(paused_store, monkeypatch):
    _home, job_id = paused_store
    real_save, real_peek = jobs._save_jobs_unlocked, jobs._peek_jobs_unlocked
    published = False

    def save(rows, **kwargs):
        nonlocal published
        result = real_save(rows, **kwargs)
        published = True
        return result

    def peek():
        rows = real_peek()
        if published:
            rows = [dict(row) for row in rows]
            rows[0]["rearm_generation"] = "unrelated-publication"
        return rows

    monkeypatch.setattr(jobs, "_save_jobs_unlocked", save)
    monkeypatch.setattr(jobs, "_peek_jobs_unlocked", peek)
    with pytest.raises(RuntimeError):
        rearm(job_id)


def test_post_unlink_interrupt_and_failed_rollback_cannot_admit(paused_store, monkeypatch):
    _home, job_id = paused_store
    real_clear, real_save = jobs._clear_rearm_dispatch_block, jobs._save_jobs_unlocked
    saves = 0

    def save(rows, **kwargs):
        nonlocal saves
        saves += 1
        if saves > 1:
            raise OSError("rollback unavailable")
        return real_save(rows, **kwargs)

    def clear_then_interrupt(target):
        real_clear(target)
        raise KeyboardInterrupt("interrupted after unlink")

    with monkeypatch.context() as faults:
        faults.setattr(jobs, "_save_jobs_unlocked", save)
        faults.setattr(jobs, "_clear_rearm_dispatch_block", clear_then_interrupt)
        with pytest.raises(RuntimeError, match="durable Job state is uncertain"):
            rearm(job_id)
    assert jobs.claim_job_for_fire(job_id, return_job=True) is False


def test_job_store_lock_failure_refuses_rearm(paused_store, monkeypatch):
    _home, job_id = paused_store
    real_acquire = jobs._acquire_flock

    def acquire(fd, timeout):
        if str(fd.name).endswith(".jobs.lock"):
            return False
        return real_acquire(fd, timeout)

    before = jobs._current_cron_store().jobs_file.read_bytes()
    monkeypatch.setattr(jobs, "_acquire_flock", acquire)
    with pytest.raises(RuntimeError):
        rearm(job_id)
    assert jobs._current_cron_store().jobs_file.read_bytes() == before


def test_old_backstop_snapshot_cannot_claim_after_rearm(paused_store, monkeypatch):
    from cron import scheduler_provider
    import threading

    _home, job_id = paused_store
    now = datetime.now(UTC)
    monkeypatch.setattr(jobs, "_hermes_now", lambda: now)
    jobs.resume_job(job_id)
    jobs.update_job(job_id, {"next_run_at": (now - timedelta(hours=1)).isoformat()})
    snapshot = jobs.load_jobs()
    jobs.pause_job(job_id, "discard overdue intent")
    rearm(job_id)
    real_load = jobs.load_jobs
    scans = 0
    effects = threading.Event()

    def load():
        nonlocal scans
        scans += 1
        return snapshot if scans == 1 else real_load()

    class Provider(scheduler_provider.CronScheduler):
        @property
        def name(self):
            return "isolated-backstop"

        def start(self, stop_event, **kwargs):
            return None

        def fire_claimed(self, claimed_job, **kwargs):
            effects.set()
            return True

    monkeypatch.setattr(jobs, "load_jobs", load)
    count = scheduler_provider.fire_overdue_jobs(Provider(), now=now)
    if count:
        assert effects.wait(10), "synthetic dispatch callback did not finish"
    assert count == 0, "old backstop intent was admitted after future-only rearm"
    assert not effects.is_set()
