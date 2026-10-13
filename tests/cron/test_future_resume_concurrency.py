"""Real thread barriers at the native store/fire/worker registration seams."""
import copy
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from datetime import timedelta

import pytest

from cron import jobs, scheduler
from cron.future_dispatch import dispatch_registration
from cron.future_resume import inspect_job_revision, resume_future_only, _store_home
from tests.cron import test_future_resume as support
from tests.cron.test_future_resume import raw, request

paused_store = support.paused_store


def scoped(home, function, *args):
    with jobs.use_cron_store(home), _store_home():
        return function(*args)


def stale_snapshot(jid):
    snapshot = copy.deepcopy(raw(jid))
    snapshot.update(enabled=True, state="scheduled", _scheduled_instant=snapshot["next_run_at"])
    return snapshot


def stage_barrier(monkeypatch):
    entered, release = Event(), Event()
    stage = jobs._stage_jobs_payload
    def blocked(*args):
        result = stage(*args)
        entered.set()
        assert release.wait(5), "test barrier timed out"
        return result
    monkeypatch.setattr(jobs, "_stage_jobs_payload", blocked)
    return entered, release


def test_resume_wins_stale_submission_rejected(paused_store, monkeypatch):
    jid, _, _, home = paused_store
    snapshot = stale_snapshot(jid)
    revision = inspect_job_revision(jid)["revision"]
    entered, release = stage_barrier(monkeypatch)
    effects = []
    with ThreadPoolExecutor(3) as racers, ThreadPoolExecutor(1) as workers:
        resume = racers.submit(scoped, home, lambda: resume_future_only(
            jid, expected_revision=revision, request_id="race"))
        assert entered.wait(5)
        attempted = Event()
        def submit():
            attempted.set()
            return scheduler._submit_with_guard(snapshot, workers, effects.append)
        queued = racers.submit(scoped, home, submit)
        assert attempted.wait(5)
        release.set()
        assert resume.result(5)["success"]
        assert queued.result(5) is None
    assert effects == []


def test_queued_ledger_blocks_resume(paused_store):
    jid, _, _, _ = paused_store
    blocker, occupied = Event(), Event()
    def occupy():
        occupied.set()
        assert blocker.wait(5)
    with ThreadPoolExecutor(1) as workers:
        holding = workers.submit(occupy)
        assert occupied.wait(5)
        try:
            queued = scheduler._submit_with_guard(stale_snapshot(jid), workers,
                lambda job: scheduler._process_due_job(job, None, None, False))
            assert queued is not None
            assert request(jid)["error"] == "BLOCKED_TARGET_JOB_NOT_QUIESCENT"
        finally:
            blocker.set()
        holding.result(5)
        queued.result(5)
    assert raw(jid)["enabled"] is False


def test_two_resumes_same_operation_are_idempotent(paused_store):
    jid, _, _, home = paused_store
    revision = inspect_job_revision(jid)["revision"]
    start = Barrier(2)
    def restore():
        start.wait(5)
        return resume_future_only(jid, expected_revision=revision, request_id="same-race")
    with ThreadPoolExecutor(2) as pool:
        attempts = [pool.submit(scoped, home, restore) for _ in range(2)]
        results = [future.result(5) for future in attempts]
    assert all(result["success"] for result in results)
    assert sum(result["replayed"] for result in results) == 1
    assert raw(jid)["dispatch_generation"] == 1


def test_two_different_resumes_only_one_cas_wins(paused_store):
    jid, _, _, home = paused_store
    revision = inspect_job_revision(jid)["revision"]
    start = Barrier(2)
    def restore(token):
        start.wait(5)
        return resume_future_only(jid, expected_revision=revision, request_id=token)
    with ThreadPoolExecutor(2) as pool:
        attempts = [pool.submit(scoped, home, restore, token) for token in ("first", "second")]
        results = [future.result(5) for future in attempts]
    assert sum(result["success"] for result in results) == 1
    assert raw(jid)["dispatch_generation"] == 1


def test_stale_advance_waiting_at_commit_is_rejected(paused_store, monkeypatch):
    jid, _, _, home = paused_store
    revision = inspect_job_revision(jid)["revision"]
    entered, release = stage_barrier(monkeypatch)
    with ThreadPoolExecutor(2) as pool:
        restore = pool.submit(scoped, home, lambda: resume_future_only(
            jid, expected_revision=revision, request_id="advance-race"))
        assert entered.wait(5)
        started = Event()
        def advance():
            started.set()
            return jobs.advance_next_runs([jid])
        stale = pool.submit(scoped, home, advance)
        assert started.wait(5)
        release.set()
        assert restore.result(5)["success"]
        assert stale.result(5) == 0
    assert raw(jid)["dispatch_generation"] == 1


def test_pause_serializes_after_resume(paused_store, monkeypatch):
    jid, _, _, home = paused_store
    revision = inspect_job_revision(jid)["revision"]
    entered, release = stage_barrier(monkeypatch)
    with ThreadPoolExecutor(2) as pool:
        restore = pool.submit(scoped, home, lambda: resume_future_only(
            jid, expected_revision=revision, request_id="pause-race"))
        assert entered.wait(5)
        pause_started = Event()
        def pause():
            pause_started.set()
            return jobs.pause_job(jid)
        paused = pool.submit(scoped, home, pause)
        assert pause_started.wait(5)
        release.set()
        assert restore.result(5)["success"]
        assert paused.result(5)["enabled"] is False
    assert raw(jid)["state"] == "paused"


def test_fire_claim_waiting_on_resume_fails_generation(paused_store, monkeypatch):
    jid, _, _, home = paused_store
    revision = inspect_job_revision(jid)["revision"]
    entered, release = stage_barrier(monkeypatch)
    with ThreadPoolExecutor(2) as pool:
        restore = pool.submit(scoped, home, lambda: resume_future_only(
            jid, expected_revision=revision, request_id="claim-race"))
        assert entered.wait(5)
        claim_started = Event()
        def claim():
            claim_started.set()
            return jobs.claim_job_for_fire(jid, expected_generation=0, return_job=True)
        stale = pool.submit(scoped, home, claim)
        assert claim_started.wait(5)
        release.set()
        assert restore.result(5)["success"]
        assert stale.result(5) is False
    assert raw(jid).get("fire_claim") is None


def test_due_snapshot_after_resume_cannot_handoff(paused_store, monkeypatch):
    jid, _, _, _ = paused_store
    jobs.update_job(jid, {"enabled": True, "state": "scheduled", "paused_at": None, "paused_reason": None,
                         "next_run_at": (paused_store[2][0] - timedelta(minutes=1)).isoformat()})
    snapshot = next(job for job in jobs.get_due_jobs() if job["id"] == jid)
    jobs.pause_job(jid)
    assert request(jid)["success"]
    launched = []
    monkeypatch.setattr(scheduler, "_launch_external_cron_worker", launched.append)
    assert scheduler.run_one_job(snapshot) is False
    assert launched == []


def test_tick_captured_snapshot_is_fenced(paused_store, monkeypatch):
    from cron import scheduler_tick
    jid, _, _, home = paused_store
    jobs.update_job(jid, {"enabled": True, "state": "scheduled", "paused_at": None, "paused_reason": None,
                         "next_run_at": (paused_store[2][0] - timedelta(minutes=1)).isoformat()})
    entered, release = Event(), Event()
    submit = scheduler._submit_with_guard
    def blocked(job, pool, callback):
        entered.set()
        assert release.wait(5)
        return submit(job, pool, callback)
    monkeypatch.setattr(scheduler, "_submit_with_guard", blocked)
    monkeypatch.setattr(scheduler, "_should_yield_tick_to_fresh_gateway", lambda: None)
    monkeypatch.setattr(scheduler, "_maybe_reap_dead_owners", lambda: None)
    monkeypatch.setattr(scheduler, "_maybe_run_worktree_maintenance", lambda: None)
    monkeypatch.setattr(scheduler, "_sweep_mcp_orphans", lambda: None)
    with ThreadPoolExecutor(1) as racer, ThreadPoolExecutor(1) as workers:
        monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda *args: workers)
        tick = racer.submit(scoped, home, lambda: scheduler_tick.tick(verbose=False))
        assert entered.wait(5)
        jobs.pause_job(jid)
        assert request(jid)["success"]
        release.set()
        assert tick.result(5) == 0


def test_profile_scoped_same_id_concurrent_resume(paused_store, tmp_path):
    jid, _, _, home = paused_store
    second = tmp_path / "second-profile"
    record = copy.deepcopy(raw(jid))
    with jobs.use_cron_store(second):
        jobs.save_jobs([record])
    start = Barrier(2)
    def restore():
        start.wait(5)
        return request(jid)
    with ThreadPoolExecutor(2) as pool:
        futures = [pool.submit(scoped, selected, restore) for selected in (home, second)]
        assert all(future.result(5)["success"] for future in futures)
    with jobs.use_cron_store(second):
        assert raw(jid)["dispatch_generation"] == 1
    assert raw(jid)["dispatch_generation"] == 1


def test_current_generation_still_rejects_historical_instant(paused_store):
    jid, _, _, _ = paused_store
    historical = raw(jid)["next_run_at"]
    assert request(jid)["success"]
    snapshot = copy.deepcopy(raw(jid))
    snapshot["_scheduled_instant"] = historical
    with dispatch_registration(snapshot) as admitted:
        assert admitted is False


def test_running_worker_blocks_restore(paused_store):
    jid, _, _, _ = paused_store
    running, release = Event(), Event()
    effects = []
    def body(job):
        running.set()
        assert release.wait(5)
        effects.append(job["id"])
    with ThreadPoolExecutor(1) as worker:
        future = scheduler._submit_with_guard(stale_snapshot(jid), worker, body)
        assert running.wait(5)
        try:
            assert request(jid)["error"] == "BLOCKED_TARGET_JOB_NOT_QUIESCENT"
        finally:
            release.set()
        future.result(5)
    # This earlier admitted isolated callback is not cancelled or reclassified.
    assert effects == [jid] and raw(jid)["enabled"] is False


def test_current_future_slot_admitted_once_by_running_guard(paused_store):
    jid, _, clock, _ = paused_store
    assert request(jid)["success"]
    clock[0] = support.datetime.fromisoformat(raw(jid)["next_run_at"])
    snapshot = next(job for job in jobs.get_due_jobs() if job["id"] == jid)
    effects, running, release = [], Event(), Event()
    def body(job):
        running.set()
        assert release.wait(5)
        effects.append(job["_scheduled_instant"])
    with ThreadPoolExecutor(2) as workers:
        future = scheduler._submit_with_guard(snapshot, workers, body)
        assert running.wait(5)
        try:
            assert scheduler._submit_with_guard(snapshot, workers, body) is None
        finally:
            release.set()
        future.result(5)
    assert effects == [snapshot["_scheduled_instant"]]


def test_unrelated_job_can_register_while_resume_waits_for_target_fence(paused_store):
    jid, other, _, home = paused_store
    started = Event()
    before = copy.deepcopy(raw(other))
    with ThreadPoolExecutor(1) as pool:
        with jobs._fire_job_lock(jid) as acquired:
            assert acquired
            revision = inspect_job_revision(jid)["revision"]
            def restore():
                started.set()
                return resume_future_only(jid, expected_revision=revision, request_id="other-job")
            waiting = pool.submit(scoped, home, restore)
            assert started.wait(5)
            with dispatch_registration(stale_snapshot(other)) as admitted:
                assert admitted
        assert waiting.result(5)["success"]
    assert raw(other) == before
