"""Behavioral contract for recurring future-only rearm."""
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import Future
from pathlib import Path

import pytest

from cron import jobs
from cron import executions
from cron.jobs import create_job, get_job, load_jobs, pause_job, save_jobs




@pytest.fixture
def isolated_cron(tmp_path, monkeypatch):
    from cron.jobs import _CronStorePaths, _cron_store_override

    home = tmp_path / "hermes"
    home.mkdir()
    cron_dir = home / "cron"
    cron_dir.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    token = _cron_store_override.set(_CronStorePaths.for_dir(cron_dir))
    yield home
    _cron_store_override.reset(token)


def _rearm(job_id, request_id, *, expected_revision=None):
    if expected_revision is None:
        expected_revision = hashlib.sha256(
            jobs._current_cron_store().jobs_file.read_bytes()).hexdigest()
    return jobs.rearm_recurring_future_only(
        job_id, request_id=request_id, expected_revision=expected_revision
    )


def test_rearm_is_strictly_future_clears_pending_and_keeps_siblings(isolated_cron, monkeypatch):
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    monkeypatch.setattr(jobs, "_hermes_now", lambda: now)
    job = create_job(prompt="synthetic", schedule="every 15m", name="future", deliver="local")
    sibling = create_job(prompt="sibling", schedule="every 1h", name="sibling", deliver="local")
    stale = (now - timedelta(hours=1)).isoformat()
    rows = load_jobs()
    target = next(row for row in rows if row["id"] == job["id"])
    dispatch_at = (now - timedelta(hours=1)).isoformat()
    # A pending occurrence may be explicitly discarded only when its dispatch stamp is internally
    # consistent and its owner is this process; malformed/foreign live ownership is tested below.
    old_owner = jobs._machine_id()
    pending = {"scheduled_at": stale, "at": dispatch_at, "by": old_owner}
    target.update(next_run_at=stale, pending_slot=pending)
    target.pop("last_dispatch", None)
    target["last_dispatch"] = {"scheduled_at": stale, "dispatched_at": dispatch_at}

    save_jobs(rows)
    pause_job(job["id"], "maintenance")
    rows = load_jobs()
    next(row for row in rows if row["id"] == job["id"])["pending_slot"] = {
        "scheduled_at": stale, "at": dispatch_at, "by": old_owner
    }
    save_jobs(rows)
    sibling_before = get_job(sibling["id"])

    result = _rearm(job["id"], "request-success")

    assert result["enabled"] is True and result["state"] == "scheduled"
    assert datetime.fromisoformat(result["next_run_at"]) > now
    assert result["schedule"] == job["schedule"]
    assert result.get("pending_slot") is None
    assert get_job(sibling["id"]) == sibling_before
    assert get_job(job["id"])["id"] == job["id"]


@pytest.mark.parametrize("age", [timedelta(minutes=4), timedelta(hours=2)])
def test_rearm_never_returns_an_overdue_occurrence_in_either_grace_band(
    isolated_cron, monkeypatch, age
):
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    monkeypatch.setattr(jobs, "_hermes_now", lambda: now)
    job = create_job(prompt="synthetic", schedule="every 15m", name="grace", deliver="local")
    jobs.update_job(job["id"], {"next_run_at": (now - age).isoformat()})
    pause_job(job["id"], "maintenance")
    result = _rearm(job["id"], f"grace-{int(age.total_seconds())}")
    assert datetime.fromisoformat(result["next_run_at"]) > now
    assert result["next_run_at"] != (now - age).isoformat()
    due = jobs.get_due_jobs()
    assert job["id"] not in {row["id"] for row in due}


def test_rearm_rejects_live_execution_and_write_failure_keeps_paused(isolated_cron, monkeypatch):
    job = create_job(prompt="synthetic", schedule="every 15m", name="guard", deliver="local")
    pause_job(job["id"], "maintenance")
    execution = executions.create_execution(job["id"], source="builtin")
    assert executions.mark_execution_running(execution["id"])
    with pytest.raises(ValueError, match="nonterminal execution"):
        _rearm(job["id"], "request-inflight")
    paused = get_job(job["id"])
    assert paused["enabled"] is False and paused["state"] == "paused"
    assert executions.finish_execution(execution["id"], success=False, error="test settled")

    original = jobs._save_jobs_unlocked
    monkeypatch.setattr(
        jobs, "_save_jobs_unlocked",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("simulated store failure")),
    )
    with pytest.raises(OSError, match="simulated store failure"):
        _rearm(job["id"], "request-write-fail")
    monkeypatch.setattr(jobs, "_save_jobs_unlocked", original)
    paused = get_job(job["id"])
    assert paused["enabled"] is False and paused["state"] == "paused"


def test_persist_replace_then_raise_rolls_back_to_durable_pause(isolated_cron, monkeypatch):
    target = create_job(prompt="synthetic", schedule="every 15m", name="after-replace", deliver="local")
    pause_job(target["id"], "maintenance")
    real_save = jobs._save_jobs_unlocked
    raised = False

    def save_then_fail(*args, **kwargs):
        nonlocal raised
        result = real_save(*args, **kwargs)
        if not raised:
            raised = True
            raise OSError("post-replace failure")
        return result

    monkeypatch.setattr(jobs, "_save_jobs_unlocked", save_then_fail)
    with pytest.raises(OSError, match="post-replace failure"):
        _rearm(target["id"], "after-replace")
    monkeypatch.setattr(jobs, "_save_jobs_unlocked", real_save)
    paused = get_job(target["id"])
    assert paused is not None and paused["enabled"] is False and paused["state"] == "paused"


def test_failed_rollback_keeps_dispatch_blocked(isolated_cron, monkeypatch):
    now = {"value": datetime.now(UTC).replace(second=0, microsecond=0)}
    monkeypatch.setattr(jobs, "_hermes_now", lambda: now["value"])
    target = create_job(prompt="synthetic", schedule="every 15m", name="rollback-block", deliver="local")
    pause_job(target["id"], "maintenance")
    real_save = jobs._save_jobs_unlocked
    saves = 0

    def save_then_fail_once(rows, **kwargs):
        nonlocal saves
        saves += 1
        if saves == 1:
            real_save(rows, **kwargs)
            raise OSError("post-replace readback lost")
        if saves == 2:
            raise OSError("rollback write failed")
        return real_save(rows, **kwargs)

    monkeypatch.setattr(jobs, "_save_jobs_unlocked", save_then_fail_once)
    with pytest.raises(RuntimeError, match="durable Job state is uncertain"):
        _rearm(target["id"], "rollback-block")

    durable = get_job(target["id"])
    assert durable is not None and durable["enabled"] is True
    now["value"] = datetime.fromisoformat(durable["next_run_at"])
    assert target["id"] not in {row["id"] for row in jobs.get_due_jobs()}
    assert jobs.claim_job_for_fire(target["id"], return_job=True) is False


def test_rearm_rejects_unknown_nonterminal_execution_status(isolated_cron, monkeypatch):
    job = create_job(prompt="synthetic", schedule="every 15m", name="unknown-status", deliver="local")
    pause_job(job["id"], "maintenance")
    monkeypatch.setattr(executions, "nonterminal_execution", lambda _job_id: {"status": "recovering"})
    with pytest.raises(ValueError, match="nonterminal execution"):
        _rearm(job["id"], "unknown-status")
    paused = get_job(job["id"])
    assert paused["enabled"] is False and paused["state"] == "paused"


def test_rearm_rejects_execution_ledger_read_failure(isolated_cron, monkeypatch):
    job = create_job(prompt="synthetic", schedule="every 15m", name="ledger-error", deliver="local")
    pause_job(job["id"], "maintenance")

    def fail_ledger(_job_id):
        raise OSError("ledger unavailable")

    monkeypatch.setattr(executions, "nonterminal_execution", fail_ledger)
    with pytest.raises(OSError, match="ledger unavailable"):
        _rearm(job["id"], "ledger-error")
    paused = get_job(job["id"])
    assert paused["enabled"] is False and paused["state"] == "paused"


def test_stale_revision_and_corrupt_store_fail_closed(isolated_cron):
    job = create_job(prompt="synthetic", schedule="every 15m", name="store", deliver="local")
    pause_job(job["id"], "maintenance")
    with pytest.raises(ValueError, match="revision changed"):
        _rearm(job["id"], "stale-revision", expected_revision="0" * 64)
    path = jobs._current_cron_store().jobs_file
    path.write_text("{broken", encoding="utf-8")
    with pytest.raises(RuntimeError, match="Job Store is corrupt"):
        _rearm(job["id"], "corrupt-store")


def test_concurrent_rearm_has_one_success(isolated_cron):
    job = create_job(prompt="synthetic", schedule="every 15m", name="parallel", deliver="local")
    pause_job(job["id"], "maintenance")
    before = get_job(job["id"])
    start = threading.Barrier(3)
    outcomes = []

    def attempt(index):
        start.wait()
        try:
            outcomes.append(("ok", _rearm(job["id"], f"request-{index}")))
        except Exception as exc:
            outcomes.append(("reject", exc))

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(2)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(timeout=10)
    assert all(not thread.is_alive() for thread in threads)
    successes = [value for status, value in outcomes if status == "ok"]
    assert len(successes) == 1, repr(outcomes)
    assert len([value for status, value in outcomes if status == "reject"]) == 1
    after = get_job(job["id"])
    assert after["id"] == before["id"]
    assert after["enabled"] is True
    assert datetime.fromisoformat(after["next_run_at"]) > datetime.now(UTC)


def test_claim_rejects_due_scan_snapshot_invalidated_by_rearm(isolated_cron, monkeypatch):
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    monkeypatch.setattr(jobs, "_hermes_now", lambda: now)
    target = create_job(prompt="synthetic", schedule="every 15m", name="claim-race", deliver="local")
    old_slot = (now - timedelta(minutes=1)).isoformat()
    jobs.update_job(target["id"], {"next_run_at": old_slot})
    due = next(row for row in jobs.get_due_jobs() if row["id"] == target["id"])
    pause_job(target["id"], "maintenance")
    rearmed = _rearm(target["id"], "race-rearm")
    assert datetime.fromisoformat(rearmed["next_run_at"]) > now
    assert jobs.claim_job_for_fire(
        target["id"], return_job=True,
        expected_scheduled_instant=due["_scheduled_instant"],
        expected_rearm_generation=due.get("_rearm_generation"),
    ) is False
    fresh = get_job(target["id"])
    assert fresh["next_run_at"] == rearmed["next_run_at"]
    assert fresh.get("fire_claim") is None


def test_paused_state_survives_new_process_reload(isolated_cron):
    job = create_job(prompt="synthetic", schedule="every 15m", name="reload", deliver="local")
    pause_job(job["id"], "maintenance")
    script = "from cron.jobs import get_job; import sys; j=get_job(sys.argv[1]); print(j['state'], j['enabled'])"
    test_python = os.environ.get("HERMES_TEST_PYTHON", sys.executable)
    result = subprocess.run(
        [test_python, "-c", script, job["id"]], cwd=Path(__file__).parents[2],
        env={**os.environ, "HERMES_HOME": str(isolated_cron)}, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "paused False"


def test_native_scheduler_dispatches_one_future_occurrence_after_rearm_and_reload(isolated_cron, monkeypatch):
    start = datetime.now(UTC).replace(second=0, microsecond=0)
    clock = {"now": start}
    monkeypatch.setattr(jobs, "_hermes_now", lambda: clock["now"])
    job = create_job(prompt="synthetic", schedule="every 15m", name="dispatch-ledger", deliver="local")
    pause_job(job["id"], "maintenance")
    result = _rearm(job["id"], "scheduler-e2e")
    first_slot = datetime.fromisoformat(result["next_run_at"])
    assert first_slot > start

    child_script = '''
import json, sys
from datetime import datetime
import cron.jobs as jobs
import cron.scheduler as scheduler
from cron import executions
job_id, now_text = sys.argv[1:]
now = datetime.fromisoformat(now_text)
jobs._hermes_now = lambda: now
scheduler._hermes_now = lambda: now
scheduler._should_yield_tick_to_fresh_gateway = lambda: None
scheduler._sweep_stale_inflight_for_tick = lambda _due: None
scheduler._sweep_mcp_orphans = lambda: None
effects = []
def run_synthetic_business(record, **_kwargs):
    owner = record["fire_claim"]["by"]
    execution_id = record["execution_id"]
    assert executions.mark_execution_running(execution_id)
    effects.append(record.get("_scheduled_instant"))
    assert jobs.mark_job_run(record["id"], True, expected_fire_owner=owner)
    assert executions.finish_execution(execution_id, success=True)
    return True
scheduler.run_one_job = run_synthetic_business
count = scheduler.tick(verbose=False, sync=True)
print(json.dumps({"count": count, "effects": effects,
                  "rows": executions.list_executions(job_id=job_id)}))
'''
    child = subprocess.run(
        [os.environ.get("HERMES_TEST_PYTHON", sys.executable), "-c", child_script,
         job["id"], result["next_run_at"]],
        cwd=Path(__file__).parents[2], env={**os.environ, "HERMES_HOME": str(isolated_cron)},
        capture_output=True, text=True, timeout=30, check=False,
    )
    assert child.returncode == 0, child.stderr
    reloaded = json.loads(child.stdout.strip())
    assert reloaded["count"] == 1
    assert reloaded["effects"] == [result["next_run_at"]]
    assert len(reloaded["rows"]) == 1 and reloaded["rows"][0]["status"] == "completed"
    execution_rows = executions.list_executions(job_id=job["id"])
    assert len(execution_rows) == 1 and execution_rows[0]["status"] == "completed"
    assert execution_rows[0]["scheduled_instant"] == result["next_run_at"]

    # Let a real due scan queue a worker, then pause/rearm before that worker claims.
    from cron import scheduler
    current = get_job(job["id"])
    assert current is not None
    clock["now"] = datetime.fromisoformat(current["next_run_at"])
    monkeypatch.setattr(scheduler, "_hermes_now", lambda: clock["now"])
    monkeypatch.setattr(scheduler, "_should_yield_tick_to_fresh_gateway", lambda: None)
    monkeypatch.setattr(scheduler, "_sweep_stale_inflight_for_tick", lambda _due: None)
    monkeypatch.setattr(scheduler, "_sweep_mcp_orphans", lambda: None)
    effects = list(reloaded["effects"])

    def run_synthetic_business(record, **_kw):
        owner = record["fire_claim"]["by"]
        execution_id = record["execution_id"]
        assert executions.mark_execution_running(execution_id)
        effects.append(record.get("_scheduled_instant"))
        assert jobs.mark_job_run(record["id"], True, expected_fire_owner=owner)
        assert executions.finish_execution(execution_id, success=True)
        return True

    monkeypatch.setattr(scheduler, "run_one_job", run_synthetic_business)
    queued = []

    class DelayedPool:
        def submit(self, fn):
            future = Future()
            queued.append((fn, future))
            return future

    monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda _workers: DelayedPool())
    assert scheduler.tick(verbose=False, sync=False) == 1
    assert len(queued) == 1
    due_job = get_job(job["id"])
    assert due_job is not None and due_job.get("pending_slot") is not None
    stale_instant = due_job["pending_slot"]["scheduled_at"]
    pause_job(job["id"], "race-test")
    with pytest.raises(ValueError, match="nonterminal execution"):
        _rearm(job["id"], "scheduler-e2e-active-claim")
    paused = get_job(job["id"])
    assert paused is not None and paused["enabled"] is False and paused["state"] == "paused"

    # The queued worker sees the pause, cannot claim or run the stale slot, and settles its
    # execution row before future-only rearm can proceed.
    worker_fn, future = queued[0]
    future.set_result(worker_fn())
    assert len(effects) == 1
    stale_attempts = executions.list_executions(job_id=job["id"])
    assert len(stale_attempts) == 2
    assert sum(row["status"] == "completed" for row in stale_attempts) == 1
    stale_attempt = next(row for row in stale_attempts if row["scheduled_instant"] == stale_instant)
    assert stale_attempt["status"] == "failed"

    second = _rearm(job["id"], "scheduler-e2e-second")
    assert datetime.fromisoformat(second["next_run_at"]) > clock["now"]
    prior_effect_count = len(effects)
    assert jobs.claim_job_for_fire(
        job["id"], return_job=True, expected_scheduled_instant=stale_instant,
        expected_rearm_generation=due_job.get("rearm_generation"),
    ) is False
    assert len(effects) == prior_effect_count

    child = subprocess.run(
        [sys.executable, "-c",
         "from cron.jobs import get_job; import sys; j=get_job(sys.argv[1]); print(j['next_run_at'], j.get('pending_slot'))",
         job["id"]], cwd=Path(__file__).parents[2],
        env={**os.environ, "HERMES_HOME": str(isolated_cron)}, capture_output=True, text=True, timeout=30, check=False,
    )
    assert child.returncode == 0, child.stderr
    current_after = get_job(job["id"])
    assert current_after is not None and current_after.get("pending_slot") is None
    assert child.stdout.strip() == f"{current_after['next_run_at']} None"


def test_rearm_interface_is_exposed_through_native_cron_management_tool(isolated_cron):
    from tools.cronjob_tools import cronjob

    job = create_job(prompt="synthetic", schedule="every 15m", name="native-tool", deliver="local")
    pause_job(job["id"], "maintenance")
    response = json.loads(cronjob(action="resume_future_only", job_id=job["id"]))
    assert response["success"] is True, response
    assert response["job"]["state"] == "scheduled"
    assert datetime.fromisoformat(response["job"]["next_run_at"]) > datetime.now(UTC)
