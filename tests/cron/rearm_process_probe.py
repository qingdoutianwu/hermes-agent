"""Isolated subprocess driver; never invokes real business or delivery code."""
import hashlib
import json
import os
from pathlib import Path
import sys
from datetime import datetime


def deny_external(event, args):
    if event in {"socket.connect", "socket.sendto", "subprocess.Popen", "os.system"}:
        raise AssertionError(f"external effect forbidden: {event}")


sys.addaudithook(deny_external)
from cron import executions, jobs, scheduler

role, job_id, now_text = sys.argv[1:]
now = datetime.fromisoformat(now_text)
jobs._hermes_now = lambda: now
scheduler._hermes_now = lambda: now
scheduler._should_yield_tick_to_fresh_gateway = lambda: None
scheduler._maybe_reap_dead_owners = lambda: None
scheduler._maybe_run_worktree_maintenance = lambda: None
scheduler._sweep_stale_inflight_for_tick = lambda _due: None
scheduler._sweep_mcp_orphans = lambda: None
from cron import bot_chat_delivery
bot_chat_delivery.drain = lambda: None


def synthetic(record, **_kwargs):
    assert executions.mark_execution_running(record["execution_id"])
    effect = {"execution_id": record["execution_id"], "slot": record["_scheduled_instant"]}
    path = Path(os.environ["HERMES_HOME"]) / "effects.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(effect) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    assert jobs.mark_job_run(record["id"], True, expected_fire_owner=record["fire_claim"]["by"])
    assert executions.finish_execution(record["execution_id"], success=True)
    return True


scheduler.run_one_job = synthetic
if role == "crash-rearm":
    clear = jobs._clear_rearm_dispatch_block

    def crash_after_unlink(target):
        clear(target)
        print("UNCOMMITTED_MARKER_ABSENT", flush=True)
        os._exit(73)

    jobs._clear_rearm_dispatch_block = crash_after_unlink
    revision = hashlib.sha256(jobs._current_cron_store().jobs_file.read_bytes()).hexdigest()
    jobs.rearm_recurring_future_only(job_id, request_id="crash-probe", expected_revision=revision)
elif role == "claim":
    from cron.rearm_admission import admitted
    record = jobs.get_job(job_id)
    print(json.dumps({"enabled": record["enabled"],
                      "marker": jobs._rearm_dispatch_is_blocked(job_id),
                      "admitted": admitted(jobs._current_cron_store().cron_dir, record),
                      "claimed": jobs.claim_job_for_fire(job_id, return_job=True)}), flush=True)
elif role == "rearm":
    normalize = jobs._normalize_job_record

    def held_result(record):
        result = normalize(record)
        print("ACTIVE_GUARDED", flush=True)
        assert sys.stdin.readline().strip() == "release"
        return result

    jobs._normalize_job_record = held_result
    revision = hashlib.sha256(jobs._current_cron_store().jobs_file.read_bytes()).hexdigest()
    result = jobs.rearm_recurring_future_only(job_id, request_id="process-probe", expected_revision=revision)
    print(json.dumps({"result": result}), flush=True)
else:
    print("READY", flush=True)
    assert sys.stdin.readline().strip() == "go"
    from cron.scheduler_provider import _profile_cron_scope
    with _profile_cron_scope(Path(os.environ["HERMES_HOME"])):
        count = scheduler.tick(verbose=False, sync=True)
    print(json.dumps({"count": count, "rows": executions.list_executions(job_id=job_id)}), flush=True)
