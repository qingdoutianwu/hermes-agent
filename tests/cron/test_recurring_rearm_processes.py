"""Native scheduler cross-process admission against a disposable store."""
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import select
import subprocess
import sys

from cron import executions, jobs


ROOT = Path(__file__).resolve().parents[2]


def line(proc):
    ready, _, _ = select.select([proc.stdout], [], [], 20)
    assert ready, "isolated process handshake timed out"
    value = proc.stdout.readline().strip()
    assert value, f"process exited early: {proc.poll()}"
    return value


def send(proc, message):
    proc.stdin.write(message + "\n")
    proc.stdin.flush()


def test_competing_native_ticks_obey_guard_then_fire_future_once(tmp_path, monkeypatch, record_property):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    monkeypatch.setattr(jobs, "_hermes_now", lambda: now)
    children = []

    def spawn(role, instant):
        proc = subprocess.Popen(
            [os.environ.get("HERMES_TEST_PYTHON", sys.executable), "-m",
             "tests.cron.rearm_process_probe", role, job_id, instant.isoformat()],
            cwd=ROOT, env={**os.environ, "HERMES_HOME": str(home)},
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        children.append(proc)
        return proc

    try:
        with jobs.use_cron_store(home):
            target = jobs.create_job(prompt="synthetic", schedule="every 15m", deliver="local")
            job_id = target["id"]
            jobs.update_job(job_id, {"next_run_at": (now - timedelta(minutes=4)).isoformat()})
            jobs.pause_job(job_id, "cross-process test")
            sibling = jobs.create_job(prompt="unrelated", schedule="every 2h", deliver="local")
            jobs.pause_job(sibling["id"], "never dispatch sibling")
            sibling_before = jobs.get_job(sibling["id"])

            rearming = spawn("rearm", now)
            assert line(rearming) == "ACTIVE_GUARDED"
            # Even with its clock beyond the new due instant a second process cannot enter
            # native scan/claim while the first process still verifies activation.
            scanner = spawn("tick", now + timedelta(minutes=16))
            assert line(scanner) == "READY"
            send(scanner, "go")
            guarded_result = json.loads(line(scanner))
            assert guarded_result["count"] == 0 and guarded_result["rows"] == []
            assert scanner.wait(timeout=20) == 0, scanner.stderr.read()
            assert not (home / "effects.jsonl").exists()

            send(rearming, "release")
            published = json.loads(line(rearming))["result"]
            assert rearming.wait(timeout=20) == 0, rearming.stderr.read()
            slot = datetime.fromisoformat(published["next_run_at"])
            assert slot > now
            stale_scanner = spawn("tick", now)
            assert line(stale_scanner) == "READY"
            send(stale_scanner, "go")
            stale_result = json.loads(line(stale_scanner))
            assert stale_scanner.wait(timeout=20) == 0, stale_scanner.stderr.read()
            assert stale_result["count"] == 0 and stale_result["rows"] == []
            assert not (home / "effects.jsonl").exists()
            # Independent fresh processes, same native tick flock and same durable ledger.
            tickers = [spawn("tick", slot) for _ in range(2)]
            for proc in tickers:
                assert line(proc) == "READY"
            for proc in tickers:
                send(proc, "go")
            results = [json.loads(line(proc)) for proc in tickers]
            for proc in tickers:
                assert proc.wait(timeout=20) == 0, proc.stderr.read()
            effects = [json.loads(row) for row in (home / "effects.jsonl").read_text().splitlines()]
            assert len(effects) == 1
            assert effects[0]["slot"] == published["next_run_at"]
            ledger = executions.list_executions(job_id=job_id)
            assert len(ledger) == 1 and ledger[0]["status"] == "completed"
            assert ledger[0]["scheduled_instant"] == published["next_run_at"]
            assert sum(result["count"] for result in results) == 1
            assert jobs.get_job(sibling["id"]) == sibling_before
            record_property("native_evidence", json.dumps({"guarded_scan": guarded_result,
                            "stale_scan": stale_result, "competing_ticks": results,
                            "ledger": ledger, "effects": effects,
                            "STALE_SLOT_DISPATCH_COUNT": 0, "STALE_SLOT_BUSINESS_EFFECT_COUNT": 0,
                            "DUPLICATE_BUSINESS_EFFECT_COUNT": 0, "FUTURE_EFFECT_COUNT": 1,
                            "GATEWAY_EQUIVALENT_RELOAD": "PASS"}))
    finally:
        for proc in children:
            if proc.poll() is None:
                proc.kill()
            proc.communicate(timeout=20)


def test_process_death_after_unlink_leaves_active_but_no_admission(tmp_path, monkeypatch, record_property):
    home = tmp_path / "crashed-home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    children = []
    with jobs.use_cron_store(home):
        target = jobs.create_job(prompt="synthetic", schedule="every 15m", deliver="local")
        job_id = target["id"]
        jobs.pause_job(job_id, "crash durability")

        def spawn(role):
            proc = subprocess.Popen([sys.executable, "-m", "tests.cron.rearm_process_probe", role,
                                     job_id, (now + timedelta(hours=1)).isoformat()],
                                    cwd=ROOT, env={**os.environ, "HERMES_HOME": str(home)},
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            children.append(proc)
            return proc

        try:
            crashed = spawn("crash-rearm")
            assert line(crashed) == "UNCOMMITTED_MARKER_ABSENT"
            assert crashed.wait(timeout=20) == 73, crashed.stderr.read()
            claim = spawn("claim")
            state = json.loads(line(claim))
            assert claim.wait(timeout=20) == 0, claim.stderr.read()
            assert state == {"enabled": True, "marker": False, "admitted": False, "claimed": False}
            scanner = spawn("tick")
            assert line(scanner) == "READY"
            send(scanner, "go")
            result = json.loads(line(scanner))
            assert scanner.wait(timeout=20) == 0, scanner.stderr.read()
            assert result == {"count": 0, "rows": []}
            assert not (home / "effects.jsonl").exists()
            record_property("crash_evidence", json.dumps({"reopened": state, "tick": result,
                            "STALE_SLOT_DISPATCH_COUNT": 0, "STALE_SLOT_BUSINESS_EFFECT_COUNT": 0,
                            "DUPLICATE_BUSINESS_EFFECT_COUNT": 0}))
        finally:
            for proc in children:
                if proc.poll() is None:
                    proc.kill()
                proc.communicate(timeout=20)
