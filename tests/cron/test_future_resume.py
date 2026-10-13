"""Isolated future-only transaction, compatibility, persistence and time tests."""
import copy
import json
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from cron import executions, jobs
from cron.future_resume import inspect_job_revision, resume_future_only


@pytest.fixture
def paused_store(tmp_path, monkeypatch):
    clock = [datetime(2026, 10, 13, 8, 1, tzinfo=UTC)]
    monkeypatch.setattr(jobs, "_hermes_now", lambda: clock[0])
    monkeypatch.setattr(jobs, "get_timezone", lambda: UTC)
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", None)
    with jobs.use_cron_store(tmp_path):
        target = jobs.create_job("isolated", "*/15 * * * *", paused=True, deliver="local")
        other = jobs.create_job("other", "1h", paused=True, deliver="local")
        jobs.update_job(target["id"], {"next_run_at": (clock[0] - timedelta(minutes=10)).isoformat()})
        yield target["id"], other["id"], clock, tmp_path


def request(job_id, **kwargs):
    return resume_future_only(job_id, expected_revision=inspect_job_revision(job_id)["revision"],
                              request_id="isolated-request", **kwargs)


def raw(job_id):
    return next(row for row in jobs._peek_jobs_unlocked() if row["id"] == job_id)


def test_atomic_resume_preserves_other_fields(paused_store):
    jid, other, clock, _ = paused_store
    before, other_before = copy.deepcopy(raw(jid)), copy.deepcopy(raw(other))
    result = request(jid)
    assert result["success"] and result["atomic_persisted"]
    after = raw(jid)
    assert after["state"] == "scheduled" and after["enabled"] is True
    assert after["paused_at"] is None and after["paused_reason"] is None
    assert datetime.fromisoformat(after["next_run_at"]) == clock[0].replace(minute=15)
    permitted = {"enabled", "state", "paused_at", "paused_reason", "next_run_at", "dispatch_generation", "future_resume"}
    assert {k: v for k, v in before.items() if k not in permitted} == {
        k: v for k, v in after.items() if k not in permitted}
    assert raw(other) == other_before


@pytest.mark.parametrize("minutes", [1, 10, 120, 1440])
def test_expired_slot_not_due_after_resume(paused_store, minutes):
    jid, _, clock, _ = paused_store
    jobs.update_job(jid, {"next_run_at": (clock[0] - timedelta(minutes=minutes)).isoformat()})
    assert request(jid)["success"]
    assert jobs.get_due_jobs() == []


def test_ordinary_resume_keeps_elapsed_slot(paused_store):
    jid, _, _, _ = paused_store
    old = raw(jid)["next_run_at"]
    assert jobs.resume_job(jid)["next_run_at"] == old
    assert "dispatch_generation" not in raw(jid)


def test_revision_conflict_and_contract_refused(paused_store):
    jid, _, _, _ = paused_store
    before = copy.deepcopy(raw(jid))
    result = resume_future_only(jid, expected_revision="wrong", request_id="test")
    assert result["error"] == "BLOCKED_REVISION_CONFLICT"
    assert request(jid, expected_enabled=True)["error"] == "BLOCKED_RESUME_CONTRACT"
    assert raw(jid) == before


@pytest.mark.parametrize("field", ["pending_slot", "fire_claim", "run_claim", "manual_run_at"])
def test_unresolved_stamps_refuse_without_cleanup(paused_store, field):
    jid, _, _, _ = paused_store
    jobs.update_job(jid, {field: {"unresolved": True}})
    before = copy.deepcopy(raw(jid))
    assert request(jid)["error"] == "BLOCKED_TARGET_JOB_NOT_QUIESCENT"
    assert raw(jid) == before


@pytest.mark.parametrize("status", ["claimed", "running", "unknown"])
def test_unresolved_execution_blocks(paused_store, status):
    from cron.future_resume import _store_home
    jid, _, _, _ = paused_store
    with _store_home():
        record = executions.create_execution(jid, source="builtin")
        with executions._transaction() as conn:
            conn.execute("UPDATE executions SET status=? WHERE id=?", (status, record["id"]))
    assert request(jid)["error"] == "BLOCKED_TARGET_JOB_NOT_QUIESCENT"
    assert raw(jid)["enabled"] is False


def test_idempotent_readback_does_not_choose_again(paused_store):
    jid, _, clock, _ = paused_store
    rev = inspect_job_revision(jid)["revision"]
    first = resume_future_only(jid, expected_revision=rev, request_id="same")
    clock[0] += timedelta(days=1)
    second = resume_future_only(jid, expected_revision=rev, request_id="same")
    assert second["replayed"] and first["operation"] == second["operation"]
    assert resume_future_only(jid, expected_revision="other", request_id="same")["error"] == "BLOCKED_REQUEST_ID_CONFLICT"


def test_near_slot_blocks(paused_store):
    jid, _, clock, _ = paused_store
    clock[0] = clock[0].replace(minute=14, second=58)
    assert request(jid)["error"] == "BLOCKED_NEXT_SLOT_TOO_CLOSE"
    assert raw(jid)["enabled"] is False


@pytest.mark.parametrize("jump", [900, -20])
def test_clock_jump_during_stage_leaves_paused(paused_store, monkeypatch, jump):
    jid, _, clock, _ = paused_store
    stage = jobs._stage_jobs_payload
    def delayed(*args):
        path = stage(*args)
        clock[0] += timedelta(seconds=jump)
        return path
    monkeypatch.setattr(jobs, "_stage_jobs_payload", delayed)
    assert request(jid)["error"] == "BLOCKED_CLOCK_DISCONTINUITY"
    assert raw(jid)["enabled"] is False


def test_write_failure_leaves_paused(paused_store, monkeypatch):
    jid, _, _, _ = paused_store
    before = copy.deepcopy(raw(jid))
    def fail(*args):
        raise OSError("isolated disk failure")
    monkeypatch.setattr(jobs, "atomic_replace", fail)
    assert request(jid)["error"] == "PERSISTENCE_OUTCOME_UNKNOWN"
    assert raw(jid) == before


def test_post_commit_error_same_request_recovers(paused_store, monkeypatch):
    jid, _, _, _ = paused_store
    rev = inspect_job_revision(jid)["revision"]
    def fail(*args):
        raise OSError("isolated directory fsync failure")
    monkeypatch.setattr(jobs, "fsync_directory", fail)
    first = resume_future_only(jid, expected_revision=rev, request_id="durable")
    assert first["error"] == "PERSISTENCE_OUTCOME_UNKNOWN"
    second = resume_future_only(jid, expected_revision=rev, request_id="durable")
    assert second["success"] is False and second["error"] == "BLOCKED_RESUME_UNCONFIRMED"
    assert jobs.is_job_runnable(raw(jid)) is False
    assert raw(jid)["dispatch_generation"] == 1


@pytest.mark.parametrize("base,expr,zone,expected", [
    ("2026-10-31T23:59:30+00:00", "0 0 * * *", "UTC", "2026-11-01T00:00:00+00:00"),
    ("2026-10-16T17:00:00+00:00", "0 9 * * 1-5", "UTC", "2026-10-19T09:00:00+00:00"),
    ("2026-03-07T18:00:00+00:00", "0 9 * * *", "America/New_York", "2026-03-08T09:00:00-04:00"),
    ("2026-11-01T06:30:00+00:00", "0 9 * * *", "America/New_York", "2026-11-01T09:00:00-05:00"),
])
def test_native_calendar_and_dst(paused_store, monkeypatch, base, expr, zone, expected):
    jid, _, clock, _ = paused_store
    clock[0] = datetime.fromisoformat(base)
    monkeypatch.setattr(jobs, "get_timezone", lambda: ZoneInfo(zone))
    jobs.update_job(jid, {"schedule": {"kind": "cron", "expr": expr}})
    assert request(jid)["success"]
    assert raw(jid)["next_run_at"] == expected


def test_expired_one_shot_refused(paused_store):
    jid, _, clock, _ = paused_store
    jobs.update_job(jid, {"schedule": {"kind": "once", "run_at": (clock[0] - timedelta(seconds=30)).isoformat()}})
    assert request(jid)["error"] == "BLOCKED_NO_FUTURE_SLOT"


@pytest.mark.parametrize("catch_up", [True, False])
def test_future_resume_independent_of_global_catch_up(paused_store, monkeypatch, catch_up):
    jid, _, _, _ = paused_store
    original = jobs._cron_config_number
    monkeypatch.setattr(jobs, "_cron_config_number", lambda key, default, cast:
                        catch_up if key == "catch_up_missed" else original(key, default, cast))
    assert request(jid)["success"]
    assert jobs.get_due_jobs() == []


@pytest.mark.parametrize("field", ["expected_enabled", "request_id"])
def test_bad_argument_types_fail_closed(paused_store, field):
    jid, _, _, _ = paused_store
    args = {"expected_revision": inspect_job_revision(jid)["revision"], "request_id": "typed"}
    args[field] = 0
    assert resume_future_only(jid, **args)["success"] is False


def test_strict_store_lock_cannot_degrade(paused_store, monkeypatch):
    jid, _, _, _ = paused_store
    revision = inspect_job_revision(jid)["revision"]
    before = copy.deepcopy(raw(jid))
    acquire = jobs._acquire_flock
    monkeypatch.setattr(jobs, "_acquire_flock", lambda fd, timeout:
                        False if fd.name.endswith(".jobs.lock") else acquire(fd, timeout))
    result = resume_future_only(jid, expected_revision=revision, request_id="lock-failure")
    assert result["success"] is False
    assert raw(jid) == before


def test_pause_after_resume_preserves_identity(paused_store):
    jid, other, _, _ = paused_store
    other_before = copy.deepcopy(raw(other))
    assert request(jid)["success"]
    jobs.pause_job(jid)
    assert raw(jid)["state"] == "paused" and jobs.get_due_jobs() == []
    assert raw(other) == other_before


def test_cli_and_tool_same_contract(paused_store, capsys):
    from argparse import ArgumentParser
    from hermes_cli.subcommands.cron import build_cron_parser
    from hermes_cli.cron import cron_command
    from tools.cronjob_tools import _cronjob_handler
    jid, _, _, _ = paused_store
    parser = ArgumentParser()
    build_cron_parser(parser.add_subparsers(), cmd_cron=cron_command)
    args = parser.parse_args(["cron", "revision", jid])
    assert cron_command(args) == 0
    revision = json.loads(capsys.readouterr().out)["revision"]
    result = json.loads(_cronjob_handler({"action": "resume_future_only", "job_id": jid,
        "expected_revision": revision, "request_id": "tool-test"}))
    assert result["success"]
    retry = parser.parse_args(["cron", "resume-future-only", jid, "--expected-revision", revision,
                               "--request-id", "tool-test"])
    assert cron_command(retry) == 0
    assert json.loads(capsys.readouterr().out)["replayed"]
