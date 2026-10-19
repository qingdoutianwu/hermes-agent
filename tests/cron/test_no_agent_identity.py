"""Native identity at the actual no-agent spawn boundary; temp homes and real children."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

from cron import executions, scheduler_script
from cron.execution_identity import enter_cron_execution, exit_cron_execution
from cron.no_agent_identity import IDENTITY_ENV, child_identity, read_child_identity


@contextmanager
def owned(
    tmp_path, monkeypatch, source="builtin", kind="on_time", job_id="fixture-job"
):
    home = tmp_path / "home"
    (home / "scripts").mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", home / "cron/executions.db")
    slot = None if source == "direct" else "2026-10-21T10:00:00+00:00"
    row = executions.create_execution(job_id, source=source, scheduled_instant=slot)
    row = executions.mark_execution_running(row["id"])
    job = {
        "id": job_id,
        "name": job_id,
        "no_agent": True,
        "execution_id": row["id"],
        "interpreter": sys.executable,
        "schedule": {"kind": "cron"},
        "last_dispatch": {
            "scheduled_at": slot,
            "dispatched_at": "2026-10-21T18:00:07+08:00",
            "kind": kind,
        },
    }
    token = enter_cron_execution(job, row["id"], row)
    try:
        yield home, job, row
    finally:
        exit_cron_execution(token)


def child_script(home, expected_job=None, tamper=False):
    script = home / "scripts/probe.py"
    core = Path(__file__).resolve().parents[2]
    script.write_text(
        "import sys,json\nfrom pathlib import Path\n"
        f"sys.path.insert(0,{str(core)!r})\n"
        "from cron.no_agent_identity import read_child_identity\n"
        "import os\n"
        f"job={expected_job!r} or os.environ.get('HERMES_CRON_JOB_ID','fixture-job')\n"
        + ("os.environ['HERMES_CRON_EXECUTION_ID']='forged'\n" if tamper else "")
        + "body=read_child_identity(expected_job_id=job,expected_launcher_path=__file__)\n"
        "print(json.dumps({'identity':body,'business':'SUCCESS'}))\n"
    )
    return script


@pytest.mark.platforms("posix")
@pytest.mark.parametrize(
    "source,kind,origin",
    [
        ("builtin", "on_time", "on_time"),
        ("builtin", "late", "late"),
        ("builtin", "catch_up", "catch_up"),
        ("direct", "on_time", "direct"),
    ],
)
def test_native_identity_reaches_real_child(
    tmp_path, monkeypatch, source, kind, origin
):
    with owned(tmp_path, monkeypatch, source, kind) as (home, job, row):
        script = child_script(home)
        ok, output = scheduler_script._run_job_script_with_claim_heartbeat(
            job, script.name
        )
        assert ok, output
        actual = json.loads(output)
        assert actual["business"] == "SUCCESS"
        assert actual["identity"]["execution_id"] == row["id"]
        assert actual["identity"]["job_id"] == job["id"]
        assert actual["identity"]["scheduled_at"] == row["scheduled_instant"]
        assert actual["identity"]["origin"] == origin


@pytest.mark.platforms("posix")
@pytest.mark.parametrize(
    "breakage",
    ["missing_id", "wrong_job", "unknown_origin", "wrong_slot", "no_running_row"],
)
def test_invalid_native_state_preserves_business_without_identity(
    tmp_path, monkeypatch, breakage
):
    with owned(tmp_path, monkeypatch) as (home, job, row):
        script = child_script(home)
        if breakage == "missing_id":
            job.pop("execution_id")
        elif breakage == "wrong_job":
            job["id"] = "other-job"
        elif breakage == "unknown_origin":
            job["last_dispatch"]["kind"] = "invented"
        elif breakage == "wrong_slot":
            job["last_dispatch"]["scheduled_at"] = "2026-10-20T10:00:00+00:00"
        else:
            executions.finish_execution(row["id"], success=True)
        ok, output = scheduler_script._run_job_script_with_claim_heartbeat(
            job, script.name
        )
        assert ok, output
        assert json.loads(output) == {"identity": None, "business": "SUCCESS"}


def test_ambient_identity_is_scrubbed_without_native_context(tmp_path):
    script = tmp_path / "probe.py"
    script.write_text("pass")
    env = {key: "forged" for key in IDENTITY_ENV}
    env.update({"HERMES_CRON_TIMEOUT": "600", "UNRELATED_SETTING": "keep"})
    with child_identity({"no_agent": True}, script, env, {}) as identity:
        assert identity is None
        assert not set(env).intersection(IDENTITY_ENV)
        assert (
            env["HERMES_CRON_TIMEOUT"] == "600" and env["UNRELATED_SETTING"] == "keep"
        )


@pytest.mark.parametrize("descriptor", ["missing", "-1", "0", "999999", "bad"])
def test_environment_only_spoof_is_rejected(descriptor):
    assert (
        read_child_identity(
            expected_job_id="job",
            expected_launcher_path="script.py",
            environ={"HERMES_CRON_IDENTITY_FD": descriptor},
        )
        is None
    )


@pytest.mark.platforms("posix")
def test_malformed_pipe_frame_rejected(tmp_path):
    reader, writer = os.pipe()
    os.write(writer, b"{bad-json")
    os.close(writer)
    assert (
        read_child_identity(
            expected_job_id="job",
            expected_launcher_path=tmp_path / "script.py",
            environ={"HERMES_CRON_IDENTITY_FD": str(reader)},
        )
        is None
    )


@pytest.mark.platforms("posix")
def test_forged_child_pid_is_rejected(tmp_path):
    reader, writer = os.pipe()
    os.write(
        writer,
        json.dumps({"child_pid": os.getpid() + 1, "owner_pid": os.getppid()}).encode(),
    )
    os.close(writer)
    assert (
        read_child_identity(
            expected_job_id="job",
            expected_launcher_path=tmp_path / "script.py",
            environ={"HERMES_CRON_IDENTITY_FD": str(reader)},
        )
        is None
    )


@pytest.mark.platforms("posix")
def test_other_noagent_without_context_is_unchanged(tmp_path, monkeypatch):
    home = tmp_path / "other"
    (home / "scripts").mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    (home / "scripts/unrelated.py").write_text("print('unrelated business success')")
    ok, output = scheduler_script._run_job_script(
        "unrelated.py", interpreter=sys.executable
    )
    assert ok and output == "unrelated business success"


@pytest.mark.platforms("posix")
@pytest.mark.parametrize("mismatch", ["job", "environment"])
def test_owned_pipe_does_not_bless_mismatched_child(tmp_path, monkeypatch, mismatch):
    with owned(tmp_path, monkeypatch) as (home, job, _row):
        script = child_script(
            home,
            expected_job="wrong-job" if mismatch == "job" else None,
            tamper=mismatch == "environment",
        )
        ok, output = scheduler_script._run_job_script_with_claim_heartbeat(
            job, script.name
        )
        assert ok, output
        assert json.loads(output) == {"identity": None, "business": "SUCCESS"}


@pytest.mark.platforms("posix")
def test_bridge_does_not_write_or_expand_existing_ledger(tmp_path, monkeypatch):
    with owned(tmp_path, monkeypatch) as (home, job, row):

        def database_state():
            with sqlite3.connect(home / "cron/executions.db") as c:
                return (
                    c.execute("PRAGMA table_info(executions)").fetchall(),
                    c.execute("SELECT * FROM executions").fetchall(),
                )

        before = database_state()
        script = child_script(home)
        ok, output = scheduler_script._run_job_script_with_claim_heartbeat(
            job, script.name
        )
        assert ok, output
        assert json.loads(output)["identity"]["execution_id"] == row["id"]
        assert database_state() == before


@pytest.mark.platforms("posix")
def test_profile_home_change_does_not_reuse_context(tmp_path, monkeypatch):
    with owned(tmp_path, monkeypatch) as (home, job, _row):
        other = tmp_path / "other"
        other.mkdir()
        monkeypatch.setenv("HERMES_HOME", str(other))
        env = {}
        with child_identity(job, home / "scripts/probe.py", env, {}) as issue:
            assert issue is None and not set(env).intersection(IDENTITY_ENV)


def test_invalid_regular_file_descriptor_is_not_closed(tmp_path):
    p = tmp_path / "not-an-identity"
    p.write_text("untrusted")
    with p.open("rb") as f:
        assert (
            read_child_identity(
                expected_job_id="job",
                expected_launcher_path="script",
                environ={"HERMES_CRON_IDENTITY_FD": str(f.fileno())},
            )
            is None
        )
        assert f.read() == b"untrusted"
