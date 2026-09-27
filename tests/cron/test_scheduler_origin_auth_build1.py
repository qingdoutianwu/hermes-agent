"""Isolated one-use origin endpoint tests; never touch the operator's store."""

import json
import os
from pathlib import Path

import pytest

from cron.scheduler_origin_auth import ORIGIN, ScriptOriginAuthority, authoritative_environment, scrub_identity


def _job(execution_id="execution-a", job_id="f0def1ac0f4f"):
    return {
        "execution_id": execution_id,
        "id": job_id,
        "_scheduled_instant": "2026-09-27T06:30:00+08:00",
    }


def _row(job, *, source="builtin", status="running"):
    from cron.occurrences import scheduled_instant

    return {
        "id": job["execution_id"],
        "job_id": job["id"],
        "source": source,
        "status": status,
        "pid": os.getpid(),
        "scheduled_instant": scheduled_instant(job["_scheduled_instant"]),
    }


def _request(job, *, nonce="0" * 32):
    return {
        "execution_id": job["execution_id"],
        "job_id": job["id"],
        "scheduled_at": job["_scheduled_instant"],
        "origin": ORIGIN,
        "pid": os.getpid(),
        "nonce": nonce,
    }


def test_scrubbed_parent_identity_is_replaced_with_scheduler_values():
    env = {"HERMES_CRON_JOB_ID": "fake", "HERMES_CRON_TRIGGER_TYPE": "fake", "FINANCE_TRIGGER_TYPE": "fake"}
    scrub_identity(env)
    assert not env
    authority = ScriptOriginAuthority(_job())
    try:
        authoritative_environment(env, authority)
        assert env["HERMES_CRON_JOB_ID"] == "f0def1ac0f4f"
        assert env["HERMES_CRON_TRIGGER_TYPE"] == ORIGIN
        assert env["HERMES_CRON_AUTH_FD"] == str(authority.child_fd)
    finally:
        authority.close()


def test_live_ledger_row_and_one_use_response(monkeypatch):
    job = _job()
    monkeypatch.setattr("cron.executions.get_execution", lambda _id: _row(job))
    authority = ScriptOriginAuthority(job)
    try:
        authority.child.sendall((json.dumps(_request(job)) + "\n").encode())
        authority.poll(os.getpid())
        response = json.loads(authority.child.recv(2048))
        assert response["status"] == "ACCEPTED"
        assert response["execution_id"] == job["execution_id"]
        assert response["child_pid"] == os.getpid()
        authority.poll(os.getpid())  # no second response; proof is consumed
    finally:
        authority.close()


def test_manual_or_wrong_job_execution_never_gets_authority(monkeypatch):
    job = _job()
    monkeypatch.setattr("cron.executions.get_execution", lambda _id: _row(job, source="direct"))
    authority = ScriptOriginAuthority(job)
    try:
        authority.child.sendall((json.dumps(_request(job)) + "\n").encode())
        authority.poll(os.getpid())
        assert json.loads(authority.child.recv(2048))["status"] == "REJECTED"
    finally:
        authority.close()

    monkeypatch.setattr("cron.executions.get_execution", lambda _id: _row(_job(job_id="other")))
    authority = ScriptOriginAuthority(job)
    try:
        authority.child.sendall((json.dumps(_request(job)) + "\n").encode())
        authority.poll(os.getpid())
        assert json.loads(authority.child.recv(2048))["status"] == "REJECTED"
    finally:
        authority.close()


def test_real_script_spawn_receives_one_use_fd_and_ledger_binding(tmp_path: Path, monkeypatch):
    """Use the real script resolver and Popen; no Production Cron state."""

    import importlib
    import hermes_constants
    import cron.executions as executions
    import cron.jobs as jobs
    import cron.scheduler_script as script

    home = tmp_path / "home"
    scripts = home / "scripts"
    scripts.mkdir(parents=True)
    (home / "cron").mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    importlib.reload(hermes_constants)
    importlib.reload(jobs)
    importlib.reload(executions)
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", home / "cron/executions.db")
    probe = scripts / "origin_probe.py"
    probe.write_text(
        "import json, os, socket\n"
        "fd=int(os.environ['HERMES_CRON_AUTH_FD'])\n"
        "s=socket.socket(fileno=fd)\n"
        "request={'execution_id':os.environ['HERMES_CRON_EXECUTION_ID'],"
        "'job_id':os.environ['HERMES_CRON_JOB_ID'],"
        "'scheduled_at':os.environ['HERMES_CRON_SCHEDULED_AT'],"
        "'origin':os.environ['HERMES_CRON_TRIGGER_TYPE'],"
        "'pid':os.getpid(),'nonce':'1'*32}\n"
        "s.sendall((json.dumps(request)+'\\n').encode())\n"
        "print(json.loads(s.recv(2048))['status'])\n",
        encoding="utf-8",
    )
    instant = "2026-09-27T06:30:00+08:00"
    record = executions.create_execution("f0def1ac0f4f", source="builtin", scheduled_instant=instant)
    assert executions.mark_execution_running(record["id"])
    job = {"id": "f0def1ac0f4f", "execution_id": record["id"], "_scheduled_instant": instant}
    ok, output = script._run_job_script("origin_probe.py", job=job)
    assert ok, output
    assert output == "ACCEPTED"


def test_missing_or_terminal_ledger_row_is_rejected(monkeypatch):
    for record in (None, _row(_job(), status="completed")):
        monkeypatch.setattr("cron.executions.get_execution", lambda _id: record)
        authority = ScriptOriginAuthority(_job())
        try:
            authority.child.sendall((json.dumps(_request(_job())) + "\n").encode())
            authority.poll(os.getpid())
            assert json.loads(authority.child.recv(2048))["status"] == "REJECTED"
        finally:
            authority.close()


def test_wrong_origin_and_cross_execution_swap_are_rejected(monkeypatch):
    jobs = (_job("execution-a"), _job("execution-b", "9d69476aec2b"))
    rows = {job["execution_id"]: _row(job) for job in jobs}
    monkeypatch.setattr("cron.executions.get_execution", lambda execution_id: rows.get(execution_id))
    a, b = (ScriptOriginAuthority(job) for job in jobs)
    try:
        wrong_origin = _request(jobs[0]) | {"origin": "MANUAL"}
        a.child.sendall((json.dumps(wrong_origin) + "\n").encode())
        a.poll(os.getpid())
        assert json.loads(a.child.recv(2048))["status"] == "REJECTED"

        b.child.sendall((json.dumps(_request(jobs[0])) + "\n").encode())
        b.poll(os.getpid())
        assert json.loads(b.child.recv(2048))["status"] == "REJECTED"
    finally:
        a.close()
        b.close()


def test_concurrent_authorities_are_distinct_and_not_reusable(monkeypatch):
    jobs = (_job("execution-a"), _job("execution-b", "9d69476aec2b"))
    rows = {job["execution_id"]: _row(job) for job in jobs}
    monkeypatch.setattr("cron.executions.get_execution", lambda execution_id: rows.get(execution_id))
    a, b = (ScriptOriginAuthority(job) for job in jobs)
    try:
        assert a.child_fd != b.child_fd
        for authority, job in ((a, jobs[0]), (b, jobs[1])):
            authority.child.sendall((json.dumps(_request(job)) + "\n").encode())
            authority.poll(os.getpid())
            assert json.loads(authority.child.recv(2048))["status"] == "ACCEPTED"
            authority.poll(os.getpid())
    finally:
        a.close()
        b.close()


def test_gateway_side_close_revokes_unverified_authority():
    authority = ScriptOriginAuthority(_job())
    try:
        authority.parent.close()
        assert authority.child.recv(1) == b""
    finally:
        authority.close()


@pytest.mark.parametrize(
    "override",
    [
        {"execution_id": "unknown"},
        {"job_id": "other"},
        {"origin": "MANUAL"},
        {"scheduled_at": "2026-09-27T07:00:00+08:00"},
        {"pid": -1},
        {"nonce": "bad"},
    ],
)
def test_mismatched_request_is_rejected_once(monkeypatch, override):
    job = _job()
    monkeypatch.setattr("cron.executions.get_execution", lambda _id: _row(job))
    authority = ScriptOriginAuthority(job)
    try:
        authority.child.sendall((json.dumps(_request(job) | override) + "\n").encode())
        authority.poll(os.getpid())
        assert json.loads(authority.child.recv(2048))["status"] == "REJECTED"
        assert authority.child.recv(1) == b""
    finally:
        authority.close()


def test_spawn_failure_closes_both_authority_endpoints(tmp_path: Path, monkeypatch):
    """The subprocess error path cannot leave an active proof channel."""
    import cron.scheduler_script as script
    import cron.scheduler_origin_auth as auth

    created = []
    real = auth.ScriptOriginAuthority

    def capture(job):
        value = real(job)
        created.append(value)
        return value

    monkeypatch.setattr(auth, "ScriptOriginAuthority", capture)
    monkeypatch.setattr("cron.scheduler_script._resolve_script_path", lambda _path: (tmp_path / "probe.py", None))
    monkeypatch.setattr("cron.scheduler_script._script_argv", lambda _path: (["nonexistent-auth2-executable"], {}, None))
    monkeypatch.setattr(script.subprocess, "Popen", lambda *_a, **_kw: (_ for _ in ()).throw(OSError("spawn failed")))
    ok, message = script._run_job_script("probe.py", job=_job())
    assert not ok and "spawn failed" in message
    assert len(created) == 1
    assert created[0].parent.fileno() == -1
    assert created[0].child.fileno() == -1
