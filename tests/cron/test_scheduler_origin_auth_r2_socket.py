"""Isolated native socket authority integration contract."""
from __future__ import annotations

import json
import hashlib
import os
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from cron.scheduler_origin_auth import ScriptOriginAuthority, authoritative_environment

JOB = "job-socket-r2"
SLOT = "2026-10-16T09:00:00+08:00"


def _home(tmp_path: Path) -> Path:
    home = tmp_path / ".hermes"
    (home / "cron").mkdir(parents=True)
    (home / "scripts").mkdir()
    return home


def test_real_child_connects_and_consumes_one_use_authority(tmp_path: Path, monkeypatch):
    home = _home(tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(home))
    from cron import executions

    executions.EXECUTIONS_FILE = home / "cron" / "executions.db"
    record = executions.create_execution(JOB, source="builtin", scheduled_instant=SLOT)
    assert record is not None
    record = executions.mark_execution_running(record["id"])
    assert record is not None
    script = home / "scripts" / "probe.py"
    script.write_text(
        "import json, os, socket\n"
        "s=socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)\n"
        "s.connect(os.environ['HERMES_CRON_AUTH_SOCKET'])\n"
        "r={'execution_id':os.environ['HERMES_CRON_EXECUTION_ID'],'job_id':os.environ['HERMES_CRON_JOB_ID'],'scheduled_at':os.environ['HERMES_CRON_SCHEDULED_AT'],'origin':os.environ['HERMES_CRON_TRIGGER_TYPE'],'pid':os.getpid(),'nonce':'a'*32}\n"
        "s.sendall(json.dumps(r,sort_keys=True).encode()+bytes([10]))\n"
        "print(s.recv(8192).decode(), end='')\n",
        encoding="utf-8",
    )
    (home / "release_manifest.json").write_text(
        json.dumps({
            "release_id": "HK-ANOMALY-ISOLATED-R2",
            "source_commit": "candidate-commit",
            "source_tree_sha256": "a" * 64,
            "production_entrypoint": "scripts/probe.py",
            "entry": "scripts/probe.py",
            "entry_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
        }, sort_keys=True), encoding="utf-8"
    )
    job = {
        "id": JOB,
        "execution_id": record["id"],
        "_scheduled_instant": SLOT,
        "script": str(script),
        "_script_path": str(script),
        "_r2_authority_required": "true",
        "_r2_release_manifest": str(home / "release_manifest.json"),
        "workdir": str(home),
    }
    authority = ScriptOriginAuthority(job)
    env = {}
    authoritative_environment(env, authority)
    env.update({"HERMES_HOME": str(home), "PYTHONPATH": str(Path(__file__).parents[2])})
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        env={**os.environ, **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    consumed: dict[str, dict] = {}

    def consume() -> None:
        consumed["response"] = authority.consume(proc.pid)

    consumer = threading.Thread(target=consume, daemon=True)
    consumer.start()
    _response_text, stderr = proc.communicate(timeout=10)
    consumer.join(timeout=10)
    assert not consumer.is_alive()
    assert proc.returncode == 0, stderr
    response = consumed["response"]
    assert response["status"] == "ACCEPTED"
    assert response["proof"]["child_pid"] == proc.pid
    assert response["proof"]["execution_id"] == record["id"]
    row = executions.get_execution(record["id"])
    assert row is not None
    assert row["origin_proof_used"] == 1
    assert row["origin_proof_sha256"] == response["proof_sha256"]
    with pytest.raises(RuntimeError, match="ALREADY_CONSUMED"):
        authority.consume(proc.pid)
    authority.close()
    # A NEW authority instance cannot reset the durable one-use CAS.
    with ScriptOriginAuthority(job) as second:
        authoritative_environment(env, second)
        duplicate = subprocess.Popen([sys.executable, str(script)], env={**os.environ, **env},
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            with pytest.raises(RuntimeError, match="CAS_REJECTED"):
                second.consume(duplicate.pid)
            duplicate.communicate(timeout=5)
            assert executions.get_execution(record["id"])["origin_proof_sha256"] == response["proof_sha256"]
        finally:
            if duplicate.poll() is None:
                duplicate.kill()
                duplicate.wait(timeout=5)


def test_authority_rejects_child_that_never_sends_a_request(tmp_path: Path, monkeypatch):
    home = _home(tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(home))
    from cron import executions
    executions.EXECUTIONS_FILE = home / "cron" / "executions.db"
    record = executions.create_execution(JOB, source="builtin", scheduled_instant=SLOT)
    executions.mark_execution_running(record["id"])
    script = home / "scripts" / "silent.py"
    script.write_text("import time; time.sleep(30)\n", encoding="utf-8")
    (home / "release_manifest.json").write_text(
        json.dumps({
            "release_id": "HK-ANOMALY-ISOLATED-R2",
            "source_commit": "candidate-commit",
            "source_tree_sha256": "a" * 64,
            "production_entrypoint": "scripts/silent.py",
            "entry": "scripts/silent.py",
            "entry_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
        }, sort_keys=True), encoding="utf-8"
    )
    authority = ScriptOriginAuthority({
        "id": JOB, "execution_id": record["id"], "_scheduled_instant": SLOT,
        "script": str(script), "_script_path": str(script),
        "_r2_authority_required": "true", "_r2_release_manifest": str(home / "release_manifest.json"),
        "workdir": str(home),
    })
    child = subprocess.Popen(
        [sys.executable, "-c", "import os,socket,time; s=socket.socket(socket.AF_UNIX); s.connect(os.environ['HERMES_CRON_AUTH_SOCKET']); time.sleep(30)"],
        env={**os.environ, "HERMES_CRON_AUTH_SOCKET": str(authority.path)},
    )
    try:
        with pytest.raises((RuntimeError, TimeoutError, OSError)):
            authority.consume(child.pid)
    finally:
        child.kill()
        child.wait(timeout=5)
        authority.close()


def test_scheduler_script_reaps_child_after_authority_failure(tmp_path, monkeypatch):
    from cron import executions, scheduler_script
    import cron.scheduler_origin_auth as auth

    home = _home(tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", home / "cron" / "executions.db")
    monkeypatch.setattr(auth, "REQUEST_READ_TIMEOUT_SECONDS", 0.2)
    record = executions.create_execution(JOB, source="builtin", scheduled_instant=SLOT)
    executions.mark_execution_running(record["id"])
    script = home / "scripts" / "blocked.py"
    script.write_text("import os,socket,threading\ns=socket.socket(socket.AF_UNIX)\ns.connect(os.environ['HERMES_CRON_AUTH_SOCKET'])\nthreading.Event().wait(30)\n")
    manifest = home / "release_manifest.json"
    manifest.write_text(json.dumps({"release_id":"ISOLATED", "source_commit":"candidate",
        "source_tree_sha256":"a"*64, "production_entrypoint":"scripts/blocked.py",
        "entry":"scripts/blocked.py", "entry_sha256":hashlib.sha256(script.read_bytes()).hexdigest()}))
    job = {"id": JOB, "script": str(script), "execution_id":record["id"],
           "_scheduled_instant":SLOT,"_r2_authority_required":"true","_r2_release_manifest":str(manifest)}
    spawned = []
    original = subprocess.Popen
    def capture(*args, **kwargs):
        proc = original(*args, **kwargs)
        if str(script) in args[0]:
            spawned.append(proc)
        return proc
    with ScriptOriginAuthority(job) as authority:
        job["_r2_authority"] = authority
        monkeypatch.setattr(scheduler_script.subprocess, "Popen", capture)
        success, error = scheduler_script._run_job_script(str(script), job=job)
        assert success is False and error
        assert spawned[-1].returncode is not None
        assert executions.get_execution(record["id"])["origin_proof_used"] == 0
    assert not authority.path.exists()
