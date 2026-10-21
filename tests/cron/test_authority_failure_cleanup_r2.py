"""Real peer/socket/CAS fault injection for the R2-S2 unreaped-script defect."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from cron import executions, scheduler_script
from cron.scheduler_origin_auth import ScriptOriginAuthority

pytestmark = pytest.mark.platforms("macos")


@pytest.fixture
def owned_script(tmp_path, monkeypatch):
    home = tmp_path / "home"
    (home / "scripts").mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", home / "cron" / "executions.db")
    children = []
    authorities = []
    real_popen = subprocess.Popen

    def capture(argv, *args, **kwargs):
        proc = real_popen(argv, *args, **kwargs)
        if any(str(home / "scripts") in str(part) for part in argv):
            children.append(proc)
        return proc

    monkeypatch.setattr(scheduler_script.subprocess, "Popen", capture)

    def make(*, exit_before_request=False):
        script = home / "scripts" / "consumer.py"
        script.write_text(
            f"import sys; sys.path.insert(0, {str(Path(__file__).resolve().parents[2])!r})\n"
            "from pathlib import Path\nimport os, signal, threading\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "home=Path(os.environ['HERMES_HOME'])\n"
            "(home/'started').touch()\n"
            + ("raise SystemExit(0)\n" if exit_before_request else
               "from cron.r2_receipt_consumer import consume_current_authority, write_receipt\n"
               "try:\n"
               "    receipt=consume_current_authority(release_manifest=home/'manifest.json', receipt_id='cleanup')\n"
               "except Exception:\n"
               "    threading.Event().wait(60)\n"
               "    raise\n"
               "write_receipt(home/'receipt.json', receipt)\n"
               "(home/'business-effect').touch()\n"
               "print('authorized-success')\n"),
            encoding="utf-8",
        )
        manifest = home / "manifest.json"
        manifest.write_text(json.dumps({
            "release_id": "ISOLATED-CLEANUP-R2", "source_commit": "candidate",
            "source_tree_sha256": "a" * 64, "production_entrypoint": "scripts/consumer.py",
            "entry": str(script), "entry_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
        }))
        record = executions.create_execution(
            "cleanup-r2", source="builtin", scheduled_instant="2026-10-20T01:00:00+00:00")
        executions.mark_execution_running(record["id"])
        job = {"id": "cleanup-r2", "script": str(script), "no_agent": True,
               "execution_id": record["id"], "_scheduled_instant": record["scheduled_instant"],
               "_r2_authority_required": "true", "_r2_release_manifest": str(manifest)}
        authority = ScriptOriginAuthority(job)
        authorities.append(authority)
        job["_r2_authority"] = authority
        return home, job, authority, children

    yield make
    # RED tests must not leak the very processes they prove the old code leaves behind.
    for proc in children:
        if proc.poll() is None:
            proc.kill()
        proc.communicate(timeout=5)
    for authority in authorities:
        authority.close()


def _assert_reaped(proc):
    assert proc.returncode is not None, "owned child was neither terminated nor reaped"
    with pytest.raises(ChildProcessError):
        os.waitpid(proc.pid, os.WNOHANG)


def _assert_untrusted(home, job):
    row = executions.get_execution(job["execution_id"])
    assert row["origin_proof_used"] == 0
    assert row["origin_proof_sha256"] is None
    assert not (home / "receipt.json").exists()
    assert not (home / "business-effect").exists()


@pytest.mark.parametrize("phase", ["update", "commit"])
@pytest.mark.parametrize("error_type", [sqlite3.OperationalError, sqlite3.IntegrityError,
                                       sqlite3.DatabaseError, sqlite3.InterfaceError])
def test_cas_database_error_reaps_real_socket_child(owned_script, monkeypatch, error_type, phase):
    home, job, _authority, children = owned_script()
    transaction = executions._transaction
    cas_attempts = []

    @contextmanager
    def failing_cas():
        with transaction() as db:
            updates = []
            class FaultAtUpdate:
                def execute(self, sql, *args):
                    if "UPDATE executions SET origin_proof_sha256" in sql:
                        cas_attempts.append(True)
                        updates.append(True)
                        if phase == "update":
                            raise error_type("injected native CAS failure")
                    return db.execute(sql, *args)
            yield FaultAtUpdate()
            if phase == "commit" and updates:
                raise error_type("injected native CAS failure")

    monkeypatch.setattr(executions, "_transaction", failing_cas)
    success, output = scheduler_script._run_job_script(job["script"], job=job,
                                                     interpreter=sys.executable)
    assert not success and "injected native CAS failure" in output
    assert cas_attempts == [True], "fault must occur at real Authority CAS, after peer authentication"
    assert (home / "started").exists()
    _assert_reaped(children[-1])
    _assert_untrusted(home, job)


@pytest.mark.parametrize("error_type", [RuntimeError, TypeError])
def test_any_authority_consume_failure_reaps_owned_child(owned_script, monkeypatch, error_type):
    home, job, authority, children = owned_script()

    def reject(_pid):
        raise error_type("injected consume failure")

    monkeypatch.setattr(authority, "consume", reject)
    success, output = scheduler_script._run_job_script(job["script"], job=job,
                                                     interpreter=sys.executable)
    assert not success and "injected consume failure" in output
    _assert_reaped(children[-1])
    _assert_untrusted(home, job)


def test_authority_failure_after_child_exit_does_not_signal_again(owned_script, monkeypatch):
    home, job, authority, children = owned_script(exit_before_request=True)
    signalled = []

    def reject_after_exit(_pid):
        proc = children[-1]
        assert proc.wait(timeout=10) == 0
        monkeypatch.setattr(proc, "kill", lambda: signalled.append(proc.pid))
        raise TypeError("failure after exit")

    monkeypatch.setattr(authority, "consume", reject_after_exit)
    success, output = scheduler_script._run_job_script(job["script"], job=job,
                                                     interpreter=sys.executable)
    assert not success and "failure after exit" in output
    _assert_reaped(children[-1])
    assert signalled == []
    _assert_untrusted(home, job)


def test_termination_exit_race_keeps_original_failure(owned_script, monkeypatch, caplog):
    home, job, authority, children = owned_script()

    def reject(_pid):
        raise ValueError("original authority fault")

    def exit_during_termination(proc):
        proc.kill()
        proc.wait(timeout=5)
        raise ProcessLookupError("injected termination race")

    monkeypatch.setattr(authority, "consume", reject)
    monkeypatch.setattr(scheduler_script, "_terminate_cron_script_tree", exit_during_termination)
    success, output = scheduler_script._run_job_script(job["script"], job=job,
                                                     interpreter=sys.executable)
    assert not success and "original authority fault" in output
    assert "injected termination race" in caplog.text
    _assert_reaped(children[-1])
    _assert_untrusted(home, job)


def test_cleanup_error_escalates_and_never_kills_unrelated_process(owned_script, monkeypatch, caplog):
    home, job, authority, children = owned_script()
    unrelated = subprocess.Popen([sys.executable, "-c", "import threading; threading.Event().wait(60)"],
                                 start_new_session=True)
    try:
        def reject(_pid):
            raise TypeError("original authority fault")

        def unavailable(_proc):
            raise PermissionError("injected tree cleanup failure")

        monkeypatch.setattr(authority, "consume", reject)
        monkeypatch.setattr(scheduler_script, "_terminate_cron_script_tree", unavailable)
        success, output = scheduler_script._run_job_script(job["script"], job=job,
                                                         interpreter=sys.executable)
        assert not success and "original authority fault" in output
        assert "injected tree cleanup failure" in caplog.text
        _assert_reaped(children[-1])
        assert unrelated.poll() is None
        _assert_untrusted(home, job)
    finally:
        unrelated.kill()
        unrelated.wait(timeout=5)


def test_interrupt_during_authority_consume_reaps_before_propagating(owned_script, monkeypatch):
    home, job, authority, children = owned_script()

    def interrupt(_pid):
        raise KeyboardInterrupt("injected authority interruption")

    monkeypatch.setattr(authority, "consume", interrupt)
    with pytest.raises(KeyboardInterrupt, match="authority interruption"):
        scheduler_script._run_job_script(job["script"], job=job, interpreter=sys.executable)
    _assert_reaped(children[-1])
    _assert_untrusted(home, job)


@pytest.mark.parametrize("stage", ["drain_error", "cleanup_interrupt", "no_signal_or_drain"])
def test_cleanup_fallbacks_keep_cause_and_reap(owned_script, monkeypatch, caplog, stage):
    home, job, authority, children = owned_script()

    def reject(_pid):
        raise ValueError("original authority fault")

    def cleanup_fault(_proc):
        if stage == "cleanup_interrupt":
            raise KeyboardInterrupt("injected cleanup interruption")
        raise OSError("injected pipe drain failure")

    monkeypatch.setattr(authority, "consume", reject)
    if stage == "drain_error":
        monkeypatch.setattr(scheduler_script, "_drain_script_pipes", cleanup_fault)
    elif stage == "cleanup_interrupt":
        monkeypatch.setattr(scheduler_script, "_terminate_cron_script_tree", cleanup_fault)
    else:
        monkeypatch.setattr(scheduler_script, "_terminate_cron_script_tree", lambda _proc: None)
        monkeypatch.setattr(scheduler_script, "_drain_script_pipes", lambda _proc: None)
    success, output = scheduler_script._run_job_script(job["script"], job=job,
                                                     interpreter=sys.executable)
    assert not success and "original authority fault" in output
    if stage != "no_signal_or_drain":
        assert "injected" in caplog.text
    proc = children[-1]
    _assert_reaped(proc)
    assert proc.stdout.closed and proc.stderr.closed
    _assert_untrusted(home, job)


def test_successful_real_authority_path_is_unchanged(owned_script):
    home, job, _authority, children = owned_script()
    success, output = scheduler_script._run_job_script(job["script"], job=job,
                                                     interpreter=sys.executable)
    assert success and output == "authorized-success"
    _assert_reaped(children[-1])
    receipt = json.loads((home / "receipt.json").read_text())
    assert receipt["trusted"] is False
    assert receipt["provenance_state"] == "PENDING_NATIVE_READONLY_VERIFICATION"
    assert executions.get_execution(job["execution_id"])["origin_proof_used"] == 1
