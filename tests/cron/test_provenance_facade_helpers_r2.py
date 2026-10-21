"""Behavior contracts for the R2-S6 Job/worker responsibility extraction."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from cron import executions, jobs
from cron.scheduler_detached_worker import prepare_external_worker_ack


@pytest.fixture
def local_store(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", home / "cron" / "executions.db")
    with jobs.use_cron_store(home):
        yield home


@pytest.mark.parametrize("value,expected", [(True, "true"), (False, "false"),
                                           ("true", "true"), ("false", "false")])
def test_extracted_update_preserves_authority_and_repeat_readback(local_store, value, expected):
    job = jobs.create_job(prompt="isolated", schedule="every 4h", deliver="local", paused=True)
    assert job["_r2_authority_required"] == "false"
    jobs.update_job(job["id"], {"repeat": {"times": None, "completed": 2}})
    updated = jobs.update_job(job["id"], {
        "_r2_authority_required": value, "_r2_release_manifest": "  candidate.json  ", "repeat": 3})
    assert updated["_r2_authority_required"] == expected
    assert updated["_r2_release_manifest"] == "candidate.json"
    assert updated["repeat"] == {"times": 3, "completed": 2}
    assert updated == jobs.get_job(job["id"])
    assert updated["enabled"] is False and updated["state"] == "paused"


def test_invalid_update_never_reaches_store(local_store):
    job = jobs.create_job(prompt="isolated", schedule="every 4h", deliver="local", paused=True)
    path = local_store / "cron" / "jobs.json"
    before = path.read_bytes()
    with pytest.raises(ValueError):
        jobs.update_job(job["id"], {"repeat": "not-a-repeat", "_r2_authority_required": True})
    assert path.read_bytes() == before
    assert jobs.get_job(job["id"])["_r2_authority_required"] == "false"


def test_ack_contains_complete_native_owner_identity(local_store):
    ack = local_store / "worker" / "ready.json"
    job = {"id": "legacy", "execution_id": "isolated-ack"}
    assert prepare_external_worker_ack(job, job["execution_id"], ack) is True
    assert json.loads(ack.read_text()) == {"pid": os.getpid(), "execution_id": job["execution_id"]}
    assert "_r2_authority" not in job
    assert not list(ack.parent.glob("*.tmp*"))


def test_authority_refusal_settles_native_row_before_ack(local_store):
    record = executions.create_execution("refused", source="builtin")
    executions.mark_execution_running(record["id"])
    ack = local_store / "ready.json"
    job = {"id": "refused", "execution_id": record["id"],
           "_r2_authority_required": "true", "no_agent": False}
    assert prepare_external_worker_ack(job, record["id"], ack) is False
    assert not ack.exists()
    row = executions.get_execution(record["id"])
    assert row["status"] == "failed"
    assert "ORIGIN_AUTHORITY_REQUIRES_NO_AGENT" in row["error"]
    assert row["origin_proof_used"] == 0


def test_ack_publish_failure_is_not_acknowledged(local_store, monkeypatch, caplog):
    from cron import scheduler
    ack = local_store / "worker" / "ready.json"

    def failed_replace(source, destination):
        assert Path(source).read_text()
        assert destination == ack
        raise OSError("isolated ACK rename fault")

    monkeypatch.setattr(scheduler.os, "replace", failed_replace)
    assert prepare_external_worker_ack({"id": "legacy"}, "isolated-ack", ack) is False
    assert not ack.exists() and not list(ack.parent.glob("*.tmp*"))
    assert "isolated ACK rename fault" in caplog.text
