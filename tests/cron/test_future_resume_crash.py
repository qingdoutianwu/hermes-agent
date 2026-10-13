"""Abrupt child exit around the real atomic rename; no production store or sends."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from cron import jobs
from cron.future_resume import inspect_job_revision, resume_future_only
from tests.cron import test_future_resume as support
from tests.cron.test_future_resume import raw

paused_store = support.paused_store


CHILD = r'''
import json, os, sys
from datetime import UTC, datetime
from cron import jobs
from cron.future_resume import resume_future_only
home, jid, revision, phase = sys.argv[1:]
jobs._hermes_now = lambda: datetime(2026, 10, 13, 8, 1, tzinfo=UTC)
jobs.get_timezone = lambda: UTC
original_stage = jobs._stage_jobs_payload
original_replace = jobs.atomic_replace
if phase == 'before_stage':
    jobs._stage_jobs_payload = lambda *args: os._exit(71)
elif phase == 'after_stage':
    def stage(*args):
        original_stage(*args)
        os._exit(72)
    jobs._stage_jobs_payload = stage
elif phase == 'after_replace':
    def replace(*args):
        original_replace(*args)
        os._exit(73)
    jobs.atomic_replace = replace
elif phase == 'before_ledger_commit':
    import cron.future_admission as admission
    original_confirm = admission.confirm_after_replace
    def confirm(*args):
        original_confirm(*args)
        os._exit(74)
    admission.confirm_after_replace = confirm
elif phase == 'after_ledger_commit':
    import cron.future_resume as future_resume
    future_resume._result = lambda *args, **kwargs: os._exit(75)
with jobs.use_cron_store(home):
    result = resume_future_only(jid, expected_revision=revision, request_id='crash-request')
print(json.dumps(result))
'''


def child(home, jid, revision, phase):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("HERMES_", "_HERMES_"))
           and not key.endswith(("_TOKEN", "_API_KEY", "_SECRET", "_PASSWORD"))}
    env["HERMES_HOME"] = str(home)
    env["HERMES_RUNTIME_DIR"] = str(home / "no-runtime")
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
    return subprocess.run([sys.executable, "-c", CHILD, str(home), jid, revision, phase],
        env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=20, check=False)


@pytest.mark.parametrize("phase,code,committed", [
    ("before_stage", 71, False), ("after_stage", 72, False), ("after_replace", 73, True),
])
def test_abrupt_exit_store_is_all_or_nothing(paused_store, phase, code, committed):
    jid, _, _, home = paused_store
    revision = inspect_job_revision(jid)["revision"]
    result = child(home, jid, revision, phase)
    assert result.returncode == code, result.stderr
    current = raw(jid)
    assert current["enabled"] is committed
    assert ("future_resume" in current) is committed
    assert (current["state"] == "scheduled") is committed
    if committed:
        retried = resume_future_only(jid, expected_revision=revision, request_id="crash-request")
        assert retried["error"] == "BLOCKED_RESUME_UNCONFIRMED" and current["dispatch_generation"] == 1
        assert jobs.is_job_runnable(current) is False
    else:
        assert current["state"] == "paused"
    assert jobs.get_due_jobs() == []


def test_success_and_same_request_survive_fresh_interpreter(paused_store):
    jid, _, _, home = paused_store
    revision = inspect_job_revision(jid)["revision"]
    first = child(home, jid, revision, "success")
    assert first.returncode == 0, first.stderr
    second = child(home, jid, revision, "success")
    assert second.returncode == 0, second.stderr
    first_result, second_result = json.loads(first.stdout), json.loads(second.stdout)
    assert first_result["operation"] == second_result["operation"]
    assert second_result["replayed"]
    assert jobs.get_due_jobs() == []


@pytest.mark.parametrize("phase,code,admitted", [
    ("before_ledger_commit", 74, False), ("after_ledger_commit", 75, True),
])
def test_admission_commit_crash_boundary(paused_store, phase, code, admitted):
    jid, _, _, home = paused_store
    revision = inspect_job_revision(jid)["revision"]
    outcome = child(home, jid, revision, phase)
    assert outcome.returncode == code, outcome.stderr
    assert jobs.is_job_runnable(raw(jid)) is admitted
    retried = resume_future_only(jid, expected_revision=revision, request_id="crash-request")
    assert retried["success"] is admitted
    assert raw(jid)["dispatch_generation"] == 1
