"""R2 safety regressions converted from preserved R1 diagnostic evidence."""
from datetime import timedelta

from cron import jobs
from cron.future_dispatch import dispatch_registration
from tests.cron import test_future_resume as support
from tests.cron.test_future_resume import raw, request

paused_store = support.paused_store


def test_commit_stall_after_last_clock_check_is_fenced(paused_store, monkeypatch):
    jid, _, clock, _ = paused_store
    replace = jobs.atomic_replace
    def stalled_replace(*args):
        # Controlled elapsed wall time AFTER the last callback but BEFORE rename.
        clock[0] += timedelta(minutes=20)
        return replace(*args)
    monkeypatch.setattr(jobs, "atomic_replace", stalled_replace)
    result = request(jid)
    assert result["success"] is False and result["dispatch_admitted"] is False
    assert support.datetime.fromisoformat(raw(jid)["next_run_at"]) <= clock[0]
    captured = dict(raw(jid), _scheduled_instant=raw(jid)["next_run_at"])
    with dispatch_registration(captured) as admitted:
        assert admitted is False
    assert jobs.get_due_jobs() == []
    assert jobs.claim_job_for_fire(jid, force=True, return_job=True) is False


def test_protected_job_manual_admission_preserves_explicit_semantics(paused_store):
    jid, _, _, _ = paused_store
    assert request(jid)["success"]
    scheduled = raw(jid)["next_run_at"]
    claim = jobs.claim_job_for_fire(jid, manual=True, return_job=True)
    assert claim["_dispatch_kind"] == "manual" and claim["_scheduled_instant"] is None
    assert raw(jid)["next_run_at"] == scheduled
    with dispatch_registration(claim) as admitted:
        assert admitted
