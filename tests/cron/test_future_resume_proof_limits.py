"""Diagnostic counterexamples, NOT acceptance proofs or production experiments.

Passing here means the documented candidate limitations are still observable;
they block candidate promotion even when the focused regression suite is green.
"""
from datetime import timedelta

from cron import jobs
from cron.future_dispatch import dispatch_registration
from tests.cron import test_future_resume as support
from tests.cron.test_future_resume import raw, request

paused_store = support.paused_store


def test_commit_stall_after_last_clock_check_is_not_closed(paused_store, monkeypatch):
    jid, _, clock, _ = paused_store
    replace = jobs.atomic_replace
    def stalled_replace(*args):
        # Controlled elapsed wall time AFTER the last callback but BEFORE rename.
        clock[0] += timedelta(minutes=20)
        return replace(*args)
    monkeypatch.setattr(jobs, "atomic_replace", stalled_replace)
    result = request(jid)
    assert result["success"]  # records the missing contract, not safe acceptance
    assert support.datetime.fromisoformat(raw(jid)["next_run_at"]) <= clock[0]
    captured = dict(raw(jid), _scheduled_instant=raw(jid)["next_run_at"])
    with dispatch_registration(captured) as admitted:
        assert admitted  # therefore commit-time strict futurity remains BLOCKED


def test_protected_job_manual_admission_requires_separate_contract(paused_store):
    jid, _, _, _ = paused_store
    assert request(jid)["success"]
    assert jobs.claim_job_for_fire(jid, manual=True, return_job=True) is False
    # An unadapted external provider/manual caller has no generation input. This
    # conservative refusal must not be mistaken for complete surface compatibility.
