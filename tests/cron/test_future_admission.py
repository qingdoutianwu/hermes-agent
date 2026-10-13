"""Post-replace durable fence, native provider/manual and business entry regressions."""
import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

from cron import jobs, scheduler
from cron.future_admission import confirmed
from cron.future_dispatch import dispatch_registration
from cron.future_resume import _store_home, inspect_job_revision, resume_future_only
from cron.scheduler_provider import InProcessCronScheduler
from tests.cron import test_future_resume as support
from tests.cron.test_future_resume import raw, request

paused_store = support.paused_store


def test_provider_current_slot_and_old_slot_are_distinct(paused_store):
    jid, _, clock, _ = paused_store
    old = raw(jid)["next_run_at"]
    assert request(jid)["success"]
    generation = raw(jid)["dispatch_generation"]
    provider = InProcessCronScheduler()
    with _store_home():
        assert provider.claim_fire(jid) is None
        assert provider.claim_fire(jid, expected_generation=generation, scheduled_instant=old) is None
        clock[0] = support.datetime.fromisoformat(raw(jid)["next_run_at"])
        selected = clock[0].isoformat()
        claim = provider.claim_fire(jid, expected_generation=generation, scheduled_instant=selected)
        assert claim and claim["_scheduled_instant"] == selected
        assert scheduler.get_execution(claim["execution_id"])["dispatch_generation"] == generation
        assert provider.claim_fire(jid, expected_generation=generation, scheduled_instant=selected) is None


def test_provider_manual_is_occurrence_free(paused_store, monkeypatch):
    jid, _, _, _ = paused_store
    assert request(jid)["success"]
    selected = raw(jid)["next_run_at"]
    with _store_home():
        claim = InProcessCronScheduler().claim_fire(jid, manual=True)
        assert claim and claim["_scheduled_instant"] is None
        assert raw(jid)["next_run_at"] == selected
        launched = []
        monkeypatch.setattr(scheduler, "_launch_external_cron_worker", lambda job: launched.append(job) or True)
        monkeypatch.setattr("cron.scheduler_worker_failure.record_unknown_worker_outcome", lambda *a, **k: False)
        assert scheduler.run_one_job(claim)
        assert len(launched) == 1


def test_raw_business_entry_denies_unregistered_protected_job(paused_store, monkeypatch):
    jid, _, _, _ = paused_store
    old = dict(raw(jid))
    assert request(jid)["success"]
    touched = []
    monkeypatch.setattr(scheduler, "_prepare_job_prompt", lambda *a: touched.append(a))
    assert scheduler.run_job(old)[-1] == "BLOCKED_EXECUTION_ADMISSION"
    current = dict(raw(jid), _scheduled_instant=raw(jid)["next_run_at"])
    assert scheduler.run_job(current)[-1] == "BLOCKED_EXECUTION_ADMISSION"
    assert touched == []


def test_direct_run_requires_native_fire_owner(paused_store, monkeypatch):
    jid, _, _, _ = paused_store
    assert request(jid)["success"]
    effects = []
    monkeypatch.setattr(scheduler, "_launch_external_cron_worker", effects.append)
    current = dict(raw(jid), _scheduled_instant=raw(jid)["next_run_at"])
    assert scheduler.run_one_job(current) is False
    assert effects == []


def test_unconfirmed_publication_survives_pause_and_ordinary_resume(paused_store, monkeypatch):
    jid, _, clock, _ = paused_store
    replace = jobs.atomic_replace
    def stall(*args):
        clock[0] += timedelta(minutes=20)
        return replace(*args)
    with monkeypatch.context() as patch:
        patch.setattr(jobs, "atomic_replace", stall)
        assert request(jid)["success"] is False
    jobs.pause_job(jid)
    jobs.resume_job(jid)
    assert not confirmed(raw(jid)) and jobs.get_due_jobs() == []


def test_tick_and_manual_wait_for_long_persistence(paused_store, monkeypatch):
    jid, _, clock, home = paused_store
    entered, release = Event(), Event()
    replace = jobs.atomic_replace
    def stalled(*args):
        entered.set()
        assert release.wait(5)
        clock[0] += timedelta(minutes=20)
        return replace(*args)
    monkeypatch.setattr(jobs, "atomic_replace", stalled)
    revision = inspect_job_revision(jid)["revision"]
    def scoped(function):
        with jobs.use_cron_store(home), _store_home():
            return function()
    with ThreadPoolExecutor(3) as pool:
        restoring = pool.submit(scoped, lambda: resume_future_only(jid, expected_revision=revision, request_id="blocked-write"))
        assert entered.wait(5)
        tick = pool.submit(scoped, jobs.get_due_jobs)
        manual = pool.submit(scoped, lambda: InProcessCronScheduler().claim_fire(jid, manual=True))
        release.set()
        assert restoring.result(5)["success"] is False
        assert tick.result(5) == [] and manual.result(5) is None


def test_confirmation_failure_does_not_enable_dispatch(paused_store, monkeypatch):
    import cron.future_admission as admission
    jid, _, _, _ = paused_store
    def fail(*args):
        raise OSError("isolated ledger fault")
    monkeypatch.setattr(admission, "confirm_after_replace", fail)
    revision = inspect_job_revision(jid)["revision"]
    first = resume_future_only(jid, expected_revision=revision, request_id="ledger-fail")
    assert first["error"] == "PERSISTENCE_OUTCOME_UNKNOWN"
    assert not jobs.is_job_runnable(raw(jid))
    assert resume_future_only(jid, expected_revision=revision, request_id="ledger-fail")["error"] == "BLOCKED_RESUME_UNCONFIRMED"


def test_old_pending_snapshot_is_rejected_even_with_current_generation(paused_store):
    jid, _, _, _ = paused_store
    old = raw(jid)["next_run_at"]
    assert request(jid)["success"]
    snapshot = dict(raw(jid), _scheduled_instant=old, pending_slot={"scheduled_at":old})
    with dispatch_registration(snapshot) as accepted:
        assert not accepted


def test_confirmation_read_is_profile_bound(paused_store, tmp_path):
    jid, _, _, _ = paused_store
    assert request(jid)["success"]
    identical = copy.deepcopy(raw(jid))
    with jobs.use_cron_store(tmp_path / "unconfirmed-profile"):
        jobs.save_jobs([identical])
        assert not jobs.is_job_runnable(raw(jid))
        assert jobs.get_due_jobs() == []


def test_native_trigger_can_run_without_old_occurrence(paused_store):
    jid, _, _, _ = paused_store
    assert request(jid)["success"]
    jobs.trigger_job(jid, extra_prompt="explicit isolated manual request")
    snapshot = next(job for job in jobs.get_due_jobs() if job["id"] == jid)
    assert snapshot["_scheduled_instant"] is None
    assert jobs.advance_next_runs([jid], expected_generations={jid: snapshot["dispatch_generation"]}) == 0
    with dispatch_registration(snapshot) as admitted:
        assert admitted
    claim = jobs.claim_job_for_fire(jid, expected_generation=snapshot["dispatch_generation"], return_job=True)
    assert claim and claim["_dispatch_kind"] == "manual"


def test_provider_claim_after_persistence_wait_is_current_only(paused_store, monkeypatch):
    jid, _, _, home = paused_store
    entered, release, attempting = Event(), Event(), Event()
    replace = jobs.atomic_replace
    def stall(*args):
        entered.set()
        assert release.wait(5)
        return replace(*args)
    monkeypatch.setattr(jobs, "atomic_replace", stall)
    revision = inspect_job_revision(jid)["revision"]
    def restore():
        with jobs.use_cron_store(home), _store_home():
            return resume_future_only(jid, expected_revision=revision, request_id="provider-race")
    def claim():
        attempting.set()
        with jobs.use_cron_store(home), _store_home():
            return InProcessCronScheduler().claim_fire(jid, expected_generation=0,
                                                      scheduled_instant="2026-10-13T07:51:00+00:00")
    with ThreadPoolExecutor(2) as pool:
        restoring = pool.submit(restore)
        assert entered.wait(5)
        claiming = pool.submit(claim)
        assert attempting.wait(5)
        release.set()
        assert restoring.result(5)["success"]
        assert claiming.result(5) is None


def test_manual_claim_after_resume_has_no_scheduled_identity(paused_store, monkeypatch):
    jid, _, _, home = paused_store
    entered, release, attempted = Event(), Event(), Event()
    replace = jobs.atomic_replace
    def stall(*args):
        entered.set()
        assert release.wait(5)
        return replace(*args)
    monkeypatch.setattr(jobs, "atomic_replace", stall)
    revision = inspect_job_revision(jid)["revision"]
    def scoped(function):
        with jobs.use_cron_store(home), _store_home():
            return function()
    def claim():
        attempted.set()
        return InProcessCronScheduler().claim_fire(jid, manual=True)
    with ThreadPoolExecutor(2) as pool:
        restoring = pool.submit(scoped, lambda: resume_future_only(jid, expected_revision=revision, request_id="manual-race"))
        assert entered.wait(5)
        claiming = pool.submit(scoped, claim)
        assert attempted.wait(5)
        release.set()
        assert restoring.result(5)["success"]
        current = claiming.result(5)
        assert current and current["_scheduled_instant"] is None
