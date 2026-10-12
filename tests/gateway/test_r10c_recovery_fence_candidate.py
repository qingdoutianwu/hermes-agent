"""Four bounded Recovery-Only fences; no startup or production binding claim."""
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from gateway import startup_isolation as policy


@pytest.fixture(autouse=True)
def isolated_policy(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    importlib.reload(policy)
    yield
    importlib.reload(policy)


def restrict():
    # Exact retained production policy shape, not a fabricated effective receipt.
    policy.resolve(False, recovery_enabled=True, external_delivery_enabled=False,
                   delivery_state_write_enabled=False)


@pytest.mark.parametrize("force", [False, True])
def test_direct_refuses_before_claim_or_force_resume(tmp_path, monkeypatch, force):
    from cron.jobs import create_job, claim_job_for_fire
    from tools import cronjob_tools

    job = create_job("fixture", "30 18 * * 1-5", script="never-run.py",
                     no_agent=True, deliver="local", paused=False)
    path = tmp_path / "cron/jobs.json"
    before = path.read_bytes()
    restrict()
    assert claim_job_for_fire(job["id"], force=force, manual=True) is False
    execute = Mock(side_effect=AssertionError("must not execute"))
    monkeypatch.setattr(cronjob_tools, "_run_claimed_job", execute)
    result = cronjob_tools._execute_job_now(job)
    assert not result["claimed"] and "STARTUP_ISOLATION_DENIED" in result["error"]
    execute.assert_not_called()
    assert path.read_bytes() == before


@pytest.mark.asyncio
async def test_startup_resume_queue_and_boot_claim_are_denied():
    from gateway.run_startup import GatewayStartupMixin

    restrict()
    event = object()
    candidates = Mock(side_effect=AssertionError("must not inspect replay candidates"))
    queue = [event]
    runner = SimpleNamespace(_startup_restore_queue=queue,
                             _resume_pending_candidates=candidates)
    assert GatewayStartupMixin._schedule_resume_pending_sessions(runner) == 0
    assert await GatewayStartupMixin._drain_startup_restore_queue(runner) == 0
    assert queue == [event]
    # Function must deny before even requiring an adapter or session store.
    assert await GatewayStartupMixin._claim_pending_obligations(runner) == []
    assert await GatewayStartupMixin._recover_unclean_sessions(runner) == (0, 0)
    await GatewayStartupMixin._await_startup_boot_sends(
        runner, planned_restart_notification_pending=True)
    candidates.assert_not_called()


@pytest.mark.asyncio
async def test_adapter_override_and_inherited_media_deny_before_io():
    from gateway.platforms.base import BasePlatformAdapter

    calls = []

    class RecordingAdapter(BasePlatformAdapter):
        async def connect(self, **kwargs):
            return True

        async def disconnect(self):
            return None

        async def get_chat_info(self, chat_id):
            return {}

        async def send(self, *args, **kwargs):
            calls.append("send")

        async def edit_message(self, *args, **kwargs):
            calls.append("edit")

    adapter = RecordingAdapter.__new__(RecordingAdapter)
    restrict()
    for call in (lambda: adapter.send("fixture", "fixture"),
                 lambda: adapter.edit_message("fixture", "fixture", "fixture"),
                 lambda: adapter.send_multiple_images("fixture", ["absent.png"])):
        with pytest.raises(policy.StartupIsolationError, match="external_delivery"):
            await call()
    assert calls == []
    from gateway.run_startup import GatewayStartupMixin
    from gateway.run_notifications import GatewayNotificationsMixin
    raw_adapter = SimpleNamespace(send=AsyncMock())
    runner = SimpleNamespace(_obligation_adapter=AsyncMock(return_value=raw_adapter))
    assert await GatewayStartupMixin._redeliver_claimed_obligations(runner, [{}]) == 0
    runner._obligation_adapter.assert_not_called()
    raw_adapter.send.assert_not_called()
    assert await GatewayNotificationsMixin._send_restart_notification(runner) is None


def test_delivery_writer_denies_preserving_prior_uncertainty(tmp_path, monkeypatch):
    from gateway import delivery_ledger
    from hermes_cli import sqlite_util

    previous = tmp_path / "prior-uncertain.json"
    previous.write_text('{"status":"SENT_UNCERTAIN","attempts":1}')
    before = previous.read_bytes()
    open_db = Mock(side_effect=AssertionError("database must not open"))
    monkeypatch.setattr(sqlite_util, "open_db", open_db)
    restrict()
    assert delivery_ledger.ledger_enabled({"gateway": {"delivery_ledger": True}}) is False
    with pytest.raises(policy.StartupIsolationError, match="delivery_state"):
        delivery_ledger.mark_delivered("fixture")
    with pytest.raises(policy.StartupIsolationError, match="delivery_state"):
        delivery_ledger.sweep_recoverable(deliverable_platforms={"telegram"})
    open_db.assert_not_called()
    assert previous.read_bytes() == before


def test_original_policy_cannot_relax_or_fabricate_registration():
    restrict()
    with pytest.raises(policy.StartupIsolationError, match="cannot be relaxed"):
        policy.resolve(False, external_delivery_enabled=True,
                       delivery_state_write_enabled=True)
    with pytest.raises(policy.StartupIsolationError, match="FENCE_MISSING"):
        policy.verify()
    assert not policy.evidence()["external_delivery_effective"]


@pytest.mark.parametrize("value", ["false", 0, None])
def test_original_policy_refuses_non_boolean_permissions(value):
    with pytest.raises(policy.StartupIsolationError, match="explicit boolean"):
        policy.resolve(False, external_delivery_enabled=value,
                       delivery_state_write_enabled=False)


def test_policy_reads_isolated_real_yaml_before_claim(tmp_path, monkeypatch):
    # Keep this diagnostic on the isolated owning-home path used by the retained policy.
    monkeypatch.setattr("hermes_constants.get_process_hermes_home", lambda: tmp_path)
    (tmp_path / "config.yaml").write_text(
        "gateway:\n  recovery_enabled: true\n  external_delivery_enabled: false\n"
        "  delivery_state_write_enabled: false\n")
    assert not policy.allowed("scheduler_dispatch")
    assert not policy.allowed("external_delivery")
    assert not policy.allowed("delivery_state")


def test_normal_mode_preserves_direct_claim():
    from cron.jobs import create_job, claim_job_for_fire

    policy.resolve(False)
    job = create_job("fixture", "30 18 * * 1-5", script="never-run.py",
                     no_agent=True, deliver="local", paused=False)
    assert claim_job_for_fire(job["id"], manual=True) is True


@pytest.mark.asyncio
async def test_normal_mode_keeps_existing_adapter_behavior():
    policy.resolve(False)
    calls = []

    @policy.outbound_admission
    async def existing_send():
        calls.append(1)
        return "accepted-test-only"

    assert await existing_send() == "accepted-test-only"
    assert calls == [1]
