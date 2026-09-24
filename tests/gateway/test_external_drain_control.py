"""Tests for the external drain-control marker contract + gateway state machine.

Task 2.2/2.3. Two layers:
  * drain_control.py — the presence-based marker contract (write/clear/read,
    HERMES_HOME-scoped, never-raises).
  * GatewayRunner enter/exit/watcher + the new-turn accept gate — the
    reversible state machine driven by the marker.

Mocked tests are necessary-not-sufficient here (the HARD live-validation gate,
Q-B, exercises a real `hermes gateway run`); these lock the unit contract.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import gateway.drain_control as dc
from gateway.run import GatewayRunner
from gateway.config import Platform
from gateway.platforms.event import MessageEvent, MessageType
from tests.gateway.restart_test_helpers import make_restart_runner, make_restart_source


# ---------------------------------------------------------------------------
# Marker contract (drain_control.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    return tmp_path


class TestMarkerContract:
    def test_absent_by_default(self, home):
        assert dc.drain_requested() is False
        assert dc.read_drain_request() is None

    def test_write_then_present(self, home):
        payload = dc.write_drain_request(principal="nas")
        assert dc.drain_requested() is True
        assert payload["action"] == "drain"
        assert payload["principal"] == "nas"
        body = dc.read_drain_request()
        assert body is not None and body["principal"] == "nas"


class TestSuppressNotification:
    """The generic suppress_notification flag on the drain marker.

    Gates ONLY the gateway's home-channel shutdown broadcast (NAS auto-update
    sets it true). Default-false so legacy/operator drains behave as before.
    The reader reuses the NS-570 epoch-staleness check so an orphaned marker
    can never silence a fresh gateway.
    """

    def test_default_false(self, home):
        payload = dc.write_drain_request(principal="nas")
        assert payload["suppress_notification"] is False
        assert dc.drain_notification_suppressed() is False

    def test_flag_round_trips_true(self, home):
        payload = dc.write_drain_request(principal="nas", suppress_notification=True)
        assert payload["suppress_notification"] is True
        body = dc.read_drain_request()
        assert body is not None and body["suppress_notification"] is True
        assert dc.drain_notification_suppressed() is True


# ---------------------------------------------------------------------------
# Instantiation-epoch staleness (NS-570: orphaned marker on durable volume)
# ---------------------------------------------------------------------------


class TestInstantiationEpoch:
    def test_write_stamps_current_epoch(self, home):
        payload = dc.write_drain_request(principal="nas")
        assert payload["epoch"] == dc.current_instantiation_epoch()
        body = dc.read_drain_request()
        assert body is not None and body["epoch"] == dc.current_instantiation_epoch()


    def test_marker_from_prior_instantiation_reads_as_absent(self, home, monkeypatch):
        # THE NS-570 REGRESSION. A begin-drain marker written by a PREVIOUS
        # container/VM instantiation survives on the durable HERMES_HOME volume
        # across a machine restart. The freshly-restarted gateway (new epoch)
        # must treat it as absent, NOT re-engage drain.
        monkeypatch.setattr(dc, "current_instantiation_epoch", lambda: "epoch-OLD")
        dc.write_drain_request(principal="nas")  # stamps "epoch-OLD"
        assert dc.drain_requested() is True  # same epoch → active

        # Simulate the restart: a brand-new instantiation epoch.
        monkeypatch.setattr(dc, "current_instantiation_epoch", lambda: "epoch-NEW")
        # The marker file is still physically present on the volume…
        assert dc.drain_request_path().exists() is True
        # …but it is ignored because its epoch belongs to a prior instantiation.
        assert dc.drain_requested() is False


    def test_current_epoch_empty_when_proc_unreadable(self, monkeypatch):
        # When neither /proc identity source is readable, the epoch is "" so
        # the staleness check is disabled rather than crashing.
        from pathlib import Path as _P

        orig_read_text = _P.read_text

        def _boom(self, *a, **k):
            if str(self).startswith("/proc/"):
                raise OSError("no /proc")
            return orig_read_text(self, *a, **k)

        dc.current_instantiation_epoch.cache_clear()
        monkeypatch.setattr(_P, "read_text", _boom)
        try:
            assert dc.current_instantiation_epoch() == ""
        finally:
            dc.current_instantiation_epoch.cache_clear()


# ---------------------------------------------------------------------------
# requested_at max-age (#85433: same-epoch orphaned marker, no restart)
# ---------------------------------------------------------------------------


class TestMarkerMaxAge:
    def test_fresh_marker_honoured(self, home):
        dc.write_drain_request(principal="nas")
        assert dc.drain_requested() is True

    def test_expired_marker_reads_as_absent(self, home):
        # THE #85433 REGRESSION. A drain-gated action completes WITHOUT a
        # machine restart, so the epoch still matches — but the writer never
        # cancelled the drain. The orphan must not wedge the gateway forever.
        from datetime import datetime, timedelta, timezone

        dc.write_drain_request(principal="nas", suppress_notification=True)
        body = dc.read_drain_request()
        assert body is not None
        body["requested_at"] = (
            datetime.now(timezone.utc)
            - timedelta(seconds=dc.DRAIN_REQUEST_MAX_AGE_SECONDS + 60)
        ).isoformat()
        dc.drain_request_path().write_text(json.dumps(body), encoding="utf-8")

        # The marker file is still physically present, with the CURRENT epoch…
        assert dc.drain_request_path().exists() is True
        # …but it is ignored because it outlived any legitimate drain.
        assert dc.drain_requested() is False
        # The suppression flag of an expired orphan is likewise ignored.
        assert dc.drain_notification_suppressed() is False

    def test_marker_without_timestamp_still_honoured(self, home):
        # Leniency contract: no requested_at (legacy/corrupt body) must fail
        # toward quiescing, exactly like the epoch check.
        payload = {"action": "drain", "epoch": dc.current_instantiation_epoch()}
        dc.drain_request_path().write_text(json.dumps(payload), encoding="utf-8")
        assert dc.drain_requested() is True

    def test_unparseable_timestamp_still_honoured(self, home):
        payload = {
            "action": "drain",
            "epoch": dc.current_instantiation_epoch(),
            "requested_at": "not-a-timestamp",
        }
        dc.drain_request_path().write_text(json.dumps(payload), encoding="utf-8")
        assert dc.drain_requested() is True

    def test_naive_timestamp_treated_as_utc(self, home):
        # A writer that stamped a tz-naive ISO string must still expire.
        from datetime import datetime, timedelta, timezone

        stale_naive = (
            datetime.now(timezone.utc)
            - timedelta(seconds=dc.DRAIN_REQUEST_MAX_AGE_SECONDS + 60)
        ).replace(tzinfo=None)
        payload = {
            "action": "drain",
            "epoch": dc.current_instantiation_epoch(),
            "requested_at": stale_naive.isoformat(),
        }
        dc.drain_request_path().write_text(json.dumps(payload), encoding="utf-8")
        assert dc.drain_requested() is False

    def test_expiry_warning_logged_once_per_marker(self, home, caplog):
        # The watcher polls every 1s; an expired orphan must warn ONCE, not
        # once per tick (~86k/day). A refreshed marker that expires again
        # warns again (new requested_at).
        import logging
        from datetime import datetime, timedelta, timezone

        def _write_expired(offset_seconds):
            dc.write_drain_request(principal="nas")
            body = dc.read_drain_request()
            assert body is not None
            body["requested_at"] = (
                datetime.now(timezone.utc)
                - timedelta(seconds=dc.DRAIN_REQUEST_MAX_AGE_SECONDS + offset_seconds)
            ).isoformat()
            dc.drain_request_path().write_text(json.dumps(body), encoding="utf-8")

        _write_expired(60)
        with caplog.at_level(logging.WARNING, logger="gateway.drain_control"):
            assert dc.drain_requested() is False
            assert dc.drain_requested() is False  # second poll tick
            assert dc.drain_requested() is False  # third poll tick
        expired_logs = [r for r in caplog.records if "expired drain marker" in r.message]
        assert len(expired_logs) == 1

        caplog.clear()
        _write_expired(120)  # a DIFFERENT requested_at that is also expired
        with caplog.at_level(logging.WARNING, logger="gateway.drain_control"):
            assert dc.drain_requested() is False
        expired_logs = [r for r in caplog.records if "expired drain marker" in r.message]
        assert len(expired_logs) == 1

    def test_rewrite_refreshes_the_clock(self, home):
        # The sanctioned keep-alive: re-writing the marker bumps requested_at,
        # so a deliberately long drain stays honoured.
        from datetime import datetime, timedelta, timezone

        dc.write_drain_request(principal="nas")
        body = dc.read_drain_request()
        assert body is not None
        body["requested_at"] = (
            datetime.now(timezone.utc)
            - timedelta(seconds=dc.DRAIN_REQUEST_MAX_AGE_SECONDS + 60)
        ).isoformat()
        dc.drain_request_path().write_text(json.dumps(body), encoding="utf-8")
        assert dc.drain_requested() is False  # expired…
        dc.write_drain_request(principal="nas")  # …keep-alive re-write
        assert dc.drain_requested() is True


# ---------------------------------------------------------------------------
# Gateway state machine (enter / exit / idempotency)
# ---------------------------------------------------------------------------


def _drain_runner():
    runner, adapter = make_restart_runner()
    runner._external_drain_active = False
    # Bind the real methods under test.
    runner._enter_external_drain = GatewayRunner._enter_external_drain.__get__(
        runner, GatewayRunner
    )
    runner._exit_external_drain = GatewayRunner._exit_external_drain.__get__(
        runner, GatewayRunner
    )
    return runner, adapter


class TestDrainStateMachine:


    def test_enter_idempotent(self):
        runner, _ = _drain_runner()
        runner._enter_external_drain()
        runner._update_runtime_status.reset_mock()
        runner._enter_external_drain()  # second call — no-op
        runner._update_runtime_status.assert_not_called()


    def test_exit_during_shutdown_does_not_revert_to_running(self):
        runner, _ = _drain_runner()
        runner._enter_external_drain()
        runner._update_runtime_status.reset_mock()
        # A shutdown drain is now in progress — exit must NOT resurrect running.
        runner._draining = True
        runner._exit_external_drain()
        assert runner._external_drain_active is False
        runner._update_runtime_status.assert_not_called()


# ---------------------------------------------------------------------------
# Watcher reconciliation
# ---------------------------------------------------------------------------


class TestDrainWatcher:

    @pytest.mark.asyncio
    async def test_watcher_enters_then_exits_with_marker(self, home):
        runner, _ = _drain_runner()
        runner._drain_control_watcher = GatewayRunner._drain_control_watcher.__get__(
            runner, GatewayRunner
        )
        # Drive a few ticks manually rather than spinning the loop.
        dc.write_drain_request()
        task = asyncio.create_task(runner._drain_control_watcher(interval=0.02))
        await asyncio.sleep(0.06)
        assert runner._external_drain_active is True
        dc.clear_drain_request()
        await asyncio.sleep(0.06)
        assert runner._external_drain_active is False
        runner._running = False
        await asyncio.sleep(0.04)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# New-turn accept gate
# ---------------------------------------------------------------------------


class TestNewTurnGate:
    @pytest.mark.asyncio
    async def test_new_turn_refused_during_external_drain(self):
        runner, _ = _drain_runner()
        runner._external_drain_active = True
        event = MessageEvent(
            text="hello",
            message_type=MessageType.TEXT,
            source=make_restart_source(),
            message_id="m1",
        )
        result = await runner._handle_message(event)
        assert result is not None
        assert "draining" in result.lower()


class TestSchedulerMaintenanceFence:
    @staticmethod
    def _owner_identity():
        from gateway.status import get_process_start_time
        import os

        return {
            "gateway_python_pid": os.getpid(),
            "gateway_process_started_at": str(get_process_start_time(os.getpid())),
            "binding_id": "legacy-fixture",
            "launch_agent_sha256": "a" * 64,
        }

    def _acquire(self, home, *, ttl=dc.SCHEDULER_MAINTENANCE_TTL_SECONDS, monkeypatch=None):
        if monkeypatch is not None:
            monkeypatch.setattr(
                dc,
                "_wait_for_scheduler_maintenance_dispatch_observation",
                lambda *_args, **_kwargs: {"source": "TEST_FIXTURE", "scheduler_dispatch_blocked": True},
            )
        return dc.acquire_scheduler_maintenance_fence(
            owner="r3c-test-owner",
            purpose="isolated scheduler maintenance regression",
            scheduler_owner_identity=self._owner_identity(),
            home=home,
            ttl_seconds=ttl,
            tick_lock_homes=[home],
        )

    def test_fence_blocks_dispatch_without_turning_into_chat_drain_and_owner_releases(self, home, monkeypatch):
        from gateway.run import GatewayRunner

        version = home / "runtime" / "scheduler-core" / "versions" / "legacy-fixture"
        version.mkdir(parents=True)
        current = home / "runtime" / "scheduler-core" / "current"
        current.parent.mkdir(parents=True, exist_ok=True)
        current.symlink_to(version, target_is_directory=True)
        monkeypatch.setattr(dc, "_current_scheduler_tick_homes", lambda: [home.resolve()])

        acquired = self._acquire(home, monkeypatch=monkeypatch)
        proof = dc.read_scheduler_maintenance_proof(acquired["proof_path"], home=home)
        assert "nonce" not in acquired
        assert dc.active_scheduler_maintenance_fence(home=home)["maintenance_id"] == acquired["maintenance_id"]
        assert dc.scheduler_dispatch_blocked(home=home) is True
        assert dc.drain_requested(home=home) is False

        runner = type("Runner", (), {
            "_draining": False,
            "_external_drain_active": False,
            "_scheduler_maintenance_home": home,
        })()
        runner._cron_dispatch_allowed = GatewayRunner._cron_dispatch_allowed.__get__(runner, type(runner))
        assert runner._cron_dispatch_allowed() is False
        with pytest.raises(dc.MaintenanceFenceOwnershipError):
            dc.clear_drain_request(home=home)

        dc.update_scheduler_maintenance_fence_state(
            acquired["proof_path"], owner=proof["owner"], nonce=proof["nonce"], state="DRAINING", home=home,
        )
        with patch("cron.scheduler.get_running_job_details", return_value=[
            {"job_id": "running-a"}, {"job_id": "running-a"},
        ]), patch.object(dc, "_current_scheduler_tick_homes", return_value=[home.resolve()]):
            draining_ack = dc.sync_scheduler_maintenance_ack(home=home)
        assert draining_ack["running_job_count"] == 2
        assert draining_ack["running_job_ids"] == ["running-a", "running-a"]

        dc.update_scheduler_maintenance_fence_state(
            acquired["proof_path"], owner=proof["owner"], nonce=proof["nonce"], state="QUIESCED", home=home,
        )
        with patch("cron.scheduler.get_running_job_details", return_value=[]), patch.object(
            dc, "_current_scheduler_tick_homes", return_value=[home.resolve()]
        ):
            quiet_ack = dc.sync_scheduler_maintenance_ack(home=home)
        assert quiet_ack["running_job_count"] == 0
        assert quiet_ack["scheduler_dispatch_blocked"] is True

        with pytest.raises(dc.MaintenanceFenceOwnershipError):
            dc.release_scheduler_maintenance_fence(
                acquired["proof_path"], owner=proof["owner"], nonce="invalid-owner-token", home=home,
            )
        receipt = dc.release_scheduler_maintenance_fence(
            acquired["proof_path"], owner=proof["owner"], nonce=proof["nonce"], home=home,
        )
        assert receipt["state"] == "RELEASED"
        assert Path(receipt["receipt_path"]).stat().st_mode & 0o777 == 0o444
        assert dc.scheduler_dispatch_blocked(home=home) is False
        assert runner._cron_dispatch_allowed() is True

    def test_expired_bounded_fence_reopens_dispatch_even_after_gateway_epoch_changes(self, home, monkeypatch):
        from datetime import datetime, timedelta, timezone

        acquired = self._acquire(home, ttl=30, monkeypatch=monkeypatch)
        proof_path = Path(acquired["proof_path"])
        proof_doc = json.loads(proof_path.read_text(encoding="utf-8"))
        fence = proof_doc["fence"]
        now = datetime.now(timezone.utc)
        fence["created_at"] = (now - timedelta(seconds=31)).isoformat(timespec="microseconds")
        fence["expires_at"] = (now - timedelta(seconds=1)).isoformat(timespec="microseconds")
        fence["ttl_seconds"] = 30.0
        fence["proof_sha256"] = dc._maintenance_proof_sha256(fence)
        proof_path.write_text(json.dumps(proof_doc), encoding="utf-8")
        proof_path.chmod(0o600)
        marker = dc.read_drain_request(home=home)
        marker["requested_at"] = fence["created_at"]
        marker["scheduler_maintenance_fence"] = fence
        dc.drain_request_path(home).write_text(json.dumps(marker), encoding="utf-8")
        dc.drain_request_path(home).chmod(0o600)
        monkeypatch.setattr(dc, "current_instantiation_epoch", lambda: "new-gateway-epoch")

        assert dc.active_scheduler_maintenance_fence(home=home) is None
        assert dc.scheduler_dispatch_blocked(home=home) is False

    def test_tick_barrier_timeout_never_publishes_a_fence(self, home):
        from cron import scheduler

        cron_dir = home / "cron"
        cron_dir.mkdir()
        held = scheduler._acquire_tick_lock(cron_dir / ".tick.lock")
        try:
            with pytest.raises(dc.DrainControlBusyError, match="TICK_BARRIER_TIMEOUT"):
                dc.acquire_scheduler_maintenance_fence(
                    owner="r3c-test-owner",
                    purpose="contended barrier fixture",
                    scheduler_owner_identity=self._owner_identity(),
                    home=home,
                    tick_lock_homes=[home],
                    tick_barrier_timeout_seconds=0.1,
                )
            assert dc.read_drain_request(home=home) is None
        finally:
            scheduler._release_tick_lock(held)

    def test_old_gateway_external_drain_status_is_fresh_owner_bound_gate_evidence(self, home):
        import os
        from datetime import datetime, timezone

        identity = self._owner_identity()
        status_path = home / "gateway_state.json"
        status_path.write_text(json.dumps({
            "pid": os.getpid(),
            "start_time": identity["gateway_process_started_at"],
            "hermes_home": str(home.resolve()),
            "gateway_state": "draining",
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
        }), encoding="utf-8")
        status_path.chmod(0o600)
        evidence = dc._legacy_external_drain_attestation(
            home,
            {"scheduler_owner_identity": identity},
        )
        assert evidence is not None
        assert evidence["source"] == "LEGACY_EXTERNAL_DRAIN_STATUS"
        assert evidence["scheduler_dispatch_blocked"] is True
