"""Scoped defer composes with, never replaces, existing Core admission."""
import asyncio
import copy
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from gateway import startup_isolation as policy, scoped_startup_defer as defer
from gateway.config import GatewayConfig
from gateway.run_startup import GatewayStartupMixin


@pytest.fixture(autouse=True)
def process_policy(monkeypatch):
    for name in ("_requested","_resolved","_effective","_recovery_requested","_delivery_requested",
                 "_state_requested","_startup_context","_no_external_messages","_registered","_denied"):
        monkeypatch.setattr(policy,name,copy.deepcopy(getattr(policy,name)))
    policy._resolved = policy._requested = policy._effective = False
    policy._startup_context = "normal"; policy._no_external_messages = False
    policy._recovery_requested = policy._delivery_requested = policy._state_requested = True


def silent():
    policy.resolve(False,external_delivery_enabled=False,delivery_state_write_enabled=False,
                   startup_context="silent_recovery",no_external_messages=True)
    policy.register(*policy.REQUIRED); policy.verify()


def obligations(home):
    home.mkdir(parents=True,exist_ok=True)
    (home / ".restart_pending.json").write_bytes(b'{"original":"preserve"}\n')
    with sqlite3.connect(home / "state.db") as c:
        c.execute("create table delivery_obligations(obligation_id text,state text,attempts integer)")
        c.executemany("insert into delivery_obligations values(?,?,0)",[("business","pending"),("control","failed"),("already-sent","delivered")])
    return {p.name:p.read_bytes() for p in home.iterdir() if p.is_file()}


def test_real_pending_defer_preserves_bytes_and_gate_rejects_bypass(tmp_path,monkeypatch):
    home = tmp_path / "home"; before = obligations(home); monkeypatch.setenv("HERMES_HOME",str(home))
    silent()
    result = defer.audit_pending()
    assert result["pending_count"] == result["deferred_count"] == 3
    assert result["transport_attempt_count"] == result["external_send_count"] == 0
    assert {p.name:p.read_bytes() for p in home.iterdir() if p.is_file()} == before
    records = [json.loads(p.read_bytes()) for p in (home / "runtime/gateway/silent-startup-deferrals").iterdir()]
    assert len(records)==3 and all(not r["obligation_consumed"] for r in records)
    assert all(r["state"]=="DEFERRED_BY_SILENT_STARTUP_POLICY" for r in records)
    assert all(r["execution_context"]=="silent_recovery" and r["policy_source"] for r in records)
    assert defer.audit_pending()["deferred_count"]==3  # same-process idempotence
    assert len(list((home / "runtime/gateway/silent-startup-deferrals").iterdir()))==3
    for authorization in ({"business_permit":True},{"control_notification_authorized":True},{"both":True}):
        # Neither authorization changes Core admission. An authorized caller still
        # must pass the transport gate; defer does not mint either authorization.
        reached=[]
        @policy.outbound_admission
        def transport(auth): reached.append(auth)
        with pytest.raises(policy.StartupIsolationError): transport(authorization)
        assert reached==[]
    for action in ("notification","pending_send","delivery_state","external_delivery"):
        with pytest.raises(policy.StartupIsolationError): policy.require(action)
    assert policy.allowed("recovery") and policy.allowed("replay")


@pytest.mark.asyncio
async def test_replay_gate_audits_before_claim_or_notification(tmp_path,monkeypatch):
    home=tmp_path/"home";before=obligations(home);monkeypatch.setenv("HERMES_HOME",str(home));silent()
    runner=SimpleNamespace(_claim_pending_obligations=AsyncMock(side_effect=AssertionError("claim")),
                           _send_restart_notification=AsyncMock(side_effect=AssertionError("send")))
    await GatewayStartupMixin._await_startup_boot_sends(runner,planned_restart_notification_pending=True)
    from gateway.run_notifications import GatewayNotificationsMixin
    await GatewayNotificationsMixin._replay_pending_planned_restart_notification(runner)
    await GatewayNotificationsMixin._send_restart_notification(runner)
    assert await GatewayNotificationsMixin._send_update_notification(runner) is False
    runner._claim_pending_obligations.assert_not_awaited();runner._send_restart_notification.assert_not_awaited()
    assert {p.name:p.read_bytes() for p in home.iterdir() if p.is_file()}==before


def test_normal_default_does_not_audit_consume_or_block(tmp_path,monkeypatch):
    home=tmp_path/"home";before=obligations(home);monkeypatch.setenv("HERMES_HOME",str(home))
    policy.resolve(False)
    assert GatewayConfig.from_dict({}).startup_context=="normal"
    assert defer.audit_pending()["context"]=="normal"
    assert not (home/"runtime").exists()
    assert all(policy.allowed(a) for a in policy.REQUIRED)
    assert {p.name:p.read_bytes() for p in home.iterdir() if p.is_file()}==before


@pytest.mark.parametrize("values", [
    {"startup_context":"invalid"}, {"startup_context":None},
    {"startup_context":"silent_recovery"},
    {"startup_context":"silent_recovery","no_external_messages":True},
    {"no_external_messages":True},
    {"startup_context":"silent_recovery","no_external_messages":"true","external_delivery_enabled":False,"delivery_state_write_enabled":False}])
def test_invalid_context_fails_closed(values):
    with pytest.raises(policy.StartupIsolationError): GatewayConfig.from_dict({"gateway":values})


def test_existing_delivery_fence_and_full_isolation_stay_stricter():
    silent()
    with pytest.raises(policy.StartupIsolationError): policy.resolve(False)
    policy.resolve(True,external_delivery_enabled=False,delivery_state_write_enabled=False,
                   startup_context="silent_recovery",no_external_messages=True)
    policy.verify()
    assert all(not policy.allowed(a) for a in policy.REQUIRED)


def test_real_config_loader_binds_context(tmp_path,monkeypatch):
    monkeypatch.setenv("HERMES_HOME",str(tmp_path))
    (tmp_path/"config.yaml").write_text('gateway:\n  startup_context: silent_recovery\n  no_external_messages: true\n  external_delivery_enabled: false\n  delivery_state_write_enabled: false\n')
    policy.resolve_config()
    assert policy.evidence()["startup_context"]=="silent_recovery"
    assert policy.evidence()["no_external_messages"] is True
    from gateway.run import _load_gateway_config
    config=GatewayConfig.from_dict(_load_gateway_config())
    assert config.startup_context=="silent_recovery" and config.no_external_messages
    assert config.to_dict()["startup_context"]=="silent_recovery"


def test_sidecar_escape_and_incomplete_fence_fail_closed(tmp_path,monkeypatch):
    monkeypatch.setenv("HERMES_HOME",str(tmp_path));silent()
    policy._registered.discard("external_delivery")
    with pytest.raises(policy.StartupIsolationError):defer.audit_pending()
    policy.register(*policy.REQUIRED);policy.verify()
    (tmp_path/"runtime").symlink_to(tmp_path.parent)
    with pytest.raises(policy.StartupIsolationError):defer.audit_pending()
