"""Recovery-only admission tests; persistent fixtures and transports remain isolated."""
import asyncio
import copy
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest

from gateway import startup_isolation as policy, delivery_ledger
from gateway.config import GatewayConfig, PlatformConfig, Platform
from gateway.platforms.base import BasePlatformAdapter
from gateway.run_startup import GatewayStartupMixin

@pytest.fixture(autouse=True)
def policy_process():
    names = ("_requested", "_resolved", "_effective", "_recovery_requested",
             "_delivery_requested", "_state_requested", "_observed", "_registered", "_denied")
    old = {name: copy.deepcopy(getattr(policy, name)) for name in names}
    policy._requested = policy._resolved = policy._effective = False
    policy._recovery_requested = policy._delivery_requested = policy._state_requested = True
    policy._observed = {"session_recovery_count": 0, "internal_replay_count": 0}
    policy._denied = {}
    yield
    for name, value in old.items(): setattr(policy, name, value)

def recovery_only():
    policy.resolve(False, recovery_enabled=True, external_delivery_enabled=False,
                   delivery_state_write_enabled=False)
    policy.register(*policy.REQUIRED)
    policy.verify()

@pytest.mark.parametrize("key", ["recovery_enabled", "external_delivery_enabled", "delivery_state_write_enabled"])
@pytest.mark.parametrize("bad", [None, 0, 1, "true", "false", {}, []])
def test_policy_config_rejects_non_boolean(key, bad):
    with pytest.raises(policy.StartupIsolationError):
        GatewayConfig.from_dict({"gateway": {key: bad}})

def test_parse_serialization_and_normal_defaults():
    cfg=GatewayConfig.from_dict({"gateway":{"recovery_enabled":True,"external_delivery_enabled":False,"delivery_state_write_enabled":False}})
    assert cfg.to_dict()["recovery_enabled"] is True
    assert cfg.to_dict()["external_delivery_enabled"] is False
    normal=GatewayConfig.from_dict({})
    assert normal.recovery_enabled and normal.external_delivery_enabled and normal.delivery_state_write_enabled

def test_inconsistent_delivery_state_rejected():
    with pytest.raises((ValueError, policy.StartupIsolationError)):
        GatewayConfig.from_dict({"gateway":{"external_delivery_enabled":False}})
    with pytest.raises(policy.StartupIsolationError):
        policy.resolve(False, external_delivery_enabled=False)

def test_uninitialized_fence_denies_recovery_then_missing_registry_fails_closed():
    policy.resolve(False, external_delivery_enabled=False, delivery_state_write_enabled=False)
    policy._registered.clear()
    assert not policy.allowed("recovery")
    assert not policy.allowed("external_delivery")
    with pytest.raises(policy.StartupIsolationError): policy.verify()
    assert not policy.evidence()["recovery_effective"]

def test_verified_fence_cannot_be_relaxed():
    recovery_only()
    with pytest.raises(policy.StartupIsolationError): policy.resolve(False)

def test_recovery_only_allowances():
    recovery_only()
    for action in ("recovery","replay","pending_tasks"): assert policy.allowed(action)
    for action in ("external_delivery","pending_send","delivery_state","notification","adapters","scheduler_dispatch"):
        assert not policy.allowed(action)
    e=policy.evidence()
    assert e["recovery_requested"] and e["recovery_effective"]
    assert not e["external_delivery_requested"] and not e["external_delivery_effective"]

def test_full_isolation_dominates_orthogonal_controls():
    policy.resolve(True)
    policy.register(*policy.REQUIRED);policy.verify()
    assert all(not policy.allowed(x) for x in policy.REQUIRED)

class CounterAdapter(BasePlatformAdapter):
    sends=0
    async def connect(self, **kw): return True
    async def disconnect(self): pass
    async def get_chat_info(self, *args, **kw): return {}
    async def send(self, *args, **kw):
        self.sends+=1
        return "fake-transport"

@pytest.mark.parametrize("platform", [Platform.TELEGRAM, Platform.LOCAL])
def test_adapter_transport_body_never_entered(platform):
    recovery_only()
    adapter=CounterAdapter(PlatformConfig(),platform)
    with pytest.raises(policy.StartupIsolationError): asyncio.run(adapter.send("fixture","pending"))
    assert adapter.sends==0

@pytest.mark.parametrize("name", ["telegram","bluebubbles","signal","matrix_via_adapter","weixin","qqbot","yuanbao"])
def test_standalone_transport_guard_precedes_body(name):
    from tools import send_message_senders
    recovery_only()
    with pytest.raises(policy.StartupIsolationError): asyncio.run(getattr(send_message_senders,"_send_"+name)())

def test_normal_fake_delivery_and_recovery_paths_retained():
    policy.resolve(False)
    adapter=CounterAdapter(PlatformConfig(),Platform.TELEGRAM)
    assert asyncio.run(adapter.send("fixture","normal"))=="fake-transport"
    assert adapter.sends==1 and policy.allowed("recovery")

@pytest.mark.parametrize("name", ["_send_to_platform", "_send_plugin_standalone"])
def test_plugin_and_webhook_dispatch_cannot_enter_transport(name):
    from tools import send_message_tool
    recovery_only()
    with pytest.raises(policy.StartupIsolationError):
        asyncio.run(getattr(send_message_tool, name)())

def test_telegram_imessage_pending_ledger_never_advances(tmp_path):
    db=delivery_ledger._db_path()
    for p in ("telegram","bluebubbles"):
        delivery_ledger.record_obligation(obligation_id=p, session_key="fixture", platform=p,
            chat_id="fixture", thread_id=None, content="pending-not-send")
    def rows():
        with sqlite3.connect("file:"+str(db)+"?mode=ro",uri=True) as c:
            return c.execute("SELECT * FROM delivery_obligations ORDER BY obligation_id").fetchall()
    before=rows();recovery_only()
    for call,args in [(delivery_ledger.mark_delivered,("telegram",)),
                      (delivery_ledger.mark_failed,("bluebubbles","fixture-error")),
                      (delivery_ledger.sweep_recoverable,())]:
        with pytest.raises(policy.StartupIsolationError): call(*args)
    assert rows()==before
    assert not delivery_ledger.ledger_enabled({"gateway":{"delivery_ledger":True}})

@pytest.mark.asyncio
async def test_real_session_recovery_changes_internal_state_not_delivery(tmp_path):
    from gateway.session import SessionStore, SessionSource
    store=SessionStore(sessions_dir=tmp_path/"sessions", config=GatewayConfig())
    entry=store.get_or_create_session(SessionSource(platform=Platform.TELEGRAM,chat_id="fixture",user_id="u",chat_type="private"))
    store.mark_turn_active(entry.session_key)
    async def recover(**kw): return store.recover_interrupted_turns(**kw)
    runner=SimpleNamespace(async_session_store=SimpleNamespace(recover_interrupted_turns=recover),
                           _ledger_crash_left_replies=AsyncMock())
    recovery_only()
    resumed,ledgered=await GatewayStartupMixin._recover_unclean_sessions(runner)
    assert resumed==1 and ledgered==0
    runner._ledger_crash_left_replies.assert_not_awaited()
    current=store._entries[entry.session_key]
    assert current.resume_pending and current.active_turn_token is None
    assert policy.evidence()["recovery_observations"]["session_recovery_count"]==1

@pytest.mark.asyncio
async def test_internal_replay_changes_fixture_while_outbound_stays_pending(tmp_path):
    replay=tmp_path/"internal-replay.json";replay.write_text(json.dumps({"state":"pending"}))
    internal=SimpleNamespace(internal=True,source="internal")
    outbound=SimpleNamespace(internal=False,source="outbound")
    unavailable=SimpleNamespace(internal=True,source="unavailable")
    adapter=CounterAdapter(PlatformConfig(),Platform.LOCAL)
    async def handle(event):
        replay.write_text(json.dumps({"state":"completed"}))
        with pytest.raises(policy.StartupIsolationError): await adapter.send("fixture","must-deny")
    runner=SimpleNamespace(_startup_restore_queue=[internal,outbound,unavailable],
        _intake_adapter_for=lambda source: SimpleNamespace(handle_message=handle) if source=="internal" else None)
    recovery_only()
    assert await GatewayStartupMixin._drain_startup_restore_queue(runner)==1
    assert json.loads(replay.read_text())["state"]=="completed"
    assert runner._startup_restore_queue==[outbound,unavailable]
    assert adapter.sends==0
    assert policy.evidence()["recovery_observations"]["internal_replay_count"]==1

@pytest.mark.asyncio
async def test_pending_send_and_startup_notice_denied_before_runner_access():
    recovery_only()
    runner=SimpleNamespace()
    await GatewayStartupMixin._await_startup_boot_sends(runner,planned_restart_notification_pending=True)
    await GatewayStartupMixin._start_post_connect_services(runner,connected_count=1)
    await GatewayStartupMixin._start_finish_wiring(runner,connected_count=1)

@pytest.mark.asyncio
async def test_recovery_only_startup_uses_real_session_and_internal_replay(tmp_path):
    from types import MethodType
    from gateway.session import SessionStore, SessionSource
    store=SessionStore(sessions_dir=tmp_path/"sessions",config=GatewayConfig())
    entry=store.get_or_create_session(SessionSource(platform=Platform.LOCAL,chat_id="internal-fixture",user_id="u",chat_type="private"))
    store.mark_turn_active(entry.session_key)
    task=tmp_path/"pending-internal-task.json"
    task.write_text(json.dumps({"state":"pending"}))
    adapter=CounterAdapter(PlatformConfig(),Platform.LOCAL)
    async def recover(**kw): return store.recover_interrupted_turns(**kw)
    async def handle(event):
        task.write_text(json.dumps({"state":"completed"}))
        store.clear_resume_pending(entry.session_key)
        with pytest.raises(policy.StartupIsolationError): await adapter.send("fixture","must-not-send")
    runner=SimpleNamespace(
        _start_install_faulthandler=lambda:None,
        _start_log_startup_environment=AsyncMock(),
        async_session_store=SimpleNamespace(recover_interrupted_turns=recover),
        _ledger_crash_left_replies=AsyncMock(),
        _startup_restore_queue=[SimpleNamespace(internal=True,source="internal")],
        _intake_adapter_for=lambda source:SimpleNamespace(handle_message=handle),
        delivery_router=SimpleNamespace(adapters={"must-not-connect":object()}),
        _update_runtime_status=lambda status:(tmp_path/"platform-health.json").write_text(status))
    runner._recover_unclean_sessions=MethodType(GatewayStartupMixin._recover_unclean_sessions,runner)
    runner._drain_startup_restore_queue=MethodType(GatewayStartupMixin._drain_startup_restore_queue,runner)
    recovery_only()
    assert await GatewayStartupMixin._start_impl(runner) is True
    assert runner._running and not runner.delivery_router.adapters
    assert json.loads(task.read_text())["state"]=="completed"
    assert not store._entries[entry.session_key].resume_pending
    assert adapter.sends==0
    evidence=policy.evidence()
    assert evidence["recovery_observations"]=={"session_recovery_count":1,"internal_replay_count":1}
    assert evidence["external_delivery_effective"] is False
    assert evidence["delivery_state_write_allowed"] is False
    runner._ledger_crash_left_replies.assert_not_awaited()

@pytest.mark.asyncio
async def test_startup_aborts_before_recovery_if_delivery_fence_lost():
    recovery_only();policy._registered.remove("external_delivery")
    runner=SimpleNamespace(_start_install_faulthandler=lambda:None,
        _start_log_startup_environment=AsyncMock(),_recover_unclean_sessions=AsyncMock())
    with pytest.raises(policy.StartupIsolationError): await GatewayStartupMixin._start_impl(runner)
    runner._recover_unclean_sessions.assert_not_awaited()
