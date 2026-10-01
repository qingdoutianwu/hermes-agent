"""Core admission regressions. All pending data and transport fakes are isolated."""
import asyncio
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway import startup_isolation as policy
from gateway import delivery_ledger
from gateway.config import GatewayConfig, PlatformConfig, Platform
from gateway.platforms.base import BasePlatformAdapter
from gateway.run_startup import GatewayStartupMixin


@pytest.fixture(autouse=True)
def process_policy_fixture():
    # Only test process state is restored; runtime forbids disabling an enabled policy.
    old = (policy._requested, policy._resolved, policy._effective, set(policy._registered), dict(policy._denied))
    policy._requested = False; policy._resolved = False; policy._effective = False; policy._denied = {}
    yield
    policy._requested, policy._resolved, policy._effective, policy._registered, policy._denied = old


@pytest.mark.parametrize("value",[None,0,1,"true","false",{},[]])
def test_strict_config_rejects_non_boolean(value):
    with pytest.raises(policy.StartupIsolationError):
        GatewayConfig.from_dict({"gateway":{"startup_isolation":value}})


@pytest.mark.parametrize("value",[True,False])
def test_config_parse_readback(value):
    config=GatewayConfig.from_dict({"gateway":{"startup_isolation":value}})
    assert config.startup_isolation is value
    assert config.to_dict()["startup_isolation"] is value


def test_enabled_process_cannot_be_live_unfenced():
    policy.resolve(True)
    with pytest.raises(policy.StartupIsolationError):policy.resolve(False)


def test_missing_admission_fails_closed():
    policy.resolve(True); policy._registered.clear()
    with pytest.raises(policy.StartupIsolationError):policy.verify()
    assert policy.evidence()["startup_isolation_effective"] is False
    with pytest.raises(policy.StartupIsolationError):policy.require("external_delivery")


@pytest.mark.parametrize("method,arguments,expected",[
    ("_schedule_resume_pending_sessions",{},0),
    ("_recover_unclean_sessions",{},(0,0)),
    ("_drain_startup_restore_queue",{},0),
    ("_start_recover_previous_run",{},None),
    ("_await_startup_boot_sends",{"planned_restart_notification_pending":True},None),
    ("_start_post_connect_services",{"connected_count":1},None),
    ("_start_finish_wiring",{"connected_count":1},None),
    ("_start_prefilter_platforms",{},(False,0,[],[])),
])
def test_recovery_and_replay_denied_before_fixture_mutation(tmp_path,method,arguments,expected):
    pending={"pending_sessions":[{"resume_pending":True,"active_turn_token":"fixture"}],
             "pending_tasks":[{"state":"queued","command":"must-not-run"}],
             "outbound":[{"platform":"telegram","state":"pending","content":"fixture"}]}
    file=tmp_path/"pending.json";file.write_text(json.dumps(pending));before=file.read_bytes()
    policy.resolve(True)
    runner=SimpleNamespace(_startup_restore_queue=[object()],_startup_restore_tasks=[object()])
    result=getattr(GatewayStartupMixin,method)(runner,**arguments)
    if asyncio.iscoroutine(result):result=asyncio.run(result)
    assert result==expected
    assert file.read_bytes()==before
    assert runner._startup_restore_tasks  # tasks were never started/cancelled/deleted


def test_real_pending_ledger_table_rows_unchanged_when_writer_admission_denied(tmp_path):
    db=tmp_path/"state.db"
    with sqlite3.connect(db) as conn:
        delivery_ledger._initialize_schema(conn)
        conn.execute("INSERT INTO delivery_obligations(obligation_id,session_key,platform,chat_id,content,state,created_at,updated_at) VALUES ('pending-1','pending-session','telegram','fixture','not-send','pending',1,1)")
    def rows():
        with sqlite3.connect("file:"+str(db)+"?mode=ro",uri=True) as conn:
            return conn.execute("SELECT * FROM delivery_obligations").fetchall()
    before=rows();policy.resolve(True)
    with pytest.raises(policy.StartupIsolationError):delivery_ledger.record_obligation(obligation_id="new",session_key="s",platform="telegram",chat_id="fixture",thread_id=None,content="no")
    with pytest.raises(policy.StartupIsolationError):delivery_ledger.sweep_recoverable()
    assert rows()==before


def test_existing_ledger_optout_is_reused_and_isolation_overrides_enabled():
    policy.resolve(True)
    assert delivery_ledger.ledger_enabled({"gateway":{"delivery_ledger":True}}) is False


class FakeTransportAdapter(BasePlatformAdapter):
    sends=0
    async def connect(self,**kw):return True
    async def disconnect(self):pass
    async def get_chat_info(self,*args,**kw):return {}
    async def send(self,*args,**kw):
        self.sends+=1
        return "normal-send"


@pytest.mark.parametrize("platform",[Platform.TELEGRAM,Platform.LOCAL])
def test_platform_subclass_send_admission_before_transport(platform):
    adapter=FakeTransportAdapter(PlatformConfig(),platform);policy.resolve(True)
    with pytest.raises(policy.StartupIsolationError):asyncio.run(adapter.send("fixture","pending"))
    assert adapter.sends==0


def test_send_admission_normal_mode_path_preserved():
    adapter=FakeTransportAdapter(PlatformConfig(),Platform.TELEGRAM);policy.resolve(False)
    assert asyncio.run(adapter.send("fixture","normal"))=="normal-send"
    assert adapter.sends==1  # fake transport only; no real Delivery


def test_platform_state_and_structured_receipt_allowed(tmp_path):
    policy.resolve(True);policy.register(*policy.REQUIRED);policy.verify()
    p=tmp_path/"platform-health.json";p.write_text(json.dumps(policy.evidence()))
    data=json.loads(p.read_text())
    assert data["platform_state_write_allowed"] is True
    assert data["startup_isolation_requested"] is True
    assert data["startup_isolation_effective"] is True
    assert data["external_delivery_allowed"] is False
    assert data["delivery_state_write_allowed"] is False


def test_normal_mode_retains_existing_recovery_and_ledger_paths():
    policy.resolve(False)
    assert policy.allowed("recovery") is True
    assert policy.allowed("pending_send") is True
    assert delivery_ledger.ledger_enabled({"gateway":{"delivery_ledger":False}}) is False
    assert delivery_ledger.ledger_enabled({"gateway":{"delivery_ledger":True}}) is True


@pytest.mark.parametrize("transport",["telegram","bluebubbles","signal","matrix_via_adapter","weixin","qqbot","yuanbao"])
def test_standalone_transport_denied_before_credentials_or_network(transport):
    from tools import send_message_senders as senders
    policy.resolve(True)
    method=getattr(senders,"_send_"+transport)
    # No valid arguments are supplied: denial must occur before the transport body.
    with pytest.raises(policy.StartupIsolationError):
        asyncio.run(method())


def test_inherited_media_method_denied_before_file_preprocessing():
    adapter=FakeTransportAdapter(PlatformConfig(),Platform.TELEGRAM);policy.resolve(True)
    with pytest.raises(policy.StartupIsolationError):
        asyncio.run(adapter.send_image("fixture","/file-must-not-be-read"))
    assert adapter.sends==0


def test_real_pending_normal_recovery_path_still_exists(tmp_path):
    from gateway.session import SessionStore,SessionSource
    store=SessionStore(sessions_dir=tmp_path/"sessions",config=GatewayConfig())
    entry=store.get_or_create_session(SessionSource(platform=Platform.TELEGRAM,chat_id="fixture-normal",user_id="u",chat_type="private"))
    store.mark_turn_active(entry.session_key)
    policy.resolve(False)
    result=store.recover_interrupted_turns(max_age_seconds=3600)
    assert result>=1
