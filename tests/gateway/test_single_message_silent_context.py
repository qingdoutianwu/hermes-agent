"""Permit validity and transport permission are separate, persistent contracts."""
import copy
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import pytest

from gateway import single_message_permit as permit, startup_isolation as policy
from gateway import scoped_startup_defer as defer
from gateway.controlled_delivery_transport import ControlledTelegramTransport


@pytest.fixture(autouse=True)
def isolated_policy(monkeypatch):
    for name in ("_resolved","_requested","_effective","_recovery_requested","_delivery_requested",
                 "_state_requested","_registered","_denied","_startup_context","_no_external_messages"):
        monkeypatch.setattr(policy,name,copy.deepcopy(getattr(policy,name)))
    policy._resolved=policy._requested=policy._effective=False
    policy._startup_context="normal";policy._no_external_messages=False
    policy._delivery_requested=policy._recovery_requested=policy._state_requested=True
    policy.resolve(False,external_delivery_enabled=False,delivery_state_write_enabled=False)
    policy.register(*policy.REQUIRED);policy.verify()


def silence():
    policy.resolve(False,external_delivery_enabled=False,delivery_state_write_enabled=False,
                   startup_context="silent_recovery",no_external_messages=True)
    policy.verify()


def message(channel="telegram"):
    return permit.ExactMessage.bind(channel,{"profile":"default","account":"fake-account",
        "chat_id":"4242","thread_id":None},"isolated exact message")


class FakeTransport:
    def __init__(self,m):
        self.channel=m.channel;self.destination_json=m.destination_json;self.calls=0
    async def send_once(self,m):
        self.calls+=1
        return {"message_id":"fixture"}


def bytes_before():
    return permit._path().read_bytes()


@pytest.mark.asyncio
@pytest.mark.parametrize("channel",["telegram","imessage"])
async def test_valid_normal_permit_cannot_transport_after_silent_context(channel):
    m=message(channel);p=permit.create_permit(m);silence();before=bytes_before()
    v=permit.validate_permit(p["permit_id"],m,ledger_entry_id=p["ledger_entry_id"])
    assert v["permit_validation"]=="PASS" and v["startup_context"]=="silent_recovery"
    assert v["silent_recovery_state"] and not v["external_send_allowed"]
    t=FakeTransport(m)
    with pytest.raises(permit.TransportAdmissionDenied) as denied:
        await permit.deliver_once(p["permit_id"],m,ledger_entry_id=p["ledger_entry_id"],transport=t)
    assert denied.value.validation["permit_validation"]=="PASS"
    assert t.calls==0 and bytes_before()==before
    e=permit.evidence(p["permit_id"])
    assert not e["permit_consumed"] and e["permit_phase"]=="CREATED"
    assert e["actual_transport_attempts"]==e["attempt_budget_spent"]==0


@pytest.mark.asyncio
async def test_permit_created_in_silent_mode_validates_without_sending():
    silence();m=message();p=permit.create_permit(m)
    before=bytes_before()
    assert permit.validate_permit(p["permit_id"],m,ledger_entry_id=p["ledger_entry_id"])["permit_validation"]=="PASS"
    with pytest.raises(permit.TransportAdmissionDenied):
        await permit.deliver_once(p["permit_id"],m,ledger_entry_id=p["ledger_entry_id"],transport=FakeTransport(m))
    assert before==bytes_before()


@pytest.mark.asyncio
async def test_core_transport_direct_bypass_is_denied_before_provider():
    m=message();permit.create_permit(m);silence()
    class Bot:
        calls=0
        async def send_message(self,**kwargs):
            self.calls+=1;return SimpleNamespace(message_id=1,chat=SimpleNamespace(id=4242))
    bot=Bot();transport=ControlledTelegramTransport(m.destination_json,bot)
    with pytest.raises(permit.TransportAdmissionDenied):await transport.send_once(m)
    assert bot.calls==0


@pytest.mark.asyncio
async def test_normal_core_one_shot_remains_allowed_and_nonretryable():
    m=message();p=permit.create_permit(m)
    v=permit.validate_permit(p["permit_id"],m,ledger_entry_id=p["ledger_entry_id"])
    assert v["permit_validation"]=="PASS" and v["external_send_allowed"]
    class Bot:
        calls=0
        async def send_message(self,**kwargs):
            self.calls+=1;return SimpleNamespace(message_id=1,chat=SimpleNamespace(id=4242))
    bot=Bot();t=ControlledTelegramTransport(m.destination_json,bot)
    out=await permit.deliver_once(p["permit_id"],m,ledger_entry_id=p["ledger_entry_id"],transport=t)
    assert out["permit_terminal_state"]=="SENT" and out["permit_consumed"] and bot.calls==1
    with pytest.raises(permit.PermitDenied,match="CONSUMED"):
        await permit.deliver_once(p["permit_id"],m,ledger_entry_id=p["ledger_entry_id"],transport=t)
    assert bot.calls==1


@pytest.mark.asyncio
async def test_pending_defer_plus_valid_permit_preserves_all_state(tmp_path,monkeypatch):
    home=tmp_path/"isolated";home.mkdir();monkeypatch.setenv("HERMES_HOME",str(home))
    m=message();p=permit.create_permit(m)
    marker=home/".restart_pending.json";marker.write_bytes(b'{"original":"obligation"}\n')
    with sqlite3.connect(permit._path()) as c:
        c.execute("create table delivery_obligations(obligation_id text,state text)")
        c.execute("insert into delivery_obligations values('pending-business','pending')")
    before=bytes_before();marker_before=marker.read_bytes();silence()
    proof=defer.audit_pending()
    assert proof["pending_count"]==proof["deferred_count"]==2
    t=FakeTransport(m)
    with pytest.raises(permit.TransportAdmissionDenied):
        await permit.deliver_once(p["permit_id"],m,ledger_entry_id=p["ledger_entry_id"],transport=t)
    assert t.calls==0 and marker.read_bytes()==marker_before and bytes_before()==before
    assert not permit.evidence(p["permit_id"])["permit_consumed"]


@pytest.mark.parametrize("bad",["message","purpose","ledger","attempts","expired"])
def test_silent_validation_keeps_real_invalid_permit_denials(bad):
    m=message();p=permit.create_permit(m);kwargs={"ledger_entry_id":p["ledger_entry_id"]}
    if bad=="message":m=permit.ExactMessage.bind(m.channel,json.loads(m.destination_json),"changed")
    elif bad=="purpose":kwargs["purpose"]="startup_notification"
    elif bad=="ledger":kwargs["ledger_entry_id"]="business-ledger"
    elif bad=="attempts":kwargs["attempts"]=2
    elif bad=="expired":
        with sqlite3.connect(permit._path()) as c:c.execute("update controlled_delivery_permits set expires_at=created_at where permit_id=?",(p["permit_id"],))
    silence();before=bytes_before()
    with pytest.raises(permit.PermitDenied) as error:permit.validate_permit(p["permit_id"],m,**kwargs)
    assert not isinstance(error.value,permit.TransportAdmissionDenied)
    assert before==bytes_before()


def test_transport_entry_cannot_bypass_silent_gate():
    m=message();p=permit.create_permit(m)
    token=permit._reserve(p["permit_id"],m,ledger_entry_id=p["ledger_entry_id"],purpose=permit.PURPOSE,attempts=1)
    silence();before=bytes_before()
    with pytest.raises(permit.TransportAdmissionDenied):permit._transport_entry(p["permit_id"],token)
    assert before==bytes_before()
    out=permit.evidence(p["permit_id"])
    assert out["permit_consumed"] and out["permit_terminal_state"]=="SENT_UNCERTAIN"
    assert out["actual_transport_attempts"]==0  # durable reservation not rolled back/adopted
