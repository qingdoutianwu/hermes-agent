"""No real transports. Permit table and all fixtures stay under isolated test HOME."""
import asyncio
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
import pytest
from gateway import startup_isolation as policy, single_message_permit as permit
from gateway import delivery_ledger

TEXT="Hermes controlled delivery test\ntask=R9\nno business action required"
DEST={"profile":"default","account":"fixture-account","chat_id":"1234","thread_id":None}

@pytest.fixture(autouse=True)
def mode():
    names=("_requested","_resolved","_effective","_recovery_requested","_delivery_requested",
           "_state_requested","_observed","_registered","_denied")
    old={n:copy.deepcopy(getattr(policy,n)) for n in names}
    policy._requested=policy._resolved=policy._effective=False
    policy._recovery_requested=policy._delivery_requested=policy._state_requested=True
    policy._denied={}
    policy.resolve(False,external_delivery_enabled=False,delivery_state_write_enabled=False)
    policy.register(*policy.REQUIRED);policy.verify()
    yield
    for n,v in old.items():setattr(policy,n,v)

def exact(channel="telegram",destination=None,text=TEXT):
    return permit.ExactMessage.bind(channel,DEST if destination is None else destination,text)

class Fake:
    def __init__(self,message,outcome="sent"):
        self.channel=message.channel;self.destination_json=message.destination_json
        self.calls=[];self.outcome=outcome
    async def send_once(self,m):
        self.calls.append({"channel":m.channel,"destination":m.destination_json,"message_sha256":m.message_hash})
        if self.outcome=="timeout":raise TimeoutError("fixture uncertainty")
        if self.outcome=="failed":raise permit.DefinitelyNotSent("fixture before submission")
        if self.outcome=="disconnect":raise ConnectionError("fixture unknown delivery")
        if self.outcome=="missing":return None
        return {"message_id":"fixture-provider-id"}

async def send(p,m,t,**kw):
    return await permit.deliver_once(p["permit_id"],m,ledger_entry_id=kw.pop("ledger_entry_id",p["ledger_entry_id"]),transport=t,**kw)

@pytest.mark.asyncio
@pytest.mark.parametrize("channel", ["telegram", "imessage"])
async def test_one_exact_send_and_reuse_denied(channel):
    m=exact(channel);p=permit.create_permit(m);t=Fake(m)
    out=await send(p,m,t)
    assert out["permit_consumed"] and out["permit_terminal_state"]=="SENT"
    assert out["actual_transport_attempts"]==1 and not out["auto_retry"]
    assert t.calls==[{"channel":channel,"destination":m.destination_json,"message_sha256":m.message_hash}]
    with pytest.raises(permit.PermitDenied,match="ALREADY_CONSUMED"):await send(p,m,t)
    assert len(t.calls)==1
    assert not policy.allowed("external_delivery") and not policy.allowed("delivery_state")
    assert policy.allowed("recovery")

@pytest.mark.parametrize("change",["channel","chat_id","thread_id","profile","account","message","purpose","attempts","ledger"])
@pytest.mark.asyncio
async def test_binding_denied_before_transport(change):
    m=exact();p=permit.create_permit(m);dest=dict(DEST);kw={}
    if change=="channel":bad=exact("imessage")
    elif change in DEST:dest[change]="changed";bad=exact(destination=dest)
    elif change=="message":bad=exact(text=TEXT+" ")
    else:
        bad=m
        kw[{"purpose":"purpose","attempts":"attempts","ledger":"ledger_entry_id"}[change]]={"purpose":"startup_notification","attempts":2,"ledger":"unrelated"}[change]
    t=Fake(bad)
    with pytest.raises(permit.PermitDenied):await send(p,bad,t,**kw)
    assert not t.calls and not permit.evidence(p["permit_id"])["permit_consumed"]

@pytest.mark.asyncio
async def test_missing_permit_denied():
    m=exact();t=Fake(m)
    with pytest.raises(permit.PermitDenied,match="MISSING"):
        await permit.deliver_once("missing",m,ledger_entry_id="missing",transport=t)
    assert not t.calls

@pytest.mark.asyncio
async def test_expiration_does_not_refresh():
    m=exact();p=permit.create_permit(m,ttl_seconds=0.05)
    expires=p["expires_at"]
    await asyncio.sleep(0.08);t=Fake(m)
    with pytest.raises(permit.PermitDenied,match="EXPIRED"):await send(p,m,t)
    assert not t.calls and permit.evidence(p["permit_id"])["expires_at"]==expires

@pytest.mark.parametrize("ttl",[0,-1,True,float("nan"),float("inf"),301,"60"])
def test_invalid_ttl_denied(ttl):
    with pytest.raises(permit.PermitDenied):permit.create_permit(exact(),ttl_seconds=ttl)

@pytest.mark.asyncio
async def test_full_isolation_overrides_existing_permit():
    m=exact();p=permit.create_permit(m);policy.resolve(True,external_delivery_enabled=False,delivery_state_write_enabled=False)
    with pytest.raises(permit.PermitDenied,match="FULL_ISOLATION"):await send(p,m,Fake(m))
    assert not permit._path().is_symlink()

@pytest.mark.parametrize("outcome,state", [("failed","FAILED"),("timeout","SENT_UNCERTAIN"),("disconnect","SENT_UNCERTAIN"),("missing","SENT_UNCERTAIN")])
@pytest.mark.asyncio
async def test_no_retry_after_any_transport_result(outcome,state):
    m=exact();p=permit.create_permit(m);t=Fake(m,outcome)
    if outcome in {"timeout","disconnect"}:
        with pytest.raises((TimeoutError,ConnectionError)):await send(p,m,t)
    else:await send(p,m,t)
    assert permit.evidence(p["permit_id"])["permit_terminal_state"]==state
    with pytest.raises(permit.PermitDenied,match="CONSUMED"):await send(p,m,t)
    assert len(t.calls)==1

def test_concurrent_threads_one_transport_invocation():
    m=exact();p=permit.create_permit(m);t=Fake(m)
    def run():
        try:return asyncio.run(send(p,m,t))
        except permit.PermitDenied as e:return str(e)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:run(),range(2)))
    assert len(t.calls)==1
    assert sum(isinstance(x,dict) for x in results)==1
    assert any("ALREADY_CONSUMED" in x for x in results if isinstance(x,str))

@pytest.mark.parametrize("after_entry",[False,True])
def test_crash_consumption_persists_and_restart_does_not_send(after_entry, tmp_path):
    m=exact();p=permit.create_permit(m)
    # Real child crash after durable reservation or transport-entry boundary.
    code="""import sys,os,json
sys.path.insert(0,sys.argv[1])
from gateway import startup_isolation as q,single_message_permit as p
q.resolve(False,external_delivery_enabled=False,delivery_state_write_enabled=False)
q.register(*q.REQUIRED);q.verify()
m=p.ExactMessage.bind('telegram',json.loads(sys.argv[3]),sys.argv[4])
token=p._reserve(sys.argv[2],m,ledger_entry_id=sys.argv[5],purpose=p.PURPOSE,attempts=1)
if sys.argv[6]=='yes':
 p._transport_entry(sys.argv[2],token)
 # Fake transport's externally recorded invocation, then abrupt death before receipt.
 fd=os.open(sys.argv[7],os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
 os.write(fd,(m.message_hash+'\\n').encode());os.fsync(fd);os.close(fd)
os._exit(91)
"""
    root=Path(__file__).resolve().parents[2]
    calls=tmp_path/"crash-transport-calls.txt"
    result=subprocess.run([sys.executable,"-I","-B","-c",code,str(root),p["permit_id"],m.destination_json,m.text,p["ledger_entry_id"],"yes" if after_entry else "no",str(calls)],env=dict(os.environ),capture_output=True,text=True)
    assert result.returncode==91,result.stderr
    e=permit.evidence(p["permit_id"]);assert e["permit_consumed"] and e["permit_terminal_state"]=="SENT_UNCERTAIN"
    assert e["actual_transport_attempts"]==int(after_entry)
    t=Fake(m)
    with pytest.raises(permit.PermitDenied,match="CONSUMED"):asyncio.run(send(p,m,t))
    assert not t.calls
    assert (calls.read_text().splitlines() if calls.exists() else [])==([m.message_hash] if after_entry else [])

@pytest.mark.asyncio
async def test_pending_ledger_and_unrelated_permit_rows_unchanged():
    from hermes_constants import get_process_hermes_home
    import sqlite3
    policy._resolved=False;policy._effective=False
    policy._requested=False;policy._delivery_requested=policy._state_requested=True
    policy.resolve(False)
    for channel in ["telegram","bluebubbles"]:
        delivery_ledger.record_obligation(obligation_id=channel,session_key="fixture",platform=channel,chat_id="1234",thread_id=None,content="pending-existing")
    policy._resolved=False;policy.resolve(False,external_delivery_enabled=False,delivery_state_write_enabled=False);policy.verify()
    db=get_process_hermes_home()/"state.db"
    # The existing ledger fixture creator uses default filesystem mode; model the
    # private Core runtime DB explicitly, without relaxing the permit owner gate.
    db.chmod(0o600)
    def rows():
        with sqlite3.connect("file:"+str(db)+"?mode=ro",uri=True) as c:
            return c.execute("SELECT * FROM delivery_obligations ORDER BY obligation_id").fetchall()
    before=rows();m=exact();other=permit.create_permit(m);other_before=permit.evidence(other["permit_id"]);p=permit.create_permit(m)
    await send(p,m,Fake(m))
    assert rows()==before and permit.evidence(other["permit_id"])==other_before
    with pytest.raises(policy.StartupIsolationError):delivery_ledger.mark_delivered("telegram")
    assert not policy.allowed("pending_send")

@pytest.mark.asyncio
async def test_telegram_one_call_transport_does_not_use_retry_helper():
    from gateway.controlled_delivery_transport import ControlledTelegramTransport
    class Bot:
        calls=0
        async def send_message(self,**kw):
            self.calls+=1
            assert kw["text"]==TEXT and kw["parse_mode"] is None
            raise TimeoutError("uncertain")
    m=exact();bot=Bot();t=ControlledTelegramTransport(m.destination_json,bot)
    p=permit.create_permit(m)
    with pytest.raises(TimeoutError):await send(p,m,t)
    assert bot.calls==1
    assert permit.evidence(p["permit_id"])["permit_terminal_state"]=="SENT_UNCERTAIN"

def test_concurrent_processes_one_durable_transport_entry(tmp_path):
    m=exact();p=permit.create_permit(m);log=tmp_path/"transport-calls.jsonl"
    code="""import sys,os,json,asyncio
sys.path.insert(0,sys.argv[1])
from gateway import startup_isolation as q,single_message_permit as p
q.resolve(False,external_delivery_enabled=False,delivery_state_write_enabled=False)
q.register(*q.REQUIRED);q.verify()
m=p.ExactMessage.bind('telegram',json.loads(sys.argv[3]),sys.argv[4])
class T:
 channel=m.channel
 destination_json=m.destination_json
 async def send_once(self,message):
  fd=os.open(sys.argv[6],os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
  os.write(fd,(json.dumps({'hash':message.message_hash})+'\\n').encode());os.fsync(fd);os.close(fd)
  return {'message_id':'fake-child-provider'}
try:
 asyncio.run(p.deliver_once(sys.argv[2],m,ledger_entry_id=sys.argv[5],transport=T()))
 print('SENT')
except p.PermitDenied as e:print(str(e))
"""
    root=Path(__file__).resolve().parents[2]
    argv=[sys.executable,"-I","-B","-c",code,str(root),p["permit_id"],m.destination_json,m.text,p["ledger_entry_id"],str(log)]
    children=[subprocess.Popen(argv,env=dict(os.environ),stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for _ in range(2)]
    results=[child.communicate(timeout=20) for child in children]
    assert all(child.returncode==0 for child in children),results
    assert sum(out.strip()=="SENT" for out,err in results)==1
    assert any("DENIED_ALREADY_CONSUMED" in out for out,err in results)
    assert len(log.read_text().splitlines())==1
    assert permit.evidence(p["permit_id"])["actual_transport_attempts"]==1
    assert permit.evidence(p["permit_id"])["permit_terminal_state"]=="SENT"

@pytest.mark.asyncio
async def test_telegram_provider_receipt_exact_destination():
    from gateway.controlled_delivery_transport import ControlledTelegramTransport
    class Bot:
        async def send_message(self,**kw):return SimpleNamespace(message_id=77,chat=SimpleNamespace(id=1234))
    m=exact();p=permit.create_permit(m)
    e=await send(p,m,ControlledTelegramTransport(m.destination_json,Bot()))
    assert e["provider_receipt"]["message_id"]=="77" and e["permit_terminal_state"]=="SENT"

@pytest.mark.asyncio
async def test_transport_route_mismatch_never_consumes():
    m=exact();p=permit.create_permit(m);t=Fake(exact("imessage"))
    with pytest.raises(permit.PermitDenied,match="TRANSPORT_ROUTE"):await send(p,m,t)
    assert not t.calls and not permit.evidence(p["permit_id"])["permit_consumed"]

@pytest.mark.parametrize("purpose",["startup_notification","restart_notification","cron_notification","pending_replay","automatic_report"])
def test_automatic_paths_cannot_create_test_permit(purpose):
    with pytest.raises(permit.PermitDenied,match="PURPOSE"):permit.create_permit(exact(),purpose=purpose)

def test_insecure_database_permissions_fail_closed():
    p=permit.create_permit(exact())
    path=permit._path();path.chmod(0o644)
    with pytest.raises(permit.PermitDenied,match="PERMISSIONS"):permit.evidence(p["permit_id"])

@pytest.mark.asyncio
async def test_global_telegram_retry_default_preserved_with_fake_bot():
    from tools.send_message_senders import _send_telegram_message_with_retry
    class Bot:
        calls=0
        async def send_message(self,**kw):
            self.calls+=1
            return "fake-normal-response"
    # Normal-mode fixture only; no permit exception changes ordinary helper semantics.
    policy._resolved=False;policy._effective=False
    policy._delivery_requested=policy._state_requested=True;policy.resolve(False)
    import inspect
    assert inspect.signature(_send_telegram_message_with_retry).parameters["attempts"].default==3
    bot=Bot();assert await _send_telegram_message_with_retry(bot,text=TEXT)=="fake-normal-response"
    assert bot.calls==1
