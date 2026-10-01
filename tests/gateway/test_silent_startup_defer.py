import hashlib
import json
import sqlite3
from unittest.mock import AsyncMock, patch

import pytest

import gateway.run as gateway_run
from gateway.config import Platform
from gateway.silent_startup import policy, require_transport_admission, SilentStartupPolicyError
from tests.gateway.restart_test_helpers import make_restart_runner


@pytest.fixture
def silent(tmp_path,monkeypatch):
    monkeypatch.setattr(gateway_run,'_hermes_home',tmp_path)
    runner,adapter=make_restart_runner()
    runner._runtime_recovery_startup={'execution_context':'runtime_recovery','reason':'silent_startup',
        'event':'gateway_restart','no_external_messages':True}
    adapter.send=AsyncMock(side_effect=AssertionError('NO_EXTERNAL_MESSAGES'))
    marker=tmp_path/'.restart_pending.json';marker.write_text('{"requested_at":1,"via_service":true}')
    requester=tmp_path/'.restart_notify.json';requester.write_text('{"platform":"telegram","chat_id":"fixture"}')
    db=tmp_path/'state.db';conn=sqlite3.connect(db)
    conn.execute('CREATE TABLE delivery_obligations(obligation_id TEXT,state TEXT,attempts INTEGER)')
    conn.execute("INSERT INTO delivery_obligations VALUES('fixture-obligation','pending',0)");conn.commit();conn.close()
    return runner,adapter,marker,requester,db,tmp_path


def audit_rows(root):
    return [json.loads(p.read_bytes()) for p in (root/'runtime/gateway/silent-startup-deferrals').glob('*.json')]


@pytest.mark.asyncio
async def test_silent_boot_preserves_marker_and_ledger_unclaimed(silent):
    runner,adapter,marker,requester,db,root=silent
    before=[p.read_bytes() for p in [marker,requester,db]]
    with patch.object(runner,'_claim_pending_obligations',side_effect=AssertionError('claim forbidden')):
        await runner._await_startup_boot_sends(planned_restart_notification_pending=True)
    adapter.send.assert_not_called()
    assert [p.read_bytes() for p in [marker,requester,db]]==before
    rows=audit_rows(root)
    assert len(rows)==3
    assert {r['obligation_id'] for r in rows}=={'marker:.restart_pending.json','marker:.restart_notify.json','fixture-obligation'}
    assert all(r['state']=='DEFERRED_BY_SILENT_STARTUP_POLICY' and not r['original_obligation_consumed'] for r in rows)
    assert sum(r['transport_attempt_count'] for r in rows)==sum(r['external_send_count'] for r in rows)==0
    assert all(p.stat().st_mode & 0o777==0o444 for p in (root/'runtime/gateway/silent-startup-deferrals').glob('*.json'))


@pytest.mark.asyncio
async def test_direct_replay_reconnect_and_timer_paths_gated(silent):
    runner,adapter,marker,requester,db,root=silent;raw=marker.read_bytes()
    await runner._replay_pending_planned_restart_notification()
    await runner._send_restart_notification()
    assert await runner._send_home_channel_startup_notifications()==set()
    assert await runner._claim_pending_obligations()==[]
    assert await runner._redeliver_claimed_obligations([{'obligation_id':'direct-row'}])==0
    assert await runner._redeliver_failed_obligations_for_platform(Platform.TELEGRAM)==0
    with patch.object(runner,'_resume_pending_candidates',side_effect=AssertionError('resume forbidden')):
        assert runner._schedule_resume_pending_sessions()==0
    runner._schedule_flood_redelivery(Platform.TELEGRAM)
    assert not await runner._send_home_channel_message(Platform.TELEGRAM,None,adapter,'payload','error')
    assert not await runner._send_update_notification()
    await runner._send_session_db_warning_notifications()
    adapter.send.assert_not_called();assert marker.read_bytes()==raw
    assert all(r['external_send_count']==0 for r in audit_rows(root))


def test_transport_bypass_is_rejected(silent):
    with pytest.raises(SilentStartupPolicyError,match='POLICY_GATE_REQUIRED'):
        require_transport_admission(silent[0])


def test_process_environment_context_is_scoped(monkeypatch):
    runner,_=make_restart_runner()
    monkeypatch.setenv('HERMES_RUNTIME_RECOVERY_STARTUP',json.dumps({
        'execution_context':'runtime_recovery','reason':'silent_startup',
        'event':'gateway_restart','no_external_messages':True}))
    assert policy(runner)['policy_source']=='HERMES_RUNTIME_RECOVERY_STARTUP'
    monkeypatch.setenv('HERMES_RUNTIME_RECOVERY_STARTUP','{')
    with pytest.raises(SilentStartupPolicyError):policy(runner)


@pytest.mark.parametrize('field,value',[('reason','other'),('execution_context','ordinary'),('event','report'),('no_external_messages',False)])
def test_invalid_context_fail_closed(silent,field,value):
    runner=silent[0];runner._runtime_recovery_startup[field]=value
    with pytest.raises(SilentStartupPolicyError):policy(runner)


@pytest.mark.asyncio
async def test_normal_startup_policy_absent_keeps_replay_path(silent):
    runner,adapter,marker,requester,db,root=silent;del runner._runtime_recovery_startup
    runner._planned_restart_notice_lock=None
    runner._served_home_channel_configs=lambda:[]
    runner._send_home_channel_startup_notifications=AsyncMock(return_value=set())
    await runner._replay_pending_planned_restart_notification()
    runner._send_home_channel_startup_notifications.assert_awaited_once()
    assert policy(runner) is None
    assert not marker.exists() # normal original lifecycle, isolated marker only


@pytest.mark.asyncio
async def test_defer_survives_runner_recreation_and_original_hash(silent):
    runner,adapter,marker,requester,db,root=silent
    sha=hashlib.sha256(marker.read_bytes()).hexdigest()
    await runner._replay_pending_planned_restart_notification()
    records=audit_rows(root);assert records[0]['original_sha256']==sha
    runner2,_=make_restart_runner();runner2._runtime_recovery_startup=runner._runtime_recovery_startup
    await runner2._replay_pending_planned_restart_notification()
    assert len(audit_rows(root))==2 and hashlib.sha256(marker.read_bytes()).hexdigest()==sha
