"""Real local Chromium lifecycle Fixture;about:blank only,no news or production Session."""
from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid
from unittest.mock import Mock

import pytest

from tools import browser_owned_lifecycle as owned
from tools import browser_cdp_tool as cdp
from tools import browser_tool as bt
from tools import browser_tool_eval_policy as policy
from tests.waiting import wait_until
from tests.tools.browser_fixture_identity import IdentityReceipt


@contextmanager
def chromium(home, task, monkeypatch):
    # Same supported headless/CDP/private-Profile flags as the Finance adapter.
    # A discard loopback proxy isolates this Fixture from all production proxy traffic.
    executable='/Applications/Brave Browser.app/Contents/MacOS/Brave Browser'
    assert Path(executable).is_file(), 'Explicit local Chromium Fixture executable unavailable'
    profile=home/'runtime/finance-daily/native-runs'/task/'profile'
    profile.mkdir(parents=True,mode=0o700)
    directory = Path(os.environ['HERMES_E2E_ARTIFACTS']).resolve(strict=True) if os.environ.get('HERMES_E2E_ARTIFACTS') else None
    receipt = IdentityReceipt(directory, task, executable, profile)
    p=subprocess.Popen([executable,'--headless=new','--remote-debugging-address=127.0.0.1',
        '--remote-debugging-port=0','--user-data-dir='+str(profile),
        '--proxy-server=http://127.0.0.1:9','--disable-background-networking',
        '--disable-component-update','--no-first-run','--no-default-browser-check','about:blank'],
        stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    try:
        receipt.process_started(p)
        active=profile/'DevToolsActivePort'
        assert wait_until(lambda:active.exists() and len(active.read_text().splitlines())==2,timeout=15)
        port,path=active.read_text().splitlines();endpoint='ws://127.0.0.1:'+port+path
        info={'owner_task_id':task,'session_key':task,'cdp_url':endpoint,'features':{'cdp_override':True}}
        monkeypatch.setitem(bt._active_sessions,task,info)
        monkeypatch.setenv('BROWSER_CDP_URL',endpoint)
        assert receipt.observe_ready(p, profile, endpoint, info), receipt.data['precheck']
        with monkeypatch.context() as registration_patch:
            receipt.instrument_registration(registration_patch, owned)
            token=owned.register_owned_chromium(task_id=task,process=p,profile=profile,endpoint=endpoint)
        receipt.data['registration'] = 'PASS'
        receipt.record('SESSION_REGISTERED')
        yield p,profile,endpoint,token
    finally:
        # Test hygiene only: act on this fixture's Popen child,never another task/browser.
        if p.poll() is None:
            p.terminate();p.wait(timeout=5)
        assert wait_until(lambda: _group_gone(p.pid),timeout=5)
        shutil.rmtree(profile)
        receipt.cleanup(p, profile, _group_gone(p.pid))


def _group_gone(group):
    import psutil
    # Ignore vanished PIDs while sampling. Only same-test child groups are asserted below.
    for pid in psutil.pids():
        try:
            if os.getpgid(pid)==group:return False
        except (ProcessLookupError,PermissionError):continue
    return True


@pytest.mark.platforms('macos')
def test_two_real_chromiums_close_only_owned_parent_and_keep_private_guard(tmp_path,monkeypatch):
    if os.environ.get('HERMES_E2E_ARTIFACTS'):
        # Exclusive marker prevents accidental repeated live invocations in this audit directory.
        with (Path(os.environ['HERMES_E2E_ARTIFACTS'])/'FIXTURE_INVOCATION.json').open('x') as f:
            json.dump({'fixture_invocation_count': 1, 'max_browser_processes': 2,
                       'target': 'about:blank', 'retry_count': 0}, f)
    home=tmp_path/'home';home.mkdir();monkeypatch.setenv('HERMES_HOME',str(home))
    with ExitStack() as stack:
        a=stack.enter_context(chromium(home,'fixture-a',monkeypatch))
        b=stack.enter_context(chromium(home,'fixture-b',monkeypatch))
        assert a[0].pid!=b[0].pid and a[1].stat().st_ino!=b[1].stat().st_ino and a[2]!=b[2]
        monkeypatch.setenv('BROWSER_CDP_URL',a[2])
        monkeypatch.setattr(policy,'_eval_ssrf_guard_active',lambda task:True)
        probe=Mock(return_value='http://127.0.0.1/private-fixture')
        monkeypatch.setattr(policy,'_current_page_private_url',probe)
        # Generic raw close still rejects at the old private-page guard.
        generic=json.loads(cdp.browser_cdp('Browser.close',task_id='fixture-a'))
        assert 'private' in generic['error'] and a[0].poll() is None
        probe.reset_mock()
        # Only the in-process opaque capability permits the exact lifecycle method.
        result=owned.close_owned_chromium(a[3],task_id='fixture-a',timeout=5)
        assert result['success'] and result['dispatched'] and result['response_received'],result
        assert result['native_exit_status']=='OWNED_PROCESS_EXIT_VERIFIED' and a[0].poll()==0
        probe.assert_not_called()
        assert b[0].poll() is None and b[1].is_dir()
        assert owned.close_owned_chromium(a[3],task_id='fixture-b')['success'] is False
        monkeypatch.setenv('BROWSER_CDP_URL',b[2])
        second=owned.close_owned_chromium(b[3],task_id='fixture-b',timeout=5)
        assert second['success'] and second['parent_exit_code']==0,second
        proof={'fixture':'REAL_LOCAL_CHROMIUM_ABOUT_BLANK_ONLY','first':result,'second':second,
               'separate_processes':True,'separate_profiles':True,'proxy_type':'HTTP_CONNECT_LOOPBACK_DISCARD',
               'process_pids':[a[0].pid,b[0].pid],
               'real_business_run_count':0,'message_send_count':0}
    proof.update(profiles_removed=not a[1].exists() and not b[1].exists(),
                 owned_process_groups_gone=_group_gone(a[0].pid) and _group_gone(b[0].pid))
    if os.environ.get('HERMES_E2E_ARTIFACTS'):
        directory=Path(os.environ['HERMES_E2E_ARTIFACTS']).resolve(strict=True)
        with (directory/('owned-chromium-fixture-'+uuid.uuid4().hex+'.json')).open('x') as f:
            json.dump(proof,f,indent=2);f.write('\n')
