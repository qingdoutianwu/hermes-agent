"""Owned close is a capability, not a private-page or arbitrary endpoint exemption."""
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock

import pytest

from tools import browser_cdp_tool as cdp
from tools import browser_owned_lifecycle as owned
from tools import browser_tool as bt
from tools import browser_tool_eval_policy as policy
from tools import browser_tool_cdp as endpoint_policy
from tests.waiting import wait_until

pytestmark = pytest.mark.platforms('posix')

CHILD = '''import socket,sys
from pathlib import Path
s=socket.socket();s.bind(('127.0.0.1',0));s.listen()
p=next(x.split('=',1)[1] for x in sys.argv if x.startswith('--user-data-dir='))
Path(p,'DevToolsActivePort').write_text(str(s.getsockname()[1])+'\\n/devtools/browser/owned-fixture\\n')
sys.stdin.buffer.read()
s.close()
'''


@pytest.fixture
def child(tmp_path, monkeypatch):
    home = tmp_path / 'home'; home.mkdir()
    monkeypatch.setenv('HERMES_HOME', str(home))
    profile = home / 'runtime/finance-daily/native-runs/fixture/profile'
    profile.mkdir(parents=True, mode=0o700)
    process = subprocess.Popen([sys.executable, '-I', '-B', '-c', CHILD,
        '--user-data-dir=' + str(profile), '--remote-debugging-address=127.0.0.1',
        '--remote-debugging-port=0'], stdin=subprocess.PIPE, start_new_session=True)
    ready = profile / 'DevToolsActivePort'
    assert wait_until(lambda: ready.exists() and len(ready.read_text().splitlines()) == 2, timeout=5)
    port, path = ready.read_text().splitlines()
    endpoint = 'ws://127.0.0.1:' + port + path
    session = {'owner_task_id': 'task-a', 'session_key': 'task-a', 'cdp_url': endpoint,
               'features': {'cdp_override': True}}
    monkeypatch.setitem(bt._active_sessions, 'task-a', session)
    monkeypatch.setenv('BROWSER_CDP_URL', endpoint)
    try:
        yield process, profile, endpoint, session
    finally:
        if process.poll() is None:
            process.stdin.close()
            process.wait(timeout=5)


def register(child):
    p, profile, endpoint, _ = child
    return owned.register_owned_chromium(task_id='task-a', process=p, profile=profile, endpoint=endpoint)


def response(monkeypatch, process, *, exit=True):
    calls = []
    async def send(endpoint, method, params, target, timeout, *, _dispatch_observer=None):
        calls.append((endpoint, method, params, target))
        _dispatch_observer()
        if exit:
            process.stdin.close()
        return {}
    monkeypatch.setattr(cdp, '_cdp_call', send)
    return calls


def test_owned_private_page_close_dispatches_and_waits(child, monkeypatch):
    token = register(child)
    probe = Mock(return_value='http://127.0.0.1/private-fixture')
    monkeypatch.setattr(policy, '_current_page_private_url', probe)
    calls = response(monkeypatch, child[0])
    result = owned.close_owned_chromium(token, task_id='task-a')
    assert result['success'] and result['dispatched'] and result['response_received']
    assert result['native_exit_status'] == 'OWNED_PROCESS_EXIT_VERIFIED'
    assert calls == [(child[2], 'Browser.close', {}, None)]
    probe.assert_not_called()  # no page read, DNS or new navigation to authorize resource teardown
    repeated = owned.close_owned_chromium(token, task_id='task-a')
    assert repeated['success'] and repeated['repeated'] and len(calls) == 1


@pytest.mark.parametrize('change', ['wrong_task', 'session_owner', 'session_endpoint',
                                  'env_endpoint', 'profile_replaced', 'devtools_changed',
                                  'process_start', 'profile_scope', 'session_replaced'])
def test_identity_changes_reject_without_dispatch(child, monkeypatch, tmp_path, change):
    token = register(child)
    calls = response(monkeypatch, child[0])
    task = 'task-a'
    if change == 'wrong_task': task = 'other-task'
    if change == 'session_owner': child[3]['owner_task_id'] = 'other-task'
    if change == 'session_replaced': monkeypatch.setitem(bt._active_sessions,'task-a',dict(child[3]))
    if change == 'session_endpoint': child[3]['cdp_url'] += '-other'
    if change == 'env_endpoint': monkeypatch.setenv('BROWSER_CDP_URL', 'ws://127.0.0.1:9/devtools/browser/other')
    if change == 'profile_replaced':
        child[1].rename(child[1].with_name('original')); child[1].mkdir(mode=0o700)
    if change == 'devtools_changed': (child[1]/'DevToolsActivePort').write_text('9\n/devtools/browser/other\n')
    if change == 'process_start':
        binding = owned._bindings[token]
        binding.start -= 100  # injected stale birth identity, never a real recycled PID signal
    if change == 'profile_scope':
        other = tmp_path/'other'; other.mkdir(); monkeypatch.setenv('HERMES_HOME',str(other))
    result = owned.close_owned_chromium(token, task_id=task)
    assert not result['success'] and not result['dispatched'] and not calls
    assert child[0].poll() is None


@pytest.mark.parametrize('endpoint', ['ws://example.invalid:99/devtools/browser/a',
    'http://127.0.0.1:99/json/version', 'ws://127.0.0.1:99/devtools/browser/a?token=x'])
def test_untrusted_endpoint_registration_rejected(child, endpoint):
    with pytest.raises(owned.OwnershipError):
        owned.register_owned_chromium(task_id='task-a',process=child[0],profile=child[1],endpoint=endpoint)


def test_unregistered_and_forged_capability_cannot_close(child, monkeypatch):
    calls = response(monkeypatch, child[0])
    for token in [None, {}, owned.OwnedBrowser()]:
        result = owned.close_owned_chromium(token, task_id='task-a')
        assert not result['success'] and not result['dispatched']
    assert not calls and child[0].poll() is None


def test_sent_ack_without_exit_is_unproven_and_not_retried(child, monkeypatch):
    token=register(child); calls=response(monkeypatch,child[0],exit=False)
    result=owned.close_owned_chromium(token,task_id='task-a',timeout=.05)
    assert result['dispatched'] and result['response_received'] and not result['success']
    assert result['native_exit_status']=='OWNED_NATIVE_EXIT_UNPROVEN'
    assert child[0].poll() is None
    owned.close_owned_chromium(token,task_id='task-a')
    assert len(calls)==1


def test_generic_close_and_private_navigation_still_guarded(child,monkeypatch):
    monkeypatch.setattr(policy,'_eval_ssrf_guard_active',lambda task:True)
    monkeypatch.setattr(policy,'_current_page_private_url',lambda task:'http://127.0.0.1/fixture')
    monkeypatch.setattr(policy,'_url_blocked',lambda bt,url:True)
    dispatch=Mock(side_effect=AssertionError('unsafe generic dispatch'))
    monkeypatch.setattr(cdp,'_run_async',dispatch)
    assert 'private' in json.loads(cdp.browser_cdp('Browser.close',task_id='task-a'))['error']
    assert 'private' in json.loads(cdp.browser_cdp('Page.navigate',{'url':'http://127.0.0.1/fixture'},task_id='task-a'))['error']
    assert 'private' in json.loads(cdp.browser_cdp('DOM.getDocument',task_id='task-a'))['error']
    dispatch.assert_not_called()


def test_duplicate_live_registration_rejected(child):
    first=register(child)
    with pytest.raises(owned.OwnershipError):register(child)
    assert first is not None


def test_failed_cdp_response_has_safe_diagnostics_and_no_repeat(child,monkeypatch):
    token=register(child); calls=[]
    async def fail(*args,**kwargs):
        kwargs['_dispatch_observer']();calls.append(True)
        raise RuntimeError('api_key=fixture-private-value;ws://private-endpoint?token=secret')
    monkeypatch.setattr(cdp,'_cdp_call',fail)
    result=owned.close_owned_chromium(token,task_id='task-a')
    assert result['dispatched'] and not result['response_received'] and not result['success']
    assert result['native_exit_status']=='OWNED_NATIVE_EXIT_UNPROVEN'
    assert 'fixture-private-value' not in json.dumps(result) and 'token=secret' not in json.dumps(result)
    owned.close_owned_chromium(token,task_id='task-a')
    assert calls==[True] and child[0].poll() is None
