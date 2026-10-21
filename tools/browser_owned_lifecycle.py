"""In-process capability for closing an application-created, isolated Chromium.

Not a model tool or an exemption for arbitrary CDP endpoints. The trusted caller
retains the Popen child it created; generic browser_cdp keeps every existing guard.
"""
from dataclasses import dataclass
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import threading
import time
from urllib.parse import urlsplit
from weakref import WeakKeyDictionary

import psutil
from websockets.exceptions import WebSocketException


class OwnershipError(ValueError):
    pass


class OwnedBrowser:
    """Opaque non-serializable Python capability, never accepted from a tool schema."""
    __slots__ = ('__weakref__',)


@dataclass
class _Binding:
    task: str
    home: Path
    process: subprocess.Popen
    start: float
    executable: str
    argv: tuple
    profile: Path
    profile_identity: tuple
    endpoint: str
    session: dict | None = None
    result: dict | None = None
    attempted: bool = False


_bindings = WeakKeyDictionary()
_lock = threading.RLock()


def _require(ok):
    if not ok:
        raise OwnershipError('OWNED_BROWSER_IDENTITY_REJECTED')


def _home():
    from hermes_constants import get_hermes_home
    return get_hermes_home().resolve()


def _directory_identity(profile):
    s = profile.lstat()
    _require(stat.S_ISDIR(s.st_mode) and not profile.is_symlink()
             and s.st_uid == os.getuid() and stat.S_IMODE(s.st_mode) == 0o700)
    return s.st_dev, s.st_ino


def _endpoint_identity(profile, endpoint):
    u = urlsplit(endpoint)
    _require(u.scheme == 'ws' and u.hostname == '127.0.0.1' and u.port is not None
             and not u.username and not u.password and not u.query and not u.fragment
             and re.fullmatch(r'/devtools/browser/[A-Za-z0-9-]+', u.path))
    active = profile / 'DevToolsActivePort'
    s = active.lstat()
    _require(stat.S_ISREG(s.st_mode) and not active.is_symlink() and s.st_nlink == 1
             and s.st_uid == os.getuid() and not s.st_mode & 0o022 and s.st_size < 1024)
    _require(active.read_text().splitlines() == [str(u.port), u.path])


def _live_process(binding):
    p = binding.process
    _require(p.poll() is None)
    process = psutil.Process(p.pid)
    _require(process.ppid() == os.getpid() and process.uids().real == os.getuid()
             and process.create_time() == binding.start
             and process.exe() == binding.executable
             and tuple(process.cmdline()) == binding.argv
             and os.getpgid(p.pid) == p.pid)
    port = urlsplit(binding.endpoint).port
    _require(any(c.status == psutil.CONN_LISTEN and c.laddr.ip == '127.0.0.1'
                 and c.laddr.port == port for c in process.net_connections(kind='tcp')))


def register_owned_chromium(*, task_id, process, profile, endpoint):
    """Bind a real direct Popen child at creation, not a PID/env/URL ownership claim.

    No browser/network call here. DevToolsActivePort's unpredictable browser ID
    binds the only allowed connection to the private Profile of this exact child.
    The owner/root trust envelope is unchanged; this is not hostile-Python isolation.
    """
    _require(os.name == 'posix' and isinstance(task_id, str) and bool(task_id)
             and isinstance(process, subprocess.Popen) and isinstance(endpoint, str))
    path = Path(profile)
    _require(path.is_absolute() and path == path.resolve(strict=True))
    home = _home()
    _require(path.is_relative_to(home / 'runtime'))
    identity = _directory_identity(path)
    args = process.args
    _require(isinstance(args, (list, tuple)) and all(isinstance(a, str) for a in args))
    _require([a for a in args if a.startswith('--user-data-dir=')] == ['--user-data-dir=' + str(path)]
             and '--remote-debugging-address=127.0.0.1' in args and '--remote-debugging-port=0' in args)
    actual = psutil.Process(process.pid)
    binding = _Binding(task_id, home, process, actual.create_time(), actual.exe(),
                       tuple(actual.cmdline()), path, identity, endpoint)
    # The original complete argv must still match, not a substring/classification guess.
    _require(tuple(args[1:]) == binding.argv[1:]
             and Path(args[0]).resolve() == Path(binding.executable).resolve())
    _live_process(binding)
    _endpoint_identity(path, endpoint)
    binding.session = _session_identity(binding)
    with _lock:
        for prior in _bindings.values():
            _require(prior.result is not None or (prior.process.pid != process.pid
                and prior.profile != path and prior.endpoint != endpoint))
        handle = OwnedBrowser()
        _bindings[handle] = binding
    return handle


def _session_identity(binding):
    from tools import browser_tool as bt
    from tools.browser_tool_cdp import _get_cdp_override_raw
    _require(_home() == binding.home)
    with bt._cleanup_lock:
        info = bt._active_sessions.get(binding.task)
        _require(isinstance(info, dict) and isinstance(info.get('features'), dict))
        _require(binding.session is None or info is binding.session)
        _require(info.get('owner_task_id') == binding.task
                 and info.get('cdp_url') == binding.endpoint
                 and info.get('session_key') == binding.task
                 and info['features'].get('cdp_override') is True and not info['features'].get('local'))
    _require(_get_cdp_override_raw() == binding.endpoint)
    return info


def _validate(binding):
    _require(_directory_identity(binding.profile) == binding.profile_identity)
    _endpoint_identity(binding.profile, binding.endpoint)
    _live_process(binding)
    return _session_identity(binding)


def close_owned_chromium(handle, *, task_id, timeout=2):
    """Send only Browser.close to the pinned endpoint and independently waitpid.

    No URL input, navigation, new target, arbitrary CDP method or process signal.
    Native Session/descendant/CDP/Profile release remains the caller's unconditional
    cleanup tail. A parent wait is explicitly not proof that all descendants exited.
    """
    result = {'success': False, 'dispatched': False, 'response_received': False,
              'native_exit_status': 'OWNED_NATIVE_EXIT_UNPROVEN',
              'exit_proof_scope': 'DIRECT_OWNED_CHILD_ONLY_NOT_DESCENDANTS',
              'error_code': 'OWNED_BROWSER_IDENTITY_REJECTED',
              'network_diagnostics': {'proxy_type': 'DIRECT_OWNED_CDP_CONTROL_CHANNEL',
                  'dns_resolution_stage': 'NOT_USED_LITERAL_PINNED_ENDPOINT',
                  'address_family': 'UNVERIFIED', 'address_classification': 'UNVERIFIED'}}
    try:
        _require(type(handle) is OwnedBrowser and isinstance(timeout, (int, float))
                 and not isinstance(timeout, bool) and math.isfinite(timeout) and 0 < timeout <= 5)
        with _lock:
            binding = _bindings.get(handle)
            _require(binding is not None and binding.task == task_id and binding.home == _home())
            if binding.result is not None:
                return dict(binding.result, repeated=True)
            _require(not binding.attempted)
            binding.attempted = True  # ambiguous send never retries automatically
        session = _validate(binding)
        result['network_diagnostics'].update(address_family='IPV4',
            address_classification='OWNED_LOOPBACK_LISTENER')
        from tools import browser_cdp_tool as cdp
        # Reuse the native transport, but never the content/URL-dependent generic guard.
        # This API has an OS-backed ownership admission instead of a URL allowlist.
        async def send():
            _require(_validate(binding) is session)
            return await cdp._cdp_call(binding.endpoint, 'Browser.close', {}, None,
                                       timeout, _dispatch_observer=lambda: result.update(dispatched=True))
        started = time.monotonic()
        reply = cdp._run_async(send())
        _require(isinstance(reply, dict))
        result.update(response_received=True, error_code='OWNED_NATIVE_EXIT_UNPROVEN')
        # Popen.wait reaps this recorded direct child, not whichever process has its PID later.
        code = binding.process.wait(timeout=max(0.01, timeout - (time.monotonic() - started)))
        result.update(parent_exit_code=code)
        if code == 0:
            result.update(success=True, native_exit_status='OWNED_PROCESS_EXIT_VERIFIED', error_code=None)
        with _lock:
            binding.result = dict(result)
        return result
    except (OwnershipError, OSError, psutil.Error, subprocess.TimeoutExpired, TimeoutError) as exc:
        if not isinstance(exc, OwnershipError) or result['dispatched']:
            result['error_code'] = 'OWNED_NATIVE_EXIT_UNPROVEN'
        return result
    except (RuntimeError, ValueError, WebSocketException):
        # Never persist a WebSocket/provider exception that might contain credentials.
        result['error_code'] = 'OWNED_NATIVE_EXIT_UNPROVEN'
        return result
