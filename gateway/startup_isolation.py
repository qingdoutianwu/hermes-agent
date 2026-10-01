"""Core-owned process-lifetime startup isolation admission policy.

The only configuration authority is gateway.startup_isolation (a YAML bool).
This policy is never injected or monkeypatched by a launcher. An enabled process
cannot silently return to normal mode; restart with false is the normal-mode route.
"""
from __future__ import annotations
import functools
import importlib
import inspect
import json
import os
from pathlib import Path
import threading
import time

REQUIRED = frozenset({"recovery", "replay", "pending_tasks", "pending_send",
                      "external_delivery", "delivery_state", "notification",
                      "adapters", "scheduler_dispatch"})
_requested = False
_resolved = False
_effective = False
_registered = set()
_denied = {}
_lock = threading.RLock()


class StartupIsolationError(RuntimeError):
    pass


def parse(value):
    if type(value) is not bool:
        raise StartupIsolationError("gateway.startup_isolation must be an explicit boolean")
    return value


def register(*points):
    with _lock:
        _registered.update(points)


def resolve(value):
    global _requested, _resolved
    requested = parse(value)
    with _lock:
        if _resolved and _requested and not requested:
            raise StartupIsolationError("startup isolation cannot be disabled in a live process")
        _requested = requested
        _resolved = True
    return requested


def resolve_config():
    if _resolved:
        return _requested
    from hermes_constants import get_process_hermes_home
    import hermes_yaml
    path = get_process_hermes_home() / "config.yaml"
    data = hermes_yaml.safe_load(path.read_text()) if path.exists() else {}
    if data is None:
        data = {}
    if not isinstance(data, dict) or not isinstance(data.get("gateway", {}), dict):
        raise StartupIsolationError("invalid startup isolation configuration")
    return resolve(data.get("gateway", {}).get("startup_isolation", False))


def enabled():
    return _requested


def allowed(action):
    if not _resolved:
        resolve_config()
    if not _requested:
        return True
    with _lock:
        _denied[action] = _denied.get(action, 0) + 1
    return False


def require(action):
    if not allowed(action):
        raise StartupIsolationError("STARTUP_ISOLATION_DENIED:" + action)


def verify():
    global _effective
    if _requested:
        missing = REQUIRED - _registered
        if missing:
            raise StartupIsolationError("STARTUP_ISOLATION_FENCE_MISSING:" + ",".join(sorted(missing)))
        _effective = True


def install():
    resolve_config()
    if _requested:
        # Import Core admission owners, not recovery tasks, adapters or plugin transports.
        for name in ("gateway.run_startup", "gateway.delivery_ledger",
                     "gateway.platforms.base", "tools.send_message_tool", "gateway.run"):
            importlib.import_module(name)
        verify()


def evidence():
    return {"schema": "hermes-core-startup-isolation.v1",
            "startup_isolation_requested": _requested,
            "startup_isolation_effective": _effective if _requested else False,
            "session_recovery_allowed": not _requested,
            "session_replay_allowed": not _requested,
            "pending_task_replay_allowed": not _requested,
            "pending_send_replay_allowed": not _requested,
            "external_delivery_allowed": not _requested,
            "delivery_state_write_allowed": not _requested,
            "startup_notification_allowed": not _requested,
            "platform_state_write_allowed": True,
            "registered_admission_points": sorted(_registered),
            "denied_admissions": dict(_denied),
            "policy_lifetime": "process_until_exit",
            "pid": os.getpid()}


def write_receipt():
    from hermes_constants import get_process_hermes_home
    path = get_process_hermes_home() / "state" / "startup-isolation.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = evidence()
    payload["observed_at_epoch"] = time.time()
    tmp = path.with_name(".startup-isolation-" + str(os.getpid()) + ".tmp")
    with tmp.open("x", encoding="utf-8") as stream:
        json.dump(payload, stream, sort_keys=True)
        stream.write("\n"); stream.flush(); os.fsync(stream.fileno())
    os.replace(tmp, path)
    return path


def outbound_admission(function):
    # A formal BasePlatformAdapter subclass contract, not a launcher monkeypatch.
    if inspect.iscoroutinefunction(function):
        @functools.wraps(function)
        async def guarded(*args, **kwargs):
            require("external_delivery")
            return await function(*args, **kwargs)
    else:
        @functools.wraps(function)
        def guarded(*args, **kwargs):
            require("external_delivery")
            return function(*args, **kwargs)
    guarded._startup_isolation_admission = True
    return guarded
