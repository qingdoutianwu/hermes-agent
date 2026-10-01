"""Core-owned process-lifetime startup isolation admission policy.

Strict gateway recovery/delivery booleans are resolved by Core before subsystem
admission. startup_isolation retains full-safe-start precedence. A delivery fence
cannot silently relax within the same process. No launcher injects this policy.
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
_recovery_requested = True
_delivery_requested = True
_state_requested = True
_startup_context = "normal"
_no_external_messages = False
_observed = {"session_recovery_count": 0, "internal_replay_count": 0}
_registered = set()
_denied = {}
_lock = threading.RLock()


class StartupIsolationError(RuntimeError):
    pass


def parse(value, name="startup_isolation"):
    if type(value) is not bool:
        raise StartupIsolationError("gateway." + name + " must be an explicit boolean")
    return value


def register(*points):
    with _lock:
        _registered.update(points)


def validate_context(context, no_external_messages, delivery, state):
    if not isinstance(context, str) or context not in {"normal", "silent_recovery"}:
        raise StartupIsolationError("invalid gateway.startup_context")
    parse(no_external_messages, "no_external_messages")
    if context == "silent_recovery" and (not no_external_messages or delivery or state):
        raise StartupIsolationError("silent_recovery requires NO_EXTERNAL_MESSAGES and delivery/state OFF")
    if context == "normal" and no_external_messages:
        raise StartupIsolationError("NO_EXTERNAL_MESSAGES requires explicit silent_recovery context")
    return context


def resolve(value, *, recovery_enabled=True, external_delivery_enabled=True,
            delivery_state_write_enabled=True, startup_context="normal", no_external_messages=False):
    global _requested, _resolved, _recovery_requested, _delivery_requested, _state_requested
    global _startup_context, _no_external_messages
    requested = parse(value)
    recovery = parse(recovery_enabled, "recovery_enabled")
    delivery = parse(external_delivery_enabled, "external_delivery_enabled")
    state = parse(delivery_state_write_enabled, "delivery_state_write_enabled")
    context = validate_context(startup_context, no_external_messages, delivery, state)
    if not requested and not delivery and state:
        raise StartupIsolationError("Delivery OFF requires delivery_state_write_enabled=false")
    with _lock:
        if _resolved and _startup_context == "silent_recovery" and context != _startup_context:
            raise StartupIsolationError("silent recovery cannot relax within a live process")
        if _resolved and _requested and not requested:
            raise StartupIsolationError("startup isolation cannot be disabled in a live process")
        if _resolved and ((not _delivery_requested and delivery) or (not _state_requested and state)):
            raise StartupIsolationError("delivery fences cannot be relaxed in a live process")
        _requested = requested
        _recovery_requested, _delivery_requested, _state_requested = recovery, delivery, state
        _startup_context, _no_external_messages = context, no_external_messages
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
    cfg = data.get("gateway", {})
    return resolve(cfg.get("startup_isolation", False),
                   recovery_enabled=cfg.get("recovery_enabled", True),
                   external_delivery_enabled=cfg.get("external_delivery_enabled", True),
                   delivery_state_write_enabled=cfg.get("delivery_state_write_enabled", True),
                   startup_context=cfg.get("startup_context", "normal"),
                   no_external_messages=cfg.get("no_external_messages", False))


def enabled():
    return _requested


def restricted():
    return _requested or not (_recovery_requested and _delivery_requested and _state_requested)


def delivery_fenced():
    return _requested or not _delivery_requested


def _permitted(action):
    if _requested or (restricted() and not _effective):
        return False
    if action in {"recovery", "replay", "pending_tasks"}:
        return _recovery_requested
    if action in {"external_delivery", "notification", "adapters"}:
        return _delivery_requested
    if action == "delivery_state":
        return _state_requested
    if action in {"pending_send", "scheduler_dispatch"}:
        return _delivery_requested and _state_requested
    raise StartupIsolationError("unknown admission action: " + action)


def observe(name, count):
    if name not in _observed or type(count) is not int or count < 0:
        raise StartupIsolationError("invalid recovery observation")
    with _lock:
        _observed[name] += count


def allowed(action):
    if not _resolved:
        resolve_config()
    if _permitted(action):
        return True
    with _lock:
        _denied[action] = _denied.get(action, 0) + 1
    return False


def require(action):
    if not allowed(action):
        raise StartupIsolationError("STARTUP_ISOLATION_DENIED:" + action)


def verify():
    global _effective
    if restricted():
        _effective = False
        missing = REQUIRED - _registered
        if missing:
            raise StartupIsolationError("STARTUP_ISOLATION_FENCE_MISSING:" + ",".join(sorted(missing)))
        _effective = True


def install():
    resolve_config()
    if restricted():
        # Import Core admission owners, not recovery tasks, adapters or plugin transports.
        for name in ("gateway.run_startup", "gateway.delivery_ledger",
                     "gateway.platforms.base", "tools.send_message_tool", "gateway.run"):
            importlib.import_module(name)
        verify()


def evidence():
    return {"schema": "hermes-core-startup-isolation.v1",
            "startup_context": _startup_context,
            "no_external_messages": _no_external_messages,
            "startup_isolation_requested": _requested,
            "startup_isolation_effective": _effective if _requested else False,
            "recovery_requested": _recovery_requested,
            "recovery_effective": _permitted("recovery"),
            "external_delivery_requested": _delivery_requested,
            "external_delivery_effective": _permitted("external_delivery"),
            "session_recovery_allowed": _permitted("recovery"),
            "session_replay_allowed": _permitted("replay"),
            "pending_task_replay_allowed": _permitted("pending_tasks"),
            "pending_send_replay_allowed": _permitted("pending_send"),
            "outbound_replay_allowed": _permitted("pending_send"),
            "external_delivery_allowed": _permitted("external_delivery"),
            "delivery_state_write_requested": _state_requested,
            "delivery_state_write_allowed": _permitted("delivery_state"),
            "startup_notification_allowed": _permitted("notification"),
            "recovery_observations": dict(_observed),
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
