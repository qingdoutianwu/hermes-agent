"""Bounded runtime-recovery replay admission. Never consumes an obligation.

Defer receipts are a separate local audit sidecar, not a delivered/failed ledger
transition. No policy means ordinary startup; malformed requested policy fails
closed rather than silently reverting to transport-enabled startup.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sqlite3
import stat
import uuid
from pathlib import Path


class SilentStartupPolicyError(RuntimeError):
    pass


def policy(runner):
    value = getattr(runner, '_runtime_recovery_startup', None)
    source = 'runtime_recovery_execution_context'
    if value is None:
        raw = os.environ.get('HERMES_RUNTIME_RECOVERY_STARTUP')
        if raw is not None:
            source = 'HERMES_RUNTIME_RECOVERY_STARTUP'
            try:
                value = json.loads(raw)
            except ValueError as exc:
                raise SilentStartupPolicyError('POLICY_GATE_REQUIRED: malformed context') from exc
    if value is None:
        platforms = getattr(getattr(runner, 'config', None), 'platforms', {})
        values = [getattr(config, 'extra', {}).get('runtime_recovery_startup') for config in platforms.values()]
        values = [item for item in values if item is not None]
        if values:
            source = 'PlatformConfig.extra.runtime_recovery_startup'
            if any(item != values[0] for item in values):
                raise SilentStartupPolicyError('POLICY_GATE_REQUIRED: conflicting profile context')
            value = values[0]
    if value is None:
        return None
    if (not isinstance(value, dict) or value.get('execution_context') != 'runtime_recovery'
            or value.get('reason') != 'silent_startup' or value.get('event') != 'gateway_restart'
            or value.get('no_external_messages') is not True):
        raise SilentStartupPolicyError('POLICY_GATE_REQUIRED: invalid scoped context')
    return {**value, 'policy_source': source}


def require_transport_admission(runner):
    if policy(runner) is not None:
        raise SilentStartupPolicyError('POLICY_GATE_REQUIRED: external replay is deferred')


def defer(runner, obligation_id: str, event_type: str, original_sha256=None):
    selected = policy(runner)
    if selected is None:
        return False
    from gateway.run import _hermes_home
    from utils import atomic_json_write
    home = Path(_hermes_home).resolve()
    directory = home/'runtime/gateway/silent-startup-deferrals'
    cursor = home
    for component in ('runtime', 'gateway', 'silent-startup-deferrals'):
        cursor = cursor/component
        if cursor.is_symlink():
            raise SilentStartupPolicyError('POLICY_GATE_REQUIRED: audit path symlink')
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    record = {
        'schema_version': 1, 'obligation_id': str(obligation_id),
        'original_event_type': event_type, 'state': 'DEFERRED_BY_SILENT_STARTUP_POLICY',
        'original_obligation_state': 'PENDING', 'original_sha256': original_sha256,
        'defer_reason': 'NO_EXTERNAL_MESSAGES', 'policy_source': selected['policy_source'],
        'defer_timestamp': dt.datetime.now(dt.timezone.utc).isoformat(),
        'execution_context': 'runtime_recovery', 'event': 'gateway_restart',
        'transport_attempt_count': 0, 'external_send_count': 0,
        'original_obligation_consumed': False,
    }
    target = directory/(uuid.uuid4().hex+'.json')
    atomic_json_write(target, record, mode=0o444, fsync_dir=True)
    if json.loads(target.read_bytes()) != record or stat.S_IMODE(target.stat().st_mode) != 0o444:
        raise SilentStartupPolicyError('POLICY_GATE_REQUIRED: defer readback failed')
    return True


def defer_marker(runner, name, event_type):
    if policy(runner) is None:
        return False
    from gateway.run import _hermes_home
    path = Path(_hermes_home)/name
    if path.exists():
        if path.is_symlink() or not path.is_file():
            raise SilentStartupPolicyError('POLICY_GATE_REQUIRED: marker authority invalid')
        raw = path.read_bytes()
        defer(runner, 'marker:'+name, event_type, hashlib.sha256(raw).hexdigest())
    return True


def defer_delivery_rows(runner):
    if policy(runner) is None:
        return False
    from gateway.run import _hermes_home
    database = Path(_hermes_home)/'state.db'
    if not database.exists():
        return True
    if database.is_symlink():
        raise SilentStartupPolicyError('POLICY_GATE_REQUIRED: ledger symlink')
    conn = sqlite3.connect(database.resolve().as_uri()+'?mode=ro', uri=True, timeout=2)
    try:
        conn.execute('PRAGMA query_only=ON')
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='delivery_obligations'").fetchone()
        if exists:
            rows = conn.execute("SELECT obligation_id FROM delivery_obligations WHERE state IN ('pending','attempting','failed')").fetchall()
            for (identity,) in rows:
                defer(runner, identity, 'PENDING_DELIVERY_REPLAY')
    finally:
        conn.close()
    return True
