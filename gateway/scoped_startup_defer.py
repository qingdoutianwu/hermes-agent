"""Audit deferred startup obligations without claiming or changing their state.

Core admission remains sole transport authority. This sidecar is not a permit,
notification authorization, delivery receipt or cancellation of an obligation.
"""
from __future__ import annotations

import datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat

from gateway import startup_isolation

MARKERS = {".restart_pending.json": "gateway_restart",
           ".restart_notify.json": "gateway_restart_requester",
           ".update_pending.json": "gateway_update",
           ".update_pending.claimed.json": "gateway_update_claimed"}


def audit_pending():
    startup_isolation.resolve_config()
    policy = startup_isolation.evidence()
    if policy["startup_context"] != "silent_recovery":
        return {"pending_count": 0, "deferred_count": 0, "transport_attempt_count": 0,
                "external_send_count": 0, "context": "normal"}
    startup_isolation.verify()
    if policy["external_delivery_allowed"] or policy["delivery_state_write_allowed"]:
        raise startup_isolation.StartupIsolationError("POLICY_GATE_REQUIRED: silent defer fence")
    from hermes_constants import get_process_hermes_home
    home = get_process_hermes_home().resolve()
    obligations = []
    for name, event in MARKERS.items():
        marker = home / name
        if marker.is_symlink():
            raise startup_isolation.StartupIsolationError("defer marker symlink")
        if marker.exists():
            if not marker.is_file():
                raise startup_isolation.StartupIsolationError("defer marker not regular")
            obligations.append(("marker:" + name, event, hashlib.sha256(marker.read_bytes()).hexdigest()))
    database = home / "state.db"
    if database.is_symlink():
        raise startup_isolation.StartupIsolationError("defer ledger symlink")
    if database.exists():
        with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=2) as conn:
            conn.execute("PRAGMA query_only=ON")
            if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='delivery_obligations'").fetchone():
                rows = conn.execute("SELECT obligation_id FROM delivery_obligations WHERE state IN ('pending','attempting','failed')").fetchall()
                obligations.extend((str(identity), "PENDING_DELIVERY_REPLAY", None) for (identity,) in rows)
    directory = home
    for name in ("runtime", "gateway", "silent-startup-deferrals"):
        directory = directory / name
        if directory.is_symlink():
            raise startup_isolation.StartupIsolationError("defer audit directory symlink")
        directory.mkdir(mode=0o700, exist_ok=True)
        info = directory.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise startup_isolation.StartupIsolationError("defer audit directory authority")
    deferred = 0
    for identity, event, original_hash in obligations:
        record = {"schema_version": 1, "obligation_id": identity, "original_event_type": event,
                  "state": "DEFERRED_BY_SILENT_STARTUP_POLICY", "original_sha256": original_hash,
                  "defer_reason": "NO_EXTERNAL_MESSAGES", "policy_source": "gateway.startup_context",
                  "defer_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  "execution_context": "silent_recovery", "pid": os.getpid(),
                  "obligation_consumed": False, "transport_attempt_count": 0, "external_send_count": 0}
        key = hashlib.sha256((str(os.getpid()) + "\0" + identity + "\0" + str(original_hash)).encode()).hexdigest()
        path = directory / (key + ".json")
        data = (json.dumps(record, sort_keys=True) + "\n").encode()
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o444)
        except FileExistsError:
            if path.is_symlink() or not path.is_file() or stat.S_IMODE(path.stat().st_mode) != 0o444:
                raise startup_isolation.StartupIsolationError("invalid existing defer audit")
            existing = json.loads(path.read_bytes())
            for field in ("obligation_id", "original_event_type", "original_sha256", "pid", "state",
                          "obligation_consumed", "transport_attempt_count", "external_send_count"):
                if existing.get(field) != record[field]:
                    raise startup_isolation.StartupIsolationError("defer audit identity mismatch")
        else:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data); stream.flush(); os.fchmod(stream.fileno(), 0o444); os.fsync(stream.fileno())
            if path.read_bytes() != data:
                raise startup_isolation.StartupIsolationError("defer audit readback mismatch")
        deferred += 1
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return {"pending_count": len(obligations), "deferred_count": deferred,
            "transport_attempt_count": 0, "external_send_count": 0, "context": "silent_recovery"}
