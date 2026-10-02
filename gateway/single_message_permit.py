"""Core one-shot controlled-test delivery; no global admission override.

The owned permit table is a separate ledger domain in Core state.db. This module
never scans or writes delivery_obligations. Only exact permit-row SQL is exposed.
Consumption is durable before a single transport call; crash reservations stay
consumed/uncertain and are never adopted or retried by Gateway recovery.
"""
from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import time
import uuid

PURPOSE = "CONTROLLED_DELIVERY_TEST"
MAX_TTL = 300
TABLE = "controlled_delivery_permits"

class PermitDenied(RuntimeError):
    pass

class TransportAdmissionDenied(PermitDenied):
    """A valid permit is not an authorization to send in silent recovery."""
    def __init__(self, validation=None):
        super().__init__("SILENT_RECOVERY_TRANSPORT_DENIED")
        self.validation = validation

class DefinitelyNotSent(RuntimeError):
    """Core transport can prove no provider-side message was submitted."""

@dataclass(frozen=True)
class ExactMessage:
    channel: str
    destination_json: str
    text: str

    @classmethod
    def bind(cls, channel, destination, text):
        if channel not in {"telegram", "imessage"} or not isinstance(destination, dict):
            raise PermitDenied("INVALID_CHANNEL_OR_DESTINATION")
        required = {"profile", "account", "chat_id", "thread_id"}
        if set(destination) != required:
            raise PermitDenied("EXACT_DESTINATION_REQUIRED")
        if any(not isinstance(destination[k], str) or not destination[k] for k in required - {"thread_id"}):
            raise PermitDenied("INVALID_DESTINATION")
        if destination["thread_id"] is not None and (not isinstance(destination["thread_id"], str) or not destination["thread_id"]):
            raise PermitDenied("INVALID_THREAD")
        if not isinstance(text, str) or not text or len(text.encode("utf-8")) > 2048:
            raise PermitDenied("INVALID_SINGLE_TEXT_PAYLOAD")
        # No normalization or markdown conversion: visible UTF-8 bytes are exact.
        return cls(channel, json.dumps(destination, sort_keys=True, separators=(",", ":"), ensure_ascii=False), text)

    def validate(self):
        try:
            exact = ExactMessage.bind(self.channel, json.loads(self.destination_json), self.text)
        except (TypeError, ValueError) as exc:
            raise PermitDenied("INVALID_CANONICAL_PAYLOAD") from exc
        if exact != self:
            raise PermitDenied("NONCANONICAL_DESTINATION")
        return exact

    @property
    def message_hash(self):
        return hashlib.sha256(self.text.encode("utf-8")).hexdigest()

    @property
    def destination_hash(self):
        return hashlib.sha256(self.destination_json.encode("utf-8")).hexdigest()

def _policy():
    from gateway import startup_isolation as policy
    policy.resolve_config()
    if policy.enabled():
        raise PermitDenied("FULL_ISOLATION")
    policy.install()
    policy.verify()
    e = policy.evidence()
    if (not e["recovery_effective"] or e["external_delivery_effective"]
            or e["delivery_state_write_allowed"] or e["outbound_replay_allowed"]):
        raise PermitDenied("CONTROLLED_RECOVERY_ONLY_MODE_REQUIRED")
    return e

def _context_evidence(policy):
    silent = policy.get("startup_context", "normal") == "silent_recovery"
    no_external = policy.get("no_external_messages", False)
    controlled_mode = (not policy["startup_isolation_requested"] and policy["recovery_effective"]
                       and not policy["external_delivery_effective"]
                       and not policy["delivery_state_write_allowed"]
                       and not policy["outbound_replay_allowed"])
    return {"execution_context": PURPOSE,
            "startup_context": policy.get("startup_context", "normal"),
            "silent_recovery_state": silent,
            "external_send_allowed": controlled_mode and not (silent or no_external)}

def require_transport_admission(validation=None):
    # Does not invalidate, consume, delete or terminalize a permit. Dedicated
    # controlled-test authority remains distinct from ordinary notification/send.
    state = _context_evidence(_policy())
    if not state["external_send_allowed"]:
        raise TransportAdmissionDenied(validation)
    return state

def _path():
    from hermes_constants import get_process_hermes_home
    home = get_process_hermes_home().resolve()
    path = home / "state.db"
    if path.is_symlink():
        raise PermitDenied("DB_SYMLINK")
    if path.exists():
        info = path.stat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise PermitDenied("DB_OWNER_OR_PERMISSIONS")
    return path

@contextmanager
def _transaction():
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(fd)
        except FileExistsError:
            pass
    conn = sqlite3.connect(path, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""CREATE TABLE IF NOT EXISTS controlled_delivery_permits (
            permit_id TEXT PRIMARY KEY, purpose TEXT NOT NULL, channel TEXT NOT NULL,
            destination_json TEXT NOT NULL, message_sha256 TEXT NOT NULL,
            canonical_text TEXT NOT NULL, max_transport_attempts INTEGER NOT NULL CHECK(max_transport_attempts=1),
            created_at REAL NOT NULL, expires_at REAL NOT NULL, nonce TEXT UNIQUE NOT NULL,
            ledger_entry_id TEXT UNIQUE NOT NULL, consumed INTEGER NOT NULL DEFAULT 0,
            phase TEXT NOT NULL, terminal_state TEXT, attempt_budget_spent INTEGER NOT NULL DEFAULT 0,
            transport_entry_count INTEGER NOT NULL DEFAULT 0, reservation_token TEXT,
            provider_receipt_json TEXT, updated_at REAL NOT NULL
        )""")
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()

def create_permit(message, *, ttl_seconds=60, purpose=PURPOSE):
    _policy()
    message.validate()
    if purpose != PURPOSE:
        raise PermitDenied("PURPOSE_MISMATCH")
    if type(ttl_seconds) not in (int, float) or not math.isfinite(ttl_seconds) or not 0 < ttl_seconds <= MAX_TTL:
        raise PermitDenied("INVALID_TTL")
    now = time.time()
    permit_id, nonce = str(uuid.uuid4()), str(uuid.uuid4())
    ledger_id = "controlled-test:" + permit_id
    with _transaction() as conn:
        conn.execute("""INSERT INTO controlled_delivery_permits
            (permit_id,purpose,channel,destination_json,message_sha256,canonical_text,
             max_transport_attempts,created_at,expires_at,nonce,ledger_entry_id,phase,updated_at)
            VALUES (?,?,?,?,?,?,1,?,?,?,?, 'CREATED',?)""",
            (permit_id,purpose,message.channel,message.destination_json,message.message_hash,
             message.text,now,now+ttl_seconds,nonce,ledger_id,now))
    return evidence(permit_id)

def _reserve(permit_id, message, *, ledger_entry_id, purpose, attempts):
    validation = validate_permit(permit_id,message,ledger_entry_id=ledger_entry_id,
                                 purpose=purpose,attempts=attempts)
    require_transport_admission(validation)
    token = str(uuid.uuid4())
    with _transaction() as conn:
        row = conn.execute("SELECT * FROM controlled_delivery_permits WHERE permit_id=?", (permit_id,)).fetchone()
        # Repeat the identical validation under the durable reservation lock.
        # The preflight is evidence, never a reusable grant across SQL races.
        _validate_row(row,message,ledger_entry_id=ledger_entry_id,purpose=purpose,attempts=attempts)
        require_transport_admission(validation)
        now = time.time()
        # Uncertain is persisted BEFORE side effect. Crash at any subsequent point is
        # terminal/non-retryable without requiring restart-time row surgery.
        conn.execute("""UPDATE controlled_delivery_permits SET consumed=1,
            phase='RESERVED',terminal_state='SENT_UNCERTAIN',attempt_budget_spent=1,
            reservation_token=?,updated_at=? WHERE permit_id=? AND consumed=0""",
            (token,now,permit_id))
    return token

def _validate_row(row, message, *, ledger_entry_id, purpose, attempts):
    message.validate()
    if type(attempts) is not int or attempts != 1:
        raise PermitDenied("RETRY_REQUESTED")
    if row is None:
        raise PermitDenied("PERMIT_MISSING")
    if row["consumed"]:
        raise PermitDenied("DENIED_ALREADY_CONSUMED")
    now = time.time()
    if not row["created_at"] <= now < row["expires_at"] or row["expires_at"]-row["created_at"] > MAX_TTL:
        raise PermitDenied("PERMIT_EXPIRED_OR_INVALID_TIME")
    if row["purpose"] != PURPOSE or purpose != PURPOSE:
        raise PermitDenied("PURPOSE_MISMATCH")
    if row["channel"] != message.channel:
        raise PermitDenied("CHANNEL_MISMATCH")
    if row["destination_json"] != message.destination_json:
        raise PermitDenied("DESTINATION_MISMATCH")
    if row["canonical_text"] != message.text or row["message_sha256"] != message.message_hash:
        raise PermitDenied("MESSAGE_HASH_MISMATCH")
    if row["ledger_entry_id"] != ledger_entry_id:
        raise PermitDenied("LEDGER_SCOPE_MISMATCH")
    if row["max_transport_attempts"] != 1 or row["phase"] != "CREATED":
        raise PermitDenied("INVALID_PERMIT_STATE")

def validate_permit(permit_id, message, *, ledger_entry_id, purpose=PURPOSE, attempts=1):
    """Read-only exact permit validation; does NOT imply transport admission."""
    policy = _policy()
    path = _path()
    if not path.exists():
        raise PermitDenied("PERMIT_MISSING")
    with sqlite3.connect(path.as_uri()+"?mode=ro",uri=True,timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(TABLE,)).fetchone():
            raise PermitDenied("PERMIT_MISSING")
        row = conn.execute("SELECT * FROM controlled_delivery_permits WHERE permit_id=?",(permit_id,)).fetchone()
        _validate_row(row,message,ledger_entry_id=ledger_entry_id,purpose=purpose,attempts=attempts)
    return {"permit_validation":"PASS","permit_id":permit_id,
            "message_sha256":message.message_hash,"ledger_entry_id":ledger_entry_id,
            **_context_evidence(policy)}

def _transport_entry(permit_id, token):
    require_transport_admission()
    with _transaction() as conn:
        changed = conn.execute("""UPDATE controlled_delivery_permits
            SET phase='TRANSPORT_INVOKED',transport_entry_count=1,updated_at=?
            WHERE permit_id=? AND reservation_token=? AND consumed=1
              AND phase='RESERVED' AND transport_entry_count=0""", (time.time(),permit_id,token)).rowcount
        if changed != 1:
            raise PermitDenied("TRANSPORT_ALREADY_ENTERED")

def _finish(permit_id, token, state, receipt=None):
    encoded = json.dumps(receipt,sort_keys=True) if receipt is not None else None
    with _transaction() as conn:
        changed = conn.execute("""UPDATE controlled_delivery_permits SET phase='TERMINAL',
            terminal_state=?,provider_receipt_json=?,updated_at=?
            WHERE permit_id=? AND reservation_token=? AND consumed=1
              AND phase='TRANSPORT_INVOKED' AND transport_entry_count=1""",
            (state,encoded,time.time(),permit_id,token)).rowcount
        if changed != 1:
            raise PermitDenied("TERMINAL_SCOPE_MISMATCH")

async def deliver_once(permit_id, message, *, ledger_entry_id, transport, purpose=PURPOSE, attempts=1):
    """Dedicated controlled-test Core request. Transport is a trusted single-call Core
    implementation, not a sender/tool/notification callable selected by external input.
    This never grants a global/contextvar bypass to ordinary send or ledger APIs.
    """
    message.validate()
    if (getattr(transport, "channel", None) != message.channel
            or getattr(transport, "destination_json", None) != message.destination_json):
        raise PermitDenied("TRANSPORT_ROUTE_MISMATCH")
    token = _reserve(permit_id,message,ledger_entry_id=ledger_entry_id,purpose=purpose,attempts=attempts)
    _transport_entry(permit_id,token)
    try:
        receipt = await transport.send_once(message)
        if not isinstance(receipt,dict) or not isinstance(receipt.get("message_id"),str) or not receipt["message_id"]:
            _finish(permit_id,token,"SENT_UNCERTAIN")
        else:
            _finish(permit_id,token,"SENT",receipt)
    except DefinitelyNotSent:
        _finish(permit_id,token,"FAILED")
    except BaseException:
        # Network/timeout/cancellation/unknown provider errors cannot prove non-delivery.
        # Keep the consumed record even if finishing fails; its reserved default is uncertain.
        _finish(permit_id,token,"SENT_UNCERTAIN")
        raise
    return evidence(permit_id)

def evidence(permit_id):
    with _transaction() as conn:
        row = conn.execute("SELECT * FROM controlled_delivery_permits WHERE permit_id=?", (permit_id,)).fetchone()
        if row is None:
            raise PermitDenied("PERMIT_MISSING")
        r = dict(row)
    from gateway import startup_isolation as policy
    policy.resolve_config()
    return {"schema":"hermes-core-single-message-permit.v1","permit_id":r["permit_id"],
        **_context_evidence(policy.evidence()),
        "purpose":r["purpose"],"channel":r["channel"],
        "destination_identity_hash":hashlib.sha256(r["destination_json"].encode()).hexdigest(),
        "message_sha256":r["message_sha256"],"max_transport_attempts":r["max_transport_attempts"],
        "created_at":r["created_at"],"expires_at":r["expires_at"],"nonce":r["nonce"],
        "actual_transport_attempts":r["transport_entry_count"],
        "attempt_budget_spent":r["attempt_budget_spent"],"auto_retry":False,
        "permit_consumed":bool(r["consumed"]),"permit_phase":r["phase"],
        "permit_terminal_state":r["terminal_state"],"ledger_entry_id":r["ledger_entry_id"],
        "provider_receipt":json.loads(r["provider_receipt_json"]) if r["provider_receipt_json"] else None,
        "unrelated_ledger_mutations":0,"unrelated_pending_replay_count":0}
