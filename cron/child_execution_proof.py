"""Opt-in single-child provenance, not business authorization or OS containment.

Signer keys live only in the trusted scheduler process. Consumers pin the public
key in their reviewed candidate code, never accept a key/home from the envelope.
The existing native executions row is the only durable ownership/replay authority.
This protects ordinary lower-trust children, not a compromised trusted interpreter
or an operator rewriting the pinned verifier/native ledger (SECURITY.md §2).
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import select
import sqlite3
import stat
import time

MAX_BYTES = 4096
MAX_TTL = 30
_signers = {}  # Trusted bootstrap only: owning home -> (memory-only key, exact entry).


class ProofError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)  # Never include an envelope or signature in error output.


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def install_signer(home, key, entry):
    """Trusted candidate/bootstrap binding; not a model tool, not env configuration."""
    home = str(Path(home).resolve())
    if home in _signers:
        raise ProofError("PROOF_SIGNER_ALREADY_BOUND")
    _signers[home] = (key, Path(entry).resolve(strict=True))


def _owned_identity(home, entry):
    from cron.execution_identity import current_cron_execution
    from cron import executions
    from cron.jobs import get_job
    from hermes_constants import get_hermes_home
    if str(get_hermes_home().resolve()) != home:
        raise ProofError("PROOF_HOME_BINDING_DENIED")
    ident = current_cron_execution()
    if ident is None:
        raise ProofError("SCHEDULER_ORIGIN_REQUIRED")
    row = executions.get_execution(ident.execution_id)
    job = get_job(ident.job_id)
    if not row or not job:
        raise ProofError("PROOF_NATIVE_ROW_REQUIRED")
    expected = {"id": ident.execution_id, "job_id": ident.job_id, "source": "builtin",
                "scheduled_instant": ident.scheduled_instant, "started_at": ident.started_at,
                "status": "running", "pid": os.getpid(), "process_id": executions._PROCESS_ID}
    if any(row.get(k) != v for k, v in expected.items()) or not ident.scheduled_instant:
        raise ProofError("PROOF_EXECUTION_OWNERSHIP_DENIED")
    if row.get("finished_at") is not None or not row.get("process_started_at"):
        raise ProofError("PROOF_EXECUTION_STATE_DENIED")
    if Path(job.get("script", "")).resolve() != entry or entry.is_symlink():
        raise ProofError("PROOF_ENTRY_BINDING_DENIED")
    return ident, row


class ChildIssue:
    def __init__(self, home, key, entry, ident, row):
        self.home, self.key, self.entry, self.ident, self.row = home, key, entry, ident, row

    def deliver(self, process):
        from cron import executions
        # Recheck ownership after Popen; bind the actual child's PID, not caller input.
        ident, row = _owned_identity(self.home, self.entry)
        if ident != self.ident or any(row[k] != self.row[k] for k in
                ("pid", "process_id", "process_started_at", "started_at", "scheduled_instant")):
            raise ProofError("PROOF_OWNER_CHANGED")
        issued = time.time()
        body = {"schema": "hermes-native-child-proof.v1", "home": self.home,
                "profile": ident.profile, "job_id": ident.job_id,
                "execution_id": ident.execution_id, "source": row["source"],
                "scheduled_instant": row["scheduled_instant"], "started_at": row["started_at"],
                "owner_pid": row["pid"], "owner_process_id": row["process_id"],
                "owner_started_at": row["process_started_at"], "child_pid": process.pid,
                "entry": str(self.entry), "entry_sha256": hashlib.sha256(self.entry.read_bytes()).hexdigest(),
                "issued_at": issued, "expires_at": issued + MAX_TTL,
                "nonce": os.urandom(16).hex()}
        envelope = {"body": body, "signature": self.key.sign(canonical(body)).hex()}
        raw = canonical(envelope)
        digest = hashlib.sha256(raw).hexdigest()
        with executions._transaction() as conn:
            cursor = conn.execute(
                "UPDATE executions SET child_proof_sha256=?, child_proof_used=0 "
                "WHERE id=? AND job_id=? AND status='running' AND source='builtin' "
                "AND process_id=? AND pid=? AND child_proof_sha256 IS NULL",
                (digest, ident.execution_id, ident.job_id, row["process_id"], os.getpid()))
            if cursor.rowcount != 1:
                raise ProofError("PROOF_ISSUANCE_ALREADY_USED")
        if len(raw) > MAX_BYTES:
            raise ProofError("PROOF_TOO_LARGE")
        process.stdin.write(raw.decode("utf-8"))
        process.stdin.close()
        process.stdin = None


def prepare_child(entry):
    from hermes_constants import get_hermes_home
    home = str(get_hermes_home().resolve())
    binding = _signers.get(home)
    if binding is None or Path(entry).resolve() != binding[1]:
        return None  # Existing unbound scripts keep their ordinary stdin behavior.
    key, entry = binding
    ident, row = _owned_identity(home, entry)
    return ChildIssue(home, key, entry, ident, row)


def read_stdin_proof():
    """Read only an inherited pipe; bounded input, no pathname/JSON existence authority."""
    if not stat.S_ISFIFO(os.fstat(0).st_mode):
        raise ProofError("SCHEDULER_ORIGIN_REQUIRED")
    deadline = time.monotonic() + 5
    raw = bytearray()
    while True:
        left = deadline - time.monotonic()
        if left <= 0 or not select.select([0], [], [], left)[0]:
            raise ProofError("PROOF_INPUT_TIMEOUT")
        block = os.read(0, MAX_BYTES + 1 - len(raw))
        if not block:
            break
        raw.extend(block)
        if len(raw) > MAX_BYTES:
            raise ProofError("PROOF_TOO_LARGE")
    if not raw:
        raise ProofError("SCHEDULER_ORIGIN_REQUIRED")
    return bytes(raw)


def _unique_pairs(items):
    out = {}
    for key, value in items:
        if key in out:
            raise ProofError("PROOF_DUPLICATE_FIELD")
        out[key] = value
    return out


def _decode_verified(raw, public_key):
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    try:
        if len(raw) > MAX_BYTES:
            raise ProofError("PROOF_TOO_LARGE")
        envelope = json.loads(raw, object_pairs_hook=_unique_pairs)
        if set(envelope) != {"body", "signature"} or not isinstance(envelope["body"], dict):
            raise ProofError("PROOF_SCHEMA_INVALID")
        body = envelope["body"]
        Ed25519PublicKey.from_public_bytes(public_key).verify(
            bytes.fromhex(envelope["signature"]), canonical(body))
        if canonical(envelope) != raw or body.get("schema") != "hermes-native-child-proof.v1":
            raise ProofError("PROOF_SCHEMA_INVALID")
        return body
    except (ValueError, TypeError, KeyError, InvalidSignature) as exc:
        raise ProofError("PROOF_SIGNATURE_INVALID") from exc


def _check_binding(body, trust, now):
    required = {"schema", "home", "profile", "job_id", "execution_id", "source",
                "scheduled_instant", "started_at", "owner_pid", "owner_process_id",
                "owner_started_at", "child_pid", "entry", "entry_sha256",
                "issued_at", "expires_at", "nonce"}
    if set(body) != required:
        raise ProofError("PROOF_SCHEMA_INVALID")
    for key in ("home", "profile", "entry", "entry_sha256"):
        if body[key] != trust[key]:
            raise ProofError("PROOF_TARGET_BINDING_DENIED")
    if body["source"] != "builtin" or not body["scheduled_instant"]:
        raise ProofError("PROOF_NATURAL_SOURCE_DENIED")
    if body["child_pid"] != os.getpid() or body["owner_pid"] != os.getppid():
        raise ProofError("PROOF_CHILD_BINDING_DENIED")
    from gateway.status import get_process_start_time, start_time_fingerprints_match
    owner_start = get_process_start_time(body["owner_pid"])
    if owner_start is None or not start_time_fingerprints_match(body["owner_started_at"], owner_start):
        raise ProofError("PROOF_OWNER_LIVENESS_DENIED")
    start, end = body["issued_at"], body["expires_at"]
    if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (start, end, now)):
        raise ProofError("PROOF_TIME_INVALID")
    if not start <= now < end or not 0 < end - start <= MAX_TTL:
        raise ProofError("PROOF_EXPIRED")
    if hashlib.sha256(Path(trust["entry"]).read_bytes()).hexdigest() != trust["entry_sha256"]:
        raise ProofError("PROOF_ENTRY_CHANGED")


def _consume_native_row(raw, body, trust):
    # Trust comes from reviewed code, never body.home or a caller-selected JSON file.
    home = Path(trust["home"])
    db = home / "cron/executions.db"
    for path in (home, home / "cron", db):
        info = path.lstat()
        if path.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o022:
            raise ProofError("PROOF_LEDGER_UNSAFE")
    from contextlib import closing
    with closing(sqlite3.connect(db.as_uri() + "?mode=rw", uri=True, timeout=3)) as conn:
        conn.row_factory = sqlite3.Row
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM executions WHERE id=?", (body["execution_id"],)).fetchone()
            expected = {"job_id": body["job_id"], "source": "builtin", "status": "running",
                        "pid": body["owner_pid"], "process_id": body["owner_process_id"],
                        "process_started_at": body["owner_started_at"], "started_at": body["started_at"],
                        "scheduled_instant": body["scheduled_instant"], "finished_at": None}
            if row is None or any(row[k] != value for k, value in expected.items()):
                raise ProofError("PROOF_NATIVE_EXECUTION_DENIED")
            digest = hashlib.sha256(raw).hexdigest()
            if row["child_proof_sha256"] != digest or row["child_proof_used"] != 0:
                raise ProofError("PROOF_REPLAY_DENIED")
            cursor = conn.execute("UPDATE executions SET child_proof_used=1 WHERE id=? "
                                  "AND status='running' AND child_proof_sha256=? AND child_proof_used=0",
                                  (body["execution_id"], digest))
            if cursor.rowcount != 1:
                raise ProofError("PROOF_REPLAY_DENIED")


def verify_proof(raw, trust, *, now=None, claimed=None):
    """Authenticate -> bind child/entry/profile/time -> consume native row once."""
    try:
        body = _decode_verified(raw, bytes.fromhex(trust["public_key"]))
        _check_binding(body, trust, time.time() if now is None else now)
        if claimed:
            names = {"HERMES_CRON_JOB_ID": "job_id", "HERMES_CRON_EXECUTION_ID": "execution_id",
                     "HERMES_CRON_SCHEDULED_AT": "scheduled_instant", "HERMES_CRON_TRIGGER_TYPE": "source",
                     "HERMES_CRON_DISPATCHED_AT": "started_at", "FINANCE_TRIGGER_TYPE": "source",
                     "FINANCE_SCHEDULED_AT": "scheduled_instant", "FINANCE_DISPATCHED_AT": "started_at"}
            for key, field in names.items():
                expected = "HERMES_SCHEDULER_CRON" if field == "source" else body[field]
                if key in claimed and claimed[key] != expected:
                    raise ProofError("PROOF_CALLER_FIELD_MISMATCH")
        _consume_native_row(raw, body, trust)
        return body
    except (OSError, sqlite3.Error, KeyError, ValueError, TypeError) as exc:
        raise ProofError("PROOF_VERIFICATION_FAILED") from exc
