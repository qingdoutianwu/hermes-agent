"""Ephemeral native Unix-socket authority for an isolated no-agent worker.

The authority is created by the adopted external worker, not by the business
process.  The child must connect to a per-dispatch socket; macOS LOCAL_PEERPID
and Hermes' process-start fingerprint bind the connection to the actual child.
"""
from __future__ import annotations

import ctypes
import ctypes.util
import hashlib
import json
import os
import socket
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

_SOL_LOCAL = 0
_LOCAL_PEERPID = 0x002
ORIGIN = "HERMES_SCHEDULER_CRON"
MAX_REQUEST_BYTES = 4096
MAX_RESPONSE_BYTES = 8192
PROOF_TTL_SECONDS = 30.0
REQUEST_READ_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class PeerIdentity:
    pid: int
    uid: int
    gid: int
    start_fingerprint: int


def current_profile() -> str:
    from hermes_cli.profiles import current_profile_name

    return current_profile_name("default") or "default"


def peer_start_fingerprint(pid: int) -> int:
    from gateway.status import get_process_start_time

    value = get_process_start_time(pid)
    if value is None:
        raise RuntimeError("peer process start fingerprint unavailable")
    return int(value)


def _peer_pid(sock: socket.socket) -> int:
    if sock.family != socket.AF_UNIX:
        raise ValueError("AF_UNIX socket required")
    libc_path = ctypes.util.find_library("c")
    if not libc_path:
        raise RuntimeError("libc unavailable")
    libc = ctypes.CDLL(libc_path, use_errno=True)
    getsockopt = libc.getsockopt
    getsockopt.argtypes = [
        ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint32),
    ]
    getsockopt.restype = ctypes.c_int
    pid = ctypes.c_int32(0)
    length = ctypes.c_uint32(ctypes.sizeof(pid))
    result = getsockopt(
        sock.fileno(), _SOL_LOCAL, _LOCAL_PEERPID,
        ctypes.byref(pid), ctypes.byref(length),
    )
    if result != 0 or length.value != ctypes.sizeof(pid) or pid.value <= 0:
        raise OSError(ctypes.get_errno(), "LOCAL_PEERPID failed")
    return int(pid.value)


def peer_identity(sock: socket.socket) -> PeerIdentity:
    """Return concrete macOS peer PID and start fingerprint or fail closed."""
    if not isinstance(sock, socket.socket):
        raise TypeError("peer socket required")
    if sock.family != socket.AF_UNIX:
        raise ValueError("AF_UNIX socket required")
    pid = _peer_pid(sock)
    return PeerIdentity(
        pid=pid,
        uid=os.getuid(),
        gid=os.getgid(),
        start_fingerprint=peer_start_fingerprint(pid),
    )


def canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def scrub_identity(env: dict[str, str]) -> None:
    for name in tuple(env):
        if name.startswith("HERMES_CRON_") or name in {
            "FINANCE_TRIGGER_TYPE", "FINANCE_SCHEDULED_AT", "FINANCE_DISPATCHED_AT",
        }:
            env.pop(name, None)


def _entry_path(job: Mapping[str, Any]) -> Path:
    from hermes_constants import get_hermes_home

    raw = str(job.get("_script_path") or job.get("script") or "").strip()
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = get_hermes_home() / "scripts" / path
    if path.is_symlink():
        raise ValueError("script symlink is not an authority entry")
    path = path.resolve(strict=True)
    scripts = (get_hermes_home() / "scripts").resolve()
    path.relative_to(scripts)
    return path


def canonical_release_manifest(job: Mapping[str, Any]) -> Path | None:
    """Resolve the release manifest from the job's isolated workdir, never from receipt claims."""
    candidates: list[Path] = []
    explicit = str(job.get("_r2_release_manifest") or "").strip()
    if explicit:
        candidates.append(Path(explicit).expanduser())
    workdir = str(job.get("workdir") or "").strip()
    if workdir:
        candidates.append(Path(workdir).expanduser() / "source" / "release_manifest.json")
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_file():
            return resolved
    return None


def _manifest_release_tuple(manifest: Mapping[str, Any]) -> tuple[str, str, str, str]:
    """Return the immutable release tuple required for candidate authority binding."""
    return (
        str(manifest.get("release_id") or "").strip(),
        str(manifest.get("source_commit") or "").strip(),
        str(manifest.get("source_tree_sha256") or "").strip(),
        str(manifest.get("production_entrypoint") or manifest.get("entrypoint") or "").strip(),
    )


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value.lower())


class ScriptOriginAuthority:
    """One accepted child connection, atomically consumed in native ledger."""

    def __init__(self, job: Mapping[str, Any]) -> None:
        from cron import executions
        from cron.occurrences import scheduled_instant

        self.execution_id = str(job.get("execution_id") or "")
        self.job_id = str(job.get("id") or "")
        self.scheduled_at = scheduled_instant(job.get("_scheduled_instant")) or ""
        self.profile = current_profile()
        self.release_manifest = canonical_release_manifest(job)
        if self.release_manifest is None:
            raise ValueError("RELEASE_MANIFEST_REQUIRED")
        try:
            manifest = json.loads(self.release_manifest.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError) as exc:
            raise ValueError("RELEASE_MANIFEST_INVALID") from exc
        self.release_id = str(manifest.get("release_id") or "").strip()
        if not self.release_id:
            raise ValueError("RELEASE_ID_REQUIRED")
        self.release_tuple = _manifest_release_tuple(manifest)
        if not self.release_tuple[1] or not _is_sha256(self.release_tuple[2]) or not self.release_tuple[3]:
            raise ValueError("CANONICAL_RELEASE_BINDING_REQUIRED")
        self.release_manifest_sha256 = hashlib.sha256(self.release_manifest.read_bytes()).hexdigest()
        if str(job.get("_r2_authority_required") or "") != "true":
            raise ValueError("R2_AUTHORITY_NOT_ENABLED")
        self.entry = _entry_path(job)
        self.entry_sha256 = hashlib.sha256(self.entry.read_bytes()).hexdigest()
        manifest_entry = str(manifest.get("entry") or "").strip()
        manifest_entry_sha256 = str(manifest.get("entry_sha256") or "").strip()
        if not manifest_entry or not _is_sha256(manifest_entry_sha256):
            raise ValueError("CANONICAL_ENTRY_BINDING_REQUIRED")
        manifest_entry_path = Path(manifest_entry).expanduser()
        if not manifest_entry_path.is_absolute():
            manifest_entry_path = self.release_manifest.parent / manifest_entry_path
        if manifest_entry_path.resolve() != self.entry:
            raise ValueError("RELEASE_ENTRY_BINDING_DENIED")
        if manifest_entry_sha256 != self.entry_sha256:
            raise ValueError("RELEASE_ENTRY_DIGEST_DENIED")
        if not (self.execution_id and self.job_id and self.scheduled_at):
            raise ValueError("NATURAL_EXECUTION_IDENTITY_REQUIRED")
        row = executions.get_execution(self.execution_id)
        if not row or row.get("source") != "builtin" or row.get("status") != "running":
            raise ValueError("NATIVE_EXECUTION_REQUIRED")
        if row.get("job_id") != self.job_id or row.get("scheduled_instant") != scheduled_instant(self.scheduled_at):
            raise ValueError("NATIVE_EXECUTION_BINDING_DENIED")
        if row.get("profile") != self.profile:
            raise ValueError("NATIVE_PROFILE_BINDING_DENIED")
        if row.get("process_id") != executions._PROCESS_ID or row.get("pid") != os.getpid():
            raise ValueError("NATIVE_OWNER_BINDING_DENIED")
        if not row.get("process_started_at"):
            raise ValueError("NATIVE_OWNER_START_REQUIRED")
        if peer_start_fingerprint(os.getpid()) != int(row["process_started_at"]):
            raise ValueError("NATIVE_OWNER_START_MISMATCH")
        self.owner_row = row
        self.root = Path(tempfile.mkdtemp(prefix="hermes-cron-auth-"))
        self.path = self.root / "authority.sock"
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(str(self.path))
        self.path.chmod(0o600)
        self.server.listen(1)
        self.server.settimeout(PROOF_TTL_SECONDS)
        self.consumed = False

    def close(self) -> None:
        try:
            self.server.close()
        finally:
            self.path.unlink(missing_ok=True)
            self.root.rmdir()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def _reject(self, conn: socket.socket) -> None:
        try:
            conn.sendall(b'{"status":"REJECTED"}\n')
        except OSError:
            pass

    def consume(self, child_pid: int) -> dict[str, Any]:
        from cron import executions
        from cron.occurrences import scheduled_instant

        if self.consumed:
            raise RuntimeError("ORIGIN_PROOF_ALREADY_CONSUMED")
        conn = None
        try:
            conn, _ = self.server.accept()
            conn.settimeout(REQUEST_READ_TIMEOUT_SECONDS)
            peer = peer_identity(conn)
            if peer.pid != child_pid or peer.uid != os.getuid() or peer.gid != os.getgid():
                raise RuntimeError("CHILD_PEER_IDENTITY_MISMATCH")
            if peer.start_fingerprint != peer_start_fingerprint(child_pid):
                raise RuntimeError("CHILD_START_IDENTITY_MISMATCH")
            raw = _recv_line(conn, MAX_REQUEST_BYTES)
            if len(raw) > MAX_REQUEST_BYTES or not raw.endswith(b"\n"):
                raise RuntimeError("ORIGIN_REQUEST_INVALID")
            request = json.loads(raw[:-1])
            if not isinstance(request, dict):
                raise RuntimeError("ORIGIN_REQUEST_INVALID")
            if (
                request.get("execution_id") != self.execution_id
                or request.get("job_id") != self.job_id
                or request.get("scheduled_at") != self.scheduled_at
                or request.get("origin") != ORIGIN
                or request.get("pid") != child_pid
                or not isinstance(request.get("nonce"), str)
                or len(request["nonce"]) != 32
                or any(c not in "0123456789abcdef" for c in request["nonce"])
            ):
                raise RuntimeError("ORIGIN_REQUEST_BINDING_DENIED")
            now = time.time()
            proof = {
                "schema": "hermes-native-origin-proof.v2",
                "origin": ORIGIN,
                "profile": self.profile,
                "job_id": self.job_id,
                "execution_id": self.execution_id,
                "scheduled_at": self.scheduled_at,
                "release_id": self.release_id or None,
                "release_manifest_sha256": self.release_manifest_sha256,
                "entry": str(self.entry),
                "entry_sha256": self.entry_sha256,
                "owner_pid": os.getpid(),
                "owner_process_id": self.owner_row["process_id"],
                "owner_started_at": self.owner_row["process_started_at"],
                "child_pid": child_pid,
                "child_started_at": peer.start_fingerprint,
                "issued_at": now,
                "expires_at": now + PROOF_TTL_SECONDS,
            }
            digest = hashlib.sha256(canonical(proof)).hexdigest()
            with executions._transaction() as db:
                cur = db.execute(
                    """UPDATE executions SET origin_proof_sha256=?, origin_proof_used=1
                       WHERE id=? AND job_id=? AND source='builtin' AND status='running'
                         AND process_id=? AND pid=? AND process_started_at=?
                         AND origin_proof_sha256 IS NULL AND origin_proof_used=0""",
                    (
                        digest, self.execution_id, self.job_id, self.owner_row["process_id"],
                        self.owner_row["pid"], self.owner_row["process_started_at"],
                    ),
                )
                if cur.rowcount != 1:
                    raise RuntimeError("ORIGIN_PROOF_CAS_REJECTED")
            response = {"status": "ACCEPTED", "proof": proof, "proof_sha256": digest}
            encoded = canonical(response) + b"\n"
            if len(encoded) > MAX_RESPONSE_BYTES:
                raise RuntimeError("ORIGIN_RESPONSE_TOO_LARGE")
            conn.sendall(encoded)
            self.consumed = True
            return response
        except Exception:
            if conn is not None:
                self._reject(conn)
            self.consumed = True
            raise
        finally:
            if conn is not None:
                conn.close()

def _recv_line(conn: socket.socket, limit: int) -> bytes:
    """Read one bounded newline-delimited request without allowing a slow child to hold the authority forever."""
    chunks: list[bytes] = []
    size = 0
    deadline = time.monotonic() + REQUEST_READ_TIMEOUT_SECONDS
    while size <= limit:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Origin request deadline exceeded")
        conn.settimeout(remaining)
        chunk = conn.recv(min(1024, limit + 1 - size))
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
        if b"\n" in chunk:
            return b"".join(chunks).split(b"\n", 1)[0] + b"\n"
    return b"".join(chunks)


def authoritative_environment(env: dict[str, str], authority: ScriptOriginAuthority) -> None:
    scrub_identity(env)
    env.update({
        "HERMES_CRON_TRIGGER_TYPE": ORIGIN,
        "HERMES_CRON_EXECUTION_ID": authority.execution_id,
        "HERMES_CRON_JOB_ID": authority.job_id,
        "HERMES_CRON_SCHEDULED_AT": authority.scheduled_at,
        "HERMES_CRON_AUTH_SOCKET": str(authority.path),
        "HERMES_CRON_PROFILE": authority.profile,
        "HERMES_CRON_RELEASE_ID": authority.release_id,
        "HERMES_CRON_RELEASE_MANIFEST": str(authority.release_manifest),
        "HERMES_CRON_RELEASE_MANIFEST_SHA256": authority.release_manifest_sha256,
    })
