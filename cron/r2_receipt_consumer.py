"""Candidate-only isolated HK Receipt consumer for native Cron authority proofs."""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import socket
from pathlib import Path
from typing import Any, Mapping

from cron.occurrences import scheduled_instant
from cron.scheduler_origin_auth import canonical


def _manifest(path: Path) -> tuple[str, str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError) as exc:
        raise ValueError("PROVENANCE_UNVERIFIED") from exc
    if not isinstance(payload, dict) or not str(payload.get("release_id") or "").strip():
        raise ValueError("PROVENANCE_UNVERIFIED")
    return str(payload["release_id"]), hashlib.sha256(path.read_bytes()).hexdigest()


def current_profile() -> str:
    from hermes_cli.profiles import current_profile_name

    return current_profile_name("default") or "default"


def consume_authority_receipt(
    authority_response: Mapping[str, Any],
    *,
    release_manifest: Path,
    profile: str,
    receipt_id: str,
) -> dict[str, Any]:
    """Build a new receipt from an accepted proof; trust remains pending verifier readback."""
    if authority_response.get("status") != "ACCEPTED":
        raise ValueError("PROVENANCE_UNVERIFIED")
    proof = authority_response.get("proof")
    digest = authority_response.get("proof_sha256")
    if not isinstance(proof, dict) or not isinstance(digest, str) or "nonce" in proof:
        raise ValueError("PROVENANCE_UNVERIFIED")
    if hashlib.sha256(canonical(proof)).hexdigest() != digest:
        raise ValueError("PROVENANCE_UNVERIFIED")
    release_id, release_hash = _manifest(release_manifest)
    required = {
        "execution_id", "job_id", "scheduled_at", "origin", "child_pid",
        "owner_pid", "owner_process_id", "owner_started_at",
        "profile", "release_id", "entry_sha256", "issued_at", "expires_at",
    }
    if (
        not required.issubset(proof)
        or proof.get("origin") != "HERMES_SCHEDULER_CRON"
        or proof.get("profile") != profile
        or proof.get("release_id") != release_id
        or proof.get("release_manifest_sha256") != release_hash
    ):
        raise ValueError("PROVENANCE_UNVERIFIED")
    if proof.get("expires_at", 0) < proof.get("issued_at", 0):
        raise ValueError("PROVENANCE_UNVERIFIED")
    if scheduled_instant(proof.get("scheduled_at")) is None:
        raise ValueError("PROVENANCE_UNVERIFIED")
    if profile != current_profile():
        raise ValueError("PROVENANCE_UNVERIFIED")
    return {
        "receipt_id": receipt_id,
        "execution_origin": "NATURAL_CRON",
        "cron_job_id": proof["job_id"],
        "execution_id": proof["execution_id"],
        "profile": profile,
        "scheduled_at": scheduled_instant(proof["scheduled_at"]),
        "release_binding": release_id,
        "release_manifest_sha256": release_hash,
        "origin_proof_sha256": digest,
        "origin_proof": proof,
        "trusted": False,
        "provenance_state": "PENDING_NATIVE_READONLY_VERIFICATION",
    }


def write_receipt(path: Path, receipt: Mapping[str, Any]) -> None:
    """Write only a new candidate receipt; never overwrite an existing artifact."""
    with path.open("x", encoding="utf-8") as stream:
        json.dump(dict(receipt), stream, sort_keys=True)


def consume_current_authority(*, release_manifest: Path, receipt_id: str) -> dict[str, Any]:
    """Request native peer-bound evidence; env values are routing hints, NOT trust."""
    from cron.scheduler_origin_auth import canonical, _recv_line, MAX_RESPONSE_BYTES

    try:
        request = {"execution_id": os.environ["HERMES_CRON_EXECUTION_ID"],  # health: allow HX002 -- native IPC request hint, independently authenticated by peer and ledger, not a behavioural setting
                   "job_id": os.environ["HERMES_CRON_JOB_ID"],  # health: allow HX002 -- native IPC request hint, never a trust source
                   "scheduled_at": os.environ["HERMES_CRON_SCHEDULED_AT"],  # health: allow HX002 -- native IPC request hint, checked against canonical ledger instant
                   "origin": os.environ["HERMES_CRON_TRIGGER_TYPE"],  # health: allow HX002 -- native IPC hint, forged values cannot establish provenance
                   "pid": os.getpid(), "nonce": secrets.token_hex(16)}
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(5)
            connection.connect(os.environ["HERMES_CRON_AUTH_SOCKET"])  # health: allow HX002 -- ephemeral native IPC address, not a policy or long-lived credential
            connection.sendall(canonical(request) + b"\n")
            raw = _recv_line(connection, MAX_RESPONSE_BYTES)
        if len(raw) > MAX_RESPONSE_BYTES or not raw.endswith(b"\n"):
            raise ValueError("PROVENANCE_UNVERIFIED")
        response = json.loads(raw)
        return consume_authority_receipt(response, release_manifest=release_manifest,
                                         profile=current_profile(), receipt_id=receipt_id)
    except (KeyError, OSError, ValueError, TypeError) as exc:
        raise ValueError("PROVENANCE_UNVERIFIED") from exc
