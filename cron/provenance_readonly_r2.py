"""Candidate-only read-only R2 provenance verifier.

The verifier requires the native execution row and the proof digest recorded by
the atomic one-use CAS.  Receipt values are join keys and claims, never trust
anchors.  Release identity is verified against an explicit isolated manifest.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import math
from pathlib import Path
from typing import Any, Mapping
from cron.occurrences import scheduled_instant
from cron.scheduler_origin_auth import canonical


def _load(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    return value if isinstance(value, dict) else None


def _ledger_row(path: Path, execution_id: str) -> dict[str, Any] | None:
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            columns = {row[1] for row in db.execute("PRAGMA table_info(executions)")}
            required = {
                "id", "job_id", "profile", "source", "process_id", "pid",
                "process_started_at", "status", "scheduled_instant",
                "origin_proof_sha256", "origin_proof_used", "finished_at",
            }
            if not required.issubset(columns):
                return None
            row = db.execute("SELECT * FROM executions WHERE id=?", (execution_id,)).fetchone()
            return dict(row) if row else None
    except (OSError, sqlite3.Error):
        return None


def _release_matches(receipt: Mapping[str, Any], release_manifest: Path | None) -> bool:
    if release_manifest is None:
        return False
    manifest = _load(release_manifest)
    if manifest is None:
        return False
    expected_id = str(manifest.get("release_id") or "")
    expected_hash = hashlib.sha256(release_manifest.read_bytes()).hexdigest()
    proof = receipt.get("origin_proof")
    return (
        isinstance(proof, dict)
        and bool(expected_id)
        and receipt.get("release_binding") == expected_id
        and receipt.get("release_manifest_sha256") == expected_hash
        and proof.get("release_manifest_sha256") == expected_hash
    )


def _proof_matches(receipt, row, expected_job_id, expected_profile):
    proof = receipt.get("origin_proof")
    if not row or not isinstance(proof, dict):
        return False
    try:
        issued, expires = float(proof["issued_at"]), float(proof["expires_at"])
        return (proof.get("schema") == "hermes-native-origin-proof.v2"
                and proof.get("origin") == "HERMES_SCHEDULER_CRON"
                and hashlib.sha256(canonical(proof)).hexdigest() == receipt.get("origin_proof_sha256")
                and row.get("origin_proof_sha256") == receipt.get("origin_proof_sha256")
                and row.get("origin_proof_used") == 1
                and proof.get("execution_id") == row["id"]
                and proof.get("job_id") == expected_job_id
                and proof.get("profile") == expected_profile
                and proof.get("owner_process_id") == row.get("process_id")
                and proof.get("owner_pid") == row.get("pid")
                and proof.get("owner_started_at") == row.get("process_started_at")
                and proof.get("scheduled_at") == receipt.get("scheduled_at")
                and proof.get("release_id") == receipt.get("release_binding")
                and math.isfinite(issued) and math.isfinite(expires) and 0 <= expires - issued <= 30)
    except (ValueError, TypeError, KeyError, OverflowError):
        return False


def verify_receipt_provenance(
    receipt_path: Path,
    *,
    ledger_path: Path,
    expected_job_id: str,
    expected_profile: str,
    release_manifest: Path | None = None,
    related_receipt_paths: tuple[Path, ...] = (),
) -> dict[str, Any]:
    receipt = _load(receipt_path)
    if receipt is None:
        return {"status": "PROVENANCE_UNVERIFIED", "trusted": False, "reasons": ["receipt_unreadable"]}
    execution_id = str(receipt.get("execution_id") or "")
    row = _ledger_row(ledger_path, execution_id)
    checks = {
        "NATIVE_EXECUTION_VERIFIED": bool(
            row
            and row.get("source") == "builtin"
            and row.get("status") == "completed"
            and row.get("finished_at") is not None
        ),
        "NATIVE_JOB_ID_MATCH": bool(row and row.get("job_id") == expected_job_id and receipt.get("cron_job_id") == expected_job_id),
        "NATIVE_PROFILE_MATCH": bool(row and row.get("profile") == expected_profile and receipt.get("profile") == expected_profile),
        "SCHEDULED_INSTANT_VERIFIED": bool(row and scheduled_instant(receipt.get("scheduled_at"))
            and scheduled_instant(receipt.get("scheduled_at")) == receipt.get("scheduled_at")
            and row.get("scheduled_instant") == receipt.get("scheduled_at")),
        "BUSINESS_RECEIPT_LINK_VERIFIED": _proof_matches(receipt, row, expected_job_id, expected_profile),
        "RELEASE_IDENTITY_MATCH": _release_matches(receipt, release_manifest),
    }
    reasons = []
    if receipt.get("execution_origin") != "NATURAL_CRON":
        reasons.append("origin_not_natural_cron")
    names = (
        ("native_execution_unverified", "NATIVE_EXECUTION_VERIFIED"),
        ("job_id_mismatch", "NATIVE_JOB_ID_MATCH"),
        ("profile_mismatch", "NATIVE_PROFILE_MATCH"),
        ("scheduled_instant_mismatch", "SCHEDULED_INSTANT_VERIFIED"),
        ("proof_link_mismatch", "BUSINESS_RECEIPT_LINK_VERIFIED"),
        ("release_identity_mismatch", "RELEASE_IDENTITY_MATCH"),
    )
    reasons.extend(reason for reason, name in names if not checks[name])
    for other_path in related_receipt_paths:
        other = _load(other_path)
        if other and other.get("receipt_id") != receipt.get("receipt_id") and other.get("execution_id") == execution_id:
            reasons.append("duplicate_execution_receipt")
    trusted = not reasons and all(checks.values())
    return {
        "status": "PROVENANCE_VERIFIED" if trusted else "PROVENANCE_UNVERIFIED",
        "trusted": trusted,
        **checks,
        "reasons": sorted(set(reasons)),
        "receipt_id": receipt.get("receipt_id"),
        "execution_id": execution_id,
    }
