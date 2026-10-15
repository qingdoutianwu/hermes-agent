"""Candidate read-only R3 integration contract for native Cron provenance."""
from __future__ import annotations
import json
from pathlib import Path

CONTRACT_FIELDS = (
    "NATIVE_EXECUTION_VERIFIED", "NATIVE_JOB_ID_MATCH", "NATIVE_PROFILE_MATCH",
    "SCHEDULED_INSTANT_VERIFIED", "BUSINESS_RECEIPT_LINK_VERIFIED", "RELEASE_IDENTITY_MATCH",
)

def build_r3_candidate(receipt_path: Path, ledger_path: Path, *, expected_job_id: str, expected_profile: str, expected_release_id: str, related_receipt_paths=()) -> dict:
    from cron.provenance_readonly import verify_receipt_provenance
    result = verify_receipt_provenance(receipt_path, ledger_path=ledger_path, expected_job_id=expected_job_id, expected_profile=expected_profile, expected_release_id=expected_release_id, related_receipt_paths=related_receipt_paths)
    return {"status": "TRUSTED_NATURAL_COVERAGE_CANDIDATE" if result.get("trusted") else "PROVENANCE_UNVERIFIED", "trusted": bool(result.get("trusted")), "checks": {field: bool(result.get(field)) for field in CONTRACT_FIELDS}, "reasons": list(result.get("reasons") or []), "receipt_id": result.get("receipt_id"), "execution_id": result.get("execution_id")}
