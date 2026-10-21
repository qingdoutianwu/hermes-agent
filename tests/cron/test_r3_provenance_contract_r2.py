"""R3 six-field verifier contract tests for the isolated candidate."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from cron.r3_provenance_contract_r2 import verify_r3
from cron.r2_receipt_consumer import consume_authority_receipt, write_receipt
from tests.cron.test_r2_receipt_consumer import _profile_patch, _response_for_manifest, _manifest, JOB, PROFILE


def test_r3_six_checks_fail_closed_without_native_row(tmp_path: Path):
    patch = _profile_patch()
    try:
        manifest = _manifest(tmp_path)
        receipt = consume_authority_receipt(_response_for_manifest(tmp_path), release_manifest=manifest, profile=PROFILE, receipt_id="r3")
        receipt_path = tmp_path / "receipt.json"
        write_receipt(receipt_path, receipt)
        ledger = tmp_path / "executions.db"
        with sqlite3.connect(ledger) as db:
            db.execute("CREATE TABLE executions(id TEXT, job_id TEXT)")
        result = verify_r3(
            receipt_path, ledger_path=ledger, release_manifest=manifest,
            expected_job_id=JOB, expected_profile=PROFILE,
        )
        assert result["trusted"] is False
        assert result["status"] == "PROVENANCE_UNVERIFIED"
        assert set(result["checks"]) == {
            "NATIVE_EXECUTION_VERIFIED", "NATIVE_JOB_ID_MATCH", "NATIVE_PROFILE_MATCH",
            "SCHEDULED_INSTANT_VERIFIED", "BUSINESS_RECEIPT_LINK_VERIFIED", "RELEASE_IDENTITY_MATCH",
        }
    finally:
        patch.undo()
