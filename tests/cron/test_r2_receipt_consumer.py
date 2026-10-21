"""R2 candidate-only contract tests for receipt consumption and fail-closed verification."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from cron.provenance_readonly_r2 import verify_receipt_provenance
from cron.r2_receipt_consumer import consume_authority_receipt, write_receipt

JOB = "job-r2"
EXEC = "execution-r2"
SLOT = "2026-10-16T09:00:00+08:00"
PROFILE = "r2-profile"
RELEASE = "HK-ANOMALY-ISOLATED-R2"


def _proof():
    return {
        "schema": "hermes-native-origin-proof.v2",
        "execution_id": EXEC,
        "job_id": JOB,
        "scheduled_at": SLOT,
        "origin": "HERMES_SCHEDULER_CRON",
        "child_pid": 1234,
        "owner_pid": 1000,
        "owner_process_id": "scheduler-process",
        "owner_started_at": 123,
        "child_started_at": 124,
        "profile": PROFILE,
        "release_id": RELEASE,
        "entry_sha256": "b" * 64,
        "release_manifest_sha256": "",
        "issued_at": 1000.0,
        "expires_at": 1030.0,
    }


def _response():
    proof = _proof()
    raw = json.dumps(proof, sort_keys=True, separators=(",", ":")).encode()
    return {"status": "ACCEPTED", "proof": proof, "proof_sha256": hashlib.sha256(raw).hexdigest()}


def _manifest(tmp_path: Path) -> Path:
    path = tmp_path / "release_manifest.json"
    path.write_text(json.dumps({
        "release_id": RELEASE,
        "source_commit": "candidate-commit",
        "source_tree_sha256": "a" * 64,
        "production_entrypoint": "scripts/probe.py",
    }, sort_keys=True), encoding="utf-8")
    return path


def _response_for_manifest(tmp_path: Path):
    response = _response()
    response["proof"]["release_manifest_sha256"] = hashlib.sha256(
        _manifest(tmp_path).read_bytes()
    ).hexdigest()
    response["proof_sha256"] = hashlib.sha256(
        json.dumps(response["proof"], sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return response


def _profile_patch():
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("cron.r2_receipt_consumer.current_profile", lambda: PROFILE)
    return monkeypatch


def test_consumer_writes_new_candidate_receipt_without_trusting_it(tmp_path: Path):
    monkeypatch = _profile_patch()
    try:
        receipt = consume_authority_receipt(
            _response_for_manifest(tmp_path), release_manifest=_manifest(tmp_path), profile=PROFILE, receipt_id="r2"
        )
        assert receipt["trusted"] is False
        assert receipt["provenance_state"] == "PENDING_NATIVE_READONLY_VERIFICATION"
        path = tmp_path / "receipt.json"
        write_receipt(path, receipt)
        assert json.loads(path.read_text())["trusted"] is False
        with pytest.raises(FileExistsError):
            write_receipt(path, receipt)
    finally:
        monkeypatch.undo()


def test_consumer_rejects_tampered_proof(tmp_path: Path):
    response = _response()
    response["proof"]["job_id"] = "spoofed"
    monkeypatch = _profile_patch()
    try:
        with pytest.raises(ValueError, match="PROVENANCE_UNVERIFIED"):
            consume_authority_receipt(
                response, release_manifest=_manifest(tmp_path), profile=PROFILE, receipt_id="r2"
            )
    finally:
        monkeypatch.undo()


def test_consumer_rejects_rejected_authority(tmp_path: Path):
    monkeypatch = _profile_patch()
    try:
        with pytest.raises(ValueError, match="PROVENANCE_UNVERIFIED"):
            consume_authority_receipt(
                {"status": "REJECTED"}, release_manifest=_manifest(tmp_path), profile=PROFILE, receipt_id="r2"
            )
    finally:
        monkeypatch.undo()


def test_manual_or_spoofed_response_cannot_become_natural(tmp_path: Path):
    response = _response()
    response["proof"]["origin"] = "MANUAL_CLI"
    raw = json.dumps(response["proof"], sort_keys=True, separators=(",", ":")).encode()
    response["proof_sha256"] = hashlib.sha256(raw).hexdigest()
    monkeypatch = _profile_patch()
    try:
        with pytest.raises(ValueError, match="PROVENANCE_UNVERIFIED"):
            consume_authority_receipt(
                response, release_manifest=_manifest(tmp_path), profile=PROFILE, receipt_id="r2"
            )
    finally:
        monkeypatch.undo()


def test_r2_verifier_fails_closed_when_native_schema_or_release_binding_is_missing(tmp_path: Path):
    monkeypatch = _profile_patch()
    try:
        receipt = consume_authority_receipt(
            _response_for_manifest(tmp_path), release_manifest=_manifest(tmp_path), profile=PROFILE, receipt_id="r2"
        )
        receipt_path = tmp_path / "receipt.json"
        write_receipt(receipt_path, receipt)
        ledger = tmp_path / "executions.db"
        with sqlite3.connect(ledger) as db:
            db.execute("CREATE TABLE executions(id TEXT, job_id TEXT)")
        result = verify_receipt_provenance(
            receipt_path,
            ledger_path=ledger,
            expected_job_id=JOB,
            expected_profile=PROFILE,
            release_manifest=tmp_path / "missing-manifest.json",
        )
        assert result["trusted"] is False
        assert result["status"] == "PROVENANCE_UNVERIFIED"
    finally:
        monkeypatch.undo()
