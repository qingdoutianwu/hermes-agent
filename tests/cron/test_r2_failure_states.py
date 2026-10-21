"""Candidate-only failure/crash state contract tests."""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from cron.r2_receipt_consumer import consume_authority_receipt


def test_rejected_authority_does_not_create_trusted_receipt():
    try:
        consume_authority_receipt(
            {"status": "REJECTED"},
            release_manifest=Path("/missing/isolated-manifest.json"),
            profile="default",
            receipt_id="r2",
        )
    except ValueError as exc:
        assert str(exc) == "PROVENANCE_UNVERIFIED"
    else:
        raise AssertionError("rejected authority was accepted")


def test_native_ledger_schema_requires_proof_columns(tmp_path: Path):
    db = tmp_path / "executions.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE executions(id TEXT, job_id TEXT, status TEXT)")
        columns = {row[1] for row in conn.execute("PRAGMA table_info(executions)")}
    assert "origin_proof_sha256" not in columns
    assert "origin_proof_used" not in columns


def test_required_script_without_authority_never_starts(tmp_path, monkeypatch):
    from cron.scheduler_script import _run_job_script
    home = tmp_path / "home"
    script = home / "scripts" / "probe.py"
    script.parent.mkdir(parents=True)
    marker = home / "side-effect"
    script.write_text(f"from pathlib import Path; Path({str(marker)!r}).touch()")
    monkeypatch.setenv("HERMES_HOME", str(home))
    success, error = _run_job_script(str(script), job={"_r2_authority_required":"true"})
    assert not success and "ORIGIN_AUTHORITY_REQUIRED" in error
    assert not marker.exists()
