"""Manual/no-authority refusal, not a fabricated pre-seeded Scheduler E2E."""
from pathlib import Path
import pytest
from cron.r2_receipt_consumer import consume_current_authority


def test_manual_cli_cannot_create_native_proof(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    for key, value in {"HERMES_CRON_EXECUTION_ID": "forged", "HERMES_CRON_JOB_ID": "forged",
                       "HERMES_CRON_SCHEDULED_AT": "2026-10-16T01:00:00+00:00",
                       "HERMES_CRON_TRIGGER_TYPE": "HERMES_SCHEDULER_CRON",
                       "HERMES_CRON_AUTH_SOCKET": str(tmp_path / "absent.sock")}.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(ValueError, match="PROVENANCE_UNVERIFIED"):
        consume_current_authority(release_manifest=tmp_path / "manifest.json", receipt_id="manual")
    assert not (tmp_path / "receipt.json").exists()
    assert not (tmp_path / "cron" / "executions.db").exists()


def test_missing_authority_fails_closed(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_CRON_AUTH_SOCKET", raising=False)
    with pytest.raises(ValueError, match="PROVENANCE_UNVERIFIED"):
        consume_current_authority(release_manifest=Path(tmp_path / "manifest.json"), receipt_id="manual")
