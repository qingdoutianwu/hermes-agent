"""Real isolated external-worker authority -> receipt -> verifier contract."""
from __future__ import annotations

import json
import hashlib
import os
import sqlite3
from pathlib import Path
import subprocess
import sys
import pytest


def _fire_source() -> str:
    return r'''
import json, os, sys
from pathlib import Path
from cron import scheduler
assert scheduler.tick(verbose=False, sync=True) == 1
'''


@pytest.mark.parametrize("profile", ["default", "isolated-r2"])
def test_real_external_worker_receipt_and_readonly_verifier(tmp_path, monkeypatch, profile):
    home = tmp_path / ".hermes"
    if profile != "default":
        home = home / "profiles" / profile
    (home / "cron").mkdir(parents=True)
    (home / "scripts").mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    release_manifest = home / "release_manifest.json"
    receipt_path = home / "receipt.json"
    script = home / "scripts" / "consumer.py"
    candidate_root = Path(__file__).resolve().parents[2]
    script.write_text(
        f"import json, os, socket, sys\n"
        f"sys.path.insert(0, {str(candidate_root)!r})\n"
        "from pathlib import Path\n"
        "from cron.r2_receipt_consumer import consume_current_authority, write_receipt\n"
        "receipt=consume_current_authority(release_manifest=Path(os.environ['HERMES_CRON_RELEASE_MANIFEST']), receipt_id='r2-external-worker')\n"
        "write_receipt(Path(os.environ['HERMES_HOME'])/'receipt.json', receipt)\n"
        "print('receipt-written')\n",
        encoding="utf-8",
    )
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    job = {
        "id": "job-r2-external-worker",
        "name": "r2-external-worker",
        "prompt": "",
        "schedule": {"kind": "interval", "minutes": 240},
        "next_run_at": now,
        "enabled": True,
        "state": "scheduled",
        "script": str(script),
        "no_agent": True,
        "deliver": "local",
        "repeat": {"times": None, "completed": 0},
        "workdir": str(home),
    }
    release_manifest.write_text(
        json.dumps({
            "release_id": "HK-ANOMALY-ISOLATED-R2",
            "source_commit": "candidate-commit",
            "source_tree_sha256": "a" * 64,
            "production_entrypoint": "scripts/consumer.py",
            "entry": "scripts/consumer.py",
            "entry_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
        }, sort_keys=True),
        encoding="utf-8",
    )
    from cron import jobs as jobs_module
    created = jobs_module.create_job(prompt="", schedule="every 4h", name=job["name"],
        script=str(script), no_agent=True, deliver="local", workdir=str(home),
        r2_authority_required=True, r2_release_manifest=str(release_manifest))
    job = jobs_module.update_job(created["id"], {"next_run_at": now})
    env = {
        k: v for k, v in os.environ.items()
        if not k.startswith(("HERMES_", "_HERMES_"))
        and not k.endswith(("_API_KEY", "_TOKEN"))
    }
    env.update({"HERMES_HOME": str(home), "HERMES_PROFILE":profile,
                "PYTHONPATH": str(Path(__file__).resolve().parents[2])})
    monkeypatch.setenv("HERMES_PROFILE", profile)
    result = subprocess.run(
        [sys.executable, "-c", _fire_source()],
        cwd=str(home), env=env, capture_output=True, text=True, timeout=90, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    if not receipt_path.exists():
        evidence = []
        for candidate in sorted(home.rglob("*")):
            if candidate.is_file() and candidate.suffix in {".md", ".log", ".stderr"}:
                try:
                    evidence.append(f"{candidate.relative_to(home)}={candidate.read_text(errors='replace')[:2000]!r}")
                except Exception:
                    evidence.append(f"{candidate.relative_to(home)}=<unreadable>")
        try:
            with sqlite3.connect(home / "cron" / "executions.db") as db:
                rows = db.execute("SELECT * FROM executions").fetchall()
        except Exception as exc:
            rows = f"ledger_error={exc!r}"
        pytest.fail(
            "R2 external-worker chain did not invoke the candidate HK Receipt consumer; "
            f"stdout={result.stdout!r}; stderr={result.stderr!r}; "
            f"files={[p.name for p in home.iterdir()]}; "
            f"cron_files={[p.name for p in (home / 'cron').iterdir()]}; "
            f"evidence={evidence!r}; ledger={rows!r}"
        )
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["trusted"] is False
    assert receipt["execution_origin"] == "NATURAL_CRON"
    assert receipt["origin_proof_sha256"]
    assert receipt["origin_proof"]["child_pid"] > 0
    from cron.r3_provenance_contract_r2 import verify_r3
    result = verify_r3(
        receipt_path,
        ledger_path=home / "cron" / "executions.db",
        release_manifest=release_manifest,
        expected_job_id=job["id"],
        expected_profile=profile,
    )
    assert result["trusted"] is True, result
    assert result["status"] == "PROVENANCE_VERIFIED"
    assert all(result["checks"].values())
    proof = receipt["origin_proof"]
    assert "nonce" not in proof and proof["child_pid"] != proof["owner_pid"]
    assert proof["scheduled_at"].endswith("+00:00")
    assert proof["entry_sha256"] == hashlib.sha256(script.read_bytes()).hexdigest()
    with sqlite3.connect(home / "cron" / "executions.db") as db:
        db.row_factory = sqlite3.Row
        row = dict(db.execute("SELECT * FROM executions").fetchone())
    assert row["status"] == "completed" and row["source"] == "builtin"
    assert row["pid"] == proof["owner_pid"] and row["origin_proof_used"] == 1
    assert row["origin_proof_sha256"] == receipt["origin_proof_sha256"]
    denied = verify_r3(receipt_path, ledger_path=home / "cron" / "executions.db",
        release_manifest=release_manifest, expected_job_id=job["id"], expected_profile="wrong-profile")
    assert not denied["trusted"]
    duplicate = dict(receipt, receipt_id="different-receipt")
    duplicate_path = home / "duplicate.json"
    duplicate_path.write_text(json.dumps(duplicate))
    denied = verify_r3(receipt_path, ledger_path=home / "cron" / "executions.db",
        release_manifest=release_manifest, expected_job_id=job["id"], expected_profile=profile,
        related_receipt_paths=(duplicate_path,))
    assert not denied["trusted"] and "duplicate_execution_receipt" in denied["reasons"]
