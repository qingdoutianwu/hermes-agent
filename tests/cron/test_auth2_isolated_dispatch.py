"""Full isolated Cron tick -> ledger -> Popen -> RunSurface for both consumers."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
HARNESS = ROOT / "tests/cron/auth2_fixture/gateway_bootstrap.py"


@pytest.mark.parametrize(
    ("component", "expected_job"),
    [("us-market-data", "f0def1ac0f4f"), ("finance-briefing", "9d69476aec2b")],
)
def test_real_isolated_dispatch_for_both_consumers(tmp_path: Path, component: str, expected_job: str):
    consumer_source = os.environ.get("AUTH2_CONSUMER_SOURCE")
    base_name = "AUTH2_MARKET_BASE" if component == "us-market-data" else "AUTH2_FINANCE_BASE"
    base = os.environ.get(base_name)
    if not consumer_source or not base:
        pytest.skip("Set AUTH2_CONSUMER_SOURCE and both sealed base paths for cross-repository integration")
    env = dict(os.environ, PYTHONPATH=str(ROOT), **{base_name: base})
    result = subprocess.run(
        [sys.executable, str(HARNESS), "gateway", "run", str(tmp_path / "isolated-home"), consumer_source, component],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload["tick_count"] == 1
    assert payload["execution"]["job_id"] == expected_job
    assert payload["execution"]["source"] == "builtin"
    assert payload["execution"]["status"] == "completed"
    assert payload["job_last_status"] == "ok"
