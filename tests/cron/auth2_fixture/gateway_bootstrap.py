"""Disposable actual Cron tick for AUTH2; never point HERMES_HOME at Production."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    if sys.argv[1:3] != ["gateway", "run"] or len(sys.argv) not in {5, 6}:
        return 2
    home = Path(sys.argv[3]).resolve()
    consumer_source = Path(sys.argv[4]).resolve()
    component = sys.argv[5] if len(sys.argv) == 6 else "us-market-data"
    if component not in {"us-market-data", "finance-briefing"}:
        return 2
    job_id = "f0def1ac0f4f" if component == "us-market-data" else "9d69476aec2b"
    assert home != Path.home() / ".hermes"
    os.environ["HERMES_HOME"] = str(home)
    (home / "scripts").mkdir(parents=True)
    (home / "cron").mkdir()

    producer_root = Path(__file__).resolve().parents[3]
    base = Path(os.environ["AUTH2_MARKET_BASE" if component == "us-market-data" else "AUTH2_FINANCE_BASE"]).resolve()
    release = home / "releases" / component / "isolated-consumer"
    shutil.copytree(base, release, symlinks=True)
    for filename in ("run_surface.py", "origin_auth.py"):
        target = release / "infra/hermes/v20" / filename
        target.parent.chmod(0o700)
        if target.exists():
            target.chmod(0o600)
        shutil.copy2(consumer_source / "infra/hermes/v20" / filename, target)

    core = home / "runtime/scheduler-core/versions/isolated-auth2"
    module = core / "cron/scheduler_script.py"
    module.parent.mkdir(parents=True)
    shutil.copy2(producer_root / "cron/scheduler_script.py", module)
    manifest = core / "scheduler-core-manifest.json"
    manifest.write_text(json.dumps({
        "version_id": core.name,
        "scheduler_module_path": "cron/scheduler_script.py",
        "scheduler_module_sha256": _sha(module),
    }), encoding="utf-8")
    (core.parent.parent / "current").symlink_to(core)
    receipt_dir = home / "runtime/scheduler-core/receipts"
    receipt_dir.mkdir(parents=True)
    receipt = {
        "pid": os.getpid(), "version_id": core.name,
        "module_path": str(module), "module_sha256": _sha(module),
        "manifest_sha256": _sha(manifest),
    }
    (receipt_dir / f"gateway-import-{os.getpid()}-1.json").write_text(json.dumps(receipt), encoding="utf-8")
    started = subprocess.run(["/bin/ps", "-p", str(os.getpid()), "-o", "lstart="], capture_output=True, text=True, check=True).stdout.strip()
    started_epoch = time.mktime(time.strptime(started, "%a %b %d %H:%M:%S %Y"))
    (home / "gateway_state.json").write_text(json.dumps({"pid": os.getpid(), "start_time": int(started_epoch * 100)}), encoding="utf-8")

    script = home / "scripts/inert_origin_probe.sh"
    script.write_text(
        "#!/bin/bash\n"
        f"export PYTHONPATH='{release}'\n"
        f"exec '{sys.executable}' -c 'from infra.hermes.v20.run_surface import scheduler_invocation; "
        f"import os; v=scheduler_invocation(os.environ,component=\"{component}\"); "
        "print(\"ORIGIN_ACCEPTED\",v.run_id,v.job_id)'\n",
        encoding="utf-8",
    )
    script.chmod(0o700)
    import cron.jobs as jobs
    import cron.scheduler as scheduler

    job = jobs.create_job(
        prompt="inert origin fixture", schedule="* * * * *", name="auth2-isolated",
        script=script.name, no_agent=True, deliver="local",
    )
    # Jobs have immutable IDs. Build the isolated store with the production
    # component's exact job ID, then let get_due_jobs/tick select it normally.
    rows = jobs.load_jobs()
    for row in rows:
        if row["id"] == job["id"]:
            row["id"] = job_id
            row["next_run_at"] = datetime.now().astimezone().replace(second=0, microsecond=0).isoformat()
    jobs.save_jobs(rows, replace=True)
    count = scheduler.tick(verbose=False, sync=True)
    from cron.executions import latest_execution
    latest = latest_execution(job_id)
    final_job = next(item for item in jobs.load_jobs() if item["id"] == job_id)
    result = {
        "tick_count": count,
        "execution": {k: latest.get(k) for k in ("id", "job_id", "source", "status", "scheduled_instant")} if latest else None,
        "job_last_status": final_job.get("last_status"),
        "job_last_error": final_job.get("last_error"),
    }
    print(json.dumps(result, sort_keys=True))
    return 0 if count == 1 and latest and latest["status"] == "completed" and final_job.get("last_status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
