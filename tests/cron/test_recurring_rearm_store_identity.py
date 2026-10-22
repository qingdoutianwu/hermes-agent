"""Store-identity regression: no real homes or production locks."""
import pytest
from cron import jobs


def test_native_lock_timeout_across_processes_then_reacquire(tmp_path, monkeypatch):
    import os
    from pathlib import Path
    import select
    import subprocess
    import sys

    home = tmp_path / "locked"
    script = """
from cron import jobs
with jobs._jobs_lock(strict=True):
    print('LOCKED', flush=True)
    input()
"""
    child = subprocess.Popen([sys.executable, "-c", script],
                             cwd=Path(__file__).resolve().parents[2],
                             env={**os.environ, "HERMES_HOME": str(home)},
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        ready, _, _ = select.select([child.stdout], [], [], 20)
        assert ready and child.stdout.readline().strip() == "LOCKED"
        monkeypatch.setattr(jobs, "_JOBS_LOCK_TIMEOUT_SECONDS", 0.1)
        with jobs.use_cron_store(home):
            with pytest.raises(RuntimeError, match="strict Job Store lock"):
                with jobs._jobs_lock(strict=True):
                    pytest.fail("cross-process lock contention admitted")
        child.stdin.write("release\n")
        child.stdin.flush()
        assert child.wait(timeout=20) == 0, child.stderr.read()
        with jobs.use_cron_store(home):
            with jobs._jobs_lock(strict=True):
                assert jobs._jobs_lock_state.cross_process
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=20)


def test_a_store_lock_cannot_certify_b_store(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    with jobs.use_cron_store(a):
        with jobs._jobs_lock(strict=True):
            with jobs.use_cron_store(b):
                with pytest.raises(RuntimeError, match="Store identity"):
                    with jobs._jobs_lock(strict=True):
                        pytest.fail("A-store ownership certified B-store")
            with jobs._jobs_lock(strict=True):
                assert jobs._jobs_lock_state.cross_process is True
    with jobs.use_cron_store(b):
        with jobs._jobs_lock(strict=True):
            assert jobs._jobs_lock_state.cross_process is True
