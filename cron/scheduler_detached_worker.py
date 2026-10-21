"""Cron: worker readiness publication and detached-worker teardown.

``ThreadPoolExecutor.shutdown(wait=False)`` after an inactivity timeout does not stop a
worker already inside ``run_conversation``. Finalizing its SessionDB from ``run_job``'s
``finally`` would close a handle the worker is still writing to — the checkpoint/WAL-unlink
overlap behind #102827. The worker's Future owns the teardown instead.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import subprocess
import threading
from pathlib import Path
from typing import Optional


def prepare_external_worker_ack(job: dict, execution_id: str, ack_path: Path) -> bool:
    """Initialize optional Authority and atomically publish an adopted worker's ACK.

    The scheduler still owns adoption, profile scope, dispatch and final teardown.
    Keep initialization before ACK and never acknowledge a refused Authority.
    Facade names are late-bound to preserve its native error-reporting seams.
    """
    from cron import scheduler
    from cron.scheduler_origin_auth import ScriptOriginAuthority
    logger = scheduler.logger

    if str(job.get("_r2_authority_required") or "").lower() == "true":
        try:
            if not job.get("no_agent"):
                raise ValueError("ORIGIN_AUTHORITY_REQUIRES_NO_AGENT")
            job["_r2_authority"] = ScriptOriginAuthority(job)
        except (OSError, ValueError, RuntimeError) as exc:
            scheduler.finish_execution(execution_id, success=False, error=f"Origin authority refused: {exc}")
            logger.exception("Cron authority initialization refused %s", execution_id)
            return False
    try:
        ack_path.parent.mkdir(parents=True, exist_ok=True)
        # An exclusive temporary file plus same-directory rename prevents a
        # polling parent from observing a zero-byte/partially-written ACK.
        ack_tmp_path = ack_path.with_name(f"{ack_path.name}.tmp{scheduler.os.getpid()}")
        fd = scheduler.os.open(ack_tmp_path, scheduler.os.O_WRONLY | scheduler.os.O_CREAT | scheduler.os.O_EXCL, 0o600)
        try:
            with scheduler.os.fdopen(fd, "w", encoding="utf-8") as ack_file:
                scheduler.json.dump({"pid": scheduler.os.getpid(), "execution_id": execution_id}, ack_file)
                ack_file.flush()
                scheduler.os.fsync(ack_file.fileno())
            scheduler.os.replace(ack_tmp_path, ack_path)
        except BaseException:
            ack_tmp_path.unlink(missing_ok=True)
            raise
    except Exception:
        logger.exception(
            "Cron external worker could not publish ready acknowledgement for %s", execution_id)
        return False
    return True


def _close_late_session_db_result(future: concurrent.futures.Future) -> None:
    """Done-callback: close a SessionDB whose constructor finished after run_job's init timeout
    (worker abandoned via ``shutdown(wait=False)``), else its .db/WAL/SHM handles leak to EMFILE.

    If the constructor later completes inside that abandoned worker, the Future's result — an open
    SessionDB holding .db / WAL / SHM file handles — would be orphaned and never closed, leaking
    descriptors until EMFILE (#72782). This callback retrieves and closes that eventual late result.
    """
    with contextlib.suppress(Exception):
        db = future.result()
        if db is not None:
            from hermes_state_registry import release_or_close
            release_or_close(db)


def defer_teardown_to_running_worker(
    future: Optional[concurrent.futures.Future], session_db, agent, job_id: str, job_name: str,
    cron_session_id: str, workdir: Optional[str] = None,
) -> bool:
    """Return True when the worker is still running and its Future will finalize the session
    and tear the agent down on completion; False when the caller must do it now."""
    if future is None or future.done():
        return False
    from cron.scheduler import _finalize_cron_session, _teardown_cron_agent

    def _finish(_future) -> None:
        try:
            if session_db:
                _finalize_cron_session(session_db, agent, job_id, job_name, cron_session_id,
                                       workdir=workdir)
        finally:
            _teardown_cron_agent(agent, job_id)

    # Runs inline if the worker finished between done() and here — still exactly once.
    future.add_done_callback(_finish)
    return True


def reap_terminal_worker_in_background(process: subprocess.Popen) -> None:
    """Keep the reap contract when the waiter returns before the worker exits.

    The ledger turning terminal lets ``_wait_for_external_cron_worker_body``
    return while the worker is still in final teardown. The gateway remains the
    worker's parent, so if nobody calls ``wait()`` afterwards the worker lingers
    as a zombie (STAT=Z) under the gateway until it is restarted (#114509). A
    short-lived daemon thread holds that single responsibility and ends with
    the process exit it waits for.
    """
    threading.Thread(
        target=process.wait,
        name=f"cron-worker-reap-{getattr(process, 'pid', '?')}",
        daemon=True,
    ).start()
