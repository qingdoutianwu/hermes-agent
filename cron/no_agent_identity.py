"""Ephemeral POSIX child identity from an owned native execution, not ambient env.

The inherited pipe binds one child PID to the existing profile ledger. It is not a
new ledger, signer, service or security boundary against a compromised same-user
Scheduler/ledger. Bare HERMES_CRON_* strings never establish native provenance.
"""

from __future__ import annotations

import json
import logging
import os
import select
import sqlite3
import stat
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)
FD_ENV = "HERMES_CRON_IDENTITY_FD"  # Per-spawn inherited capability, not configuration.
IDENTITY_ENV = (
    "HERMES_CRON_JOB_ID",
    "HERMES_CRON_EXECUTION_ID",
    "HERMES_CRON_SCHEDULED_AT",
    "HERMES_CRON_DISPATCHED_AT",
    "HERMES_CRON_LAUNCHER_PATH",
    "HERMES_CRON_TRIGGER_TYPE",
    "HERMES_CRON_EXECUTION_OWNER",
    "HERMES_CRON_SCHEDULED_BY",
    "HERMES_CRON_OUTPUT_PATH",
    "HERMES_CRON_OUTPUT_PATH_STATUS",
    FD_ENV,
)
_TRIGGERS = {
    "on_time": "HERMES_SCHEDULER_CRON",
    "late": "HERMES_SCHEDULER_LATE",
    "catch_up": "HERMES_SCHEDULER_CATCH_UP",
    "direct": "HERMES_DIRECT",
}
_MAX_BYTES = 8192


def _same_instant(a, b):
    try:
        first, second = datetime.fromisoformat(str(a)), datetime.fromisoformat(str(b))
        return (
            first.utcoffset() is not None
            and second.utcoffset() is not None
            and first == second
        )
    except (ValueError, TypeError):
        return False


def _read_row(home, execution_id):
    database = Path(home) / "cron" / "executions.db"
    if database.is_symlink() or not database.is_file():
        return None
    with sqlite3.connect(
        database.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.5
    ) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone()
        return dict(row) if row else None


def _parent_identity(job, launcher):
    from cron import executions
    from cron.execution_identity import current_cron_execution
    from hermes_constants import get_hermes_home

    identity = current_cron_execution()
    if (
        identity is None
        or not identity.execution_id
        or identity.job_id != job.get("id")
        or identity.execution_id != job.get("execution_id")
    ):
        return None
    home = str(get_hermes_home().resolve())
    row = _read_row(home, identity.execution_id)
    expected = {
        "job_id": identity.job_id,
        "id": identity.execution_id,
        "source": identity.source,
        "status": "running",
        "pid": os.getpid(),
        "process_id": executions._PROCESS_ID,
        "started_at": identity.started_at,
        "scheduled_instant": identity.scheduled_instant,
    }
    if (
        not row
        or any(row.get(k) != v for k, v in expected.items())
        or row.get("finished_at") is not None
        or not row.get("process_started_at")
    ):
        return None
    if identity.source == "direct":
        origin, dispatch = "direct", None
    elif identity.source == "builtin":
        stamp = job.get("last_dispatch") or {}
        if stamp.get("kind") not in {
            "on_time",
            "late",
            "catch_up",
        } or not _same_instant(stamp.get("scheduled_at"), identity.scheduled_instant):
            return None
        dispatch = stamp.get("dispatched_at")
        if not _same_instant(dispatch, dispatch):
            return None
        origin = stamp["kind"]
    else:
        # External providers do not prove builtin/direct semantics by their name alone.
        return None
    return {
        "job_id": identity.job_id,
        "execution_id": identity.execution_id,
        "source": identity.source,
        "origin": origin,
        "scheduled_at": identity.scheduled_instant,
        "dispatched_at": dispatch,
        "started_at": identity.started_at,
        "profile": identity.profile,
        "home": home,
        "launcher": str(launcher.resolve()),
        "owner_pid": row["pid"],
        "owner_process_id": row["process_id"],
        "owner_started_at": row["process_started_at"],
    }


def _environment_values(body):
    return {
        "HERMES_CRON_JOB_ID": body["job_id"],
        "HERMES_CRON_EXECUTION_ID": body["execution_id"],
        "HERMES_CRON_SCHEDULED_AT": body["scheduled_at"] or "",
        "HERMES_CRON_DISPATCHED_AT": body["dispatched_at"] or "",
        "HERMES_CRON_LAUNCHER_PATH": body["launcher"],
        "HERMES_CRON_TRIGGER_TYPE": _TRIGGERS[body["origin"]],
        "HERMES_CRON_EXECUTION_OWNER": "Hermes Scheduler",
        "HERMES_CRON_SCHEDULED_BY": "hermes cron",
        "HERMES_CRON_OUTPUT_PATH_STATUS": "UNAVAILABLE_AT_RUNTIME",
    }


class _ChildIdentity:
    def __init__(self, body):
        self.body = body
        self.reader, self.writer = os.pipe()

    def deliver(self, process):
        os.close(self.reader)
        self.reader = -1
        data = json.dumps(
            dict(self.body, child_pid=process.pid), separators=(",", ":")
        ).encode()
        try:
            if len(data) <= _MAX_BYTES:
                os.write(self.writer, data)
        except OSError:
            # An unrelated script may exit without consuming the additive capability.
            logger.debug("no-agent child exited without consuming native identity")
        finally:
            os.close(self.writer)
            self.writer = -1
        logger.info(
            "no-agent identity job=%s execution=%s origin=%s slot=%s child_pid=%s",
            self.body["job_id"],
            self.body["execution_id"],
            self.body["origin"],
            self.body["scheduled_at"],
            process.pid,
        )

    def close(self):
        for descriptor in (self.reader, self.writer):
            if descriptor >= 0:
                os.close(descriptor)
        self.reader = self.writer = -1


@contextmanager
def child_identity(job, launcher, env, popen_kwargs):
    """Scrub ambient identity, then attach an owned context for the actual spawn."""
    for key in IDENTITY_ENV:
        env.pop(key, None)
    issue = None
    try:
        if os.name == "posix" and job and job.get("no_agent"):
            try:
                body = _parent_identity(job, launcher)
            except (OSError, sqlite3.Error, ValueError, TypeError):
                body = None
                logger.debug("native no-agent identity unavailable", exc_info=True)
            if body is not None:
                issue = _ChildIdentity(body)
                env.update(_environment_values(body))
                env[FD_ENV] = str(issue.reader)
                popen_kwargs["pass_fds"] = (
                    *popen_kwargs.get("pass_fds", ()),
                    issue.reader,
                )
        yield issue
    finally:
        if issue is not None:
            issue.close()


def read_child_identity(*, expected_job_id, expected_launcher_path, environ=None):
    """Consume one inherited frame and verify native row, live parent and target."""
    from gateway.status import get_process_start_time, start_time_fingerprints_match

    env = os.environ if environ is None else environ
    descriptor = None
    consumed_pipe = False
    try:
        descriptor = int(env.get(FD_ENV, "-1"))
        if env is os.environ:
            os.environ.pop(FD_ENV, None)
        if descriptor < 3 or not stat.S_ISFIFO(os.fstat(descriptor).st_mode):
            return None
        consumed_pipe = True
        if not select.select([descriptor], [], [], 2.0)[0]:
            return None
        data = os.read(descriptor, _MAX_BYTES + 1)
        if not data or len(data) > _MAX_BYTES:
            return None
        body = json.loads(data)
        if not isinstance(body, dict):
            return None
        if (
            body.get("child_pid") != os.getpid()
            or body.get("owner_pid") != os.getppid()
        ):
            return None
        if body.get("job_id") != expected_job_id or body.get("launcher") != str(
            Path(expected_launcher_path).resolve()
        ):
            return None
        if str(Path(env.get("HERMES_HOME", "")).resolve()) != body.get("home"):
            return None
        row = _read_row(body["home"], body["execution_id"])
        expected = {
            "id": body["execution_id"],
            "job_id": body["job_id"],
            "source": body["source"],
            "status": "running",
            "pid": body["owner_pid"],
            "process_id": body["owner_process_id"],
            "process_started_at": body["owner_started_at"],
            "started_at": body["started_at"],
            "scheduled_instant": body["scheduled_at"],
            "finished_at": None,
        }
        if not row or any(row.get(k) != v for k, v in expected.items()):
            return None
        live_start = get_process_start_time(os.getppid())
        if live_start is None or not start_time_fingerprints_match(
            live_start, body["owner_started_at"]
        ):
            return None
        if (
            body["source"] not in {"builtin", "direct"}
            or body["origin"] not in _TRIGGERS
        ):
            return None
        if (body["source"] == "direct") != (body["origin"] == "direct"):
            return None
        if body["source"] == "builtin" and not _same_instant(
            body["scheduled_at"], body["scheduled_at"]
        ):
            return None
        if any(env.get(k) != v for k, v in _environment_values(body).items()):
            return None
        return body
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
        return None
    finally:
        if consumed_pipe:
            try:
                os.close(descriptor)
            except OSError:
                pass  # Invalid ambient descriptors are untrusted input.
