"""One-use, per-dispatch origin proof for a natural no-agent Cron child.

The socket is not a bearer token: the consumer must also verify the kernel's
peer PID against the sealed gateway before it considers a response authentic.
"""

from __future__ import annotations

import json
import os
import select
import socket
from datetime import datetime
from typing import Any


ORIGIN = "HERMES_SCHEDULER_CRON"
FD_ENV = "HERMES_CRON_AUTH_FD"
MAX_REQUEST_BYTES = 2048


def scrub_identity(env: dict[str, str]) -> None:
    """An inherited or job-provided value is never Scheduler identity."""

    for name in tuple(env):
        if name.startswith("HERMES_CRON_") or name in {
            "FINANCE_TRIGGER_TYPE", "FINANCE_SCHEDULED_AT", "FINANCE_DISPATCHED_AT",
        }:
            env.pop(name, None)


class ScriptOriginAuthority:
    """Gateway-held endpoint bound to exactly one existing Cron execution row."""

    def __init__(self, job: dict[str, Any]) -> None:
        self.execution_id = str(job.get("execution_id") or "")
        self.job_id = str(job.get("id") or "")
        self.scheduled_at = str(job.get("_scheduled_instant") or "")
        if not (self.execution_id and self.job_id and self.scheduled_at):
            raise ValueError("NATURAL_EXECUTION_IDENTITY_REQUIRED")
        self.parent, self.child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        self.parent.setblocking(False)
        self._buffer = bytearray()
        self._consumed = False

    @property
    def child_fd(self) -> int:
        return self.child.fileno()

    def close_child_in_parent(self) -> None:
        self.child.close()

    def close(self) -> None:
        self.parent.close()
        self.child.close()

    def poll(self, spawned_pid: int) -> None:
        if self._consumed or not select.select([self.parent], [], [], 0)[0]:
            return
        try:
            chunk = self.parent.recv(MAX_REQUEST_BYTES + 1 - len(self._buffer))
        except (BlockingIOError, OSError):
            return
        if not chunk:
            self.close()
            self._consumed = True
            return
        self._buffer.extend(chunk)
        if len(self._buffer) > MAX_REQUEST_BYTES:
            self._reject()
            return
        if b"\n" not in self._buffer:
            return
        try:
            raw, trailing = bytes(self._buffer).split(b"\n", 1)
            if trailing:
                raise ValueError("TRAILING_REQUEST_DATA")
            request = json.loads(raw)
            if not isinstance(request, dict):
                raise ValueError("REQUEST_NOT_OBJECT")
            from cron.executions import get_execution
            from cron.occurrences import scheduled_instant

            row = get_execution(self.execution_id)
            valid = (
                row is not None
                and row.get("id") == self.execution_id
                and row.get("job_id") == self.job_id
                and row.get("source") == "builtin"
                and row.get("status") == "running"
                and row.get("pid") == os.getpid()
                and row.get("scheduled_instant") == scheduled_instant(self.scheduled_at)
                and request.get("execution_id") == self.execution_id
                and request.get("job_id") == self.job_id
                and request.get("scheduled_at") == self.scheduled_at
                and request.get("origin") == ORIGIN
                and request.get("pid") == spawned_pid
                and isinstance(request.get("nonce"), str)
                and len(request["nonce"]) == 32
            )
            if not valid:
                self._reject()
                return
            response = {
                "status": "ACCEPTED",
                "nonce": request["nonce"],
                "execution_id": self.execution_id,
                "job_id": self.job_id,
                "scheduled_at": self.scheduled_at,
                "origin": ORIGIN,
                "gateway_pid": os.getpid(),
                "child_pid": spawned_pid,
            }
            self.parent.sendall((json.dumps(response, sort_keys=True) + "\n").encode())
        except (ValueError, UnicodeError, OSError, json.JSONDecodeError):
            self._reject()
            return
        finally:
            self._consumed = True
            self.parent.close()

    def _reject(self) -> None:
        try:
            self.parent.sendall(b'{"status":"REJECTED"}\n')
        except OSError:
            pass
        self._consumed = True
        self.parent.close()


def authoritative_environment(env: dict[str, str], authority: ScriptOriginAuthority) -> None:
    scrub_identity(env)
    env.update({
        "HERMES_CRON_TRIGGER_TYPE": ORIGIN,
        "HERMES_CRON_EXECUTION_ID": authority.execution_id,
        "HERMES_CRON_JOB_ID": authority.job_id,
        "HERMES_CRON_SCHEDULED_AT": authority.scheduled_at,
        "HERMES_CRON_DISPATCHED_AT": datetime.now().astimezone().isoformat(),
        FD_ENV: str(authority.child_fd),
    })
