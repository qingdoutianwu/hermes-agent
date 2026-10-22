"""Cross-process serialization for a paused recurring rearm versus scheduler due admission."""
from __future__ import annotations

import contextlib
import os
from pathlib import Path
import threading

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows uses msvcrt
    fcntl = None

_local_locks: dict[str, threading.RLock] = {}
_local_locks_guard = threading.Lock()
_state = threading.local()


@contextlib.contextmanager
def store_and_dispatch_fence(cron_dir: Path):
    """Take the scheduler tick file lock and serialize same-process entrants."""
    lock_file = cron_dir / ".tick.lock"
    cron_dir.mkdir(parents=True, exist_ok=True)
    key = str(lock_file.resolve())
    with _local_locks_guard:
        local_lock = _local_locks.setdefault(key, threading.RLock())
    with local_lock:
        held = getattr(_state, "held", {})
        if key in held:
            held[key]["depth"] += 1
            try:
                yield
            finally:
                held[key]["depth"] -= 1
            return
        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(lock_file, flags, 0o600)
        lock_fd = os.fdopen(descriptor, "a+")
        try:
            if fcntl is not None:
                from cron.jobs import _acquire_flock
                acquired = _acquire_flock(lock_fd, 30.0)
                if not acquired:
                    raise RuntimeError("timed out waiting for scheduler dispatch lock")
            else:  # pragma: no cover
                import msvcrt
                lock_fd.seek(0)
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_LOCK, 1)
        except BaseException:
            lock_fd.close()
            raise
        held = dict(held)
        held[key] = {"fd": lock_fd, "depth": 1}
        _state.held = held
        try:
            yield
        finally:
            held.pop(key, None)
            if fcntl is not None:
                from cron.jobs import _release_flock
                _release_flock(lock_fd)
            else:  # pragma: no cover
                import msvcrt
                lock_fd.seek(0)
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
                lock_fd.close()


def can_reenter_dispatch_scan(cron_dir: Path) -> bool:
    key = str((cron_dir / ".tick.lock").resolve())
    return key in getattr(_state, "held", {})


def dispatch_scan_fence_active(cron_dir: Path) -> bool:
    return can_reenter_dispatch_scan(cron_dir)
