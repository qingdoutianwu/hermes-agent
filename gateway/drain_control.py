"""External drain-control marker contract (dashboard → gateway).

No control channel exists into a running gateway, so begin/cancel-drain writes
(or removes) ``{HERMES_HOME}/.drain_request.json`` and a gateway watcher reacts;
an ACTIVE marker means ``gateway_state -> "draining"``.  Two lenient staleness
signals (either suffices): epoch mismatch (HERMES_HOME is a durable volume on
Hermes Cloud, so a marker survives the restart a drain-gated action ends in and
would park the fresh gateway in ``draining`` forever) and expiry (same-epoch
orphan past :data:`DRAIN_REQUEST_MAX_AGE_SECONDS`; re-writing refreshes it).
Reading never raises: a malformed file reads as ``{}`` — still drain-active
(fail-safe toward quiescing).  Staleness rejects only on a *definite* verdict.
"""
from __future__ import annotations

import contextlib
import functools
import hashlib
import hmac
import json
import logging
import math
import os
import re
import secrets
import stat
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from gateway.memory_status import _parse_iso
from hermes_constants import get_hermes_home
from utils import atomic_json_write

_log = logging.getLogger(__name__)

_DRAIN_REQUEST_FILENAME = ".drain_request.json"
# Drain-gated lifecycle actions complete in minutes; an hour bounds the wedge a leaked
# marker can cause. Long drains refresh the marker instead of raising this.
# Max-age fallback for a same-epoch orphaned marker (#85433). Long-running drains refresh the marker via
# write_drain_request() (idempotent re-write bumps ``requested_at``) rather than raising this bound.
DRAIN_REQUEST_MAX_AGE_SECONDS = 3600.0
# Scheduler maintenance is a bounded lease. 300s covers the activation's 45s service
# waits, fresh-ticker readback and the non-business identity fixture, while still
# automatically reopening dispatch if the controller disappears.
SCHEDULER_MAINTENANCE_FENCE_SCHEMA = "hermes-scheduler-maintenance-fence-v1"
SCHEDULER_MAINTENANCE_PROOF_SCHEMA = "hermes-scheduler-maintenance-proof-v1"
SCHEDULER_MAINTENANCE_ACK_SCHEMA = "hermes-scheduler-maintenance-ack-v1"
SCHEDULER_MAINTENANCE_RECEIPT_SCHEMA = "hermes-scheduler-maintenance-receipt-v1"
SCHEDULER_MAINTENANCE_TTL_SECONDS = 300.0
SCHEDULER_MAINTENANCE_MIN_TTL_SECONDS = 30.0
_DRAIN_MARKER_LOCK_TIMEOUT_SECONDS = 2.0
_SCHEDULER_TICK_BARRIER_TIMEOUT_SECONDS = 10.0
_DRAIN_MARKER_THREAD_LOCK = threading.RLock()
# Dedup for the expired-marker warning (the watcher re-reads every second); keyed by
# ``requested_at`` so a keep-alive re-write that later expires logs again.
_expiry_logged_for: Optional[str] = None


class DrainControlError(RuntimeError):
    """A drain marker could not be safely read or changed."""


class DrainControlBusyError(DrainControlError):
    """Another active drain or a concurrent marker writer owns the control file."""


class MaintenanceFenceOwnershipError(DrainControlError):
    """A maintenance-fence release/update did not present its exact owner token."""


class MaintenanceFenceExpiredError(DrainControlError):
    """The bounded maintenance lease is no longer active."""


def _locked_drain_request(home: Optional[Path] = None):
    """Serialize all writers of the shared external-drain marker across threads/processes."""
    @contextlib.contextmanager
    def _hold():
        marker = drain_request_path(home)
        marker.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock_path = marker.with_name(f"{marker.name}.lock")
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        with _DRAIN_MARKER_THREAD_LOCK:
            try:
                fd = os.open(lock_path, flags, 0o600)
            except OSError as exc:
                raise DrainControlError("E_DRAIN_MARKER_LOCK_OPEN") from exc
            handle = None
            acquired = False
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise DrainControlError("E_DRAIN_MARKER_LOCK_NOT_REGULAR")
                with contextlib.suppress(OSError):
                    os.fchmod(fd, 0o600)
                handle = os.fdopen(fd, "r+")
                from gateway.status import _release_file_lock, _try_acquire_file_lock

                deadline = time.monotonic() + _DRAIN_MARKER_LOCK_TIMEOUT_SECONDS
                while not _try_acquire_file_lock(handle):
                    if time.monotonic() >= deadline:
                        raise DrainControlBusyError("E_DRAIN_MARKER_LOCK_TIMEOUT")
                    time.sleep(0.01)
                acquired = True
                yield marker
            finally:
                if handle is not None:
                    if acquired:
                        _release_file_lock(handle)
                    handle.close()
                else:
                    os.close(fd)

    return _hold()


def _maintenance_identity(fence: dict[str, Any]) -> dict[str, Any]:
    """Fields sealed by the nonce digest; mutable lifecycle state is intentionally excluded."""
    return {
        key: fence.get(key)
        for key in (
            "schema_version", "maintenance_id", "owner", "purpose", "created_at",
            "expires_at", "ttl_seconds", "scheduler_owner_identity", "scheduler_tick_homes", "nonce",
        )
    }


def _maintenance_proof_sha256(fence: dict[str, Any]) -> str:
    payload = json.dumps(
        _maintenance_identity(fence), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _current_scheduler_tick_homes() -> list[Path]:
    """Resolve the same live profile roster used by Gateway's in-process Cron ticker."""
    try:
        from gateway.run import _cron_tick_profile_homes

        entries = _cron_tick_profile_homes(None)
        homes = {Path(entry[1] if isinstance(entry, tuple) else entry).expanduser().resolve(strict=True)
                 for entry in entries}
    except Exception as exc:
        raise DrainControlError("E_MAINTENANCE_TICK_HOME_ROSTER") from exc
    if not homes:
        raise DrainControlError("E_MAINTENANCE_TICK_HOME_ROSTER_EMPTY")
    return sorted(homes, key=str)


@contextlib.contextmanager
def _scheduler_tick_lock_barrier(homes: list[Path], *, timeout_seconds: float):
    """Hold every served profile's actual tick lock through fence publication."""
    from cron import scheduler as cron_scheduler

    deadline = time.monotonic() + timeout_seconds
    acquired = []
    try:
        for profile_home in homes:
            cron_dir = profile_home / "cron"
            cron_scheduler._ensure_cron_dir(cron_dir)
            lock_path = cron_dir / ".tick.lock"
            while True:
                fd = cron_scheduler._acquire_tick_lock(lock_path)
                if fd is not None:
                    acquired.append(fd)
                    break
                if time.monotonic() >= deadline:
                    raise DrainControlBusyError("E_MAINTENANCE_TICK_BARRIER_TIMEOUT")
                time.sleep(0.05)
        yield
    finally:
        for fd in reversed(acquired):
            cron_scheduler._release_tick_lock(fd)


def scheduler_maintenance_fence_dir(home: Optional[Path] = None) -> Path:
    root = Path(home if home is not None else get_hermes_home()).expanduser()
    return root / "runtime" / "scheduler-core" / "activation" / "maintenance-fences"


def scheduler_maintenance_proof_path(home: Optional[Path], maintenance_id: str) -> Path:
    return scheduler_maintenance_fence_dir(home) / f"{maintenance_id}.proof.json"


def scheduler_maintenance_ack_path(home: Optional[Path], maintenance_id: str) -> Path:
    return scheduler_maintenance_fence_dir(home) / f"{maintenance_id}.ack.json"


@functools.lru_cache(maxsize=1)
def current_instantiation_epoch() -> str:
    """Identity of THIS container / VM instantiation ("<boot_id>:<pid1_start>").

    Stable for the life of PID 1 (a gateway-only respawn keeps honouring an
    in-flight drain) but changes when the machine is recreated: boot_id on a VM
    reboot, PID 1's start time on ``docker restart``.  ``""`` when neither is
    readable (non-Linux, no ``/proc``) disables the epoch check — never fail-closed.
    """
    boot_id = pid1_start = ""
    with contextlib.suppress(OSError):
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    with contextlib.suppress(OSError, IndexError):
        # "<pid> (<comm>) <state> ...": comm may contain spaces/parens, so split on the
        # LAST ')'. starttime is field 22 (1-indexed) = tail index 19.
        pid1_start = Path("/proc/1/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()[19]
    return f"{boot_id}:{pid1_start}" if (boot_id or pid1_start) else ""


def drain_request_path(home: Optional[Path] = None) -> Path:
    """Absolute path to the drain-request marker, respecting HERMES_HOME."""
    return Path(home if home is not None else get_hermes_home()) / _DRAIN_REQUEST_FILENAME


def write_drain_request(
    *, principal: str = "drain-control", suppress_notification: bool = False, home: Optional[Path] = None
) -> dict[str, Any]:
    """Write the begin-drain marker atomically; returns the payload.

    Re-writing refreshes ``requested_at`` (keep-alive past the max-age).
    ``suppress_notification`` skips ONLY the home-channel "gateway shutting down"
    broadcast (the per-session interrupt ping is never suppressed); which drains
    are quiet is the caller's policy.  Stamped with the instantiation epoch so a
    copy surviving a machine restart on the durable volume reads as stale.
    """
    with _locked_drain_request(home) as path:
        raw_existing = read_drain_request(home=home)
        if raw_existing == {}:
            raise DrainControlBusyError("E_DRAIN_MARKER_UNREADABLE")
        existing = _active_drain_body(home)
        if existing and isinstance(existing.get("scheduler_maintenance_fence"), dict):
            raise DrainControlBusyError("E_SCHEDULER_MAINTENANCE_FENCE_ACTIVE")
        payload = {
            "action": "drain", "requested_at": datetime.now(timezone.utc).isoformat(), "principal": principal,
            "epoch": current_instantiation_epoch(), "suppress_notification": bool(suppress_notification),
        }
        atomic_json_write(path, payload)
    return payload


def clear_drain_request(*, home: Optional[Path] = None) -> bool:
    """Remove the drain marker (cancel-drain, idempotent). Returns True if one existed."""
    with _locked_drain_request(home) as path:
        raw_existing = read_drain_request(home=home)
        if raw_existing == {}:
            raise DrainControlError("E_DRAIN_MARKER_UNREADABLE")
        existing = _active_drain_body(home)
        if existing and isinstance(existing.get("scheduler_maintenance_fence"), dict):
            raise MaintenanceFenceOwnershipError("E_SCHEDULER_MAINTENANCE_RELEASE_TOKEN_REQUIRED")
        try:
            path.unlink()
            return True
        except OSError as e:
            if not isinstance(e, FileNotFoundError):
                _log.warning("drain-control: failed to remove %s: %s", path, e)
            return False


def _marker_is_expired(body: dict[str, Any]) -> bool:
    """True iff ``requested_at`` parses AND is older than the max-age.

    Missing/unparseable and future-dated (clock skew) timestamps are honoured.
    Logged once per marker, not per poll — the operator's breadcrumb for a leak.

    See #85433.
    """
    global _expiry_logged_for
    maintenance = body.get("scheduler_maintenance_fence")
    if isinstance(maintenance, dict):
        if maintenance.get("state") in {"RELEASED", "EXPIRED"}:
            return True
        expires_at = _parse_iso(maintenance.get("expires_at"))
        if expires_at is not None:
            return datetime.now(timezone.utc) >= expires_at
    raw = body.get("requested_at")
    requested_at = _parse_iso(raw)
    if requested_at is None:
        return False
    age = (datetime.now(timezone.utc) - requested_at).total_seconds()
    if age <= DRAIN_REQUEST_MAX_AGE_SECONDS:
        return False
    if _expiry_logged_for != raw:
        _expiry_logged_for = raw
        _log.warning(
            "drain-control: ignoring expired drain marker (requested_at=%s, age=%.0fs > max %.0fs, principal=%s) "
            "— the drain that wrote it was never cancelled; treating as stale so the gateway keeps accepting turns.",
            raw, age, DRAIN_REQUEST_MAX_AGE_SECONDS, body.get("principal"),
        )
    return True


def _active_drain_body(home: Optional[Path]) -> Optional[dict[str, Any]]:
    """Marker body if present AND not stale (definite epoch mismatch or expired), else None."""
    body = read_drain_request(home=home)
    if body is None:
        return None
    current, marker_epoch = current_instantiation_epoch(), body.get("epoch")
    # A maintenance lease must survive Gateway replacement during a controlled activation.
    # Its bounded expires_at, not the process/container epoch, is the staleness boundary.
    maintenance_fence = isinstance(body.get("scheduler_maintenance_fence"), dict)
    if (not maintenance_fence and current and marker_epoch and marker_epoch != current) or _marker_is_expired(body):
        return None
    return body


def drain_requested(*, home: Optional[Path] = None) -> bool:
    """True iff an active (present, same-epoch, unexpired) begin-drain marker exists.

    A marker whose ``epoch`` does not match the current instantiation epoch is treated as absent: it
    survived a container/VM restart (HERMES_HOME is a durable Fly volume on Hermes Cloud) and the lifecycle
    action that triggered the drain has already completed — honouring it would wedge the freshly-restarted
    gateway in ``draining`` (NS-570). A marker whose ``requested_at`` is older than
    :data:`DRAIN_REQUEST_MAX_AGE_SECONDS` is likewise treated as absent: it is a same-epoch orphan whose
    drain-gated action completed without a restart and was never cancelled (#85433). Both staleness checks
    are lenient (see :func:`_marker_epoch_is_stale` / :func:`_marker_is_expired`): a legacy/corrupt marker
    with no epoch and no timestamp, or an environment without ``/proc``, still reads as drain-active.
    """
    body = _active_drain_body(home)
    if body is None:
        return False
    # Scheduler maintenance uses the same marker transport but only closes Cron dispatch;
    # it must not silently turn into a broader message-ingress drain.
    return not isinstance(body.get("scheduler_maintenance_fence"), dict)


def drain_notification_suppressed(*, home: Optional[Path] = None) -> bool:
    """True iff an ACTIVE marker asks to suppress the shutdown broadcast.

    Same activeness rule as :func:`drain_requested`, so an orphan can never silence
    a fresh gateway; a marker without the field reads False (fail toward louder).

    "Active" means exactly what :func:`drain_requested` means — a marker present AND stamped with the
    current instantiation epoch AND not past its max-age. A stale (other-epoch) marker that survived a
    machine restart on the durable HERMES_HOME volume, or an expired same-epoch orphan (#85433), is ignored
    here just as it is for drain state (NS-570): we must never let an orphaned marker's flag silence a
    *fresh* gateway's legitimate shutdown broadcast.
    """
    body = _active_drain_body(home)
    return bool(body and not isinstance(body.get("scheduler_maintenance_fence"), dict)
                and body.get("suppress_notification"))


def read_drain_request(*, home: Optional[Path] = None) -> Optional[dict[str, Any]]:
    """Return the marker payload, ``{}`` if present but unparseable, ``None`` if absent. Never raises."""
    path = drain_request_path(home)
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as e:
        if not isinstance(e, FileNotFoundError):
            _log.warning("drain-control: failed to read %s: %s", path, e)
            return {}  # unreadable existing marker fails closed toward paused dispatch
        return None
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _safe_fence_text(value: object, label: str, *, limit: int = 256) -> str:
    text = str(value or "").strip()
    if not text or len(text) > limit or any(ord(char) < 32 for char in text):
        raise ValueError(f"invalid maintenance fence {label}")
    return text


def _maintenance_proof_dir(home: Optional[Path]) -> Path:
    directory = scheduler_maintenance_fence_dir(home)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    info = directory.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise DrainControlError("E_MAINTENANCE_FENCE_DIRECTORY_INVALID")
    with contextlib.suppress(OSError):
        os.chmod(directory, 0o700)
    return directory


def _read_scheduler_maintenance_proof(proof_path: str | Path, *, home: Optional[Path] = None) -> dict[str, Any]:
    directory = _maintenance_proof_dir(home).resolve(strict=True)
    path = Path(proof_path).expanduser()
    try:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise DrainControlError("E_MAINTENANCE_PROOF_FILE_TYPE")
        resolved = path.resolve(strict=True)
        resolved.relative_to(directory)
        if stat.S_IMODE(info.st_mode) != 0o600:
            raise DrainControlError("E_MAINTENANCE_PROOF_FILE_MODE")
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except DrainControlError:
        raise
    except (OSError, ValueError, TypeError) as exc:
        raise DrainControlError("E_MAINTENANCE_PROOF_UNREADABLE") from exc
    fence = payload.get("fence") if isinstance(payload, dict) else None
    if not isinstance(fence, dict) or payload.get("schema_version") != SCHEDULER_MAINTENANCE_PROOF_SCHEMA:
        raise DrainControlError("E_MAINTENANCE_PROOF_SCHEMA")
    if fence.get("schema_version") != SCHEDULER_MAINTENANCE_FENCE_SCHEMA:
        raise DrainControlError("E_MAINTENANCE_FENCE_SCHEMA")
    try:
        ttl = float(fence.get("ttl_seconds"))
        created_at = _parse_iso(fence.get("created_at"))
        expires_at = _parse_iso(fence.get("expires_at"))
        identity = fence.get("scheduler_owner_identity")
        tick_homes = fence.get("scheduler_tick_homes")
        if (
            not math.isfinite(ttl)
            or not SCHEDULER_MAINTENANCE_MIN_TTL_SECONDS <= ttl <= SCHEDULER_MAINTENANCE_TTL_SECONDS
            or created_at is None
            or expires_at is None
            or abs((expires_at - created_at).total_seconds() - ttl) > 2
            or not re.fullmatch(r"[0-9a-f]{32}", str(fence.get("maintenance_id") or ""))
            or not re.fullmatch(r"[0-9a-f]{64}", str(fence.get("proof_sha256") or ""))
            or len(str(fence.get("nonce") or "")) < 32
            or not _safe_fence_text(fence.get("owner"), "owner")
            or not _safe_fence_text(fence.get("purpose"), "purpose")
            or not isinstance(identity, dict)
            or not isinstance(tick_homes, list)
            or not tick_homes
        ):
            raise ValueError("invalid fence shape")
        if int(identity.get("gateway_python_pid")) < 2:
            raise ValueError("invalid owner PID")
        for key in ("gateway_process_started_at", "binding_id", "launch_agent_sha256"):
            _safe_fence_text(identity.get(key), f"scheduler_owner_identity.{key}")
        if not re.fullmatch(r"[0-9a-f]{64}", str(identity.get("launch_agent_sha256") or "")):
            raise ValueError("invalid LaunchAgent digest")
        canonical_homes = [str(Path(item).expanduser().resolve(strict=True)) for item in tick_homes]
        if canonical_homes != sorted(set(canonical_homes)):
            raise ValueError("invalid tick home roster")
    except (TypeError, ValueError, OSError) as exc:
        raise DrainControlError("E_MAINTENANCE_FENCE_FIELDS") from exc
    expected = _maintenance_proof_sha256(fence)
    supplied = str(fence.get("proof_sha256") or "")
    if not supplied or not hmac.compare_digest(expected, supplied):
        raise DrainControlError("E_MAINTENANCE_PROOF_INTEGRITY")
    return dict(fence)


def _marker_matches_maintenance_fence(body: dict[str, Any], fence: dict[str, Any]) -> bool:
    marker_fence = body.get("scheduler_maintenance_fence")
    if not isinstance(marker_fence, dict):
        return False
    for key, value in _maintenance_identity(fence).items():
        if key == "nonce":
            if not hmac.compare_digest(str(marker_fence.get(key) or ""), str(value or "")):
                return False
        elif marker_fence.get(key) != value:
            return False
    return marker_fence.get("proof_sha256") == fence.get("proof_sha256")


def read_scheduler_maintenance_proof(proof_path: str | Path, *, home: Optional[Path] = None) -> dict[str, Any]:
    """Read the private owner token. Callers must not print or log the returned nonce."""
    return _read_scheduler_maintenance_proof(proof_path, home=home)


def active_scheduler_maintenance_fence(*, home: Optional[Path] = None) -> Optional[dict[str, Any]]:
    body = _active_drain_body(home)
    if body is None:
        return None
    marker_fence = body.get("scheduler_maintenance_fence")
    if not isinstance(marker_fence, dict):
        return None
    proof_path = scheduler_maintenance_proof_path(home, str(marker_fence.get("maintenance_id") or ""))
    try:
        fence = _read_scheduler_maintenance_proof(proof_path, home=home)
    except DrainControlError:
        return None
    if not _marker_matches_maintenance_fence(body, fence):
        return None
    if marker_fence.get("state") != fence.get("state"):
        return None
    if fence.get("state") not in {"ACQUIRED", "DRAINING", "QUIESCED"}:
        return None
    return fence


def scheduler_dispatch_blocked(*, home: Optional[Path] = None) -> bool:
    """Fail-closed Cron gate, including the first tick before the async watcher runs."""
    try:
        homes = [Path(home if home is not None else get_hermes_home()).expanduser()]
        if home is None and os.environ.get("HERMES_HOME"):
            launch_home = Path(os.environ["HERMES_HOME"]).expanduser()
            if launch_home.resolve(strict=False) not in {item.resolve(strict=False) for item in homes}:
                homes.append(launch_home)
        return any(_active_drain_body(item) is not None for item in homes)
    except Exception as exc:
        _log.error("scheduler dispatch fence could not be read; holding dispatch: %s", exc)
        return True


@contextlib.contextmanager
def scheduler_dispatch_admission(*, home: Optional[Path] = None):
    """Serialize non-tick fire claims with fence publication and report admission."""
    homes = [Path(home if home is not None else get_hermes_home()).expanduser()]
    if home is None and os.environ.get("HERMES_HOME"):
        homes.append(Path(os.environ["HERMES_HOME"]).expanduser())
    unique = sorted({item.resolve(strict=False): item for item in homes}.values(), key=lambda item: str(item.resolve(strict=False)))
    with contextlib.ExitStack() as stack:
        for control_home in unique:
            stack.enter_context(_locked_drain_request(control_home))
        yield not any(_active_drain_body(item) is not None for item in unique)


def acquire_scheduler_maintenance_fence(
    *,
    owner: str,
    purpose: str,
    scheduler_owner_identity: dict[str, Any],
    home: Optional[Path] = None,
    ttl_seconds: float = SCHEDULER_MAINTENANCE_TTL_SECONDS,
    tick_lock_homes: Optional[list[str | Path]] = None,
    tick_barrier_timeout_seconds: float = _SCHEDULER_TICK_BARRIER_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Acquire the existing drain marker as a bounded, owner-token Cron maintenance fence."""
    try:
        ttl = float(ttl_seconds)
    except (TypeError, ValueError) as exc:
        raise ValueError("maintenance TTL must be numeric") from exc
    if not math.isfinite(ttl) or not SCHEDULER_MAINTENANCE_MIN_TTL_SECONDS <= ttl <= SCHEDULER_MAINTENANCE_TTL_SECONDS:
        raise ValueError("maintenance TTL is outside the bounded policy")
    fence_owner = _safe_fence_text(owner, "owner")
    fence_purpose = _safe_fence_text(purpose, "purpose")
    if not isinstance(scheduler_owner_identity, dict):
        raise ValueError("scheduler owner identity must be an object")
    scheduler_identity = dict(scheduler_owner_identity)
    try:
        gateway_pid = int(scheduler_identity.get("gateway_python_pid"))
    except (TypeError, ValueError) as exc:
        raise ValueError("scheduler owner identity needs gateway_python_pid") from exc
    if gateway_pid < 2:
        raise ValueError("scheduler owner identity has an invalid gateway PID")
    for key in ("gateway_process_started_at", "binding_id", "launch_agent_sha256"):
        scheduler_identity[key] = _safe_fence_text(scheduler_identity.get(key), f"scheduler_owner_identity.{key}")
    if not re.fullmatch(r"[0-9a-f]{64}", scheduler_identity["launch_agent_sha256"]):
        raise ValueError("scheduler owner identity has an invalid LaunchAgent digest")

    try:
        barrier_timeout = float(tick_barrier_timeout_seconds)
    except (TypeError, ValueError) as exc:
        raise ValueError("tick barrier timeout must be numeric") from exc
    if not math.isfinite(barrier_timeout) or not 0 < barrier_timeout <= 60:
        raise ValueError("tick barrier timeout is outside the bounded policy")
    if tick_lock_homes is None:
        resolved_tick_homes = _current_scheduler_tick_homes()
        verify_roster_after_lock = True
    else:
        try:
            resolved_tick_homes = sorted(
                {Path(item).expanduser().resolve(strict=True) for item in tick_lock_homes}, key=str
            )
        except (OSError, TypeError, ValueError) as exc:
            raise ValueError("invalid scheduler tick home roster") from exc
        verify_roster_after_lock = False
    if not resolved_tick_homes:
        raise ValueError("scheduler tick home roster must not be empty")

    home_path = Path(home if home is not None else get_hermes_home()).expanduser()
    if home_path.resolve(strict=True) not in resolved_tick_homes:
        raise ValueError("maintenance marker home is not in the active scheduler tick roster")
    created = datetime.now(timezone.utc)
    created_at = created.isoformat(timespec="microseconds")
    expires_at = (created + timedelta(seconds=ttl)).isoformat(timespec="microseconds")
    maintenance_id = uuid.uuid4().hex
    proof_path = scheduler_maintenance_proof_path(home_path, maintenance_id)
    fence: dict[str, Any] = {
        "schema_version": SCHEDULER_MAINTENANCE_FENCE_SCHEMA,
        "maintenance_id": maintenance_id,
        "owner": fence_owner,
        "purpose": fence_purpose,
        "created_at": created_at,
        "expires_at": expires_at,
        "ttl_seconds": ttl,
        "scheduler_owner_identity": scheduler_identity,
        "scheduler_tick_homes": [str(item) for item in resolved_tick_homes],
        "state": "ACQUIRED",
        "nonce": secrets.token_urlsafe(32),
    }
    fence["proof_sha256"] = _maintenance_proof_sha256(fence)
    proof_payload = {"schema_version": SCHEDULER_MAINTENANCE_PROOF_SCHEMA, "fence": fence}
    proof_dir = _maintenance_proof_dir(home_path)

    with _scheduler_tick_lock_barrier(resolved_tick_homes, timeout_seconds=barrier_timeout):
        if verify_roster_after_lock and [str(item) for item in _current_scheduler_tick_homes()] != [str(item) for item in resolved_tick_homes]:
            raise DrainControlBusyError("E_MAINTENANCE_TICK_HOME_ROSTER_CHANGED")
        with _locked_drain_request(home_path) as marker_path:
            if _active_drain_body(home_path) is not None:
                raise DrainControlBusyError("E_DRAIN_OR_MAINTENANCE_FENCE_ALREADY_ACTIVE")
            if proof_path.exists() or proof_path.is_symlink():
                raise DrainControlBusyError("E_MAINTENANCE_PROOF_PATH_ALREADY_EXISTS")
            marker = {
                "action": "drain",
                "requested_at": created_at,
                "principal": fence_owner,
                "epoch": current_instantiation_epoch(),
                "suppress_notification": True,
                "scheduler_maintenance_fence": fence,
            }
            atomic_json_write(proof_path, proof_payload, mode=0o600, fsync_dir=True)
            try:
                atomic_json_write(marker_path, marker, mode=0o600, fsync_dir=True)
            except Exception:
                with contextlib.suppress(OSError):
                    proof_path.unlink()
                raise
    return {
        "maintenance_id": maintenance_id,
        "owner": fence_owner,
        "purpose": fence_purpose,
        "created_at": created_at,
        "expires_at": expires_at,
        "ttl_seconds": ttl,
        "scheduler_owner_identity": scheduler_identity,
        "proof_sha256": fence["proof_sha256"],
        "proof_path": str(proof_path),
        "fence_directory": str(proof_dir),
    }


def update_scheduler_maintenance_fence_state(
    proof_path: str | Path,
    *,
    owner: str,
    nonce: str,
    state: str,
    home: Optional[Path] = None,
) -> dict[str, Any]:
    """Owner-token transition for ACQUIRED → DRAINING → QUIESCED."""
    allowed_next = {"ACQUIRED": "DRAINING", "DRAINING": "QUIESCED", "QUIESCED": "QUIESCED"}
    if state not in set(allowed_next.values()):
        raise ValueError("invalid scheduler maintenance fence state")
    home_path = Path(home if home is not None else get_hermes_home()).expanduser()
    proof_file = Path(proof_path).expanduser()
    with _locked_drain_request(home_path) as marker_path:
        fence = _read_scheduler_maintenance_proof(proof_file, home=home_path)
        if fence.get("owner") != owner or not hmac.compare_digest(str(fence.get("nonce") or ""), str(nonce or "")):
            raise MaintenanceFenceOwnershipError("E_MAINTENANCE_FENCE_OWNER_OR_TOKEN_MISMATCH")
        if _marker_is_expired({"requested_at": fence.get("created_at"), "scheduler_maintenance_fence": fence}):
            raise MaintenanceFenceExpiredError("E_MAINTENANCE_FENCE_EXPIRED")
        current = str(fence.get("state") or "")
        if current == state:
            return {key: value for key, value in fence.items() if key != "nonce"}
        if allowed_next.get(current) != state:
            raise DrainControlError("E_MAINTENANCE_FENCE_STATE_TRANSITION")
        marker = read_drain_request(home=home_path)
        if not isinstance(marker, dict) or not _marker_matches_maintenance_fence(marker, fence):
            raise MaintenanceFenceOwnershipError("E_MAINTENANCE_FENCE_MARKER_MISMATCH")
        fence["state"] = state
        marker["scheduler_maintenance_fence"] = fence
        proof_payload = {"schema_version": SCHEDULER_MAINTENANCE_PROOF_SCHEMA, "fence": fence}
        atomic_json_write(proof_file, proof_payload, mode=0o600, fsync_dir=True)
        atomic_json_write(marker_path, marker, mode=0o600, fsync_dir=True)
    return {key: value for key, value in fence.items() if key != "nonce"}


def sync_scheduler_maintenance_ack(*, home: Optional[Path] = None) -> Optional[dict[str, Any]]:
    """Publish Gateway-owned proof that Cron dispatch is gated and in-flight jobs are visible."""
    home_path = Path(home if home is not None else get_hermes_home()).expanduser()
    fence = active_scheduler_maintenance_fence(home=home_path)
    if fence is None:
        marker = read_drain_request(home=home_path)
        stale = marker.get("scheduler_maintenance_fence") if isinstance(marker, dict) else None
        if isinstance(stale, dict):
            ack_path = scheduler_maintenance_ack_path(home_path, str(stale.get("maintenance_id") or ""))
            with contextlib.suppress(OSError):
                ack_path.unlink()
        return None
    try:
        from cron.scheduler import get_running_job_details
        from gateway.status import get_process_start_time

        # Details preserve duplicate job IDs across different profile homes; the summary helper
        # intentionally de-duplicates IDs and would undercount multiplexed work here.
        running_ids = sorted(str(item["job_id"]) for item in get_running_job_details())
        process_started_at = get_process_start_time(os.getpid())
        current = home_path / "runtime" / "scheduler-core" / "current"
        scheduler_binding_id = current.resolve(strict=True).name
    except Exception as exc:
        raise DrainControlError("E_MAINTENANCE_FENCE_ACK_READBACK") from exc
    if process_started_at is None or not scheduler_dispatch_blocked(home=home_path):
        raise DrainControlError("E_MAINTENANCE_FENCE_ACK_NOT_QUIESCED")
    try:
        current_tick_homes = [str(item) for item in _current_scheduler_tick_homes()]
    except DrainControlError:
        current_tick_homes = []
    ack = {
        "schema_version": SCHEDULER_MAINTENANCE_ACK_SCHEMA,
        "maintenance_id": fence["maintenance_id"],
        "owner": fence["owner"],
        "proof_sha256": fence["proof_sha256"],
        "fence_state": fence["state"],
        "gateway_pid": os.getpid(),
        "gateway_process_started_at": process_started_at,
        "scheduler_binding_id": scheduler_binding_id,
        "scheduler_tick_homes": current_tick_homes,
        "scheduler_tick_home_roster_matches": current_tick_homes == fence["scheduler_tick_homes"],
        "scheduler_dispatch_blocked": True,
        "running_job_count": len(running_ids),
        "running_job_ids": running_ids,
        "observed_at": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
    }
    ack_path = scheduler_maintenance_ack_path(home_path, fence["maintenance_id"])
    # Serialize publication against state transitions and explicit release. A watcher that
    # sampled the old fence must never recreate an ACK after its receipt closed the lease.
    with _locked_drain_request(home_path):
        current_fence = active_scheduler_maintenance_fence(home=home_path)
        if (current_fence is None
                or current_fence.get("maintenance_id") != fence.get("maintenance_id")
                or current_fence.get("proof_sha256") != fence.get("proof_sha256")
                or current_fence.get("state") != fence.get("state")):
            return None
        atomic_json_write(ack_path, ack, mode=0o600, fsync_dir=True)
    return ack


def release_scheduler_maintenance_fence(
    proof_path: str | Path,
    *,
    owner: str,
    nonce: str,
    home: Optional[Path] = None,
) -> dict[str, Any]:
    """Release the existing drain marker only for its exact maintenance owner and nonce."""
    home_path = Path(home if home is not None else get_hermes_home()).expanduser()
    proof_file = Path(proof_path).expanduser()
    with _locked_drain_request(home_path) as marker_path:
        fence = _read_scheduler_maintenance_proof(proof_file, home=home_path)
        if fence.get("owner") != owner or not hmac.compare_digest(str(fence.get("nonce") or ""), str(nonce or "")):
            raise MaintenanceFenceOwnershipError("E_MAINTENANCE_FENCE_OWNER_OR_TOKEN_MISMATCH")
        marker = read_drain_request(home=home_path)
        if not isinstance(marker, dict) or not _marker_matches_maintenance_fence(marker, fence):
            raise MaintenanceFenceOwnershipError("E_MAINTENANCE_FENCE_MARKER_MISMATCH")
        now = datetime.now(timezone.utc)
        expires = _parse_iso(fence.get("expires_at"))
        release_state = "EXPIRED" if expires is not None and now >= expires else "RELEASED"
        receipt = {
            "schema_version": SCHEDULER_MAINTENANCE_RECEIPT_SCHEMA,
            "maintenance_id": fence["maintenance_id"],
            "owner": fence["owner"],
            "purpose": fence["purpose"],
            "created_at": fence["created_at"],
            "expires_at": fence["expires_at"],
            "scheduler_owner_identity": fence["scheduler_owner_identity"],
            "proof_sha256": fence["proof_sha256"],
            "state": release_state,
            "released_at": now.isoformat(timespec="microseconds"),
        }
        receipt_path = proof_file.with_name(f"{fence['maintenance_id']}.receipt.json")
        if receipt_path.exists() or receipt_path.is_symlink():
            raise DrainControlError("E_MAINTENANCE_FENCE_RECEIPT_EXISTS")
        atomic_json_write(receipt_path, receipt, mode=0o444, fsync_dir=True)
        try:
            marker_path.unlink()
        except OSError:
            with contextlib.suppress(OSError):
                receipt_path.unlink()
            raise
        try:
            proof_file.unlink()
        except OSError as exc:
            # The dispatch fence is already released; keep the private proof rather than
            # misreporting release failure after its marker is gone.
            _log.error("maintenance fence released but owner proof could not be removed: %s", exc)
        with contextlib.suppress(OSError):
            scheduler_maintenance_ack_path(home_path, fence["maintenance_id"]).unlink()
    return {**receipt, "receipt_path": str(receipt_path)}
