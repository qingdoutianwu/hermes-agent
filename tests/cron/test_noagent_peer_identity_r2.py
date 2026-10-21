"""RED unit contract for the native macOS peer-identity seam.

The implementation is intentionally absent on the base branch.  The test
specifies the required fail-closed behavior without using real Hermes state.
"""
from __future__ import annotations

import socket

import pytest


def test_macos_peer_identity_returns_pid_and_start_fingerprint_or_rejects():
    from cron.scheduler_origin_auth import peer_identity

    auth_start = __import__("pytest").MonkeyPatch()
    auth_start.setattr("cron.scheduler_origin_auth.peer_start_fingerprint", lambda _pid: 123456)

    parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        identity = peer_identity(parent)
        assert identity is not None
        assert identity.pid > 0
        assert identity.uid >= 0
        assert identity.gid >= 0
        assert identity.start_fingerprint == 123456
    finally:
        parent.close()
        child.close()
        auth_start.undo()


def test_peer_identity_rejects_non_unix_or_unavailable_credentials():
    from cron.scheduler_origin_auth import peer_identity

    with pytest.raises(Exception):
        peer_identity(object())
