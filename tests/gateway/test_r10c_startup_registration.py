"""Startup refuses an incomplete reused policy instead of silently serving."""
import importlib
from unittest.mock import Mock

import pytest


@pytest.mark.asyncio
async def test_official_start_gateway_refuses_before_owner_with_missing_fences(
    tmp_path, monkeypatch,
):
    from gateway import run, startup_isolation

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr("hermes_constants.get_process_hermes_home", lambda: tmp_path)
    (tmp_path / "config.yaml").write_text(
        "gateway:\n  recovery_enabled: true\n  external_delivery_enabled: false\n"
        "  delivery_state_write_enabled: false\n")
    importlib.reload(startup_isolation)
    owner = Mock(side_effect=AssertionError("owner must not be acquired"))
    monkeypatch.setattr(run, "_host_attach_or_none", owner)
    try:
        with pytest.raises(startup_isolation.StartupIsolationError, match="FENCE_MISSING"):
            await run.start_gateway()
        owner.assert_not_called()
        assert not startup_isolation.evidence()["external_delivery_allowed"]
        assert not startup_isolation.evidence()["delivery_state_write_allowed"]
    finally:
        importlib.reload(startup_isolation)
