"""Real owners declare fences; absent declarations still deny all startup."""
import importlib
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from gateway import startup_isolation as policy


def load_real_owners():
    # A policy reset in tests requires replaying real module declarations, not fake names.
    for name in ("cron.jobs", "gateway.run_notifications", "gateway.run_startup",
                 "gateway.delivery_ledger", "gateway.platforms.base"):
        importlib.reload(importlib.import_module(name))


@pytest.fixture(autouse=True)
def configured_recovery(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr("hermes_constants.get_process_hermes_home", lambda: tmp_path)
    (tmp_path / "config.yaml").write_text(
        "gateway:\n  recovery_enabled: true\n  external_delivery_enabled: false\n"
        "  delivery_state_write_enabled: false\n")
    importlib.reload(policy)
    load_real_owners()
    yield
    importlib.reload(policy)


def test_actual_owners_complete_registration_before_activation():
    policy.install()
    assert policy.evidence()["registered_admission_points"] == sorted(policy.REQUIRED)
    assert policy.evidence()["recovery_effective"]
    assert not policy.allowed("pending_send")
    assert not policy.allowed("external_delivery")
    assert not policy.allowed("delivery_state")
    assert not policy.allowed("scheduler_dispatch")


@pytest.mark.parametrize("point", sorted(policy.REQUIRED))
def test_each_missing_real_registration_refuses_startup(point):
    policy._registered.remove(point)  # Negative fault injection only, never a production register.
    with pytest.raises(policy.StartupIsolationError, match="FENCE_MISSING:" + point):
        policy.install()
    assert not policy.allowed("external_delivery")
    assert not policy.allowed("scheduler_dispatch")


@pytest.mark.asyncio
async def test_adapters_and_pending_tasks_deny_before_actual_owners_are_accessed():
    from gateway.run_startup import GatewayStartupMixin

    policy.install()
    runner = SimpleNamespace()
    assert await GatewayStartupMixin._start_prefilter_platforms(runner) == (False, 0, [], [])
    await GatewayStartupMixin._start_post_connect_services(runner, 0)
    # A bypass would access the runner's adapter config, worker, hooks, or directory here.


def test_scheduler_admission_denies_before_any_store_lock(monkeypatch):
    from cron import scheduler, scheduler_tick

    policy.install()
    locks = Mock(side_effect=AssertionError("store lock must not be touched"))
    monkeypatch.setattr(scheduler_tick, "_acquire_tick_lock_or_degrade", locks)
    assert scheduler_tick._tick_admitted() == 0
    locks.assert_not_called()
