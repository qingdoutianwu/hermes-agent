"""Cron-test fixtures.

Provides a default ``HERMES_MODEL`` for cron run_job tests so each one
doesn't have to spell out a model. The global conftest blanks
HERMES_MODEL hermetically; without this autouse fixture every cron test
that exercises ``run_job`` would hit the fail-fast guard added in
``cron/scheduler.py`` (see issue #23979) and have to be rewritten.

Tests that specifically need ``HERMES_MODEL`` unset — model-resolution
edge cases — call ``monkeypatch.delenv("HERMES_MODEL", raising=False)``
inside the test, which overrides this fixture's value for that scope.
"""

import pytest


@pytest.fixture(autouse=True)
def _skip_unrelated_checkout_update_recovery(monkeypatch):
    """Cron tests must not probe linked-worktree update metadata at import time.

    These tests exercise Cron behavior, not interrupted source-update recovery. Hermes' bootstrap
    invokes that recovery before importing its runtime, and a linked worktree stores its Git
    metadata under the operator's guarded ~/.hermes root. Keep the real-home guard active; replace
    only the unrelated recovery probe for this test scope.
    """
    from hermes_cli import _early_recovery
    from pm import environments
    from tools.environments import local

    monkeypatch.setattr(_early_recovery, "restore_interrupted_pull", lambda *_args, **_kwargs: False)
    # scripts/run_tests.sh has already selected this checkout's isolated test environment.
    # Re-activating via hermes_bootstrap probes an install manifest beside the checkout, which
    # is outside the disposable test HOME for linked worktrees.
    monkeypatch.setattr(environments, "activate_dependencies", lambda *_args, **_kwargs: None)
    # Cron behavior tests do not verify the console-script discovery policy. Its source-checkout
    # fallback probes <checkout-parent>/manifest.json, which can be under guarded ~/.hermes for a
    # linked Hermes worktree; resolver behavior has dedicated tools/environment tests.
    monkeypatch.setattr(local, "_resolve_hermes_bin_dir", lambda: None)


@pytest.fixture()
def make_cron_provider():
    """Factory for minimal CronScheduler test doubles.

    ``make_cron_provider(register_job=...)`` returns a real ``CronScheduler``
    subclass instance whose ``register_job`` is the given callable — so tests
    exercising the creation-registration contract share one stub instead of
    redefining inline spy/failing classes, and an ABC rename breaks them
    loudly instead of silently passing a duck-type.
    """
    from cron.scheduler_provider import CronScheduler

    def _make(register_job=None, name="stub"):
        class _StubProvider(CronScheduler):
            @property
            def name(self):  # pragma: no cover - trivial
                return name

            def start(self, stop_event, **kw):  # pragma: no cover - unused
                pass

            def register_job(self, job):
                if register_job is not None:
                    return register_job(job)
                return None

        return _StubProvider()

    return _make


@pytest.fixture(autouse=True)
def _no_managed_store(tmp_path, monkeypatch):
    """Point PM's store at an empty dir: script runs must not select the host install's
    dependency venv (POSIX cron scripts run on it when a store is committed)."""
    monkeypatch.setenv("HERMES_RUNTIME_DIR", str(tmp_path / "no-pm-store"))


@pytest.fixture(autouse=True)
def _default_cron_test_model(monkeypatch):
    """Pin a default HERMES_MODEL so cron run_job tests have a resolvable model."""
    monkeypatch.setenv("HERMES_MODEL", "test-cron-default-model")
    yield


@pytest.fixture(autouse=True)
def _reset_session_context_vars():
    """Restore session ContextVars around cron tests that call run_job directly.

    Production confines each cron run to a copied context, but direct unit tests
    share the pytest context. ``run_job`` intentionally clears ordinary session
    variables to explicit empty values, which would otherwise shadow legacy env
    fallbacks used by later approval tests in the same process.
    """
    from gateway.session_context import _UNSET, _VAR_MAP

    def _reset_all():
        for var in _VAR_MAP.values():
            var.set(_UNSET)

    _reset_all()
    yield
    _reset_all()
