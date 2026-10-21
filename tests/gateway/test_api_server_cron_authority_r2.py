"""Real HTTP -> Cron storage checks; no scheduler start or business execution."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from aiohttp.test_utils import TestClient, TestServer

from cron import jobs
from gateway.platforms import api_server
from gateway.config import PlatformConfig
from tests.gateway.test_api_server_jobs import _create_app

KEY = "isolated-fake-api-key-for-tests"
AUTH = {"Authorization": f"Bearer {KEY}"}


@pytest.fixture
def api_env(tmp_path, monkeypatch):
    home = tmp_path / "profile"
    (home / "scripts").mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    for key, value in {"HERMES_DIR": home, "CRON_DIR": home / "cron",
                       "JOBS_FILE": home / "cron" / "jobs.json",
                       "OUTPUT_DIR": home / "cron" / "output"}.items():
        monkeypatch.setattr(jobs, key, value)
    entry = home / "scripts" / "never_execute.py"
    entry.write_text("raise AssertionError('API test must not run business')\n")
    manifest = home / "releases" / "candidate" / "release_manifest.json"
    manifest.parent.mkdir(parents=True)
    payload = {"release_id": "ISOLATED-API-R2", "source_commit": "candidate",
               "source_tree_sha256": "a" * 64, "production_entrypoint": "scripts/never_execute.py",
               "entry": str(entry), "entry_sha256": hashlib.sha256(entry.read_bytes()).hexdigest()}
    manifest.write_text(json.dumps(payload))
    job = jobs.create_job(prompt="local only", schedule="every 4h", name="existing",
                          script=str(entry), no_agent=True, deliver="local", paused=True)
    adapter = api_server.APIServerAdapter(PlatformConfig(enabled=True, extra={"key": KEY}))
    monkeypatch.setattr(api_server, "_CRON_AVAILABLE", True)
    from cron.scheduler import create_job_with_scheduler_registration
    create = Mock(wraps=create_job_with_scheduler_registration)
    update = Mock(wraps=jobs.update_job)
    monkeypatch.setattr(api_server, "_cron_create", create)
    monkeypatch.setattr(api_server, "_cron_update", update)
    monkeypatch.setattr(api_server, "_cron_get", jobs.get_job)
    monkeypatch.setattr(api_server, "_notify_cron_provider_jobs_changed", Mock())
    return SimpleNamespace(home=home, job=job, entry=entry, manifest=manifest, payload=payload,
                           adapter=adapter, app=_create_app(adapter), create=create, update=update)


def _body(**options):
    return {"name": "created", "schedule": "every 4h", "prompt": "local only",
            "deliver": "local", "paused": True, **options}


async def _request(client, env, mode, options, headers=AUTH):
    if mode == "create":
        return await client.post("/api/jobs", json=_body(**options), headers=headers)
    return await client.patch(f"/api/jobs/{env.job['id']}", json=options, headers=headers)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["create", "update"])
async def test_authority_fields_reach_core_and_read_back(api_env, mode):
    env = api_env
    options = {"r2_authority_required": True, "r2_release_manifest": str(env.manifest)}
    async with TestClient(TestServer(env.app)) as client:
        response = await _request(client, env, mode, options)
        assert response.status == 200, await response.text()
        job = (await response.json())["job"]
        assert job["_r2_authority_required"] == "true"
        assert job["_r2_release_manifest"] == str(env.manifest.resolve())
        assert "r2_authority_required" not in job and "r2_release_manifest" not in job
        response = await client.get(f"/api/jobs/{job['id']}", headers=AUTH)
        assert response.status == 200
        assert (await response.json())["job"] == jobs.get_job(job["id"])
        raw = json.loads((env.home / "cron" / "jobs.json").read_text())
        stored = next(j for j in raw["jobs"] if j["id"] == job["id"])
        # Native update stores bool; read normalization exposes the canonical string.
        assert stored["_r2_authority_required"] == (True if mode == "update" else "true")
        assert stored["_r2_release_manifest"] == str(env.manifest.resolve())
        if mode == "update":
            assert env.update.call_args.args[1] == {
                "_r2_authority_required": True, "_r2_release_manifest": str(env.manifest.resolve())}
        else:
            assert env.create.call_args.kwargs["r2_authority_required"] is True
    assert not (env.home / "cron" / "executions.db").exists()


@pytest.mark.asyncio
async def test_enable_uses_existing_manifest_and_disable_preserves_binding(api_env):
    env = api_env
    jobs.update_job(env.job["id"], {"_r2_release_manifest": str(env.manifest)})
    async with TestClient(TestServer(env.app)) as client:
        response = await _request(client, env, "update", {"r2_authority_required": True})
        assert response.status == 200
        job = (await response.json())["job"]
        assert job["_r2_authority_required"] == "true"
        assert job["_r2_release_manifest"] == str(env.manifest)
        response = await _request(client, env, "update", {"r2_authority_required": False})
        assert response.status == 200
        job = (await response.json())["job"]
        assert job["_r2_authority_required"] == "false"
        assert job["_r2_release_manifest"] == str(env.manifest)


@pytest.mark.asyncio
async def test_default_and_ordinary_update_never_enable_authority(api_env):
    env = api_env
    async with TestClient(TestServer(env.app)) as client:
        response = await _request(client, env, "create", {})
        assert response.status == 200
        job = (await response.json())["job"]
        assert job["_r2_authority_required"] == "false"
        response = await _request(client, env, "update", {
            "name": "renamed", "prompt": "updated", "deliver": "local", "unknown": "ignored"})
        assert response.status == 200
        updated = (await response.json())["job"]
        assert updated["_r2_authority_required"] == "false"
        assert updated["script"] == env.job["script"] and updated["no_agent"] is True
        assert env.update.call_args.args[1] == {"name": "renamed", "prompt": "updated", "deliver": "local"}


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["create", "update"])
@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong-test-key"}])
async def test_http_auth_precedes_authority_inputs(api_env, mode, headers):
    env = api_env
    before = (env.home / "cron" / "jobs.json").read_bytes()
    async with TestClient(TestServer(env.app)) as client:
        response = await _request(client, env, mode, {
            "r2_authority_required": True, "r2_release_manifest": "/unreadable/manifest.json"}, headers)
        assert response.status == 401
    assert (env.home / "cron" / "jobs.json").read_bytes() == before
    env.create.assert_not_called()
    env.update.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["create", "update"])
@pytest.mark.parametrize("field", ["_r2_authority_required", "_r2_release_manifest", "_r2_authority"])
async def test_private_authority_field_injection_is_rejected(api_env, mode, field):
    env = api_env
    before = (env.home / "cron" / "jobs.json").read_bytes()
    async with TestClient(TestServer(env.app)) as client:
        response = await _request(client, env, mode, {"name": "unchanged", field: "forged"})
        assert response.status == 400
    assert (env.home / "cron" / "jobs.json").read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["create", "update"])
@pytest.mark.parametrize("options", [
    {"r2_authority_required": "false"}, {"r2_authority_required": 1},
    {"r2_authority_required": None}, {"r2_release_manifest": {"release_id": "self-claimed"}},
    {"r2_release_manifest": "relative.json"}, {"r2_authority_required": True},
])
async def test_invalid_types_or_missing_binding_do_not_write(api_env, mode, options):
    env = api_env
    before = (env.home / "cron" / "jobs.json").read_bytes()
    async with TestClient(TestServer(env.app)) as client:
        response = await _request(client, env, mode, options)
        assert response.status == 400
    assert (env.home / "cron" / "jobs.json").read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["create", "update"])
@pytest.mark.parametrize("kind", ["outside_profile", "symlink", "bad_digest", "non_object",
                                 "incomplete_binding", "outside_script_root"])
async def test_manifest_must_be_profile_local_and_match_real_entry(api_env, tmp_path, mode, kind):
    env = api_env
    outside = tmp_path / "outside.py"
    outside.write_text("print('must never run')")
    paths = {"outside_profile": tmp_path / "unapproved.json",
             "symlink": env.manifest.parent / "alias.json"}
    documents = {"outside_profile": env.payload,
                 "bad_digest": {**env.payload, "entry_sha256": "b" * 64},
                 "non_object": [], "incomplete_binding": {"release_id": "self-claimed"},
                 "outside_script_root": {**env.payload, "entry": str(outside),
                                         "entry_sha256": hashlib.sha256(outside.read_bytes()).hexdigest()}}
    path = paths.get(kind, env.manifest)
    if kind == "symlink":
        path.symlink_to(env.manifest)
    else:
        path.write_text(json.dumps(documents[kind]))
    before = (env.home / "cron" / "jobs.json").read_bytes()
    async with TestClient(TestServer(env.app)) as client:
        response = await _request(client, env, mode, {
            "r2_authority_required": True, "r2_release_manifest": str(path)})
        assert response.status == 400
    assert (env.home / "cron" / "jobs.json").read_bytes() == before


@pytest.mark.asyncio
async def test_active_manifest_cannot_be_cleared_without_explicit_disable(api_env):
    env = api_env
    jobs.update_job(env.job["id"], {"_r2_authority_required": True,
                                  "_r2_release_manifest": str(env.manifest)})
    before = (env.home / "cron" / "jobs.json").read_bytes()
    async with TestClient(TestServer(env.app)) as client:
        response = await _request(client, env, "update", {"r2_release_manifest": None})
        assert response.status == 400
        assert (env.home / "cron" / "jobs.json").read_bytes() == before
        response = await _request(client, env, "update", {
            "r2_authority_required": False, "r2_release_manifest": None})
        assert response.status == 200
        job = (await response.json())["job"]
        assert job["_r2_authority_required"] == "false" and job["_r2_release_manifest"] is None


@pytest.mark.asyncio
async def test_legacy_no_key_wiring_cannot_provision_authority(api_env):
    env = api_env
    env.adapter._api_key = ""
    before = (env.home / "cron" / "jobs.json").read_bytes()
    async with TestClient(TestServer(env.app)) as client:
        response = await _request(client, env, "update", {
            "r2_authority_required": True, "r2_release_manifest": str(env.manifest)}, {})
        assert response.status == 400
    assert (env.home / "cron" / "jobs.json").read_bytes() == before
