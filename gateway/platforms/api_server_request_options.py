"""Per-request ``model_options`` decoding, lifted from ``platforms/api_server.py``.

Keeps the api_server facade under its frozen line cap (root AGENTS.md god-file
guidance: move code into a topical sibling). ``api_server.py`` re-exports
``_request_service_tier`` and ``_request_reasoning_config`` so existing importers
are unaffected; the helpers those two read stay in the facade and are imported
lazily (call time) to avoid an import cycle.
"""

from typing import Any, Dict, Optional


_CRON_AUTHORITY_FIELDS = {
    "r2_authority_required": "_r2_authority_required",
    "r2_release_manifest": "_r2_release_manifest",
}


def _cron_release_manifest(value: Any, current: dict) -> str:
    """Validate configuration, not provenance: only native Authority may grant trust.

    HTTP may reference a profile-local file, not supply release/proof JSON. Reuse
    native entry containment and manifest tuple rules; execution/owner/CAS checks
    still run independently in the worker. A management API key is an operator
    credential, not protection from a hostile same-user filesystem writer.
    """
    import hashlib
    import json
    from pathlib import Path
    from hermes_constants import get_hermes_home
    from cron.scheduler_origin_auth import _entry_path, _is_sha256, _manifest_release_tuple

    if not isinstance(value, str) or not value.strip():
        raise ValueError("r2_release_manifest must reference an existing profile-local manifest")
    try:
        path = Path(value).expanduser()
        if not path.is_absolute() or path.is_symlink():
            raise ValueError("Manifest must be absolute and not a symlink")
        path = path.resolve(strict=True)
        path.relative_to(get_hermes_home().resolve())
        manifest = json.loads(path.read_bytes())
        if not isinstance(manifest, dict):
            raise ValueError("Manifest must be an object")
        release_id, commit, tree_hash, entrypoint = _manifest_release_tuple(manifest)
        if not (release_id and commit and entrypoint and _is_sha256(tree_hash)):
            raise ValueError("Canonical release binding required")
        entry = Path(manifest.get("entry") or "")
        if not entry.is_absolute():
            entry = path.parent / entry
        entry = _entry_path({"script": str(entry)})
        if current.get("script") and entry != _entry_path(current):
            raise ValueError("Manifest must bind the stored Job entry")
        if manifest.get("entry_sha256") != hashlib.sha256(entry.read_bytes()).hexdigest():
            raise ValueError("Canonical entry digest mismatch")
        return str(path)
    except (OSError, TypeError, ValueError) as exc:
        raise ValueError("Invalid profile-local canonical release manifest") from exc


def _cron_authority_options(body: dict, *, authenticated: bool, job_id: Optional[str] = None) -> dict:
    """Single public-to-Core mapping; absent flags never opt a Job into Authority."""
    if not isinstance(body, dict) or any(key.startswith("_r2_") for key in body):
        raise ValueError("Private Authority fields are not API inputs")
    supplied = _CRON_AUTHORITY_FIELDS.keys() & body.keys()
    if not supplied:
        return {}
    if not authenticated:
        raise ValueError("Authority configuration requires API key authentication")
    if "r2_authority_required" in body and type(body["r2_authority_required"]) is not bool:
        raise ValueError("r2_authority_required must be a boolean")
    current = {}
    if job_id is not None:
        from gateway.platforms import api_server
        current = api_server._cron_get(job_id) or {}
    enabled = body.get("r2_authority_required", current.get("_r2_authority_required") == "true")
    options = {key: body[key] for key in supplied}
    if "r2_release_manifest" in options or enabled:
        from cron.scheduler_origin_auth import canonical_release_manifest
        stored = canonical_release_manifest(current) if current else None
        value = options.get("r2_release_manifest", str(stored) if stored else None)
        if value is None and not enabled:
            options["r2_release_manifest"] = None
        else:
            validated = _cron_release_manifest(value, current)
            if "r2_release_manifest" in options:
                options["r2_release_manifest"] = validated
    return {(_CRON_AUTHORITY_FIELDS[key] if job_id is not None else key): value
            for key, value in options.items()}


def _cron_create_options(body: dict, *, origin: dict, authenticated: bool) -> dict:
    """Build existing Cron create kwargs; keep HTTP validation/auth in its handler."""
    kwargs = {"prompt": body.get("prompt", ""), "schedule": (body.get("schedule") or "").strip(),
              "name": (body.get("name") or "").strip(), "deliver": body.get("deliver", "local"),
              "origin": origin}
    kwargs.update((key, body[key]) for key in ("paused", "paused_reason") if key in body)
    if body.get("skills"):
        kwargs["skills"] = body["skills"]
    if body.get("repeat") is not None:
        kwargs["repeat"] = body["repeat"]
    kwargs.update(_cron_authority_options(body, authenticated=authenticated))
    return kwargs


def _cron_update_options(body: dict, allowed_fields: set, *, authenticated: bool, job_id: str) -> dict:
    """Retain the input whitelist, then translate only its public Authority keys."""
    authority = _cron_authority_options(body, authenticated=authenticated, job_id=job_id)
    return {key: value for key, value in body.items()
            if key in allowed_fields and key not in _CRON_AUTHORITY_FIELDS} | authority


def _request_reasoning_config(model_options: Any) -> Optional[dict[str, Any]]:
    """Translate model_options (structured ``reasoning`` or legacy ``reasoning_effort``) into
    AIAgent reasoning_config; unknown effort values are ignored, never raised."""
    if not isinstance(model_options, dict):
        return None
    reasoning = model_options.get("reasoning")
    enabled: Any = None
    effort: Any = None
    if isinstance(reasoning, dict):
        enabled = reasoning.get("enabled")
        effort = reasoning.get("effort")
    effort = model_options.get("reasoning_effort", effort)
    effort_norm = str(effort).strip().lower() if effort is not None else ""
    if enabled is False or effort_norm == "none":
        return {"enabled": False}
    from gateway.platforms.api_server import _REASONING_EFFORTS

    if effort_norm in _REASONING_EFFORTS and effort_norm != "none":
        return {"enabled": True, "effort": effort_norm}
    if enabled is True:
        return {"enabled": True}
    return None


def _request_service_tier(model_options: Any) -> Any:
    """Return a per-request service_tier override or _REQUEST_OPTION_MISSING."""
    from gateway.platforms.api_server import _REQUEST_OPTION_MISSING, _clean_request_string, _coerce_request_bool

    if not isinstance(model_options, dict):
        return _REQUEST_OPTION_MISSING
    if "service_tier" in model_options:
        raw_tier = model_options.get("service_tier")
        return _clean_request_string(raw_tier) if isinstance(raw_tier, str) else raw_tier
    if "fast" in model_options:
        return "priority" if _coerce_request_bool(model_options.get("fast"), default=False) else None
    return _REQUEST_OPTION_MISSING
