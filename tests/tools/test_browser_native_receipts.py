"""Native return-path contracts; no live Browser/HTTP/transport proof is claimed."""
import hashlib
import json
from unittest.mock import Mock, patch

import pytest

from tools import browser_tool as browser
from tools import browser_tool_lifecycle as lifecycle
from tools import browser_tool_session as session


@pytest.fixture(autouse=True)
def isolated_browser_state(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(browser, "_active_sessions", {})
    monkeypatch.setattr(browser, "_session_last_activity", {})
    monkeypatch.setattr(browser, "_last_active_session_key", {})
    monkeypatch.setattr(browser, "_session_owner_homes", {})
    monkeypatch.setattr(browser, "_cleanup_failures", {})
    monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", Mock())
    monkeypatch.setattr(browser, "_is_camofox_mode", lambda: False)
    monkeypatch.setattr(browser, "_maybe_stop_recording", Mock())


def tracked():
    info = {"session_name": "h_fixture", "session_key": "run-A", "owner_task_id": "run-A",
            "bb_session_id": None, "cdp_url": None, "features": {"local": True}}
    browser._active_sessions["run-A"] = info
    return info


def test_close_result_survives_return_chain_without_becoming_exit_proof(tmp_path):
    tracked()
    ack = {"success": True, "data": {"closed": True}, "error": "secret-do-not-persist"}
    with patch.object(session, "_run_browser_command", return_value=ack) as call, \
         patch.object(browser, "_socket_safe_tmpdir", return_value=str(tmp_path)):
        result = lifecycle.cleanup_browser("run-A")
    call.assert_called_once_with("run-A", "close", [], timeout=10)
    item = result["sessions"][0]
    assert item["session_id"] == "h_fixture"
    assert item["ownership"] == "VERIFIED_TASK_BINDING"
    assert item["close_object"] == "SESSION"
    assert item["close_method_invoked"] is True
    assert item["close_result"]["success"] is True
    assert item["close_result"]["reported_closed"] is True
    assert item["close_result"]["result_sha256"] == hashlib.sha256(json.dumps(ack, sort_keys=True).encode()).hexdigest()
    assert item["native_cleanup_completed"] is True
    assert result["exit_proof"] == item["exit_proof"] == "UNPROVEN"
    assert item["process_identity"] is None
    assert result["execution_id"] is None
    assert "secret-do-not-persist" not in json.dumps(result)


@pytest.mark.parametrize("result", [{"success": False, "error": "private-secret"}, {}, None, {"success": "true"}])
def test_failed_or_malformed_close_remains_failed(tmp_path, result):
    tracked()
    with patch.object(session, "_run_browser_command", return_value=result), \
         patch.object(browser, "_socket_safe_tmpdir", return_value=str(tmp_path)):
        receipt = lifecycle.cleanup_browser("run-A")
    assert receipt["status"] == "FAILED"
    assert receipt["exit_proof"] == "UNPROVEN"
    assert receipt["sessions"][0]["failure_reason"] == "NATIVE_CLOSE_NOT_ACKNOWLEDGED"
    assert "private-secret" not in json.dumps(receipt)


def test_close_exception_survives_as_safe_error(tmp_path):
    tracked()
    with patch.object(session, "_run_browser_command", side_effect=TimeoutError("private-secret")), \
         patch.object(browser, "_socket_safe_tmpdir", return_value=str(tmp_path)):
        receipt = lifecycle.cleanup_browser("run-A")
    item = receipt["sessions"][0]
    assert item["close_error_class"] == "TimeoutError"
    assert item["status"] == "FAILED"
    assert item["native_cleanup_completed"] is True
    assert "private-secret" not in json.dumps(receipt)


def test_foreign_owner_is_not_closed_or_untracked():
    info = tracked()
    info["owner_task_id"] = "other-task"
    with patch.object(session, "_run_browser_command") as close:
        receipt = lifecycle.cleanup_browser("run-A")
    close.assert_not_called()
    lifecycle._cdp._stop_cdp_supervisor.assert_not_called()
    assert browser._active_sessions["run-A"] is info
    assert receipt["sessions"][0]["ownership"] == "REJECTED"
    assert receipt["status"] == "FAILED"


def test_generation_replaced_before_close_is_not_followed():
    tracked()
    browser._maybe_stop_recording.side_effect = lambda task: browser._active_sessions.update({task: {"owner_task_id": "foreign"}})
    with patch.object(session, "_run_browser_command") as close:
        result = lifecycle.cleanup_browser("run-A")
    close.assert_not_called()
    assert result["sessions"][0]["failure_reason"] == "SESSION_CHANGED_BEFORE_CLOSE"
    assert browser._active_sessions["run-A"]["owner_task_id"] == "foreign"


def test_untracked_does_not_invent_a_past_owned_exit():
    result = lifecycle.cleanup_browser("run-A")
    assert result["exit_proof"] == "UNPROVEN"
    assert result["sessions"][0]["session_id"] is None
    assert result["sessions"][0]["failure_reason"] == "SESSION_NOT_TRACKED_EXIT_UNPROVEN"


def test_primary_and_sidecar_receipts_keep_exact_native_ownership(tmp_path):
    tracked()
    browser._active_sessions["run-A::local"] = {"session_name": "h_local",
        "session_key": "run-A::local", "owner_task_id": "run-A", "bb_session_id": None}
    with patch.object(session, "_run_browser_command", return_value={"success": True}) as close, \
         patch.object(browser, "_socket_safe_tmpdir", return_value=str(tmp_path)):
        result = lifecycle.cleanup_browser("run-A")
    assert close.call_count == 2
    assert {r["session_id"] for r in result["sessions"]} == {"h_fixture", "h_local"}
    assert all(r["ownership"] == "VERIFIED_TASK_BINDING" for r in result["sessions"])
    assert result["exit_proof"] == "UNPROVEN"


def test_native_release_error_returns_failure_not_a_cleanup_confirmation():
    tracked()
    with patch.object(session, "_run_browser_command", return_value={"success": True}), \
         patch.object(lifecycle, "_release_session_resources", side_effect=PermissionError("private-secret")):
        result = lifecycle.cleanup_browser("run-A")
    item = result["sessions"][0]
    assert result["status"] == "FAILED"
    assert item["native_cleanup_completed"] is False
    assert item["cleanup_error_class"] == "PermissionError"
    assert item["exit_proof"] == "UNPROVEN"
    assert "private-secret" not in json.dumps(result)


def test_external_cdp_ack_only_describes_session_not_browser_process(tmp_path):
    info = tracked()
    info["cdp_url"] = "ws://fixture.invalid/devtools/browser/not-for-output"
    with patch.object(session, "_run_browser_command", return_value={"success": True}), \
         patch.object(browser, "_socket_safe_tmpdir", return_value=str(tmp_path)):
        result = lifecycle.cleanup_browser("run-A")
    assert result["sessions"][0]["close_object"] == "SESSION"
    assert result["exit_proof"] == "UNPROVEN"
    assert "ws://" not in json.dumps(result)


def navigate_with(data):
    info = tracked()
    with patch.object(browser, "_secret_url_error_normalized", side_effect=lambda url: (url, None)), \
         patch.object(browser, "_navigation_session_key", side_effect=lambda task, url: task), \
         patch.object(browser, "_url_policy_error", return_value=None), \
         patch.object(session, "_get_session_info", return_value=info), \
         patch.object(session, "_run_browser_command", return_value={"success": True, "data": data}), \
         patch.object(browser, "_maybe_start_recording"), \
         patch.object(browser, "_post_redirect_block", return_value=None), \
         patch.object(browser, "_add_navigate_warnings"), \
         patch.object(browser, "_attach_auto_snapshot"):
        return json.loads(browser.browser_navigate("https://fixture.invalid/article", task_id="run-A"))


def test_main_document_reported_fields_reach_navigation_caller():
    # Fixture fields verify propagation only; no claim today's backend emits them.
    result = navigate_with({"title": "Fixture", "url": "https://fixture.invalid/final",
        "navigation_id": "nav-1", "main_document_request_id": "doc-2", "final_http_status": 404,
        "redirect_chain_status": "COMPLETE", "request_completion": "COMPLETED"})
    receipt = result["navigation_receipt"]
    assert receipt["navigation_id"] == "nav-1"
    assert receipt["main_document_request_id"] == "doc-2"
    assert receipt["final_http_status"] == 404
    assert receipt["redirect_chain_status"] == "COMPLETE"
    assert receipt["request_completion"] == "COMPLETED"
    assert receipt["session_id"] == "h_fixture"
    assert receipt["main_document_proof"] == receipt["proxy_correlation"] == "UNPROVEN"


def test_subresource_status_or_title_cannot_supply_missing_document_fields():
    result = navigate_with({"title": "200 OK", "url": "https://fixture.invalid/article",
        "status": 200, "request_id": "image-1", "resource_type": "Image",
        "proxy": "http://127.0.0.1:7897"})
    receipt = result["navigation_receipt"]
    assert receipt["main_document_request_id"] is None
    assert receipt["final_http_status"] is None
    assert receipt["request_completion"] is None
    assert receipt["proxy_correlation"] == "UNPROVEN"


@pytest.mark.parametrize("status", [True, "200", 0, 999, None])
def test_invalid_or_missing_http_value_is_not_coerced_to_success(status):
    result = session.navigation_receipt("run-A", tracked(), {"final_http_status": status})
    assert result["final_http_status"] is None


def test_redirect_marker_does_not_supply_final_document_or_connection_identity():
    result = session.navigation_receipt("run-A", tracked(), {"redirect_chain_status": "COMPLETE",
        "redirects": [{"status": 302}], "proxy_verified": True, "final_http_status": 302})
    assert result["main_document_request_id"] is None
    assert result["final_http_status"] == 302
    assert result["proxy_correlation"] == "UNPROVEN"


def test_secret_shaped_identifiers_and_raw_transport_payload_are_not_exported():
    value = {"main_document_request_id": "https://user:secret@host/?token=secret",
             "headers": {"Authorization": "secret"}, "proxy": "http://user:secret@127.0.0.1:7897"}
    result = session.navigation_receipt("run-A", tracked(), value)
    assert result["main_document_request_id"] is None
    assert "secret" not in json.dumps(result)


def test_url_safety_rejection_does_not_emit_navigation_receipt_or_call_backend():
    with patch.object(browser, "_secret_url_error_normalized", return_value=("https://fixture.invalid", None)), \
         patch.object(browser, "_navigation_session_key", return_value="run-A"), \
         patch.object(browser, "_url_policy_error", return_value={"success": False, "error": "blocked"}), \
         patch.object(session, "_run_browser_command") as backend:
        result = json.loads(browser.browser_navigate("https://fixture.invalid", task_id="run-A"))
    backend.assert_not_called()
    assert result["success"] is False
    assert "navigation_receipt" not in result
