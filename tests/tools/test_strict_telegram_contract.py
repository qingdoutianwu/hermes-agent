"""Strict Topic behavioral contracts with actual imports and mocked native requests."""
import argparse
import asyncio
import json
import contextlib
import io
import subprocess
import sys
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter
from hermes_cli.send_cmd import cmd_send, register_send_subparser
from hermes_cli.send_cmd import _emit_result
from tools import send_message_senders as native
from tools import send_message_tool as tool_module

class StrictTelegramContractTests(unittest.TestCase):
    def setUp(self):
        self.require = Mock()
        admission = types.ModuleType("gateway.startup_isolation")
        admission.require = self.require
        module_patch = patch.dict(sys.modules, {"gateway.startup_isolation": admission})
        module_patch.start()
        self.addCleanup(module_patch.stop)
        self.factory = Mock(return_value=object())
        self.call = AsyncMock()
        self.native_request_helper = native._send_telegram_message_with_retry
        self.splitter = Mock(side_effect=lambda text,*a,**kw:[text])
        overrides = {
            "_telegram_format":lambda s:(s,None,False), "_telegram_bot":self.factory,
            "_telegram_thread_kwargs":lambda t:{"message_thread_id":int(t)},
            "_send_telegram_message_with_retry":self.call}
        native_patch = patch.multiple(native, **overrides)
        native_patch.start()
        self.addCleanup(native_patch.stop)
        splitter_patch = patch.object(BasePlatformAdapter,"truncate_message",self.splitter)
        splitter_patch.start()
        self.addCleanup(splitter_patch.stop)
    def response(self, chat=-10042, topic=7, mid=9):
        return types.SimpleNamespace(chat=types.SimpleNamespace(id=chat),
                                     message_thread_id=topic,message_id=mid)
    def send(self, **kw):
        return asyncio.run(native._send_telegram(
            "synthetic-not-a-credential", "-10042", "synthetic",
            thread_id=7, strict_topic=True, **kw))
    def test_correct_chat_topic_receipt(self):
        self.call.return_value = self.response()
        r=self.send()
        self.assertTrue(r["confirmed"]);self.assertEqual(r["status"],"sent")
        self.assertEqual(r["requested_chat_id"],r["actual_chat_id"])
        self.assertEqual(r["requested_thread_id"],r["actual_thread_id"])
        self.assertEqual(r["message_id"],9);self.assertFalse(r["fallback_used"])
        self.assertEqual(r["route_confirmation"],"response_thread")
    def test_missing_topic_unknown(self):
        self.call.return_value=self.response(topic=None)
        r=self.send();self.assertEqual(r["status"],"unknown");self.assertFalse(r["confirmed"])
    def test_topic_mismatch_unknown(self):
        self.call.return_value=self.response(topic=8)
        self.assertEqual(self.send()["status"],"unknown")
    def test_chat_mismatch_unknown(self):
        self.call.return_value=self.response(chat=-10043)
        self.assertEqual(self.send()["status"],"unknown")
    def test_missing_message_id_unknown(self):
        self.call.return_value=self.response(mid=None)
        self.assertEqual(self.send()["status"],"unknown")
    def test_api_success_without_route_unknown(self):
        self.call.return_value=types.SimpleNamespace(message_id=9)
        r=self.send();self.assertEqual(r["status"],"unknown");self.assertFalse(r["safe_to_auto_retry"])
    def test_topic_failure_never_falls_back(self):
        self.call.side_effect=ValueError("thread not found")
        r=self.send()
        self.assertEqual(self.call.await_count,1)
        self.assertEqual(self.call.call_args.kwargs["message_thread_id"],7)
        self.assertEqual(r["status"],"unknown");self.assertFalse(r["fallback_used"])
    def test_timeout_unknown_no_retry(self):
        self.call.side_effect=TimeoutError("timeout")
        r=self.send()
        self.assertEqual(r["status"],"unknown");self.assertFalse(r["safe_to_auto_retry"])
        self.assertEqual(self.call.await_count,1)
    def test_recovery_delivery_denied_before_bot(self):
        self.require.side_effect=RuntimeError("synthetic native admission denied")
        r=self.send()
        self.assertEqual(r["status"],"failed_preconnect");self.assertFalse(r["external_attempted"])
        self.factory.assert_not_called();self.call.assert_not_awaited()
        self.require.assert_called_once_with("external_delivery")
    def test_missing_native_admission_before_bot(self):
        with patch.dict(sys.modules,{"gateway.startup_isolation":None}):
            r=self.send()
        self.assertFalse(r["external_attempted"]);self.factory.assert_not_called()
    def test_strict_media_rejected(self):
        r=self.send(media_files=[("synthetic.png",False)])
        self.assertFalse(r["external_attempted"]);self.factory.assert_not_called()
    def test_general_topic_not_silently_mapped(self):
        r=asyncio.run(native._send_telegram("synthetic","-10042","synthetic",thread_id=1,strict_topic=True))
        self.assertFalse(r["external_attempted"]);self.factory.assert_not_called()
    def test_second_chunk_not_sent_after_bad_route(self):
        self.splitter.side_effect=lambda *a,**kw:["first","second"]
        self.call.return_value=self.response(topic=8)
        r=self.send();self.assertEqual(r["status"],"unknown");self.assertEqual(self.call.await_count,1)
    def test_each_chunk_has_response_proof(self):
        base=sys.modules["gateway.platforms.base"]
        base.BasePlatformAdapter.truncate_message=lambda *a,**kw:["first","second"]
        self.call.side_effect=[self.response(mid=9),self.response(mid=10)]
        r=self.send();self.assertEqual(len(r["receipts"]),2);self.assertTrue(r["confirmed"])
    def test_legacy_topic_fallback_preserved(self):
        self.call.side_effect=[ValueError("thread not found"),self.response(topic=None)]
        kwargs={"message_thread_id":7}
        asyncio.run(native._telegram_send_text_chunk(object(),-10042,"synthetic",None,False,kwargs))
        self.assertEqual(self.call.await_count,2);self.assertNotIn("message_thread_id",kwargs)
    def test_cli_option_forwarded_to_real_tool_boundary(self):
        parser=argparse.ArgumentParser()
        register_send_subparser(parser.add_subparsers())
        args=parser.parse_args(["send","--strict-topic","--to","telegram:-10042:7","--json","synthetic"])
        with patch("hermes_cli.send_cmd._load_hermes_env"), \
             patch("tools.send_message_tool.send_message_tool",return_value='{"success":false,"status":"unknown"}') as tool:
            with self.assertRaises(SystemExit) as exit:
                cmd_send(args)
        self.assertEqual(exit.exception.code,1)
        self.assertTrue(tool.call_args.args[0]["strict_topic"])
        self.assertEqual(tool.call_args.args[0]["target"],"telegram:-10042:7")

    def test_handle_send_blocked_before_resolver_or_config(self):
        self.require.side_effect=RuntimeError("denied")
        with patch.object(tool_module,"_resolve_tool_target",side_effect=AssertionError("resolver reached")):
            r=tool_module._handle_send({"target":"telegram:-10042:7","message":"synthetic","strict_topic":True})
        if isinstance(r, str):
            r=json.loads(r)
        self.assertIn("ADMISSION",r["error"])

    def test_platform_strict_option_forwarded(self):
        forward=AsyncMock(return_value={"synthetic":True})
        with patch.object(tool_module,"_send_telegram",forward):
            asyncio.run(tool_module._send_to_platform(Platform.TELEGRAM,
                types.SimpleNamespace(token="synthetic"),"-10042","synthetic",thread_id=7,strict_topic=True))
        self.assertTrue(forward.call_args.kwargs["strict_topic"])

    def test_strict_topic_mapping_blocked_before_bot(self):
        with patch.object(native,"_telegram_thread_kwargs",return_value={}):
            r=self.send()
        self.assertFalse(r["external_attempted"])
        self.factory.assert_not_called()

    def test_strict_format_error_does_not_resend(self):
        self.call.side_effect=[ValueError("can't parse markdown"),self.response()]
        r=self.send()
        self.assertFalse(r["confirmed"])
        self.assertEqual(r["status"], "unknown")
        self.assertTrue(r["external_attempted"])
        self.assertFalse(r["safe_to_auto_retry"])
        self.assertEqual(self.call.await_count,1)
        self.assertTrue(all(c.kwargs["message_thread_id"]==7 for c in self.call.call_args_list))
        self.assertTrue(all(c.kwargs["attempts"]==1 for c in self.call.call_args_list))

    def _assert_ambiguous_once(self, error):
        # A hypothetical later success must never conceal the first UNKNOWN.
        self.call.side_effect=[error,self.response()]
        result=self.send()
        self.assertEqual(self.call.await_count,1)
        self.assertEqual(result["status"],"unknown")
        self.assertFalse(result["success"])
        self.assertFalse(result["confirmed"])
        self.assertTrue(result["external_attempted"])
        self.assertFalse(result["safe_to_auto_retry"])

    def test_r2_exact_html_timeout_reproduction(self):
        self._assert_ambiguous_once(TimeoutError("timeout retrieving HTML response"))

    def test_subprocess_timeout_html_never_retries(self):
        self._assert_ambiguous_once(subprocess.TimeoutExpired(["synthetic"], 1, output="<HTML>"))

    def test_native_timedout_html_never_retries(self):
        from telegram.error import TimedOut
        self._assert_ambiguous_once(TimedOut("HTML format parsing timed out"))

    def test_connection_reset_after_request_never_retries(self):
        self._assert_ambiguous_once(ConnectionResetError("HTML connection reset after request"))

    def test_uncertain_html_response_never_retries(self):
        self._assert_ambiguous_once(RuntimeError("HTML response: result unsupported or unknown"))

    def test_format_and_network_timeout_never_retries(self):
        self._assert_ambiguous_once(TimeoutError("parse markdown format network timeout"))

    def test_unknown_transport_exception_never_retries(self):
        self._assert_ambiguous_once(RuntimeError("unsupported format; transport state unknown"))

    def test_nth_chunk_timeout_stops_following_chunks(self):
        self.splitter.side_effect=lambda *a,**kw:["first","second","third"]
        self.call.side_effect=[self.response(mid=9),TimeoutError("HTML timeout"),self.response(mid=11)]
        result=self.send()
        self.assertEqual(self.call.await_count,2)
        self.assertEqual(result["status"],"unknown")
        self.assertFalse(result["confirmed"])
        self.assertFalse(result["safe_to_auto_retry"])
        self.assertTrue(result["external_attempted"])
        self.assertEqual([r["message_id"] for r in result["receipts"]],[9])

    def test_real_request_helper_does_not_retry_ambiguous_send(self):
        from tools.send_message_telegram_strict import send_strict_topic
        request_bot=types.SimpleNamespace(send_message=AsyncMock(
            side_effect=[TimeoutError("HTML timeout"),self.response()]))
        # Restore the actual helper; mock only the final native Bot request.
        with patch.object(native,"_telegram_bot",return_value=request_bot), \
             patch.object(native,"_send_telegram_message_with_retry",self.native_request_helper):
            result=asyncio.run(send_strict_topic("synthetic","-10042","synthetic",7))
        self.assertEqual(request_bot.send_message.await_count,1)
        self.assertEqual(result["status"],"unknown")

    def test_legacy_format_fallback_remains_unchanged(self):
        self.call.side_effect=[ValueError("can't parse markdown"),self.response()]
        kwargs={"message_thread_id":7}
        result=asyncio.run(native._telegram_send_text_chunk(
            object(),-10042,"synthetic","MarkdownV2",False,kwargs))
        self.assertEqual(result.message_id,9)
        self.assertEqual(self.call.await_count,2)
        self.assertEqual(self.call.call_args.kwargs["message_thread_id"],7)

    def test_unknown_cli_json_keeps_status_and_failure_exit(self):
        self.call.side_effect=[TimeoutError("HTML timeout"),self.response()]
        result=self.send()
        stream=io.StringIO()
        with contextlib.redirect_stdout(stream):
            exit_code=_emit_result(json.dumps(result),json_mode=True,quiet=False)
        emitted=json.loads(stream.getvalue())
        self.assertEqual(exit_code,1)
        self.assertEqual(emitted["status"],"unknown")
        self.assertFalse(emitted["success"])
        self.assertFalse(emitted["confirmed"])
        self.assertTrue(emitted["external_attempted"])
        self.assertFalse(emitted["safe_to_auto_retry"])
        self.assertEqual(self.call.await_count,1)

    def test_fence_revoked_between_chunks_blocks_future_action(self):
        self.splitter.side_effect=lambda *a,**kw:["first","second"]
        self.require.side_effect=[None,None,None,RuntimeError("revoked")]
        self.call.return_value=self.response()
        r=self.send()
        self.assertEqual(r["status"],"unknown")
        self.assertEqual(self.call.await_count,1)
        self.assertFalse(r["safe_to_auto_retry"])

if __name__=="__main__":
    unittest.main()
