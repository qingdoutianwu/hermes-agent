"""Strict text-only policy for the existing native Telegram sender, not a client."""
import re


def delivery_admission_error():
    """Delegate to the canonical native fence; absence is not authorization."""
    try:
        from gateway.startup_isolation import require
        require("external_delivery")
    except Exception:
        return "STRICT_TELEGRAM_NATIVE_ADMISSION_UNAVAILABLE_OR_DENIED"
    return None


def _result(status, attempted, **fields):
    return {"success": status == "sent", "status": status,
            "confirmed": status == "sent", "external_attempted": attempted,
            "safe_to_auto_retry": False, **fields}


def _receipt(message, chat_id, thread_id):
    actual_chat = getattr(getattr(message, "chat", None), "id", None)
    actual_thread = getattr(message, "message_thread_id", None)
    message_id = getattr(message, "message_id", None)
    confirmed = (bool(message_id) and str(actual_chat) == str(chat_id)
                 and actual_thread == thread_id)
    return confirmed, {
        "message_id": message_id, "requested_chat_id": str(chat_id),
        "actual_chat_id": str(actual_chat) if actual_chat is not None else None,
        "chat_id": str(actual_chat) if actual_chat is not None else None,
        "requested_thread_id": thread_id, "actual_thread_id": actual_thread,
        "message_thread_id": actual_thread, "fallback_used": False,
        "thread_fallback": False,
        "route_confirmation": "response_thread" if confirmed else "unproven_response_route"}


async def send_strict_topic(token, chat_id, message, thread_id, media_files=None):
    """Reuse native formatting/request/splitting; certify every chunk's response."""
    if (not re.fullmatch(r"-?\d+", str(chat_id))
            or not str(thread_id).isdigit() or int(thread_id) <= 1 or media_files):
        return _result("failed_preconnect", False,
                       error="STRICT_TELEGRAM_EXPLICIT_TEXT_ROUTE_REQUIRED")
    denied = delivery_admission_error()
    if denied:
        return _result("failed_preconnect", False, error=denied)
    attempted, receipts = False, []
    try:
        from tools import send_message_senders as native
        from gateway.platforms.base import BasePlatformAdapter, utf16_len
        from plugins.platforms.telegram.telegram_ids import normalize_telegram_chat_id
        topic = int(thread_id)
        kwargs = native._telegram_thread_kwargs(thread_id)
        if kwargs.get("message_thread_id") != topic:
            return _result("failed_preconnect", False,
                           error="STRICT_TELEGRAM_TOPIC_MAPPING_CHANGED")
        text, mode, has_html = native._telegram_format(message)
        chunks = BasePlatformAdapter.truncate_message(text, 4096, len_fn=utf16_len) if text.strip() else []
        if not chunks:
            return _result("failed_preconnect", False, error="STRICT_TELEGRAM_EMPTY_TEXT")
        bot = native._telegram_bot(token)
        for chunk in chunks:
            # Recheck the same native admission for each external action.
            denied = delivery_admission_error()
            if denied:
                return _result("unknown" if attempted else "failed_preconnect",
                               attempted, error=denied, receipts=receipts)
            attempted = True
            response = await native._telegram_send_text_chunk(
                bot, normalize_telegram_chat_id(chat_id), chunk, mode, has_html,
                kwargs, strict_topic=True)
            confirmed, receipt = _receipt(response, chat_id, topic)
            receipts.append(receipt)
            if not confirmed:
                return _result("unknown", True, **receipt, receipts=receipts)
        return _result("sent", True, **receipts[-1], receipts=receipts)
    except Exception as exc:
        return _result("unknown" if attempted else "failed_preconnect", attempted,
                       requested_chat_id=str(chat_id), requested_thread_id=int(thread_id),
                       fallback_used=False, thread_fallback=False,
                       route_confirmation="unproven_response_route",
                       receipts=receipts, error=type(exc).__name__)
