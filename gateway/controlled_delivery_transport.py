"""Exact-route one-call transport for Core controlled delivery.

No fallback, media, Markdown conversion, topic retry or normal helper retry.
The owning Core route resolver must supply a client verified against the permit's
profile/account identity; accepting untrusted clients is not a routing contract.
"""
from dataclasses import dataclass
from gateway.single_message_permit import ExactMessage, PermitDenied, require_transport_admission

@dataclass(frozen=True)
class ControlledTelegramTransport:
    destination_json: str
    bot: object
    channel: str = "telegram"

    async def send_once(self, message: ExactMessage):
        import json
        message.validate()
        if message.channel != self.channel or message.destination_json != self.destination_json:
            raise PermitDenied("TRANSPORT_ROUTE_MISMATCH")
        # A permit's validity or a trusted direct call cannot override silent
        # runtime context. This precedes any provider invocation.
        require_transport_admission()
        destination=json.loads(self.destination_json)
        thread=destination["thread_id"]
        if thread is not None and (not thread.isdecimal() or int(thread) <= 1):
            # Core maps General topic to no thread; permit binds that effective identity.
            raise PermitDenied("EFFECTIVE_THREAD_ID_REQUIRED")
        # Exactly one provider API call. Exceptions propagate to permit's uncertainty handling.
        result=await self.bot.send_message(
            chat_id=destination["chat_id"],text=message.text,parse_mode=None,
            **({"message_thread_id":int(thread)} if thread is not None else {}))
        message_id=getattr(result,"message_id",None)
        chat=getattr(getattr(result,"chat",None),"id",None)
        if message_id is None or chat is None or str(chat)!=destination["chat_id"]:
            return None
        return {"message_id":str(message_id),"chat_id":str(chat),
                "thread_id":destination["thread_id"],"channel":"telegram"}
