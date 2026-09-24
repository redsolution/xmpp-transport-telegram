import asyncio
import hashlib
import hmac
import logging
import secrets
import time
from typing import Awaitable, Callable, Optional
from xml.etree import ElementTree as ET

from slixmpp import ComponentXMPP
from slixmpp.exceptions import IqError
from slixmpp.jid import JID

from xmpp_transport_telegram.core.commands import ControlResponse
from xmpp_transport_telegram.runtime.config import Settings
from xmpp_transport_telegram.xmpp.groups_protocol import XabberGroupsXml
from xmpp_transport_telegram.xmpp.message_xml import XmppMessageXml
from xmpp_transport_telegram.xmpp.models import XmppIncomingMessage, XmppReplyReference
from xmpp_transport_telegram.xmpp.namespaces import (
    GROUPS_NS,
    PUBSUB_AVATAR_METADATA_NS,
    PUBSUB_EVENT_NS,
    TRANSPORT_FAKE_OUTGOING_TAG,
    TRANSPORT_TELEGRAM_NS,
)


log = logging.getLogger(__name__)


CommandHandler = Callable[
    [str, str, Callable[[str], Awaitable[None]]],
    Awaitable[ControlResponse],
]
DirectMessageHandler = Callable[[XmppIncomingMessage], Awaitable[None]]


class TelegramCommandComponent(ComponentXMPP):
    def __init__(
        self,
        settings: Settings,
        command_handler: CommandHandler,
        direct_message_handler: Optional[DirectMessageHandler] = None,
    ) -> None:
        super().__init__(
            settings.xmpp_component_jid,
            settings.xmpp_component_secret,
            settings.xmpp_component_host,
            settings.xmpp_component_port,
        )
        self.settings = settings
        self.command_handler = command_handler
        self.direct_message_handler = direct_message_handler
        self.bot_jid = "bot@%s" % settings.xmpp_component_jid
        self.transport_server_domain = settings.transport_server_domain
        self.component_domain = settings.xmpp_component_jid
        self._session_ready = asyncio.Event()
        self.add_event_handler("session_start", self._handle_session_start)
        self.add_event_handler("disconnected", self._handle_disconnected)
        self.add_event_handler("message", self._handle_message)

    async def _handle_session_start(self, _event) -> None:
        log.info("XMPP component session started for %s", self.boundjid.bare)
        self._session_ready.set()

    def _handle_disconnected(self, _event) -> None:
        self._session_ready.clear()

    async def wait_until_ready(self, timeout: int) -> None:
        await asyncio.wait_for(self._session_ready.wait(), timeout=timeout)

    def _handle_message(self, message) -> None:
        asyncio.create_task(self._handle_message_async(message))

    async def _handle_message_async(self, message) -> None:
        message_type = message["type"]
        if message_type == "error":
            return

        to_jid = JID(message["to"])
        from_jid = str(JID(message["from"]).bare)
        group_sender_jid = XmppMessageXml.group_sender_jid(message)
        if not self._is_allowed_sender_jid(from_jid):
            self._send_forbidden_error(message)
            return
        if (
            group_sender_jid is not None
            and not self._is_allowed_sender_jid(group_sender_jid)
        ):
            self._send_forbidden_error(message)
            return
        if message_type not in ("chat", "normal", ""):
            return

        body = str(message["body"] or "").strip()
        if message.xml.find(TRANSPORT_FAKE_OUTGOING_TAG) is not None:
            return

        if (
            to_jid.bare == self.bot_jid
            and group_sender_jid is None
            and self._is_transport_group_jid(from_jid)
        ):
            fallback_group_sender_jid = self._fallback_group_sender_jid(from_jid, body)
            if fallback_group_sender_jid is None:
                log.debug(
                    "Ignoring Xabber group service message from %s body_length=%s",
                    from_jid,
                    len(body),
                )
                return
            log.debug(
                "Routing Xabber group message without sender marker from=%s owner=%s body_length=%s",
                from_jid,
                fallback_group_sender_jid,
                len(body),
            )
            await self._handle_direct_message(message, from_jid, to_jid, body, fallback_group_sender_jid)
            return
        if to_jid.bare == self.bot_jid and group_sender_jid is not None:
            await self._handle_direct_message(message, from_jid, to_jid, body, group_sender_jid)
            return
        if to_jid.bare != self.bot_jid:
            await self._handle_direct_message(message, from_jid, to_jid, body, group_sender_jid)
            return
        if not body:
            return

        async def notify(reply_body: str) -> None:
            self._send_reply(message["from"], reply_body)

        try:
            response = await self.command_handler(from_jid, body, notify)
        except (RuntimeError, ValueError) as exc:
            self._send_reply(message["from"], str(exc))
            return
        except Exception:
            log.exception("Telegram command failed for %s", from_jid)
            self._send_reply(message["from"], "Telegram command failed. Try again later.")
            return
        self._send_reply(message["from"], response.body, media=response.media)

    async def _handle_direct_message(
        self,
        message,
        from_jid: str,
        to_jid: JID,
        body: str,
        group_sender_jid: Optional[str] = None,
    ) -> None:
        if self.direct_message_handler is None:
            log.debug("Ignoring direct message addressed to %s without handler", to_jid.bare)
            return
        if to_jid.bare != self.bot_jid and to_jid.domain != self.component_domain:
            log.debug("Ignoring direct message addressed outside component domain: %s", to_jid.bare)
            return
        try:
            reply_to_message_ids, body = XmppMessageXml.extract_reply_to_message_ids(message)
            reply_to_message_id = reply_to_message_ids[0] if reply_to_message_ids else None
            body, forwarded_media, forward_references = XmppMessageXml.extract_forwarded_body_media_and_references(
                message,
                body,
            )
            reference_media = XmppMessageXml.extract_media_references(message)
            body_media = XmppMessageXml.extract_body_media_urls(body, reference_media)
            media = reference_media + forwarded_media + body_media
            body = XmppMessageXml.strip_media_fallback_body(body, media).strip()
            await self.direct_message_handler(
                XmppIncomingMessage(
                    sender=from_jid,
                    recipient=to_jid.bare,
                    body=body.strip(),
                    media=media,
                    forward_references=forward_references,
                    group_sender_jid=group_sender_jid,
                    message_id=str(message["id"] or "") or None,
                    message_ids=XmppMessageXml.message_candidate_ids(message),
                    reply_to_message_id=reply_to_message_id,
                    reply_to_message_ids=reply_to_message_ids,
                )
            )
        except (RuntimeError, ValueError) as exc:
            self._send_chat(str(from_jid), str(to_jid.bare), str(exc))
        except Exception:
            log.exception("Telegram direct message failed from %s to %s", from_jid, to_jid.bare)
            self._send_chat(
                str(from_jid),
                str(to_jid.bare),
                "Telegram message failed. Try again later.",
            )

    def _send_reply(self, to_jid: str, body: str, media: tuple = ()) -> None:
        body, media_references = XmppMessageXml.body_with_media_references(body, media)
        message = self._make_chat_message(to_jid, self.bot_jid, body)
        for reference in media_references:
            message.xml.append(reference)
        message.send()

    def send_direct_message(
        self,
        to_jid: str,
        peer_id: int,
        body: str,
        message_id: Optional[str] = None,
        reply_reference: Optional[XmppReplyReference] = None,
        forward_references: tuple = (),
        media: tuple = (),
        fake_outgoing: bool = False,
    ) -> None:
        from_jid = "chat-%s@%s" % (peer_id, self.component_domain)
        log.debug(
            "Sending incoming Telegram message as XMPP stanza from=%s to=%s body_length=%s",
            from_jid,
            to_jid,
            len(body),
        )
        if reply_reference is not None:
            body = XmppMessageXml.reply_fallback_prefix(reply_reference) + body
        body, forward_reference_elements = XmppMessageXml.body_with_forward_references(body, forward_references)
        body, media_reference_elements = XmppMessageXml.body_with_media_references(body, media)
        message = self._make_chat_message(to_jid, from_jid, body)
        if message_id:
            message["id"] = message_id
            for marker in XabberGroupsXml.message_markers(message_id):
                message.xml.append(marker)
        if fake_outgoing:
            message.xml.append(ET.Element(TRANSPORT_FAKE_OUTGOING_TAG))
        if reply_reference is not None:
            message.xml.append(XmppMessageXml.reply_reference_element(reply_reference))
        for reference in forward_reference_elements:
            message.xml.append(reference)
        for reference in media_reference_elements:
            message.xml.append(reference)
        message.send()

    def send_avatar_metadata_event(
        self,
        sender: str,
        recipient: str,
        *,
        avatar_id: str,
        url: str,
        mime_type: str,
        bytes_count: int = 0,
    ) -> None:
        message = self.make_message(mfrom=sender, mto=recipient, mtype="headline")
        event = ET.Element("{%s}event" % PUBSUB_EVENT_NS)
        items = ET.SubElement(event, "{%s}items" % PUBSUB_EVENT_NS, {"node": PUBSUB_AVATAR_METADATA_NS})
        item = ET.SubElement(items, "{%s}item" % PUBSUB_EVENT_NS, {"id": avatar_id})
        metadata = ET.SubElement(item, "{%s}metadata" % PUBSUB_AVATAR_METADATA_NS)
        ET.SubElement(
            metadata,
            "info",
            {
                "bytes": str(max(bytes_count, 0)),
                "id": avatar_id,
                "type": mime_type,
                "url": url,
            },
        )
        message.xml.append(event)
        message.send()

    def send_xabber_group_message(
        self,
        sender: str,
        group_jid: str,
        body: str,
        message_id: str,
        reply_reference: Optional[XmppReplyReference] = None,
        forward_references: tuple = (),
        media: tuple = (),
        fake_outgoing: bool = False,
    ) -> None:
        if reply_reference is not None:
            body = XmppMessageXml.reply_fallback_prefix(reply_reference) + body
        body, forward_reference_elements = XmppMessageXml.body_with_forward_references(body, forward_references)
        body, media_reference_elements = XmppMessageXml.body_with_media_references(body, media)
        message = self.make_message(
            mfrom=sender,
            mto=group_jid,
            mbody=body,
            mtype="chat",
        )
        message["id"] = message_id
        for marker in XabberGroupsXml.message_markers(message_id):
            message.xml.append(marker)
        if fake_outgoing:
            message.xml.append(ET.Element(TRANSPORT_FAKE_OUTGOING_TAG))
        if reply_reference is not None:
            message.xml.append(XmppMessageXml.reply_reference_element(reply_reference))
        for reference in forward_reference_elements:
            message.xml.append(reference)
        for reference in media_reference_elements:
            message.xml.append(reference)
        message.send()

    def _send_chat(self, to_jid: str, from_jid: str, body: str) -> None:
        self._make_chat_message(to_jid, from_jid, body).send()

    def _make_chat_message(self, to_jid: str, from_jid: str, body: str):
        return self.make_message(
            mto=to_jid,
            mfrom=from_jid,
            mbody=body,
            mtype="chat",
        )

    def _is_allowed_sender_jid(self, jid: str) -> bool:
        try:
            return JID(jid).domain == self.transport_server_domain
        except ValueError:
            return False

    @staticmethod
    def _send_forbidden_error(message) -> None:
        error = message.reply(clear=False)
        error["type"] = "error"
        error["error"]["type"] = "auth"
        error["error"]["condition"] = "forbidden"
        error["error"]["text"] = "This Telegram transport is available only to local users."
        error.send()

    def _is_transport_group_jid(self, jid: str) -> bool:
        suffix = "@%s" % self.transport_server_domain
        if not jid.endswith(suffix):
            return False
        localpart = jid[: -len(suffix)]
        return localpart.startswith("telegramg-")

    def _fallback_group_sender_jid(self, group_jid: str, body: str) -> Optional[str]:
        if not body or self._looks_like_xabber_group_service_message(body):
            return None
        suffix = "@%s" % self.transport_server_domain
        if not group_jid.endswith(suffix):
            return None
        localpart = group_jid[: -len(suffix)]
        if not localpart.startswith("telegramg-"):
            return None
        payload = localpart.removeprefix("telegramg-")
        if "-" not in payload:
            return None
        owner_hex, _chat_id = payload.split("-", 1)
        try:
            owner_jid = bytes.fromhex(owner_hex).decode("utf-8")
        except ValueError:
            return None
        return owner_jid if owner_jid else None

    @staticmethod
    def _looks_like_xabber_group_service_message(body: str) -> bool:
        normalized = " ".join(body.lower().split())
        service_fragments = (
            " joined the group",
            " left the group",
            " was invited to the group",
            " was removed from the group",
        )
        return any(fragment in normalized for fragment in service_fragments)

    async def send_transport_operation(
        self,
        operation: str,
        fields: dict,
        groups: tuple = (),
        timeout: int = 10,
    ) -> str:
        timestamp = str(int(time.time()))
        nonce = secrets.token_hex(16)
        signature = self._transport_operation_signature(
            operation, fields, groups, timestamp, nonce
        )
        query = self._transport_query_element(
            operation, fields, groups, timestamp, nonce, signature
        )
        iq = self.make_iq_set(
            sub=query,
            ito=self.transport_server_domain,
            ifrom=self.component_domain,
        )
        # Server-owned roster changes go through the privileged module. The
        # Python transport records only what it asked to sync, never roster rows.
        result = await iq.send(timeout=timeout)
        query_result = result.xml.find("{%s}query" % TRANSPORT_TELEGRAM_NS)
        if query_result is None:
            return "ok"
        return query_result.attrib.get("status", "ok")

    def _transport_query_element(
        self,
        operation: str,
        fields: dict,
        groups: tuple,
        timestamp: Optional[str] = None,
        nonce: Optional[str] = None,
        signature: Optional[str] = None,
    ) -> ET.Element:
        attrs = {"op": operation}
        if timestamp is not None:
            attrs["auth-timestamp"] = timestamp
        if nonce is not None:
            attrs["auth-nonce"] = nonce
        if signature is not None:
            attrs["auth-signature"] = signature
        query = ET.Element(
            "{%s}query" % TRANSPORT_TELEGRAM_NS,
            attrs,
        )
        for name, value in fields.items():
            field = ET.SubElement(query, "field", {"name": str(name)})
            field.text = str(value)
        for group in groups:
            group_el = ET.SubElement(query, "group")
            group_el.text = str(group)
        return query

    @staticmethod
    def _canonical_auth_part(value: str) -> bytes:
        encoded = str(value).encode("utf-8")
        return str(len(encoded)).encode("ascii") + b":" + encoded + b","

    def _transport_operation_signature(
        self, operation: str, fields: dict, groups: tuple, timestamp: str, nonce: str
    ) -> str:
        values = [
            "v1", timestamp, nonce, self.component_domain,
            self.transport_server_domain, operation, str(len(fields)),
        ]
        for name in sorted(fields):
            values.extend((str(name), str(fields[name])))
        values.append(str(len(groups)))
        values.extend(str(group) for group in groups)
        canonical = b"".join(self._canonical_auth_part(value) for value in values)
        return hmac.new(
            self.settings.transport_iq_auth_secret.encode("utf-8"),
            canonical,
            hashlib.sha256,
        ).hexdigest()

    async def create_xabber_group(
        self,
        owner_jid: str,
        actor_jid: str,
        localpart: str,
        title: str,
        description: str,
        timeout: int = 10,
    ) -> str:
        iq = self.make_iq_set(
            sub=XabberGroupsXml.create_group(
                localpart=localpart,
                title=title,
                description=description,
                privacy="public",
                membership="private",
                index="none",
            ),
            ito=self.transport_server_domain,
            ifrom=actor_jid,
        )
        try:
            result = await iq.send(timeout=timeout)
        except IqError as exc:
            error = getattr(exc, "iq", None)
            if (
                error is not None
                and error.xml.find(".//{urn:ietf:params:xml:ns:xmpp-stanzas}conflict")
                is not None
            ):
                log.debug("XEP-GROUPS create found existing Telegram group localpart=%s", localpart)
                return "%s@%s" % (localpart, self.transport_server_domain)
            raise
        group = result.xml.find("{%s}group" % GROUPS_NS)
        if group is not None and group.attrib.get("jid"):
            return group.attrib["jid"]
        return "%s@%s" % (localpart, self.transport_server_domain)

    async def update_xabber_group_info(
        self,
        owner_jid: str,
        actor_jid: str,
        group_jid: str,
        title: str,
        timeout: int = 10,
    ) -> None:
        iq = self.make_iq_set(
            sub=XabberGroupsXml.update_info(title=title),
            ito=group_jid,
            ifrom=actor_jid,
        )
        await iq.send(timeout=timeout)

    async def update_xabber_group_avatar(
        self,
        owner_jid: str,
        actor_jid: str,
        group_jid: str,
        avatar_id: str,
        url: str,
        mime_type: str,
        bytes_count: int,
        timeout: int = 10,
    ) -> None:
        del owner_jid
        iq = self.make_iq_set(
            sub=XabberGroupsXml.update_info(
                avatar={
                    "bytes": bytes_count,
                    "id": avatar_id,
                    "type": mime_type,
                    "url": url,
                }
            ),
            ito=group_jid,
            ifrom=actor_jid,
        )
        await iq.send(timeout=timeout)

    async def invite_xabber_group_member(
        self,
        owner_jid: str,
        actor_jid: str,
        group_jid: str,
        member_jid: str,
        send: bool = False,
        reason: Optional[str] = None,
        timeout: int = 10,
    ) -> None:
        del owner_jid
        iq = self.make_iq_set(
            sub=XabberGroupsXml.invite(jid=member_jid, send=send, reason=reason),
            ito=group_jid,
            ifrom=actor_jid,
        )
        await iq.send(timeout=timeout)

    def send_xabber_group_invite(
        self,
        from_jid: str,
        to_jid: str,
        group_jid: str,
        reason: Optional[str] = None,
    ) -> None:
        message = self.make_message(
            mfrom=from_jid,
            mto=to_jid,
            mbody="You have been invited to the Telegram group %s." % group_jid,
            mtype="chat",
        )
        message.xml.append(XabberGroupsXml.direct_invite(group_jid, reason=reason))
        message.send()

    def join_xabber_group(self, member_jid: str, group_jid: str, nickname: str) -> None:
        subscribe = self.make_presence(
            pfrom=member_jid,
            pto=group_jid,
            ptype="subscribe",
        )
        subscribe.xml.append(XabberGroupsXml.nick(nickname))
        subscribe.send()
        self.send_presence(pfrom=member_jid, pto=group_jid, ptype="subscribed")

    def parse_component_localpart(self, jid: str) -> Optional[str]:
        suffix = "@%s" % self.component_domain
        if not jid.endswith(suffix):
            return None
        return jid[: -len(suffix)]


class XmppComponent:
    def __init__(
        self,
        settings: Settings,
        command_handler: CommandHandler,
        direct_message_handler: Optional[DirectMessageHandler] = None,
    ) -> None:
        self.settings = settings
        self.client = TelegramCommandComponent(settings, command_handler, direct_message_handler)
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        attempt = 1
        while not self._stopped.is_set():
            try:
                await self.client.connect(
                    self.settings.xmpp_component_host,
                    self.settings.xmpp_component_port,
                )
                await self.client.wait_until_ready(self.settings.xmpp_component_connect_timeout)
            except asyncio.TimeoutError:
                await self._disconnect_after_failed_start()
                log.warning(
                    "XMPP component connection attempt %s to %s:%s timed out; retrying in %s seconds",
                    attempt,
                    self.settings.xmpp_component_host,
                    self.settings.xmpp_component_port,
                    self.settings.xmpp_component_retry_interval,
                )
                attempt += 1
                await self._wait_before_retry()
                continue
            except OSError as exc:
                await self._disconnect_after_failed_start()
                log.warning(
                    "XMPP component connection attempt %s to %s:%s failed: %s; retrying in %s seconds",
                    attempt,
                    self.settings.xmpp_component_host,
                    self.settings.xmpp_component_port,
                    exc,
                    self.settings.xmpp_component_retry_interval,
                )
                attempt += 1
                await self._wait_before_retry()
                continue

            log.info("XMPP component connected as %s", self.settings.xmpp_component_jid)
            return

    async def _disconnect_after_failed_start(self) -> None:
        if self.client.is_connected():
            await self.client.disconnect()

    async def _wait_before_retry(self) -> None:
        try:
            await asyncio.wait_for(
                self._stopped.wait(),
                timeout=self.settings.xmpp_component_retry_interval,
            )
        except asyncio.TimeoutError:
            return

    async def stop(self) -> None:
        self._stopped.set()
        if self.client.is_connected():
            await self.client.disconnect()

    async def wait_stopped(self) -> None:
        await self._stopped.wait()
