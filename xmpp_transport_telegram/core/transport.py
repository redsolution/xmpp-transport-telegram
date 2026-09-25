import asyncio
import logging
import posixpath
import secrets
import shutil
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from urllib.parse import quote

from aiohttp import web
from telethon import events, types

from xmpp_transport_telegram.core.avatar_cache import AvatarCache
from xmpp_transport_telegram.core.commands import CommandService
from xmpp_transport_telegram.core.qr_store import QrCodeStore
from xmpp_transport_telegram.core.session_manager import SessionCipher
from xmpp_transport_telegram.core.state import DirectReplyContext
from xmpp_transport_telegram.runtime.config import Settings
from xmpp_transport_telegram.storage.repository import Repository
from xmpp_transport_telegram.telegram.backend import TelegramBackend
from xmpp_transport_telegram.telegram.models import TelegramDialog, TelegramForwardReference, TelegramMedia
from xmpp_transport_telegram.xmpp.component import XmppComponent
from xmpp_transport_telegram.xmpp.models import XmppForwardReference, XmppIncomingMessage, XmppReplyReference


log = logging.getLogger(__name__)

MEDIA_PROXY_CONNECT_TIMEOUT_SECONDS = 15
MEDIA_PROXY_LOOKUP_TIMEOUT_SECONDS = 15
GROUP_MEMBER_SYNC_TIMEOUT_SECONDS = 2
GROUP_ECHO_SUPPRESS_SECONDS = 60
XABBER_VOICE_MIME_TYPE = "audio/webm;codecs=opus"
MEDIA_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Expose-Headers": "Content-Length, Content-Type, Content-Disposition",
}


class TelegramTransport:
    TELEGRAM_CONTACTS_CIRCLE = "Telegram"

    def __init__(self, settings: Settings, repository: Repository) -> None:
        self.settings = settings
        self.repository = repository
        self.telegram = TelegramBackend(settings)
        self.avatar_cache = AvatarCache(
            settings.avatar_storage_dir,
            settings.avatar_base_url,
            settings.avatar_max_bytes,
        )
        self.session_cipher = SessionCipher(settings.session_encryption_key)
        self.qr_store = QrCodeStore(settings.qr_storage_dir, "%s/qr" % settings.qr_base_url)
        self.commands = CommandService(
            repository,
            self.telegram,
            self.session_cipher,
            self.qr_store,
            self._ensure_telegram_contact,
            self._replace_telegram_listener,
        )
        self.xmpp = XmppComponent(settings, self.commands.handle, self.send_direct_message)
        self._telegram_clients: Dict[str, object] = {}
        self._direct_reply_contexts: Dict[tuple, DirectReplyContext] = {}
        self._direct_reply_aliases: Dict[tuple, str] = {}
        self._group_reply_contexts: Dict[tuple, DirectReplyContext] = {}
        self._group_reply_aliases: Dict[tuple, str] = {}
        self._group_ensure_signatures: Dict[tuple, str] = {}
        self._group_protocol_members: set = set()
        self._group_echo_message_ids: Dict[tuple, datetime] = {}
        self._group_echo_bodies: Dict[tuple, datetime] = {}
        self._media_stream_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._qr_cleanup_task: Optional[asyncio.Task] = None
        self._avatar_cleanup_task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()

    async def stream_media(self, request: web.Request) -> web.StreamResponse:
        token = request.match_info["token"]
        row = await self.repository.get_media_reference(token)
        if row is None:
            raise web.HTTPNotFound()
        session_data = await self._load_connected_session_data(row["owner_jid"])
        semaphore = self._media_stream_semaphore(row["owner_jid"])
        await semaphore.acquire()
        client = self.telegram.media_client_for_session(session_data)
        log.debug(
            "Starting Telegram media proxy owner=%s peer_id=%s message_id=%s bytes=%s",
            row["owner_jid"],
            row["peer_id"],
            row["message_id"],
            row["bytes_count"],
        )
        await asyncio.wait_for(client.connect(), timeout=MEDIA_PROXY_CONNECT_TIMEOUT_SECONDS)
        response = None
        try:
            if not await client.is_user_authorized():
                raise web.HTTPNotFound()
            media = await asyncio.wait_for(
                self.telegram.get_message_media(
                    client,
                    int(row["peer_id"]),
                    str(row["message_id"]),
                ),
                timeout=MEDIA_PROXY_LOOKUP_TIMEOUT_SECONDS,
            )
            if self._is_non_downloadable_telegram_media(media):
                log.debug(
                    "Telegram media proxy found non-downloadable media owner=%s peer_id=%s message_id=%s type=%s",
                    row["owner_jid"],
                    row["peer_id"],
                    row["message_id"],
                    type(media).__name__,
                )
                raise web.HTTPNotFound()
            headers = {
                "Content-Type": row["mime_type"],
                "Content-Disposition": 'inline; filename="%s"' % self._http_header_filename(row["file_name"]),
                "Cache-Control": "private, max-age=300",
            }
            headers.update(MEDIA_CORS_HEADERS)
            if row["bytes_count"] is not None and not self._is_xabber_voice_mime_type(row["mime_type"]):
                headers["Content-Length"] = str(row["bytes_count"])
            response = web.StreamResponse(status=200, headers=headers)
            await response.prepare(request)
            if self._is_xabber_voice_mime_type(row["mime_type"]):
                await self._stream_converted_voice_media(response, client, media, row["bytes_count"])
            else:
                async for chunk in self.telegram.iter_media_download(
                    client,
                    media,
                    request_size=self.settings.media_stream_request_size,
                    file_size=row["bytes_count"],
                ):
                    await response.write(bytes(chunk))
            await response.write_eof()
            log.debug(
                "Finished Telegram media proxy owner=%s peer_id=%s message_id=%s",
                row["owner_jid"],
                row["peer_id"],
                row["message_id"],
            )
            return response
        except ConnectionResetError:
            log.debug(
                "Telegram media proxy client disconnected owner=%s peer_id=%s message_id=%s",
                row["owner_jid"],
                row["peer_id"],
                row["message_id"],
            )
            return response if response is not None else web.Response(status=204)
        except FileNotFoundError:
            raise web.HTTPNotFound()
        except asyncio.TimeoutError:
            log.warning(
                "Telegram media proxy timed out owner=%s peer_id=%s message_id=%s",
                row["owner_jid"],
                row["peer_id"],
                row["message_id"],
            )
            raise web.HTTPGatewayTimeout()
        finally:
            try:
                await client.disconnect()
            finally:
                semaphore.release()

    async def _stream_converted_voice_media(self, response: web.StreamResponse, client, media, bytes_count) -> None:
        if shutil.which("ffmpeg") is None:
            raise web.HTTPInternalServerError(reason="ffmpeg is required to convert Telegram voice messages")
        process = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            "pipe:0",
            "-c:a",
            "copy",
            "-f",
            "webm",
            "pipe:1",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        writer = asyncio.create_task(self._write_media_to_ffmpeg(process, client, media, bytes_count))
        stderr = b""
        try:
            while True:
                chunk = await process.stdout.read(65536)
                if not chunk:
                    break
                await response.write(chunk)
            await writer
            stderr = await process.stderr.read()
            returncode = await process.wait()
            if returncode != 0:
                log.warning("Telegram voice conversion failed with ffmpeg status=%s stderr=%s", returncode, stderr[:500])
                raise web.HTTPBadGateway(reason="Telegram voice conversion failed")
        finally:
            if not writer.done():
                writer.cancel()
                try:
                    await writer
                except asyncio.CancelledError:
                    pass
            if process.returncode is None:
                process.kill()
                await process.wait()

    async def _write_media_to_ffmpeg(self, process, client, media, bytes_count) -> None:
        try:
            async for chunk in self.telegram.iter_media_download(
                client,
                media,
                request_size=self.settings.media_stream_request_size,
                file_size=bytes_count,
            ):
                process.stdin.write(bytes(chunk))
                await process.stdin.drain()
        finally:
            process.stdin.close()
            await process.stdin.wait_closed()

    def _media_stream_semaphore(self, owner_jid: str) -> asyncio.Semaphore:
        semaphore = self._media_stream_semaphores.get(owner_jid)
        if semaphore is None:
            semaphore = asyncio.Semaphore(1)
            self._media_stream_semaphores[owner_jid] = semaphore
        return semaphore

    async def run_forever(self) -> None:
        await self.xmpp.start()
        self._qr_cleanup_task = asyncio.create_task(self._qr_cleanup_loop())
        self._avatar_cleanup_task = asyncio.create_task(self._avatar_cleanup_loop())
        await self._sync_connected_contacts_after_restart()
        log.info("Telegram transport backend started")
        await self._stopped.wait()

    def request_stop(self) -> None:
        """Ask the main coroutine to perform an orderly asynchronous shutdown."""
        self._stopped.set()

    async def stop(self) -> None:
        self.request_stop()
        if self._qr_cleanup_task is not None:
            self._qr_cleanup_task.cancel()
            try:
                await self._qr_cleanup_task
            except asyncio.CancelledError:
                pass
            self._qr_cleanup_task = None
        if self._avatar_cleanup_task is not None:
            self._avatar_cleanup_task.cancel()
            try:
                await self._avatar_cleanup_task
            except asyncio.CancelledError:
                pass
            self._avatar_cleanup_task = None
        await self.commands.stop()
        await self._stop_all_telegram_listeners()
        await self.xmpp.stop()

    async def send_direct_message(
        self,
        message: XmppIncomingMessage,
    ) -> None:
        xmpp_jid = message.sender
        contact_jid = message.recipient
        body = message.body
        group_route = self._bot_group_fanout_route(xmpp_jid, contact_jid, message.group_sender_jid)
        if group_route is not None:
            owner_jid, chat_id = group_route
            stripped_body = self._strip_xabber_group_sender_prefix(body, message.group_sender_jid)
            if self._consume_group_echo(owner_jid, chat_id, message.message_id, stripped_body):
                log.debug(
                    "Ignoring reflected Telegram group message owner=%s chat_id=%s message_id=%s",
                    owner_jid,
                    chat_id,
                    message.message_id,
                )
                return
            await self._send_xabber_group_message_to_telegram(
                owner_jid,
                chat_id,
                message,
                body=stripped_body,
            )
            return
        if contact_jid == self.xmpp.client.bot_jid and message.group_sender_jid is not None:
            return

        localpart = self.xmpp.client.parse_component_localpart(contact_jid)
        if localpart is not None and localpart.startswith("group-") and localpart.removeprefix("group-"):
            await self._send_xabber_group_message_to_telegram(
                xmpp_jid,
                localpart.removeprefix("group-"),
                message,
                body=body,
            )
            return

        peer_id = self._peer_id_from_contact_jid(contact_jid)
        session_data = await self._load_connected_session_data(xmpp_jid)
        await self._ensure_telegram_listener(xmpp_jid, session_data)
        client = self._telegram_clients.get(xmpp_jid)
        if client is None:
            raise RuntimeError("Telegram session expired. Send /login again.")
        log.debug(
            "Sending XMPP direct message to Telegram peer xmpp_jid=%s contact_jid=%s peer_id=%s body_length=%s",
            xmpp_jid,
            contact_jid,
            peer_id,
            len(body),
        )
        reply_to_message_id, body = self._resolve_direct_reply_payload(xmpp_jid, str(peer_id), message)
        forward_reference = None if reply_to_message_id or message.media else self._telegram_forward_reference_from_xmpp(
            xmpp_jid,
            message,
        )
        body = message.body if forward_reference is not None else self._flatten_forward_body(message, body=body)
        sent_message_id = await self.telegram.send_direct_message(
            client,
            peer_id,
            body,
            reply_to_message_id=reply_to_message_id,
            forward_reference=forward_reference,
            media=message.media,
        )
        if sent_message_id:
            self._remember_direct_reply_context(
                xmpp_jid=xmpp_jid,
                peer_id=str(peer_id),
                context=DirectReplyContext(
                    message_id=sent_message_id,
                    body=body,
                    sender=xmpp_jid,
                    recipient=contact_jid,
                    fake_outgoing=True,
                ),
            )
            if message.message_id:
                self._remember_direct_reply_alias(
                    xmpp_jid=xmpp_jid,
                    peer_id=str(peer_id),
                    source_message_id=message.message_id,
                    target_message_id=sent_message_id,
                )
        log.debug(
            "Sent XMPP direct message to Telegram peer xmpp_jid=%s peer_id=%s",
            xmpp_jid,
            peer_id,
        )

    async def _ensure_telegram_contact(self, xmpp_jid: str, contact) -> None:
        contact_jid = CommandService.contact_jid(self.settings.xmpp_component_jid, contact)
        sync_signature = CommandService.contact_sync_signature(contact)
        stored_signature = await self.repository.get_synced_roster_item_signature(
            xmpp_jid,
            contact_jid,
        )
        if stored_signature == sync_signature:
            return

        # Xabber represents contact circles as roster groups.  Keeping all
        # synced Telegram direct chats in one group makes the import
        # visible without inventing a separate server-side abstraction.
        await self.xmpp.client.send_transport_operation(
            "add-roster-contact",
            {
                "owner_jid": xmpp_jid,
                "contact_jid": contact_jid,
                "name": contact.title,
                "create_chat": "true",
            },
            groups=(self.TELEGRAM_CONTACTS_CIRCLE,),
        )
        if contact.avatar is not None:
            cached_avatar = await self.avatar_cache.store(
                self.repository,
                owner_jid=xmpp_jid,
                contact_jid=contact_jid,
                peer_id=contact.peer_id,
                avatar=contact.avatar,
            )
            if cached_avatar is not None:
                self.xmpp.client.send_avatar_metadata_event(
                    sender=contact_jid,
                    recipient=xmpp_jid,
                    avatar_id=cached_avatar.avatar_id,
                    url=cached_avatar.url,
                    mime_type=cached_avatar.mime_type,
                    bytes_count=cached_avatar.bytes_count,
                )
        elif contact.avatar_photo_id is None and not contact.avatar_download_failed:
            await self.repository.delete_contact_avatar(xmpp_jid, contact_jid)
        if contact.avatar_download_failed:
            return
        await self.repository.set_synced_roster_item_signature(
            xmpp_jid,
            contact_jid,
            "contact",
            sync_signature,
        )

    async def _avatar_cleanup_loop(self) -> None:
        interval = max(self.settings.avatar_cleanup_interval_seconds, 1)
        ttl_days = max(self.settings.avatar_unreferenced_ttl_days, 0)
        while not self._stopped.is_set():
            try:
                removed = await self.avatar_cache.cleanup_unreferenced(self.repository, ttl_days)
                if removed:
                    log.info("Removed %s unreferenced Telegram avatar cache file(s)", removed)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Telegram avatar cache cleanup failed")
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    async def _qr_cleanup_loop(self) -> None:
        interval = max(self.settings.qr_cleanup_interval_seconds, 1)
        max_age_seconds = max(self.settings.qr_max_age_seconds, 0)
        while not self._stopped.is_set():
            try:
                removed = self.qr_store.cleanup(
                    self.settings.qr_storage_dir,
                    max_age_seconds=max_age_seconds,
                )
                if removed:
                    log.info("Removed %s expired Telegram login QR file(s)", removed)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Telegram login QR cleanup failed")
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    async def _ensure_telegram_group_chat(self, xmpp_jid: str, chat: TelegramDialog) -> None:
        group_jid = self._group_jid(str(chat.peer_id), xmpp_jid)
        sync_signature = self._group_sync_signature(chat)
        group_key = (xmpp_jid, group_jid)
        if self._group_ensure_signatures.get(group_key) == sync_signature:
            await self._ensure_group_protocol_owner_member(
                owner_jid=xmpp_jid,
                group_jid=group_jid,
            )
            return
        stored_signature = await self.repository.get_synced_roster_item_signature(xmpp_jid, group_jid)
        if stored_signature == sync_signature:
            await self._ensure_group_protocol_owner_member(
                owner_jid=xmpp_jid,
                group_jid=group_jid,
            )
            self._group_ensure_signatures[group_key] = sync_signature
            return

        transport_member_jid = self._group_transport_member_jid()
        created_group_jid = await self.xmpp.client.create_xabber_group(
            owner_jid=xmpp_jid,
            actor_jid=transport_member_jid,
            localpart=self._group_localpart(xmpp_jid, str(chat.peer_id)),
            title=chat.title,
            description="Telegram group %s" % chat.peer_id,
        )
        if created_group_jid != group_jid:
            log.warning(
                "XEP-GROUPS create returned unexpected Telegram group_jid=%s expected=%s",
                created_group_jid,
                group_jid,
            )
        await self._ensure_group_protocol_owner_member(
            owner_jid=xmpp_jid,
            group_jid=group_jid,
        )
        await self._sync_telegram_group_avatar(xmpp_jid, group_jid, chat)
        await self.repository.set_synced_roster_item_signature(
            xmpp_jid,
            group_jid,
            "group",
            sync_signature,
        )
        self._group_ensure_signatures[group_key] = sync_signature

    async def _sync_telegram_group_avatar(self, xmpp_jid: str, group_jid: str, chat: TelegramDialog) -> None:
        if chat.avatar is not None:
            cached_avatar = await self.avatar_cache.store(
                self.repository,
                owner_jid=xmpp_jid,
                contact_jid=group_jid,
                peer_id=chat.peer_id,
                avatar=chat.avatar,
            )
            if cached_avatar is not None:
                try:
                    await self.xmpp.client.update_xabber_group_avatar(
                        owner_jid=xmpp_jid,
                        actor_jid=self._group_transport_member_jid(),
                        group_jid=group_jid,
                        avatar_id=cached_avatar.avatar_id,
                        url=cached_avatar.url,
                        mime_type=cached_avatar.mime_type,
                        bytes_count=cached_avatar.bytes_count,
                        timeout=2,
                    )
                except Exception as exc:
                    log.warning(
                        "XEP-GROUPS avatar update failed; continuing Telegram group sync owner=%s group_jid=%s error=%s",
                        xmpp_jid,
                        group_jid,
                        exc,
                    )
        elif chat.avatar_photo_id is None and not chat.avatar_download_failed:
            await self.repository.delete_contact_avatar(xmpp_jid, group_jid)

    async def _ensure_group_protocol_owner_member(self, owner_jid: str, group_jid: str) -> None:
        invited = await self._ensure_group_protocol_member(
            owner_jid=owner_jid,
            group_jid=group_jid,
            member_jid=owner_jid,
            nickname=owner_jid.split("@", 1)[0],
            auto_join=False,
        )
        if not invited:
            return
        self.xmpp.client.send_xabber_group_invite(
            from_jid=self._group_transport_member_jid(),
            to_jid=owner_jid,
            group_jid=group_jid,
            reason="Telegram group member",
        )

    async def _ensure_group_protocol_member(
        self,
        owner_jid: str,
        group_jid: str,
        member_jid: str,
        nickname: str,
        auto_join: bool,
    ) -> bool:
        if member_jid == self._group_transport_member_jid():
            return False
        member_key = (owner_jid, group_jid, member_jid)
        if member_key in self._group_protocol_members:
            return False
        try:
            await self.xmpp.client.invite_xabber_group_member(
                owner_jid=owner_jid,
                actor_jid=self._group_transport_member_jid(),
                group_jid=group_jid,
                member_jid=member_jid,
                send=False,
                reason="Telegram group member",
                timeout=GROUP_MEMBER_SYNC_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            if self._is_group_member_already_invited_error(exc):
                log.debug(
                    "XEP-GROUPS member already invited owner=%s group=%s member=%s",
                    owner_jid,
                    group_jid,
                    member_jid,
                )
                if auto_join:
                    self.xmpp.client.join_xabber_group(
                        member_jid=member_jid,
                        group_jid=group_jid,
                        nickname=nickname,
                    )
                self._group_protocol_members.add(member_key)
                return True
            log.debug(
                "XEP-GROUPS invite returned non-fatal result owner=%s group=%s member=%s error=%s",
                owner_jid,
                group_jid,
                member_jid,
                exc,
            )
            return False
        if auto_join:
            self.xmpp.client.join_xabber_group(
                member_jid=member_jid,
                group_jid=group_jid,
                nickname=nickname,
            )
        self._group_protocol_members.add(member_key)
        return True

    @staticmethod
    def _is_group_member_already_invited_error(exc: Exception) -> bool:
        iq = getattr(exc, "iq", None)
        xml = getattr(iq, "xml", None)
        if xml is None:
            return False
        conflict = xml.find(".//{urn:ietf:params:xml:ns:xmpp-stanzas}conflict")
        if conflict is None:
            return False
        text = xml.find(".//{urn:ietf:params:xml:ns:xmpp-stanzas}text")
        return text is not None and "already invited" in (text.text or "").lower()

    async def _sync_connected_contacts_after_restart(self) -> None:
        sessions = await self.repository.list_connected_telegram_sessions()
        if not sessions:
            return

        log.info("Synchronizing Telegram direct chats after restart for %s account(s)", len(sessions))
        for session in sessions:
            xmpp_jid = session["xmpp_jid"]
            try:
                synced_count = await self._sync_contacts_for_stored_session(
                    xmpp_jid,
                    session["encrypted_session"],
                )
            except Exception:
                log.exception("Telegram restart contact sync failed for %s", xmpp_jid)
            else:
                log.info(
                    "Synchronized %s Telegram direct chat(s) after restart for %s",
                    synced_count,
                    xmpp_jid,
                )

    async def _sync_contacts_for_stored_session(self, xmpp_jid: str, encrypted_session: str) -> int:
        session_data = self.session_cipher.decrypt(encrypted_session)
        client = self.telegram.client_for_session(session_data)
        await client.connect()
        synced_count = 0
        try:
            if not await client.is_user_authorized():
                log.warning("Skipping restart contact sync for expired Telegram session %s", xmpp_jid)
                return 0
            contacts = await self.telegram.list_contacts(client, include_avatars=False)
            # Reuse the same idempotent roster write path used by /add, /sync-contacts,
            # and first login so restart recovery cannot create duplicate roster churn.
            for contact in contacts:
                try:
                    await self._ensure_telegram_contact(xmpp_jid, contact)
                except Exception:
                    log.exception(
                        "Telegram restart contact sync failed for %s contact_id=%s",
                        xmpp_jid,
                        getattr(contact, "peer_id", "unknown"),
                    )
                else:
                    synced_count += 1
        finally:
            await client.disconnect()
        await self._start_telegram_listener(xmpp_jid, session_data)
        return synced_count

    async def _authorized_client(self, xmpp_jid: str):
        session_data = await self._load_connected_session_data(xmpp_jid)
        return await self._authorized_client_for_session(session_data)

    async def _load_connected_session_data(self, xmpp_jid: str) -> str:
        account_id = await self.repository.ensure_xmpp_account(xmpp_jid)
        row = await self.repository.get_telegram_session(account_id)
        if row is None or not row["connected"] or not row["encrypted_session"]:
            raise RuntimeError("Telegram is not connected. Send /login first.")
        return self.session_cipher.decrypt(row["encrypted_session"])

    async def _authorized_client_for_session(self, session_data: str):
        client = self.telegram.client_for_session(session_data)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError("Telegram session expired. Send /login again.")
        return client

    async def _ensure_telegram_listener(self, xmpp_jid: str, session_data: str) -> None:
        client = self._telegram_clients.get(xmpp_jid)
        if client is not None and getattr(client, "is_connected", lambda: True)():
            log.debug("Telegram listener already active for %s", xmpp_jid)
            return
        log.debug("Telegram listener missing or disconnected for %s; starting it", xmpp_jid)
        await self._start_telegram_listener(xmpp_jid, session_data)

    async def _replace_telegram_listener(
        self,
        xmpp_jid: str,
        session_data: str,
        previous_owner: Optional[str],
    ) -> None:
        if previous_owner and previous_owner != xmpp_jid:
            await self._stop_telegram_listener(previous_owner)
        await self._start_telegram_listener(xmpp_jid, session_data)

    async def _start_telegram_listener(self, xmpp_jid: str, session_data: str) -> None:
        log.debug("Starting Telegram direct-message listener for %s", xmpp_jid)
        await self._stop_telegram_listener(xmpp_jid)
        listener_started_at = datetime.now(timezone.utc)
        client = self.telegram.client_for_session(session_data)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            log.warning("Skipping Telegram listener for expired session %s", xmpp_jid)
            return

        async def handle_event(event) -> None:
            try:
                if self._is_stale_telegram_event(event, listener_started_at):
                    return
                await self._handle_incoming_telegram_message(xmpp_jid, event)
            except Exception:
                log.exception("Telegram incoming message handler failed for %s", xmpp_jid)

        client.add_event_handler(handle_event, events.NewMessage())
        self._telegram_clients[xmpp_jid] = client
        log.info("Started Telegram message listener for %s", xmpp_jid)

    @classmethod
    def _is_stale_telegram_event(cls, event, listener_started_at: datetime) -> bool:
        event_date = cls._telegram_event_date(event)
        if event_date is None:
            return False
        if event_date.tzinfo is None:
            event_date = event_date.replace(tzinfo=timezone.utc)
        return event_date < listener_started_at

    @staticmethod
    def _telegram_event_date(event):
        event_date = getattr(event, "date", None)
        if event_date is not None:
            return event_date
        message = getattr(event, "message", None)
        return getattr(message, "date", None)

    async def _stop_telegram_listener(self, xmpp_jid: str) -> None:
        client = self._telegram_clients.pop(xmpp_jid, None)
        if client is not None:
            log.debug("Stopping Telegram direct-message listener for %s", xmpp_jid)
            await client.disconnect()

    async def _stop_all_telegram_listeners(self) -> None:
        xmpp_jids = list(self._telegram_clients)
        for xmpp_jid in xmpp_jids:
            await self._stop_telegram_listener(xmpp_jid)

    async def _handle_incoming_telegram_message(self, xmpp_jid: str, event) -> None:
        raw_text = str(getattr(event, "raw_text", "") or "")
        log.debug(
            "Received Telegram NewMessage event xmpp_jid=%s out=%s is_private=%s chat_id=%s sender_id=%s raw_text_length=%s",
            xmpp_jid,
            getattr(event, "out", None),
            getattr(event, "is_private", None),
            getattr(event, "chat_id", None),
            getattr(event, "sender_id", None),
            len(raw_text),
        )
        is_outgoing = getattr(event, "out", False)
        is_group_chat = self._is_telegram_group_chat_event(event)
        peer_id = self._peer_id_from_incoming_event(event)
        if is_group_chat and self._is_telegram_group_avatar_update_event(event):
            if peer_id is None:
                log.debug("Ignoring Telegram group avatar update without peer id for %s", xmpp_jid)
                return
            await self._handle_telegram_group_avatar_update(xmpp_jid, event, int(peer_id))
            return
        body = raw_text.strip()
        media = await self._media_reference_from_event(xmpp_jid, event)
        if not body and media is None:
            log.debug("Ignoring Telegram event without text body or media for %s", xmpp_jid)
            return
        if peer_id is None:
            log.debug("Ignoring Telegram message without peer id for %s", xmpp_jid)
            return
        if is_group_chat:
            await self._handle_incoming_telegram_group_message(xmpp_jid, event, int(peer_id), body)
            return
        if is_outgoing:
            log.debug(
                "Ignoring outgoing Telegram direct message for XMPP xmpp_jid=%s peer_id=%s",
                xmpp_jid,
                peer_id,
            )
            return
        log.debug(
            "Delivering incoming Telegram message to XMPP xmpp_jid=%s peer_id=%s body_length=%s",
            xmpp_jid,
            peer_id,
            len(body),
        )
        message_id = self._incoming_telegram_message_id(event)
        reply_reference = self._direct_reply_reference(
            xmpp_jid,
            str(peer_id),
            self._incoming_telegram_reply_to_message_id(event),
        )
        forward_reference = self._forward_reference_from_telegram_event(
            event,
            fallback_recipient=xmpp_jid,
        )
        outgoing_body = "" if forward_reference is not None else body
        fake_outgoing = bool(is_outgoing)
        self.xmpp.client.send_direct_message(
            xmpp_jid,
            int(peer_id),
            outgoing_body,
            message_id=message_id,
            reply_reference=reply_reference,
            forward_references=(forward_reference,) if forward_reference is not None else (),
            media=(media,) if media is not None else (),
            fake_outgoing=fake_outgoing,
        )
        contact_jid = "chat-%s@%s" % (peer_id, self.settings.xmpp_component_jid)
        self._remember_direct_reply_context(
            xmpp_jid=xmpp_jid,
            peer_id=str(peer_id),
            context=DirectReplyContext(
                message_id=message_id,
                body=body,
                sender=xmpp_jid if fake_outgoing else contact_jid,
                recipient=contact_jid if fake_outgoing else xmpp_jid,
                fake_outgoing=fake_outgoing,
            ),
        )
        log.debug(
            "Delivered incoming Telegram message to XMPP xmpp_jid=%s peer_id=%s",
            xmpp_jid,
            peer_id,
        )

    @staticmethod
    def _peer_id_from_incoming_event(event):
        peer_id = getattr(event, "chat_id", None)
        if peer_id is not None:
            return peer_id
        return getattr(event, "sender_id", None)

    @staticmethod
    def _is_telegram_group_chat_event(event) -> bool:
        if getattr(event, "is_group", False) is True:
            return True
        if getattr(event, "is_channel", False) is True:
            return True
        if getattr(event, "is_private", None) is False:
            return True
        chat_id = getattr(event, "chat_id", None)
        return isinstance(chat_id, int) and chat_id < 0

    @staticmethod
    def _is_telegram_group_avatar_update_event(event) -> bool:
        message = getattr(event, "message", None)
        action = getattr(message, "action", None)
        if action is None:
            return False
        if isinstance(action, (types.MessageActionChatEditPhoto, types.MessageActionChatDeletePhoto)):
            return True
        return action.__class__.__name__ in (
            "MessageActionChatEditPhoto",
            "MessageActionChatDeletePhoto",
        )

    async def _handle_telegram_group_avatar_update(self, xmpp_jid: str, event, peer_id: int) -> None:
        group_jid = self._group_jid(str(peer_id), xmpp_jid)
        stored_signature = await self.repository.get_synced_roster_item_signature(xmpp_jid, group_jid)
        if stored_signature is None:
            log.debug(
                "Ignoring Telegram group avatar update for unsynced group xmpp_jid=%s group_id=%s",
                xmpp_jid,
                peer_id,
            )
            return
        chat = await self._telegram_group_dialog_for_event(xmpp_jid, event, peer_id)
        await self._sync_telegram_group_avatar(xmpp_jid, group_jid, chat)
        if chat.avatar_download_failed:
            return
        await self.repository.set_synced_roster_item_signature(
            xmpp_jid,
            group_jid,
            "group",
            self._group_sync_signature(chat),
        )

    async def _handle_incoming_telegram_group_message(
        self,
        xmpp_jid: str,
        event,
        peer_id: int,
        body: str,
    ) -> None:
        chat = await self._telegram_group_dialog_for_event(xmpp_jid, event, peer_id)
        await self._ensure_telegram_group_chat(xmpp_jid, chat)
        if getattr(event, "out", False):
            sender = self._group_transport_member_jid()
            sender_name = self._group_sender_nickname(sender)
        else:
            sender_id = getattr(event, "sender_id", None)
            if sender_id is not None:
                sender = self._telegram_user_jid(int(sender_id))
            else:
                sender = self._group_transport_member_jid()
            sender_name = await self._telegram_group_sender_name(event, sender)
        group_jid = self._group_jid(str(peer_id), xmpp_jid)
        await self._ensure_group_protocol_member(
            owner_jid=xmpp_jid,
            group_jid=group_jid,
            member_jid=sender,
            nickname=sender_name,
            auto_join=sender.endswith("@%s" % self.settings.xmpp_component_jid),
        )
        media = await self._media_reference_from_event(xmpp_jid, event)
        forward_reference = self._forward_reference_from_telegram_event(
            event,
            fallback_recipient=self._group_jid(str(peer_id), xmpp_jid),
        )
        if forward_reference is not None:
            body = ""
        context_body = forward_reference.body if forward_reference is not None else body
        message_id = self._incoming_telegram_message_id(event)
        reply_reference = self._group_reply_reference(
            xmpp_jid,
            str(peer_id),
            self._incoming_telegram_reply_to_message_id(event),
        )
        log.debug(
            "Delivering incoming Telegram group message to Xabber xmpp_jid=%s group_jid=%s sender=%s body_length=%s",
            xmpp_jid,
            group_jid,
            sender,
            len(body),
        )
        self.xmpp.client.send_xabber_group_message(
            sender=sender,
            group_jid=group_jid,
            body=body,
            message_id=message_id,
            reply_reference=reply_reference,
            forward_references=(forward_reference,) if forward_reference is not None else (),
            media=(media,) if media is not None else (),
            fake_outgoing=True,
        )
        self._remember_group_reply_context(
            xmpp_jid=xmpp_jid,
            peer_id=str(peer_id),
            context=DirectReplyContext(
                message_id=message_id,
                body=context_body,
                sender=sender,
                recipient=group_jid,
                fake_outgoing=True,
            ),
        )
        self._remember_group_echo(
            xmpp_jid=xmpp_jid,
            peer_id=str(peer_id),
            message_id=message_id,
            body=context_body,
        )

    async def _send_xabber_group_message_to_telegram(
        self,
        xmpp_jid: str,
        chat_id: str,
        message: XmppIncomingMessage,
        body: str,
    ) -> None:
        session_data = await self._load_connected_session_data(xmpp_jid)
        await self._ensure_telegram_listener(xmpp_jid, session_data)
        client = self._telegram_clients.get(xmpp_jid)
        if client is None:
            raise RuntimeError("Telegram session expired. Send /login again.")
        log.debug(
            "Sending Xabber group message to Telegram xmpp_jid=%s chat_id=%s body_length=%s",
            xmpp_jid,
            chat_id,
            len(body),
        )
        reply_to_message_id, body = self._resolve_group_reply_payload(xmpp_jid, chat_id, message, body)
        forward_reference = None if reply_to_message_id or message.media else self._telegram_forward_reference_from_xmpp(
            xmpp_jid,
            message,
        )
        body = "" if forward_reference is not None else self._flatten_forward_body(message, body=body)
        sent_message_id = await self.telegram.send_group_message(
            client,
            int(chat_id),
            body,
            reply_to_message_id=reply_to_message_id,
            forward_reference=forward_reference,
            media=message.media,
        )
        if sent_message_id:
            group_jid = self._group_jid(chat_id, xmpp_jid)
            self._remember_group_reply_context(
                xmpp_jid=xmpp_jid,
                peer_id=chat_id,
                context=DirectReplyContext(
                    message_id=sent_message_id,
                    body=body,
                    sender=xmpp_jid,
                    recipient=group_jid,
                    fake_outgoing=True,
                ),
            )
            if message.message_id:
                self._remember_group_reply_alias(
                    xmpp_jid=xmpp_jid,
                    peer_id=chat_id,
                    source_message_id=message.message_id,
                    target_message_id=sent_message_id,
                )

    async def _telegram_group_dialog_for_event(self, xmpp_jid: str, event, peer_id: int) -> TelegramDialog:
        title = None
        chat_entity = None
        get_chat = getattr(event, "get_chat", None)
        if get_chat is not None:
            chat_entity = await get_chat()
            title = getattr(chat_entity, "title", None) or getattr(chat_entity, "username", None)
        avatar = None
        avatar_photo_id = None
        avatar_download_failed = False
        client = self._telegram_clients.get(xmpp_jid) or getattr(event, "client", None)
        if client is not None and chat_entity is not None:
            avatar, avatar_photo_id, avatar_download_failed = await self.telegram.small_avatar(
                client,
                chat_entity,
            )
        return TelegramDialog(
            peer_id=peer_id,
            title=title or "Telegram group %s" % peer_id,
            is_group=True,
            is_channel=bool(getattr(event, "is_channel", False)),
            avatar=avatar,
            avatar_photo_id=avatar_photo_id,
            avatar_download_failed=avatar_download_failed,
        )

    def _bot_group_fanout_route(
        self,
        sender_jid: str,
        recipient_jid: str,
        group_sender_jid: Optional[str],
    ) -> Optional[tuple]:
        if recipient_jid != self.xmpp.client.bot_jid:
            return None
        sender_suffix = "@%s" % self.settings.transport_server_domain
        if not sender_jid.endswith(sender_suffix):
            return None
        sender_localpart = sender_jid[: -len(sender_suffix)]
        if not sender_localpart.startswith("telegramg-"):
            return None
        if group_sender_jid is None:
            return None
        if group_sender_jid == self._group_transport_member_jid():
            return None
        if group_sender_jid.endswith("@%s" % self.settings.xmpp_component_jid):
            return None
        route = self._parse_group_jid_localpart(sender_localpart)
        if route is None:
            return None
        owner_jid, _chat_id = route
        if group_sender_jid != owner_jid:
            return None
        return route

    def _remember_group_echo(self, xmpp_jid: str, peer_id: str, message_id: str, body: str) -> None:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=GROUP_ECHO_SUPPRESS_SECONDS)
        if message_id:
            self._group_echo_message_ids[(xmpp_jid, peer_id, message_id)] = expires_at
        normalized_body = self._normalized_group_echo_body(body)
        if normalized_body:
            self._group_echo_bodies[(xmpp_jid, peer_id, normalized_body)] = expires_at

    def _consume_group_echo(
        self,
        xmpp_jid: str,
        peer_id: str,
        message_id: Optional[str],
        body: str,
    ) -> bool:
        if message_id and self._consume_group_echo_key(self._group_echo_message_ids, (xmpp_jid, peer_id, message_id)):
            return True
        normalized_body = self._normalized_group_echo_body(body)
        if normalized_body and self._consume_group_echo_key(self._group_echo_bodies, (xmpp_jid, peer_id, normalized_body)):
            return True
        return False

    @staticmethod
    def _consume_group_echo_key(store: Dict[tuple, datetime], key: tuple) -> bool:
        expires_at = store.pop(key, None)
        if expires_at is None:
            return False
        return expires_at >= datetime.now(timezone.utc)

    @staticmethod
    def _normalized_group_echo_body(body: str) -> str:
        return str(body or "").strip()

    @staticmethod
    def _incoming_telegram_message_id(event) -> str:
        message = getattr(event, "message", None)
        message_id = getattr(message, "id", None) if message is not None else None
        if message_id is None:
            message_id = getattr(event, "id", None)
        return str(message_id) if message_id is not None else "telegram-message"

    @staticmethod
    def _incoming_telegram_reply_to_message_id(event) -> Optional[str]:
        message = getattr(event, "message", None)
        reply_to_msg_id = getattr(message, "reply_to_msg_id", None) if message is not None else None
        if reply_to_msg_id is None and message is not None:
            reply_to = getattr(message, "reply_to", None)
            reply_to_msg_id = getattr(reply_to, "reply_to_msg_id", None) if reply_to is not None else None
        return str(reply_to_msg_id) if reply_to_msg_id is not None else None

    async def _media_reference_from_event(self, xmpp_jid: str, event) -> Optional[TelegramMedia]:
        message = getattr(event, "message", None)
        media = getattr(message, "media", None) if message is not None else None
        if media is None:
            return None
        if not self._is_downloadable_telegram_media(message):
            log.debug(
                "Ignoring non-downloadable Telegram media preview type=%s",
                type(media).__name__,
            )
            return None
        peer_id = self._peer_id_from_incoming_event(event)
        if peer_id is None:
            return None
        message_id = self._incoming_telegram_message_id(event)
        file_info = getattr(message, "file", None)
        voice = self._is_telegram_voice_message(message)
        mime_type = XABBER_VOICE_MIME_TYPE if voice else (
            getattr(file_info, "mime_type", None) or self._telegram_media_mime_type(message)
        )
        file_name = self._safe_media_filename(
            getattr(file_info, "name", None),
            mime_type,
            message_id,
        )
        bytes_count = None if voice else getattr(file_info, "size", None)
        width = getattr(file_info, "width", None)
        height = getattr(file_info, "height", None)
        duration = self._telegram_media_duration(message, file_info)
        token = secrets.token_urlsafe(24)
        url = "%s/media/%s/%s" % (
            self.settings.media_base_url,
            token,
            quote(file_name),
        )
        await self.repository.create_media_reference(
            token=token,
            owner_jid=xmpp_jid,
            peer_id=int(peer_id),
            message_id=message_id,
            file_name=file_name,
            mime_type=mime_type,
            bytes_count=bytes_count,
            width=width,
            height=height,
        )
        return TelegramMedia(
            url=url,
            name=file_name,
            mime_type=mime_type,
            size=bytes_count,
            width=width,
            height=height,
            duration=duration,
            voice=voice,
        )

    @staticmethod
    def _is_downloadable_telegram_media(message) -> bool:
        if getattr(message, "file", None) is not None:
            return True
        if getattr(message, "photo", None) is not None:
            return True
        if getattr(message, "document", None) is not None:
            return True
        return TelegramTransport._is_telegram_voice_message(message)

    @staticmethod
    def _is_non_downloadable_telegram_media(media) -> bool:
        return isinstance(media, types.MessageMediaWebPage) or media.__class__.__name__ == "MessageMediaWebPage"

    @staticmethod
    def _telegram_media_mime_type(message) -> str:
        if getattr(message, "photo", None) is not None:
            return "image/jpeg"
        if TelegramTransport._is_telegram_voice_message(message):
            return XABBER_VOICE_MIME_TYPE
        return "application/octet-stream"

    @staticmethod
    def _is_xabber_voice_mime_type(mime_type: str) -> bool:
        return str(mime_type or "").replace(" ", "").lower() == XABBER_VOICE_MIME_TYPE

    @staticmethod
    def _is_telegram_voice_message(message) -> bool:
        if getattr(message, "voice", None) is not None:
            return True
        document = getattr(message, "document", None)
        for attr in getattr(document, "attributes", ()) or ():
            if isinstance(attr, types.DocumentAttributeAudio) and bool(getattr(attr, "voice", False)):
                return True
        return False

    @staticmethod
    def _telegram_media_duration(message, file_info) -> Optional[int]:
        duration = getattr(file_info, "duration", None)
        if duration is None:
            document = getattr(message, "document", None)
            for attr in getattr(document, "attributes", ()) or ():
                if isinstance(attr, types.DocumentAttributeAudio):
                    duration = getattr(attr, "duration", None)
                    break
        try:
            duration_int = int(round(float(duration)))
        except (TypeError, ValueError):
            return None
        return duration_int if duration_int > 0 else None

    @staticmethod
    def _safe_media_filename(name, mime_type: str, message_id: str) -> str:
        value = posixpath.basename(str(name or "").strip())
        if value in ("", ".", ".."):
            if mime_type == "image/jpeg":
                extension = ".jpg"
            elif TelegramTransport._is_xabber_voice_mime_type(mime_type):
                extension = ".webm"
            else:
                extension = ".bin"
            value = "telegram-%s%s" % (message_id, extension)
        return "".join(char if char.isalnum() or char in "._- " else "_" for char in value)

    @staticmethod
    def _http_header_filename(name) -> str:
        return str(name).replace("\\", "_").replace('"', "_")

    def _remember_direct_reply_context(
        self,
        xmpp_jid: str,
        peer_id: str,
        context: DirectReplyContext,
    ) -> None:
        self._direct_reply_contexts[(xmpp_jid, peer_id, context.message_id)] = context
        self._remember_direct_reply_alias(
            xmpp_jid=xmpp_jid,
            peer_id=peer_id,
            source_message_id=context.message_id,
            target_message_id=context.message_id,
        )

    def _remember_direct_reply_alias(
        self,
        xmpp_jid: str,
        peer_id: str,
        source_message_id: str,
        target_message_id: str,
    ) -> None:
        self._direct_reply_aliases[(xmpp_jid, peer_id, source_message_id)] = target_message_id

    def _resolve_direct_reply_target(
        self,
        xmpp_jid: str,
        peer_id: str,
        message: XmppIncomingMessage,
    ) -> Optional[str]:
        candidate_ids = message.reply_to_message_ids or (
            (message.reply_to_message_id,) if message.reply_to_message_id else ()
        )
        for candidate_id in candidate_ids:
            if (xmpp_jid, peer_id, candidate_id) in self._direct_reply_contexts:
                return candidate_id
            alias = self._direct_reply_aliases.get((xmpp_jid, peer_id, candidate_id))
            if alias:
                return alias
        if message.reply_to_message_id and self._is_telegram_message_id(message.reply_to_message_id):
            return message.reply_to_message_id
        return None

    def _resolve_direct_reply_payload(
        self,
        xmpp_jid: str,
        peer_id: str,
        message: XmppIncomingMessage,
    ) -> tuple:
        reply_to_message_id = self._resolve_direct_reply_target(xmpp_jid, peer_id, message)
        if reply_to_message_id:
            return reply_to_message_id, message.body

        fallback_reply_to, stripped_body = self._resolve_direct_reply_from_fallback(
            xmpp_jid,
            peer_id,
            message.body,
        )
        if fallback_reply_to:
            return fallback_reply_to, stripped_body
        return None, message.body

    @staticmethod
    def _is_telegram_message_id(value: str) -> bool:
        try:
            return int(value) > 0
        except (TypeError, ValueError):
            return False

    def _resolve_direct_reply_from_fallback(
        self,
        xmpp_jid: str,
        peer_id: str,
        body: str,
    ) -> tuple:
        quoted_body, stripped_body = self._split_quoted_reply_fallback(body)
        if not quoted_body or stripped_body == body:
            return None, body
        normalized_quote = self._normalized_reply_text(quoted_body)
        if not normalized_quote:
            return None, body
        prefix = (xmpp_jid, peer_id)
        for key, context in reversed(list(self._direct_reply_contexts.items())):
            if key[:2] != prefix:
                continue
            normalized_context = self._normalized_reply_text(context.body)
            if normalized_context and normalized_context in normalized_quote:
                return context.message_id, stripped_body
        return None, body

    @staticmethod
    def _split_quoted_reply_fallback(body: str) -> tuple:
        lines = body.splitlines()
        quoted_lines = []
        index = 0
        while index < len(lines) and lines[index].startswith(">"):
            line = lines[index]
            quoted_lines.append(line[2:] if line.startswith("> ") else line[1:])
            index += 1
        if not quoted_lines or index >= len(lines):
            return "", body
        return "\n".join(quoted_lines), "\n".join(lines[index:]).lstrip("\n")

    @staticmethod
    def _normalized_reply_text(body: str) -> str:
        return "\n".join(line.strip() for line in body.splitlines()).strip()

    def _direct_reply_reference(
        self,
        xmpp_jid: str,
        peer_id: str,
        reply_to_message_id: Optional[str],
    ) -> Optional[XmppReplyReference]:
        if not reply_to_message_id:
            return None
        context = self._direct_reply_contexts.get((xmpp_jid, peer_id, reply_to_message_id))
        if context is None:
            return None
        return XmppReplyReference(
            message_id=context.message_id,
            body=context.body,
            sender=context.sender,
            recipient=context.recipient,
            fake_outgoing=context.fake_outgoing,
        )

    def _remember_group_reply_context(
        self,
        xmpp_jid: str,
        peer_id: str,
        context: DirectReplyContext,
    ) -> None:
        self._group_reply_contexts[(xmpp_jid, peer_id, context.message_id)] = context
        self._remember_group_reply_alias(
            xmpp_jid=xmpp_jid,
            peer_id=peer_id,
            source_message_id=context.message_id,
            target_message_id=context.message_id,
        )

    def _remember_group_reply_alias(
        self,
        xmpp_jid: str,
        peer_id: str,
        source_message_id: str,
        target_message_id: str,
    ) -> None:
        self._group_reply_aliases[(xmpp_jid, peer_id, source_message_id)] = target_message_id

    def _resolve_group_reply_payload(
        self,
        xmpp_jid: str,
        peer_id: str,
        message: XmppIncomingMessage,
        body: str,
    ) -> tuple:
        reply_to_message_id = self._resolve_group_reply_target(xmpp_jid, peer_id, message)
        if reply_to_message_id:
            _quoted_body, stripped_body = self._split_quoted_reply_fallback(body)
            return reply_to_message_id, stripped_body
        fallback_reply_to, stripped_body = self._resolve_group_reply_from_fallback(xmpp_jid, peer_id, body)
        if fallback_reply_to:
            return fallback_reply_to, stripped_body
        return None, body

    def _resolve_group_reply_target(
        self,
        xmpp_jid: str,
        peer_id: str,
        message: XmppIncomingMessage,
    ) -> Optional[str]:
        candidate_ids = message.reply_to_message_ids or (
            (message.reply_to_message_id,) if message.reply_to_message_id else ()
        )
        for candidate_id in candidate_ids:
            if (xmpp_jid, peer_id, candidate_id) in self._group_reply_contexts:
                return candidate_id
            alias = self._group_reply_aliases.get((xmpp_jid, peer_id, candidate_id))
            if alias:
                return alias
        if message.reply_to_message_id and self._is_telegram_message_id(message.reply_to_message_id):
            return message.reply_to_message_id
        return None

    def _resolve_group_reply_from_fallback(
        self,
        xmpp_jid: str,
        peer_id: str,
        body: str,
    ) -> tuple:
        quoted_body, stripped_body = self._split_quoted_reply_fallback(body)
        if not quoted_body or stripped_body == body:
            return None, body
        normalized_quote = self._normalized_reply_text(quoted_body)
        if not normalized_quote:
            return None, body
        prefix = (xmpp_jid, peer_id)
        for key, context in reversed(list(self._group_reply_contexts.items())):
            if key[:2] != prefix:
                continue
            normalized_context = self._normalized_reply_text(context.body)
            if normalized_context and normalized_context in normalized_quote:
                return context.message_id, stripped_body
        return None, body

    def _group_reply_reference(
        self,
        xmpp_jid: str,
        peer_id: str,
        reply_to_message_id: Optional[str],
    ) -> Optional[XmppReplyReference]:
        if not reply_to_message_id:
            return None
        context = self._group_reply_contexts.get((xmpp_jid, peer_id, reply_to_message_id))
        if context is None:
            return None
        return XmppReplyReference(
            message_id=context.message_id,
            body=context.body,
            sender=context.sender,
            recipient=context.recipient,
            fake_outgoing=context.fake_outgoing,
        )

    def _telegram_forward_reference_from_xmpp(
        self,
        xmpp_jid: str,
        message: XmppIncomingMessage,
    ) -> Optional[TelegramForwardReference]:
        for reference in message.forward_references:
            source_peer_id = self._peer_id_from_forward_jid(reference.sender)
            if source_peer_id is None:
                source_peer_id = self._peer_id_from_forward_jid(reference.recipient)
            if source_peer_id is None:
                continue
            message_id = self._telegram_forward_message_id_from_xmpp_reference(
                xmpp_jid,
                str(source_peer_id),
                reference.message_id,
            )
            if message_id is None:
                continue
            return TelegramForwardReference(
                source_peer_id=source_peer_id,
                message_id=message_id,
            )
        return None

    def _telegram_forward_message_id_from_xmpp_reference(
        self,
        xmpp_jid: str,
        peer_id: str,
        message_id: str,
    ) -> Optional[str]:
        if not message_id:
            return None
        direct_alias = self._direct_reply_aliases.get((xmpp_jid, peer_id, message_id))
        if direct_alias and self._is_telegram_message_id(direct_alias):
            return direct_alias
        group_alias = self._group_reply_aliases.get((xmpp_jid, peer_id, message_id))
        if group_alias and self._is_telegram_message_id(group_alias):
            return group_alias
        if self._is_telegram_message_id(message_id):
            return message_id
        return None

    def _flatten_forward_body(self, message: XmppIncomingMessage, *, body: Optional[str] = None) -> str:
        base_body = message.body if body is None else body
        if not message.forward_references:
            return base_body
        forwarded_parts = [reference.body for reference in message.forward_references if reference.body]
        if base_body:
            forwarded_parts.append(base_body)
        return "\n\n".join(forwarded_parts)

    def _peer_id_from_forward_jid(self, jid: str) -> Optional[int]:
        localpart = self.xmpp.client.parse_component_localpart(jid)
        if localpart is not None and localpart.startswith("chat-"):
            peer_id = self._int_or_none(localpart.removeprefix("chat-"))
            if peer_id is not None:
                return peer_id
        if localpart is not None and localpart.startswith("group-"):
            peer_id = self._int_or_none(localpart.removeprefix("group-"))
            if peer_id is not None:
                return peer_id
        bare_localpart = jid.split("@", 1)[0]
        if bare_localpart.startswith("telegramg-"):
            parsed = self._parse_group_jid_localpart(bare_localpart)
            if parsed is not None:
                _owner_jid, chat_id = parsed
                return self._int_or_none(chat_id)
        return None

    def _forward_reference_from_telegram_event(
        self,
        event,
        *,
        fallback_recipient: str,
    ) -> Optional[XmppForwardReference]:
        message = getattr(event, "message", None)
        fwd_from = getattr(message, "fwd_from", None) if message is not None else None
        if fwd_from is None:
            return None
        source_peer_id = self._telegram_forward_source_peer_id(fwd_from)
        source_name = str(getattr(fwd_from, "from_name", "") or "").strip()
        if source_peer_id is not None:
            sender = self._telegram_forward_source_jid(source_peer_id)
        elif source_name:
            sender = self._group_transport_member_jid()
        else:
            return None
        forwarded_body = str(getattr(event, "raw_text", "") or "").strip()
        if not forwarded_body:
            return None
        if source_peer_id is None and source_name:
            forwarded_body = "Forwarded from %s\n%s" % (source_name, forwarded_body)
        message_id = self._telegram_forward_message_id(fwd_from)
        return XmppForwardReference(
            message_id=message_id or "",
            body=forwarded_body,
            sender=sender,
            recipient=fallback_recipient,
            fake_outgoing=False,
        )

    def _telegram_forward_source_jid(self, source_peer_id: int) -> str:
        if source_peer_id < 0:
            return "group-%s@%s" % (source_peer_id, self.settings.xmpp_component_jid)
        return self._telegram_user_jid(source_peer_id)

    @classmethod
    def _telegram_forward_source_peer_id(cls, fwd_from) -> Optional[int]:
        for attr in ("saved_from_peer", "from_id"):
            peer = getattr(fwd_from, attr, None)
            peer_id = cls._telegram_peer_id(peer)
            if peer_id is not None:
                return peer_id
        return None

    @staticmethod
    def _telegram_forward_message_id(fwd_from) -> Optional[str]:
        for attr in ("saved_from_msg_id", "channel_post"):
            message_id = getattr(fwd_from, attr, None)
            if message_id is not None:
                return str(message_id)
        return None

    @staticmethod
    def _telegram_peer_id(peer) -> Optional[int]:
        if peer is None:
            return None
        user_id = getattr(peer, "user_id", None)
        if user_id is not None:
            return int(user_id)
        chat_id = getattr(peer, "chat_id", None)
        if chat_id is not None:
            return -int(chat_id)
        channel_id = getattr(peer, "channel_id", None)
        if channel_id is not None:
            return int("-100%s" % channel_id)
        return None

    @staticmethod
    def _int_or_none(value: str) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _group_jid(self, chat_id: str, owner_jid: str) -> str:
        return "%s@%s" % (
            self._group_localpart(owner_jid, chat_id),
            self.settings.transport_server_domain,
        )

    @staticmethod
    def _group_localpart(owner_jid: str, chat_id: str) -> str:
        return "telegramg-%s-%s" % (
            owner_jid.encode("utf-8").hex(),
            TelegramTransport._safe_group_token(chat_id),
        )

    @staticmethod
    def _parse_group_jid_localpart(localpart: str) -> Optional[tuple]:
        payload = localpart.removeprefix("telegramg-")
        if "-" not in payload:
            return None
        owner_hex, chat_id = payload.split("-", 1)
        if not owner_hex or not chat_id:
            return None
        try:
            owner_jid = bytes.fromhex(owner_hex).decode("utf-8")
        except ValueError:
            return None
        return owner_jid, chat_id

    @staticmethod
    def _safe_group_token(value: str) -> str:
        return "".join(char for char in str(value) if char.isalnum() or char in "-_") or "unknown"

    @staticmethod
    def _group_sync_signature(chat: TelegramDialog) -> str:
        avatar_photo_id = chat.avatar.photo_id if chat.avatar is not None else chat.avatar_photo_id or ""
        avatar_variant = chat.avatar.variant if chat.avatar is not None else "small" if chat.avatar_photo_id else ""
        return "%s\n%s\n%s\n%s\n%s" % (
            chat.title,
            chat.is_group,
            chat.is_channel,
            avatar_photo_id,
            avatar_variant,
        )

    def _group_transport_member_jid(self) -> str:
        return "bot@%s" % self.settings.xmpp_component_jid

    def _telegram_user_jid(self, user_id: int) -> str:
        return "chat-%s@%s" % (user_id, self.settings.xmpp_component_jid)

    @staticmethod
    def _group_sender_nickname(sender_jid: str) -> str:
        localpart = sender_jid.split("@", 1)[0]
        if localpart.startswith("chat-"):
            return "Telegram user %s" % localpart.removeprefix("chat-")
        return sender_jid.split("@", 1)[0]

    async def _telegram_group_sender_name(self, event, sender_jid: str) -> str:
        get_sender = getattr(event, "get_sender", None)
        if get_sender is not None:
            try:
                sender = await get_sender()
            except Exception:
                log.debug("Failed to load Telegram group sender name", exc_info=True)
            else:
                title = self._telegram_entity_title(sender)
                if title:
                    return title
        return self._group_sender_nickname(sender_jid)

    @staticmethod
    def _telegram_entity_title(entity) -> Optional[str]:
        if entity is None:
            return None
        title = getattr(entity, "title", None)
        if title:
            return str(title)
        first_name = str(getattr(entity, "first_name", "") or "").strip()
        last_name = str(getattr(entity, "last_name", "") or "").strip()
        full_name = " ".join(part for part in (first_name, last_name) if part)
        if full_name:
            return full_name
        username = getattr(entity, "username", None)
        if username:
            return str(username)
        return None

    @staticmethod
    def _strip_xabber_group_sender_prefix(body: str, sender_jid: Optional[str]) -> str:
        if not sender_jid:
            return body
        prefixes = [
            "%s:\n" % sender_jid,
            "%s:\r\n" % sender_jid,
            "%s: " % sender_jid,
            "%s:" % sender_jid,
        ]
        bare_name = sender_jid.split("@", 1)[0]
        prefixes.extend(
            [
                "%s:\n" % bare_name,
                "%s:\r\n" % bare_name,
                "%s: " % bare_name,
                "%s:" % bare_name,
            ]
        )
        for prefix in prefixes:
            if body.startswith(prefix):
                return body[len(prefix):]
        return body

    def _peer_id_from_contact_jid(self, contact_jid: str) -> int:
        prefix = "chat-"
        suffix = "@%s" % self.settings.xmpp_component_jid
        if not contact_jid.startswith(prefix) or not contact_jid.endswith(suffix):
            raise ValueError("Unsupported Telegram contact JID.")
        value = contact_jid[len(prefix) : -len(suffix)]
        try:
            return int(value)
        except ValueError:
            raise ValueError("Unsupported Telegram contact JID.")
