import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import timezone
from typing import Awaitable, Callable, Dict, List, Optional

from telethon.errors import SessionPasswordNeededError

from xmpp_transport_telegram.core.qr_store import QrCodeStore, StoredQrImage
from xmpp_transport_telegram.core.session_manager import SessionCipher
from xmpp_transport_telegram.core.state import AuthStatus
from xmpp_transport_telegram.telegram.models import TelegramContact
from xmpp_transport_telegram.xmpp.models import TelegramContactJid


HELP_TEXT = """Telegram transport commands:
/login - start Telegram QR authorization
/password <password> - complete Telegram cloud password after QR scan
/status
/contacts [page]
/add <number>
/sync-contacts
/logout
/help"""


NotifyCallback = Callable[[str], Awaitable[None]]
EnsureContactCallback = Callable[[str, TelegramContact], Awaitable[None]]
ConnectedSessionCallback = Callable[[str, str, Optional[str]], Awaitable[None]]


@dataclass
class QrLoginAttempt:
    account_id: int
    client: object
    qr_login: object
    qr_image: StoredQrImage
    notify: NotifyCallback
    status: AuthStatus
    task: asyncio.Task


@dataclass(frozen=True)
class ControlResponse:
    body: str
    media: tuple = ()


log = logging.getLogger(__name__)


def command_response(body: str) -> str:
    text = body.strip()
    command = text.split(None, 1)[0].lower() if text else "/help"

    if command == "/help":
        return HELP_TEXT
    if command == "/login":
        return "Telegram QR authorization requires the running transport service."
    if command == "/code":
        return "Telegram QR authorization does not use SMS code commands. Send /login to start QR authorization."
    if command == "/password":
        return "Send /password only after scanning a Telegram QR login link that asks for a cloud password."
    if command == "/status":
        return "Telegram account is not connected."
    if command == "/contacts":
            return "Telegram direct chat listing requires the running transport service."
    if command == "/add":
            return "Telegram direct chat add requires the running transport service."
    if command == "/sync-contacts":
            return "Telegram direct chat sync requires the running transport service."
    if command == "/logout":
        return "Telegram logout requires the running transport service."
    return "Unknown command.\n\n%s" % HELP_TEXT


class CommandService:
    CONTACTS_PAGE_SIZE = 50

    def __init__(
        self,
        repository,
        telegram,
        session_cipher: SessionCipher,
        qr_store: QrCodeStore,
        ensure_contact: EnsureContactCallback,
        connected_session: Optional[ConnectedSessionCallback] = None,
    ) -> None:
        self.repository = repository
        self.telegram = telegram
        self.session_cipher = session_cipher
        self.qr_store = qr_store
        self.ensure_contact = ensure_contact
        self.connected_session = connected_session
        self._qr_attempts: Dict[str, QrLoginAttempt] = {}

    async def handle(self, xmpp_jid: str, body: str, notify: NotifyCallback) -> ControlResponse:
        text = body.strip()
        if not text:
            return self._response(HELP_TEXT)

        parts = text.split(None, 1)
        command = parts[0].lower()
        argument = parts[1].strip() if len(parts) > 1 else ""

        if command == "/help":
            return self._response(HELP_TEXT)
        if command == "/login":
            return await self._start_qr_login(xmpp_jid, notify)
        if command == "/password":
            return self._response(await self._complete_password(xmpp_jid, argument))
        if command == "/code":
            return self._response(
                "Telegram QR authorization does not use SMS code commands. Send /login to start QR authorization."
            )
        if command == "/status":
            return self._response(await self._status(xmpp_jid))
        if command == "/contacts":
            return self._response(await self._contacts_response(xmpp_jid, argument))
        if command == "/add":
            return self._response(await self._add_contact_response(xmpp_jid, argument))
        if command == "/sync-contacts":
            return self._response(await self._sync_contacts_response(xmpp_jid))
        if command == "/logout":
            return self._response(await self._logout(xmpp_jid))
        return self._response("Unknown command.\n\n%s" % HELP_TEXT)

    async def _start_qr_login(self, xmpp_jid: str, notify: NotifyCallback) -> ControlResponse:
        existing_attempt = self._qr_attempts.get(xmpp_jid)
        if existing_attempt is not None and not existing_attempt.task.done():
            return self._qr_response(existing_attempt.qr_login, existing_attempt.qr_image)

        account_id = await self.repository.ensure_xmpp_account(xmpp_jid)
        session_data = await self._load_session(account_id)
        client = self.telegram.client_for_session(session_data)
        await client.connect()

        if await client.is_user_authorized():
            user = await client.get_me()
            previous_owner = await self._save_connected_session(xmpp_jid, account_id, client, user)
            sync_message = await self._sync_contacts_after_login(xmpp_jid, client)
            session_data = client.session.save()
            await client.disconnect()
            await self._notify_connected_session(xmpp_jid, session_data, previous_owner)
            return self._response(
                self._connected_message(user, sync_message, previous_owner)
            )

        qr_login = await client.qr_login()
        qr_image = self.qr_store.create(qr_login.url)
        task = asyncio.create_task(self._wait_for_qr_login(xmpp_jid))
        self._qr_attempts[xmpp_jid] = QrLoginAttempt(
            account_id=account_id,
            client=client,
            qr_login=qr_login,
            qr_image=qr_image,
            notify=notify,
            status=AuthStatus.WAITING_QR,
            task=task,
        )
        return self._qr_response(qr_login, qr_image)

    async def _wait_for_qr_login(self, xmpp_jid: str) -> None:
        attempt = self._qr_attempts.get(xmpp_jid)
        if attempt is None:
            return

        try:
            user = await attempt.qr_login.wait()
        except SessionPasswordNeededError:
            attempt.status = AuthStatus.WAITING_PASSWORD
            await attempt.notify(
                "Telegram QR scan accepted. Send /password <password> to complete authorization."
            )
            return
        except asyncio.TimeoutError:
            await self._finish_failed_attempt(
                xmpp_jid,
                "Telegram QR authorization expired. Send /login to create a new QR login link.",
            )
        except Exception:
            log.exception("Telegram QR authorization failed for %s", xmpp_jid)
            await self._finish_failed_attempt(
                xmpp_jid,
                "Telegram QR authorization failed. Send /login to try again.",
            )
        else:
            try:
                previous_owner = await self._save_connected_session(
                    xmpp_jid,
                    attempt.account_id,
                    attempt.client,
                    user,
                )
                sync_message = await self._sync_contacts_after_login(xmpp_jid, attempt.client)
                session_data = attempt.client.session.save()
            except Exception:
                log.exception("Telegram login finalization failed for %s", xmpp_jid)
                await self._finish_failed_attempt(
                    xmpp_jid,
                    "Telegram login was accepted, but session finalization failed. Send /login to try again.",
                )
            else:
                await self._discard_attempt(xmpp_jid)
                await self._notify_connected_session(xmpp_jid, session_data, previous_owner)
                await attempt.notify(self._connected_message(user, sync_message, previous_owner))

    async def _complete_password(self, xmpp_jid: str, password: str) -> str:
        if not password:
            return "Usage: /password <password>"

        attempt = self._qr_attempts.get(xmpp_jid)
        if attempt is None or attempt.status != AuthStatus.WAITING_PASSWORD:
            return "No Telegram QR authorization is waiting for a cloud password. Send /login first."

        try:
            user = await attempt.client.sign_in(password=password)
        except Exception:
            log.exception("Telegram cloud password failed for %s", xmpp_jid)
            return "Telegram cloud password was rejected. Send /password <password> to try again."

        previous_owner = await self._save_connected_session(
            xmpp_jid,
            attempt.account_id,
            attempt.client,
            user,
        )
        sync_message = await self._sync_contacts_after_login(xmpp_jid, attempt.client)
        session_data = attempt.client.session.save()
        await self._discard_attempt(xmpp_jid)
        await self._notify_connected_session(xmpp_jid, session_data, previous_owner)
        return self._connected_message(user, sync_message, previous_owner)

    async def _status(self, xmpp_jid: str) -> str:
        attempt = self._qr_attempts.get(xmpp_jid)
        if attempt is not None and not attempt.task.done():
            if attempt.status == AuthStatus.WAITING_PASSWORD:
                return "Telegram QR scan accepted. Waiting for /password <password>."
            return "Telegram QR authorization is waiting for scan."

        account_id = await self.repository.ensure_xmpp_account(xmpp_jid)
        row = await self.repository.get_telegram_session(account_id)
        if row is not None and row["connected"]:
            user_id = row["telegram_user_id"]
            return "Telegram account is connected%s." % (" as %s" % user_id if user_id else "")
        return "Telegram account is not connected."

    async def _logout(self, xmpp_jid: str) -> str:
        await self._discard_attempt(xmpp_jid)
        account_id = await self.repository.ensure_xmpp_account(xmpp_jid)
        session_data = await self._load_session(account_id)
        if session_data:
            client = self.telegram.client_for_session(session_data)
            try:
                await client.connect()
                if await client.is_user_authorized():
                    await client.log_out()
            finally:
                await client.disconnect()
        await self.repository.delete_telegram_session(account_id)
        return "Telegram account disconnected."

    async def _contacts_response(self, xmpp_jid: str, argument: str) -> str:
        page = self._parse_contacts_page(argument)
        # Telegram returns direct dialogs as a full result set.  We keep that
        # complete ordering for stable /add numbers, then page only the XMPP text.
        contacts = await self._list_contacts(xmpp_jid)
        if not contacts:
            return "Telegram returned no direct chats."

        start = (page - 1) * self.CONTACTS_PAGE_SIZE
        if start >= len(contacts):
            return "There is no Telegram direct chats page %s." % page

        shown = contacts[start : start + self.CONTACTS_PAGE_SIZE]
        total_pages = (len(contacts) + self.CONTACTS_PAGE_SIZE - 1) // self.CONTACTS_PAGE_SIZE
        lines = ["Telegram direct chats, page %s/%s:" % (page, total_pages)]
        lines.extend(
            "%s. %s%s"
            % (
                start + index,
                contact.title,
                self._contact_suffix(contact),
            )
            for index, contact in enumerate(shown, start=1)
        )
        lines.append("")
        lines.append("Add to Xabber: /add <number>")
        lines.append("Sync all listed Telegram direct chats: /sync-contacts")
        if page < total_pages:
            lines.append("Next page: /contacts %s" % (page + 1))
        return "\n".join(lines)

    async def _add_contact_response(self, xmpp_jid: str, argument: str) -> str:
        contact = await self._resolve_contact_selection(xmpp_jid, argument)
        await self.ensure_contact(xmpp_jid, contact)
        return "Telegram contact added to Xabber: %s" % contact.title

    async def _sync_contacts_response(self, xmpp_jid: str) -> str:
        contacts = await self._list_contacts(xmpp_jid)
        if not contacts:
            return "Telegram returned no direct chats."
        # Each contact goes through the same idempotent roster path as /add, so
        # bulk sync is safe to repeat after reconnects or metadata changes.
        for contact in contacts:
            await self.ensure_contact(xmpp_jid, contact)
        return "Telegram direct chats synchronized with Xabber: %s." % len(contacts)

    async def _sync_contacts_after_login(self, xmpp_jid: str, client) -> str:
        try:
            contacts = await self.telegram.list_contacts(client)
            for contact in contacts:
                await self.ensure_contact(xmpp_jid, contact)
        except Exception:
            log.exception("Telegram contact sync after login failed for %s", xmpp_jid)
            return "Contact sync failed; send /sync-contacts to retry."
        if not contacts:
            return "Telegram returned no direct chats to sync."
        return "Synced %s Telegram direct chats into the Telegram circle." % len(contacts)

    async def _list_contacts(self, xmpp_jid: str) -> List[TelegramContact]:
        client = await self._authorized_client(xmpp_jid)
        try:
            return await self.telegram.list_contacts(client)
        finally:
            await client.disconnect()

    async def _authorized_client(self, xmpp_jid: str):
        account_id = await self.repository.ensure_xmpp_account(xmpp_jid)
        session_data = await self._load_session(account_id)
        if not session_data:
            raise RuntimeError("Telegram is not connected. Send /login first.")
        client = self.telegram.client_for_session(session_data)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError("Telegram session expired. Send /login again.")
        return client

    async def _resolve_contact_selection(self, xmpp_jid: str, argument: str) -> TelegramContact:
        try:
            selection = int(argument.strip())
        except ValueError:
            raise ValueError("Usage: /add <number> from /contacts")
        contacts = await self._list_contacts(xmpp_jid)
        if selection < 1 or selection > len(contacts):
            raise ValueError("Telegram contact number is out of range.")
        return contacts[selection - 1]

    async def _load_session(self, account_id: int) -> Optional[str]:
        row = await self.repository.get_telegram_session(account_id)
        if row is None or not row["encrypted_session"]:
            return None
        return self.session_cipher.decrypt(row["encrypted_session"])

    async def _save_connected_session(self, xmpp_jid: str, account_id: int, client, user):
        session_data = client.session.save()
        encrypted_session = self.session_cipher.encrypt(session_data)
        previous_owner = await self.repository.upsert_telegram_session(
            account_id,
            int(user.id),
            getattr(user, "phone", None),
            encrypted_session,
            True,
        )
        return previous_owner

    async def _notify_connected_session(
        self,
        xmpp_jid: str,
        session_data: str,
        previous_owner: Optional[str],
    ) -> None:
        if self.connected_session is not None:
            await self.connected_session(xmpp_jid, session_data, previous_owner)

    async def _finish_failed_attempt(self, xmpp_jid: str, message: str) -> None:
        attempt = self._qr_attempts.get(xmpp_jid)
        if attempt is not None:
            await attempt.notify(message)
        await self._discard_attempt(xmpp_jid)

    async def _discard_attempt(self, xmpp_jid: str) -> None:
        attempt = self._qr_attempts.pop(xmpp_jid, None)
        if attempt is None:
            return
        if not attempt.task.done() and attempt.task is not asyncio.current_task():
            attempt.task.cancel()
            try:
                await attempt.task
            except asyncio.CancelledError:
                pass
        await attempt.client.disconnect()

    async def stop(self) -> None:
        """Cancel login attempts and close their Telethon connections."""
        for xmpp_jid in list(self._qr_attempts):
            await self._discard_attempt(xmpp_jid)

    def _qr_response(self, qr_login, qr_image: StoredQrImage) -> ControlResponse:
        return ControlResponse(
            body=self._format_qr_response(qr_login, qr_image),
            media=(qr_image,),
        )

    @classmethod
    def _response(cls, body: str) -> ControlResponse:
        return ControlResponse(body=body)

    def _format_qr_response(self, qr_login, qr_image: StoredQrImage) -> str:
        expires = qr_login.expires.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        return (
            "Scan this Telegram login QR SVG before %s:\n\n"
            "%s\n\n"
            "Direct Telegram login link:\n%s\n\n"
            "In Telegram, use Settings > Devices > Link Desktop Device. "
            "If Telegram asks for a cloud password after accepting the login, send /password <password>."
        ) % (expires, qr_image.url, qr_login.url)

    def _format_user(self, user) -> str:
        username = getattr(user, "username", None)
        if username:
            return "@%s" % username
        first_name = getattr(user, "first_name", None)
        last_name = getattr(user, "last_name", None)
        full_name = " ".join(part for part in (first_name, last_name) if part)
        return full_name or str(getattr(user, "id", "unknown user"))

    def _connected_message(self, user, sync_message: str, previous_owner: Optional[str]) -> str:
        suffix = " Previous XMPP binding %s was replaced." % previous_owner if previous_owner else ""
        return "Telegram account connected as %s. %s%s" % (
            self._format_user(user),
            sync_message,
            suffix,
        )

    @staticmethod
    def contact_jid(component_domain: str, contact: TelegramContact) -> str:
        return TelegramContactJid(contact.peer_id, component_domain).jid

    @staticmethod
    def contact_sync_signature(contact: TelegramContact) -> str:
        avatar_photo_id = contact.avatar.photo_id if contact.avatar is not None else contact.avatar_photo_id or ""
        avatar_variant = contact.avatar.variant if contact.avatar is not None else "small" if contact.avatar_photo_id else ""
        value = "\n".join(
            [
                contact.title,
                contact.username or "",
                contact.phone or "",
                avatar_photo_id,
                avatar_variant,
            ]
        )
        # The signature is just a cheap idempotency key for roster sync, not a
        # trust or tamper-proofing mechanism.
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _parse_contacts_page(argument: str) -> int:
        if not argument:
            return 1
        try:
            page = int(argument)
        except ValueError:
            raise ValueError("Usage: /contacts [page]")
        if page < 1:
            raise ValueError("Usage: /contacts [page]")
        return page

    @staticmethod
    def _contact_suffix(contact: TelegramContact) -> str:
        details = []
        if contact.username:
            details.append("@%s" % contact.username)
        if contact.phone:
            details.append("+%s" % contact.phone)
        return " (%s)" % ", ".join(details) if details else ""
