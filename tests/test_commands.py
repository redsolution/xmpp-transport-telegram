import asyncio
from datetime import datetime, timezone

from cryptography.fernet import Fernet

from xmpp_transport_telegram.core.commands import CommandService, HELP_TEXT, command_response
from xmpp_transport_telegram.core.session_manager import SessionCipher
from xmpp_transport_telegram.telegram.models import TelegramContact


def test_help_command_returns_command_list():
    assert command_response("/help") == HELP_TEXT


def test_empty_message_returns_help():
    assert command_response("   ") == HELP_TEXT


def test_status_is_disconnected_dummy_response():
    assert command_response("/status") == "Telegram account is not connected."


def test_known_placeholder_command():
    assert "requires the running transport service" in command_response("/login")


def test_unknown_command_includes_help():
    response = command_response("/unknown")

    assert response.startswith("Unknown command.")
    assert HELP_TEXT in response


class FakeRepository:
    def __init__(self):
        self.sessions = {}
        self.signatures = {}
        self.previous_owner = None

    async def ensure_xmpp_account(self, xmpp_jid):
        return 1

    async def get_telegram_session(self, xmpp_account_id):
        return self.sessions.get(xmpp_account_id)

    async def upsert_telegram_session(
        self,
        xmpp_account_id,
        telegram_user_id,
        phone,
        encrypted_session,
        connected,
    ):
        self.sessions[xmpp_account_id] = {
            "telegram_user_id": telegram_user_id,
            "phone": phone,
            "encrypted_session": encrypted_session,
            "connected": connected,
        }
        return self.previous_owner

    async def delete_telegram_session(self, xmpp_account_id):
        self.sessions.pop(xmpp_account_id, None)

    async def get_synced_roster_item_signature(self, xmpp_jid, item_jid):
        return self.signatures.get((xmpp_jid, item_jid))

    async def set_synced_roster_item_signature(
        self,
        xmpp_jid,
        item_jid,
        item_kind,
        sync_signature,
    ):
        self.signatures[(xmpp_jid, item_jid)] = sync_signature


class FakeSession:
    def save(self):
        return "saved-session"


class FakeQrLogin:
    url = "tg://login?token=test-token"
    expires = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)

    def __init__(self):
        self._event = asyncio.Event()

    async def wait(self):
        await self._event.wait()
        return FakeUser()

    def complete(self):
        self._event.set()


class FakeUser:
    id = 42
    username = "telegram_user"
    phone = "+15551234567"


class FakeClient:
    def __init__(self, authorized=False):
        self.session = FakeSession()
        self.qr_login_value = FakeQrLogin()
        self.authorized = authorized
        self.disconnected = False

    async def connect(self):
        pass

    async def disconnect(self):
        self.disconnected = True

    async def is_user_authorized(self):
        return self.authorized

    async def qr_login(self):
        return self.qr_login_value


class FakeTelegramBackend:
    def __init__(self, authorized=False, contacts=()):
        self.client = FakeClient(authorized=authorized)
        self.contacts = list(contacts)

    def client_for_session(self, session_data=None):
        return self.client

    async def list_contacts(self, client):
        return self.contacts


class FakeQrStore:
    def create(self, qr_link):
        return type(
            "FakeStoredQrImage",
            (),
            {
                "url": "https://transport.example/qr/telegram-login-qr-test.svg",
                "name": "telegram-login-qr-test.svg",
                "mime_type": "image/svg+xml",
                "size": 123,
            },
        )()


async def fake_ensure_contact(_xmpp_jid, _contact):
    pass


def test_login_starts_qr_authorization():
    asyncio.run(_test_login_starts_qr_authorization())


async def _test_login_starts_qr_authorization():
    repository = FakeRepository()
    contacts = [
        TelegramContact(peer_id=100, title="Alice", username="alice"),
        TelegramContact(peer_id=200, title="Bob", phone="15551230000"),
    ]
    telegram = FakeTelegramBackend(contacts=contacts)
    cipher = SessionCipher(Fernet.generate_key().decode("ascii"))
    synced = []

    async def ensure_contact(xmpp_jid, contact):
        synced.append((xmpp_jid, contact.peer_id))

    service = CommandService(repository, telegram, cipher, FakeQrStore(), ensure_contact)
    notifications = []

    async def notify(body):
        notifications.append(body)

    response = await service.handle("user@example.com", "/login", notify)

    assert "https://transport.example/qr/telegram-login-qr-test.svg" in response.body
    assert "tg://login?token=test-token" in response.body
    assert response.media[0].mime_type == "image/svg+xml"
    status = await service.handle("user@example.com", "/status", notify)
    assert status.body == (
        "Telegram QR authorization is waiting for scan."
    )

    telegram.client.qr_login_value.complete()
    await asyncio.wait_for(telegram.client.qr_login_value._event.wait(), timeout=1)
    await asyncio.sleep(0)

    assert notifications == [
        "Telegram account connected as @telegram_user. Synced 2 Telegram direct chats into the Telegram circle."
    ]
    assert synced == [
        ("user@example.com", 100),
        ("user@example.com", 200),
    ]
    assert repository.sessions[1]["connected"] is True


def test_contacts_uses_full_contact_list_and_pages_output():
    asyncio.run(_test_contacts_uses_full_contact_list_and_pages_output())


async def _test_contacts_uses_full_contact_list_and_pages_output():
    cipher = SessionCipher(Fernet.generate_key().decode("ascii"))
    repository = FakeRepository()
    repository.sessions[1] = {
        "telegram_user_id": 42,
        "phone": None,
        "encrypted_session": cipher.encrypt("stored-session"),
        "connected": True,
    }
    contacts = [
        TelegramContact(peer_id=index, title="Contact %03d" % index)
        for index in range(1, 56)
    ]
    telegram = FakeTelegramBackend(authorized=True, contacts=contacts)
    service = CommandService(repository, telegram, cipher, FakeQrStore(), fake_ensure_contact)

    async def notify(_body):
        pass

    response = await service.handle("user@example.com", "/contacts 2", notify)

    assert "Telegram direct chats, page 2/2:" in response.body
    assert "51. Contact 051" in response.body
    assert "55. Contact 055" in response.body
    assert "Sync all listed Telegram direct chats: /sync-contacts" in response.body


def test_add_and_sync_contacts_call_roster_callback():
    asyncio.run(_test_add_and_sync_contacts_call_roster_callback())


async def _test_add_and_sync_contacts_call_roster_callback():
    cipher = SessionCipher(Fernet.generate_key().decode("ascii"))
    repository = FakeRepository()
    repository.sessions[1] = {
        "telegram_user_id": 42,
        "phone": None,
        "encrypted_session": cipher.encrypt("stored-session"),
        "connected": True,
    }
    contacts = [
        TelegramContact(peer_id=100, title="Alice", username="alice"),
        TelegramContact(peer_id=200, title="Bob", phone="15551230000"),
    ]
    telegram = FakeTelegramBackend(authorized=True, contacts=contacts)
    synced = []

    async def ensure_contact(xmpp_jid, contact):
        synced.append((xmpp_jid, contact.peer_id))

    service = CommandService(repository, telegram, cipher, FakeQrStore(), ensure_contact)

    async def notify(_body):
        pass

    add_response = await service.handle("user@example.com", "/add 2", notify)
    sync_response = await service.handle("user@example.com", "/sync-contacts", notify)

    assert add_response.body == "Telegram contact added to Xabber: Bob"
    assert sync_response.body == "Telegram direct chats synchronized with Xabber: 2."
    assert synced == [
        ("user@example.com", 200),
        ("user@example.com", 100),
        ("user@example.com", 200),
    ]


def test_login_reports_replaced_previous_xmpp_binding():
    asyncio.run(_test_login_reports_replaced_previous_xmpp_binding())


async def _test_login_reports_replaced_previous_xmpp_binding():
    repository = FakeRepository()
    repository.previous_owner = "old@example.com"
    telegram = FakeTelegramBackend()
    cipher = SessionCipher(Fernet.generate_key().decode("ascii"))
    service = CommandService(repository, telegram, cipher, FakeQrStore(), fake_ensure_contact)
    notifications = []

    async def notify(body):
        notifications.append(body)

    await service.handle("new@example.com", "/login", notify)
    telegram.client.qr_login_value.complete()
    await asyncio.wait_for(telegram.client.qr_login_value._event.wait(), timeout=1)
    await asyncio.sleep(0)

    assert notifications == [
        "Telegram account connected as @telegram_user. "
        "Telegram returned no direct chats to sync. "
        "Previous XMPP binding old@example.com was replaced."
    ]


def test_logout_disconnects_client_when_connect_is_cancelled():
    asyncio.run(_test_logout_disconnects_client_when_connect_is_cancelled())


async def _test_logout_disconnects_client_when_connect_is_cancelled():
    cipher = SessionCipher(Fernet.generate_key().decode("ascii"))
    repository = FakeRepository()
    repository.sessions[1] = {
        "telegram_user_id": 42,
        "phone": None,
        "encrypted_session": cipher.encrypt("stored-session"),
        "connected": True,
    }
    telegram = FakeTelegramBackend()

    async def cancelled_connect():
        raise asyncio.CancelledError()

    telegram.client.connect = cancelled_connect
    service = CommandService(repository, telegram, cipher, FakeQrStore(), fake_ensure_contact)

    try:
        await service._logout("user@example.com")
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("logout cancellation was not propagated")

    assert telegram.client.disconnected is True


def test_service_stop_cancels_login_attempt_and_disconnects_client():
    asyncio.run(_test_service_stop_cancels_login_attempt_and_disconnects_client())


async def _test_service_stop_cancels_login_attempt_and_disconnects_client():
    repository = FakeRepository()
    telegram = FakeTelegramBackend()
    cipher = SessionCipher(Fernet.generate_key().decode("ascii"))
    service = CommandService(repository, telegram, cipher, FakeQrStore(), fake_ensure_contact)

    async def notify(_body):
        pass

    await service.handle("user@example.com", "/login", notify)
    task = service._qr_attempts["user@example.com"].task
    await service.stop()

    assert task.cancelled()
    assert telegram.client.disconnected is True
    assert service._qr_attempts == {}
