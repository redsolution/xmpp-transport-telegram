import asyncio
import hashlib
from xml.etree import ElementTree as ET

from cryptography.fernet import Fernet

from xmpp_transport_telegram.core.session_manager import SessionCipher
from xmpp_transport_telegram.core.transport import TelegramTransport
from xmpp_transport_telegram.runtime.config import Settings
from xmpp_transport_telegram.telegram.models import TelegramAvatar, TelegramContact, TelegramDialog
from xmpp_transport_telegram.xmpp.component import XmppComponent


class FakeRepository:
    def __init__(self):
        self.signatures = {}
        self.connected_sessions = []
        self.avatar_files = {}
        self.contact_avatars = {}
        self.deleted_contact_avatars = []

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

    async def list_connected_telegram_sessions(self):
        return self.connected_sessions

    async def upsert_avatar_file(self, content_hash, relative_path, mime_type, bytes_count):
        self.avatar_files[content_hash] = {
            "relative_path": relative_path,
            "mime_type": mime_type,
            "bytes_count": bytes_count,
        }

    async def upsert_contact_avatar(
        self,
        owner_jid,
        contact_jid,
        peer_id,
        photo_id,
        variant,
        content_hash,
        avatar_id,
        url,
        mime_type,
        bytes_count,
    ):
        self.contact_avatars[(owner_jid, contact_jid, variant)] = {
            "peer_id": peer_id,
            "photo_id": photo_id,
            "content_hash": content_hash,
            "avatar_id": avatar_id,
            "url": url,
            "mime_type": mime_type,
            "bytes_count": bytes_count,
        }

    async def delete_contact_avatar(self, owner_jid, contact_jid, variant="small"):
        self.deleted_contact_avatars.append((owner_jid, contact_jid, variant))
        self.contact_avatars.pop((owner_jid, contact_jid, variant), None)


class FakeSession:
    def __init__(self, session_data):
        self._session_data = session_data

    def save(self):
        return self._session_data


class FakeTelegramClient:
    def __init__(self, authorized=True):
        self.authorized = authorized
        self.connected = False
        self.disconnected = False
        self.session = FakeSession("stored-session")
        self.handlers = []

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.disconnected = True

    async def is_user_authorized(self):
        return self.authorized

    def add_event_handler(self, handler, event_builder):
        self.handlers.append((handler, event_builder))


class FakeTelegramBackend:
    def __init__(self, contacts, authorized=True, groups=()):
        self.contacts = contacts
        self.authorized = authorized
        self.groups = list(groups)
        self.clients = []

    def client_for_session(self, session_data=None):
        client = FakeTelegramClient(authorized=self.authorized)
        self.clients.append(client)
        return client

    async def list_contacts(self, client, include_avatars=True):
        self.include_avatars = include_avatars
        return self.contacts

    async def list_group_chats(self, client, include_avatars=True):
        return self.groups


class FakeXmppClient:
    def __init__(self):
        self.operations = []
        self.created_groups = []
        self.updated_groups = []
        self.invites = []
        self.direct_invites = []
        self.avatar_events = []

    async def send_transport_operation(self, operation, fields, groups=(), timeout=10):
        self.operations.append((operation, fields, groups))
        return "updated"

    async def create_xabber_group(self, **kwargs):
        self.created_groups.append(kwargs)
        return "%s@example.com" % kwargs["localpart"]

    async def update_xabber_group_info(self, **kwargs):
        self.updated_groups.append(kwargs)

    async def update_xabber_group_avatar(self, **kwargs):
        self.updated_groups.append(kwargs)

    async def invite_xabber_group_member(self, **kwargs):
        self.invites.append(kwargs)

    def send_xabber_group_invite(self, **kwargs):
        self.direct_invites.append(kwargs)

    def send_avatar_metadata_event(self, **kwargs):
        self.avatar_events.append(kwargs)


class FakeXmpp:
    def __init__(self):
        self.client = FakeXmppClient()


def test_ensure_telegram_contact_pushes_contact_into_telegram_circle():
    asyncio.run(_test_ensure_telegram_contact_pushes_contact_into_telegram_circle())


async def _test_ensure_telegram_contact_pushes_contact_into_telegram_circle():
    repository = FakeRepository()
    transport = TelegramTransport(_settings(), repository)
    transport.xmpp = FakeXmpp()
    contact = TelegramContact(peer_id=100, title="Alice")

    await transport._ensure_telegram_contact("user@example.com", contact)
    await transport._ensure_telegram_contact("user@example.com", contact)

    assert transport.xmpp.client.operations == [
        (
            "add-roster-contact",
            {
                "owner_jid": "user@example.com",
                "contact_jid": "chat-100@telegram.example.com",
                "name": "Alice",
                "create_chat": "true",
            },
            ("Telegram",),
        )
    ]


def test_ensure_telegram_contact_caches_avatar_by_content_hash(tmp_path):
    asyncio.run(_test_ensure_telegram_contact_caches_avatar_by_content_hash(tmp_path))


async def _test_ensure_telegram_contact_caches_avatar_by_content_hash(tmp_path):
    settings = _settings()
    settings = Settings(
        **{
            **settings.__dict__,
            "avatar_storage_dir": str(tmp_path / "avatars"),
            "avatar_base_url": "http://transport.example",
            "avatar_max_bytes": 524288,
        }
    )
    repository = FakeRepository()
    transport = TelegramTransport(settings, repository)
    transport.xmpp = FakeXmpp()
    content = b"same-avatar"
    content_hash = hashlib.sha256(content).hexdigest()

    await transport._ensure_telegram_contact(
        "first@example.com",
        TelegramContact(
            peer_id=100,
            title="Alice",
            avatar=TelegramAvatar(photo_id="111", content=content),
        ),
    )
    await transport._ensure_telegram_contact(
        "second@example.com",
        TelegramContact(
            peer_id=200,
            title="Alice Copy",
            avatar=TelegramAvatar(photo_id="222", content=content),
        ),
    )

    assert (tmp_path / "avatars" / ("%s.jpg" % content_hash)).read_bytes() == content
    assert list(repository.avatar_files) == [content_hash]
    assert repository.contact_avatars[
        ("first@example.com", "chat-100@telegram.example.com", "small")
    ]["content_hash"] == content_hash
    assert repository.contact_avatars[
        ("second@example.com", "chat-200@telegram.example.com", "small")
    ]["content_hash"] == content_hash
    assert transport.xmpp.client.avatar_events[0]["url"] == "http://transport.example/avatar/%s.jpg" % content_hash


def test_ensure_telegram_contact_removes_avatar_metadata_without_photo():
    asyncio.run(_test_ensure_telegram_contact_removes_avatar_metadata_without_photo())


async def _test_ensure_telegram_contact_removes_avatar_metadata_without_photo():
    repository = FakeRepository()
    repository.contact_avatars[("user@example.com", "chat-100@telegram.example.com", "small")] = {}
    transport = TelegramTransport(_settings(), repository)
    transport.xmpp = FakeXmpp()

    await transport._ensure_telegram_contact("user@example.com", TelegramContact(peer_id=100, title="Alice"))

    assert repository.deleted_contact_avatars == [
        ("user@example.com", "chat-100@telegram.example.com", "small")
    ]


def test_ensure_telegram_contact_keeps_avatar_metadata_after_download_failure():
    asyncio.run(_test_ensure_telegram_contact_keeps_avatar_metadata_after_download_failure())


async def _test_ensure_telegram_contact_keeps_avatar_metadata_after_download_failure():
    repository = FakeRepository()
    old_signature = "old-avatar-signature"
    repository.signatures[("user@example.com", "chat-100@telegram.example.com")] = old_signature
    transport = TelegramTransport(_settings(), repository)
    transport.xmpp = FakeXmpp()

    await transport._ensure_telegram_contact(
        "user@example.com",
        TelegramContact(peer_id=100, title="Alice", avatar_download_failed=True),
    )

    assert repository.deleted_contact_avatars == []
    assert repository.signatures[("user@example.com", "chat-100@telegram.example.com")] == old_signature


def test_ensure_telegram_contact_rejects_oversized_avatar(tmp_path):
    asyncio.run(_test_ensure_telegram_contact_rejects_oversized_avatar(tmp_path))


async def _test_ensure_telegram_contact_rejects_oversized_avatar(tmp_path):
    settings = _settings()
    settings = Settings(
        **{
            **settings.__dict__,
            "avatar_storage_dir": str(tmp_path / "avatars"),
            "avatar_max_bytes": 4,
        }
    )
    repository = FakeRepository()
    transport = TelegramTransport(settings, repository)
    transport.xmpp = FakeXmpp()

    await transport._ensure_telegram_contact(
        "user@example.com",
        TelegramContact(
            peer_id=100,
            title="Alice",
            avatar=TelegramAvatar(photo_id="111", content=b"too-large"),
        ),
    )

    assert not repository.avatar_files
    assert not list((tmp_path / "avatars").glob("*"))
    assert repository.deleted_contact_avatars == [
        ("user@example.com", "chat-100@telegram.example.com", "small")
    ]


def test_ensure_telegram_group_chat_caches_avatar(tmp_path):
    asyncio.run(_test_ensure_telegram_group_chat_caches_avatar(tmp_path))


async def _test_ensure_telegram_group_chat_caches_avatar(tmp_path):
    settings = _settings()
    settings = Settings(
        **{
            **settings.__dict__,
            "avatar_storage_dir": str(tmp_path / "avatars"),
            "avatar_base_url": "http://transport.example",
        }
    )
    repository = FakeRepository()
    transport = TelegramTransport(settings, repository)
    transport.xmpp = FakeXmpp()
    content = b"group-avatar"
    content_hash = hashlib.sha256(content).hexdigest()
    chat = TelegramDialog(
        peer_id=-100500,
        title="Team",
        is_group=True,
        avatar=TelegramAvatar(photo_id="777", content=content),
    )
    group_jid = "telegramg-75736572406578616d706c652e636f6d--100500@example.com"

    await transport._ensure_telegram_group_chat("user@example.com", chat)

    assert (tmp_path / "avatars" / ("%s.jpg" % content_hash)).read_bytes() == content
    assert repository.contact_avatars[("user@example.com", group_jid, "small")]["content_hash"] == content_hash
    assert transport.xmpp.client.updated_groups == [
        {
            "owner_jid": "user@example.com",
            "actor_jid": "bot@telegram.example.com",
            "group_jid": group_jid,
            "avatar_id": "telegram--100500-777-%s" % content_hash[:16],
            "url": "http://transport.example/avatar/%s.jpg" % content_hash,
            "mime_type": "image/jpeg",
            "bytes_count": len(content),
            "timeout": 2,
        }
    ]
    assert transport.xmpp.client.avatar_events == []


def test_restart_sync_pushes_contacts_for_connected_sessions():
    asyncio.run(_test_restart_sync_pushes_contacts_for_connected_sessions())


async def _test_restart_sync_pushes_contacts_for_connected_sessions():
    settings = _settings()
    repository = FakeRepository()
    encrypted_session = SessionCipher(settings.session_encryption_key).encrypt("stored-session")
    repository.connected_sessions = [
        {
            "xmpp_jid": "user@example.com",
            "encrypted_session": encrypted_session,
        }
    ]
    transport = TelegramTransport(settings, repository)
    transport.xmpp = FakeXmpp()
    transport.telegram = FakeTelegramBackend(
        [
            TelegramContact(peer_id=100, title="Alice"),
            TelegramContact(peer_id=200, title="Bob"),
        ]
    )

    await transport._sync_connected_contacts_after_restart()

    assert transport.telegram.include_avatars is False
    assert transport.xmpp.client.operations == [
        (
            "add-roster-contact",
            {
                "owner_jid": "user@example.com",
                "contact_jid": "chat-100@telegram.example.com",
                "name": "Alice",
                "create_chat": "true",
            },
            ("Telegram",),
        ),
        (
            "add-roster-contact",
            {
                "owner_jid": "user@example.com",
                "contact_jid": "chat-200@telegram.example.com",
                "name": "Bob",
                "create_chat": "true",
            },
            ("Telegram",),
        ),
    ]


def test_restart_sync_does_not_create_xabber_groups_for_telegram_groups():
    asyncio.run(_test_restart_sync_does_not_create_xabber_groups_for_telegram_groups())


async def _test_restart_sync_does_not_create_xabber_groups_for_telegram_groups():
    settings = _settings()
    repository = FakeRepository()
    encrypted_session = SessionCipher(settings.session_encryption_key).encrypt("stored-session")
    repository.connected_sessions = [
        {
            "xmpp_jid": "user@example.com",
            "encrypted_session": encrypted_session,
        }
    ]
    transport = TelegramTransport(settings, repository)
    transport.xmpp = FakeXmpp()
    transport.telegram = FakeTelegramBackend(
        [],
        groups=[TelegramDialog(peer_id=-100500, title="Telegram Team", is_group=True)],
    )

    await transport._sync_connected_contacts_after_restart()

    assert transport.xmpp.client.created_groups == []
    assert transport.xmpp.client.updated_groups == []
    assert transport.xmpp.client.invites == []
    assert transport.xmpp.client.direct_invites == []


def test_restart_sync_skips_expired_telegram_session():
    asyncio.run(_test_restart_sync_skips_expired_telegram_session())


async def _test_restart_sync_skips_expired_telegram_session():
    settings = _settings()
    repository = FakeRepository()
    encrypted_session = SessionCipher(settings.session_encryption_key).encrypt("stored-session")
    repository.connected_sessions = [
        {
            "xmpp_jid": "user@example.com",
            "encrypted_session": encrypted_session,
        }
    ]
    transport = TelegramTransport(settings, repository)
    transport.xmpp = FakeXmpp()
    transport.telegram = FakeTelegramBackend(
        [TelegramContact(peer_id=100, title="Alice")],
        authorized=False,
    )

    await transport._sync_connected_contacts_after_restart()

    assert transport.xmpp.client.operations == []


def test_transport_query_xml_has_single_namespace_declaration():
    asyncio.run(_test_transport_query_xml_has_single_namespace_declaration())


async def _test_transport_query_xml_has_single_namespace_declaration():
    component = XmppComponent(_settings(), None)

    query = component.client._transport_query_element(
        "add-roster-contact",
        {
            "owner_jid": "user@example.com",
            "contact_jid": "chat-100@telegram.example.com",
            "name": "Alice",
        },
        ("Telegram",),
    )
    serialized = ET.tostring(query).decode("utf-8")

    assert serialized.count("urn:xabber:transport:telegram:1") == 1
    assert 'op="add-roster-contact"' in serialized


def _settings():
    return Settings(
        database_url="postgresql://example",
        session_encryption_key=Fernet.generate_key().decode("ascii"),
        xmpp_component_jid="telegram.example.com",
        xmpp_component_secret="secret",
        xmpp_component_host="127.0.0.1",
        xmpp_component_port=5238,
        xmpp_component_connect_timeout=20,
        xmpp_component_retry_interval=10,
        telegram_api_id=123,
        telegram_api_hash="hash",
        telegram_session_storage_dir="data/telegram_sessions",
        transport_server_domain="example.com",
        transport_pid_file="run/xmpp_transport_telegram.pid",
        health_host="127.0.0.1",
        health_port=8089,
        qr_storage_dir="data/login_qr",
        qr_base_url="http://127.0.0.1:8089",
        avatar_storage_dir="data/avatars",
        avatar_base_url="http://127.0.0.1:8089",
        avatar_max_bytes=524288,
        avatar_unreferenced_ttl_days=7,
        avatar_cleanup_interval_seconds=86400,
        media_base_url="http://127.0.0.1:8089",
        media_stream_request_size=524288,
        log_level="INFO",
        log_file="",
        log_max_bytes=10485760,
        log_backup_count=5,
    )


def test_transport_iq_signature_known_vector():
    settings = _settings()
    object.__setattr__(settings, "transport_iq_auth_secret", "x" * 32)
    component = XmppComponent(settings, None)

    signature = component.client._transport_operation_signature(
        "add-roster-contact",
        {
            "owner_jid": "user@example.com",
            "contact_jid": "chat-100@telegram.example.com",
            "name": "Alice",
        },
        ("Telegram",),
        "1700000000",
        "00112233445566778899aabbccddeeff",
    )

    assert signature == "48cd62e90ae8c18587f6a78e149aef4254c6f2239ea1694bd00af400a436068d"
