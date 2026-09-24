import asyncio
from xml.etree import ElementTree as ET

from xmpp_transport_telegram.core.commands import ControlResponse
from xmpp_transport_telegram.runtime.config import Settings
from xmpp_transport_telegram.xmpp import component as component_module
from xmpp_transport_telegram.xmpp.component import TelegramCommandComponent, XmppComponent
from xmpp_transport_telegram.xmpp.groups_protocol import XabberGroupsXml
from xmpp_transport_telegram.xmpp.namespaces import GROUPS_NS


def make_settings():
    return Settings(
        database_url="postgresql://example",
        session_encryption_key="key",
        xmpp_component_jid="telegram.example.com",
        xmpp_component_secret="secret",
        xmpp_component_host="127.0.0.1",
        xmpp_component_port=5238,
        xmpp_component_connect_timeout=1,
        xmpp_component_retry_interval=0,
        telegram_api_id=123,
        telegram_api_hash="hash",
        telegram_session_storage_dir="data/telegram_sessions",
        transport_server_domain="example.com",
        transport_pid_file="run/transport.pid",
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


class FakeComponentClient:
    def __init__(self, _settings, _command_handler, _direct_message_handler):
        self.connect_count = 0
        self.disconnect_count = 0
        self.connected = False

    async def connect(self, _host, _port):
        self.connect_count += 1
        self.connected = True

    async def wait_until_ready(self, _timeout):
        if self.connect_count == 1:
            raise asyncio.TimeoutError()

    def is_connected(self):
        return self.connected

    async def disconnect(self):
        self.disconnect_count += 1
        self.connected = False


class FakeMessage:
    def __init__(self, from_jid, to_jid, body, message_type="chat", xml=None, message_id="xmpp-1"):
        self.values = {
            "from": from_jid,
            "to": to_jid,
            "body": body,
            "type": message_type,
            "id": message_id,
        }
        self.xml = xml if xml is not None else ET.Element("message")
        self.error_reply = FakeErrorReply()

    def __getitem__(self, key):
        return self.values[key]

    def reply(self, clear=True):
        assert clear is False
        return self.error_reply


class FakeErrorReply:
    def __init__(self):
        self.values = {"error": {}}
        self.sent = False

    def __getitem__(self, key):
        return self.values[key]

    def __setitem__(self, key, value):
        self.values[key] = value

    def send(self):
        self.sent = True


def test_xmpp_component_start_retries_after_ready_timeout(monkeypatch):
    async def run_test():
        monkeypatch.setattr(component_module, "TelegramCommandComponent", FakeComponentClient)

        async def command_handler(_from_jid, _body, _notify):
            raise AssertionError("not used")

        xmpp = XmppComponent(make_settings(), command_handler)

        await xmpp.start()

        assert xmpp.client.connect_count == 2
        assert xmpp.client.disconnect_count == 1
        assert xmpp.client.connected is True

    asyncio.run(run_test())


def test_xabber_group_info_update_can_include_external_avatar_metadata():
    info = XabberGroupsXml.update_info(
        avatar={
            "bytes": 524288,
            "id": "avatar-id",
            "type": "image/jpeg",
            "url": "http://transport.example/avatar/hash.jpg",
        }
    )

    avatar = info.find("avatar")
    assert avatar is not None
    metadata = avatar.find("{urn:xmpp:avatar:metadata}info")
    assert metadata is not None
    assert metadata.attrib["id"] == "avatar-id"
    assert metadata.attrib["url"] == "http://transport.example/avatar/hash.jpg"
    assert metadata.attrib["bytes"] == "524288"
    assert metadata.attrib["type"] == "image/jpeg"


def test_xabber_group_service_message_to_bot_does_not_run_command_handler():
    async def run_test():
        command_calls = []

        async def command_handler(_from_jid, _body, _notify):
            command_calls.append((_from_jid, _body))
            return ControlResponse("unexpected")

        async def direct_message_handler(_message):
            raise AssertionError("direct handler should not be called")

        client = TelegramCommandComponent(
            make_settings(),
            command_handler,
            direct_message_handler,
        )
        await client._handle_message_async(
            FakeMessage(
                from_jid="telegramg-74657374406578616d706c652e636f6d--5386493808@example.com/Group",
                to_jid="bot@telegram.example.com",
                body="test@example.com joined the group.",
            )
        )

        assert command_calls == []

    asyncio.run(run_test())


def test_xabber_group_user_message_without_sender_marker_uses_group_owner():
    async def run_test():
        direct_calls = []

        async def command_handler(_from_jid, _body, _notify):
            raise AssertionError("command handler should not be called")

        async def direct_message_handler(message):
            direct_calls.append(message)

        client = TelegramCommandComponent(
            make_settings(),
            command_handler,
            direct_message_handler,
        )
        await client._handle_message_async(
            FakeMessage(
                from_jid="telegramg-7465737431406578616d706c652e636f6d--5386493808@example.com/Group",
                to_jid="bot@telegram.example.com",
                body="test",
            )
        )

        assert len(direct_calls) == 1
        assert direct_calls[0].sender == "telegramg-7465737431406578616d706c652e636f6d--5386493808@example.com"
        assert direct_calls[0].recipient == "bot@telegram.example.com"
        assert direct_calls[0].body == "test"
        assert direct_calls[0].group_sender_jid == "test1@example.com"

    asyncio.run(run_test())


def test_xabber_group_user_message_to_bot_runs_direct_handler():
    async def run_test():
        direct_calls = []

        async def command_handler(_from_jid, _body, _notify):
            raise AssertionError("command handler should not be called")

        async def direct_message_handler(message):
            direct_calls.append(message)

        x = ET.Element("{%s}x" % GROUPS_NS)
        user = ET.SubElement(x, "{%s}user" % GROUPS_NS)
        jid = ET.SubElement(user, "jid")
        jid.text = "test@example.com"
        xml = ET.Element("message")
        xml.append(x)

        client = TelegramCommandComponent(
            make_settings(),
            command_handler,
            direct_message_handler,
        )
        await client._handle_message_async(
            FakeMessage(
                from_jid="telegramg-74657374406578616d706c652e636f6d--5386493808@example.com/Group",
                to_jid="bot@telegram.example.com",
                body="hello tg",
                xml=xml,
            )
        )

        assert len(direct_calls) == 1
        assert direct_calls[0].sender == "telegramg-74657374406578616d706c652e636f6d--5386493808@example.com"
        assert direct_calls[0].recipient == "bot@telegram.example.com"
        assert direct_calls[0].body == "hello tg"
        assert direct_calls[0].group_sender_jid == "test@example.com"

    asyncio.run(run_test())


def test_foreign_user_command_is_rejected_with_forbidden_error():
    async def run_test():
        command_calls = []

        async def command_handler(from_jid, body, _notify):
            command_calls.append((from_jid, body))
            return ControlResponse("unexpected")

        client = TelegramCommandComponent(make_settings(), command_handler)
        message = FakeMessage(
            from_jid="attacker@foreign.example/resource",
            to_jid="bot@telegram.example.com",
            body="/status",
        )

        await client._handle_message_async(message)

        assert command_calls == []
        assert message.error_reply.sent is True
        assert message.error_reply["type"] == "error"
        assert message.error_reply["error"]["type"] == "auth"
        assert message.error_reply["error"]["condition"] == "forbidden"

    asyncio.run(run_test())


def test_foreign_user_direct_message_is_rejected_with_forbidden_error():
    async def run_test():
        direct_calls = []

        async def command_handler(_from_jid, _body, _notify):
            raise AssertionError("command handler should not be called")

        async def direct_message_handler(message):
            direct_calls.append(message)

        client = TelegramCommandComponent(
            make_settings(), command_handler, direct_message_handler
        )
        message = FakeMessage(
            from_jid="attacker@foreign.example/resource",
            to_jid="chat-100@telegram.example.com",
            body="hello",
        )

        await client._handle_message_async(message)

        assert direct_calls == []
        assert message.error_reply.sent is True
        assert message.error_reply["error"]["condition"] == "forbidden"

    asyncio.run(run_test())


def test_foreign_group_sender_marker_is_rejected_with_forbidden_error():
    async def run_test():
        direct_calls = []

        async def command_handler(_from_jid, _body, _notify):
            raise AssertionError("command handler should not be called")

        async def direct_message_handler(message):
            direct_calls.append(message)

        x = ET.Element("{%s}x" % GROUPS_NS)
        user = ET.SubElement(x, "{%s}user" % GROUPS_NS)
        jid = ET.SubElement(user, "jid")
        jid.text = "attacker@foreign.example"
        xml = ET.Element("message")
        xml.append(x)
        client = TelegramCommandComponent(
            make_settings(), command_handler, direct_message_handler
        )
        message = FakeMessage(
            from_jid="telegramg-owner--100@example.com/Group",
            to_jid="bot@telegram.example.com",
            body="hello",
            xml=xml,
        )

        await client._handle_message_async(message)

        assert direct_calls == []
        assert message.error_reply.sent is True
        assert message.error_reply["error"]["condition"] == "forbidden"

    asyncio.run(run_test())
