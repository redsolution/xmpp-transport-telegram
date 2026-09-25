from unittest.mock import call, patch

from xmpp_transport_telegram.runtime.daemon import running_pid, stop


def test_running_pid_removes_stale_pid_file(tmp_path):
    pid_file = tmp_path / "transport.pid"
    pid_file.write_text("123", encoding="ascii")

    with patch("xmpp_transport_telegram.runtime.daemon.os.kill", side_effect=ProcessLookupError):
        assert running_pid(str(pid_file)) is None

    assert not pid_file.exists()


def test_stop_handles_process_exiting_before_sigterm(tmp_path):
    pid_file = tmp_path / "transport.pid"
    pid_file.write_text("123", encoding="ascii")

    with patch(
        "xmpp_transport_telegram.runtime.daemon.os.kill",
        side_effect=[None, ProcessLookupError],
    ) as kill:
        assert stop(str(pid_file)) is False

    assert kill.call_args_list == [call(123, 0), call(123, 15)]
    assert not pid_file.exists()


def test_stop_waits_for_process_and_removes_pid_file(tmp_path):
    pid_file = tmp_path / "transport.pid"
    pid_file.write_text("123", encoding="ascii")

    with patch(
        "xmpp_transport_telegram.runtime.daemon.os.kill",
        side_effect=[None, None, None, ProcessLookupError, ProcessLookupError],
    ), patch("xmpp_transport_telegram.runtime.daemon.time.sleep"):
        assert stop(str(pid_file)) is True

    assert not pid_file.exists()
