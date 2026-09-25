import os
import signal
import sys
import time
from typing import Optional


def read_pid(pid_file: str) -> Optional[int]:
    try:
        with open(pid_file, "r", encoding="ascii") as handle:
            return int(handle.read().strip())
    except (FileNotFoundError, ValueError):
        return None


def write_pid(pid_file: str) -> None:
    directory = os.path.dirname(pid_file)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(pid_file, "w", encoding="ascii") as handle:
        handle.write(str(os.getpid()))


def remove_pid(pid_file: str, expected_pid: Optional[int] = None) -> None:
    if expected_pid is not None and read_pid(pid_file) != expected_pid:
        return
    try:
        os.unlink(pid_file)
    except FileNotFoundError:
        pass


def process_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def running_pid(pid_file: str) -> Optional[int]:
    pid = read_pid(pid_file)
    if pid is None:
        return None
    if process_is_running(pid):
        return pid
    remove_pid(pid_file, expected_pid=pid)
    return None


def stop(pid_file: str, timeout: float = 10.0) -> bool:
    pid = running_pid(pid_file)
    if pid is None:
        return False
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        remove_pid(pid_file, expected_pid=pid)
        return False

    deadline = time.monotonic() + timeout
    while process_is_running(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    if not process_is_running(pid):
        remove_pid(pid_file, expected_pid=pid)
    return True


def daemonize(pid_file: str) -> None:
    if os.fork() > 0:
        sys.exit(0)
    os.setsid()
    if os.fork() > 0:
        sys.exit(0)
    sys.stdin.close()
    write_pid(pid_file)
