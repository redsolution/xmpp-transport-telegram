import argparse
import asyncio
import logging
import logging.handlers
import os
from typing import Optional

from aiohttp import web

from xmpp_transport_telegram.core.transport import TelegramTransport
from xmpp_transport_telegram.runtime.config import load_settings, validate_settings
from xmpp_transport_telegram.runtime.daemon import daemonize, remove_pid, running_pid, stop
from xmpp_transport_telegram.runtime.web import create_app
from xmpp_transport_telegram.storage.repository import Repository


def configure_logging(level: str, log_file: str, max_bytes: int, backup_count: int) -> None:
    handlers = [logging.StreamHandler()]
    if log_file:
        directory = os.path.dirname(log_file)
        if directory:
            os.makedirs(directory, exist_ok=True)
        handlers.append(
            logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
        )
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=handlers,
    )
    for noisy_logger in ("telethon", "slixmpp", "aiohttp.access"):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)


async def run(config_path: str) -> None:
    settings = load_settings(config_path)
    validate_settings(settings)
    configure_logging(
        settings.log_level,
        settings.log_file,
        settings.log_max_bytes,
        settings.log_backup_count,
    )

    repository = Repository(settings.database_url)
    await repository.connect()
    await repository.migrate()

    transport = TelegramTransport(settings, repository)
    app = create_app(
        settings.qr_storage_dir,
        settings.avatar_storage_dir,
        repository,
        media_handler=transport.stream_media,
    )
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, settings.health_host, settings.health_port)
    await site.start()

    try:
        await transport.run_forever()
    finally:
        await transport.stop()
        await runner.cleanup()
        await repository.close()


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.ini")
    parser.add_argument("--daemon", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--stop", action="store_true")
    parser.add_argument("--pid-file")
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)
    settings = load_settings(args.config)
    pid_file = args.pid_file or settings.transport_pid_file

    if args.status:
        pid = running_pid(pid_file)
        print("running: %s" % pid if pid else "stopped")
        return
    if args.stop:
        print("stopping" if stop(pid_file) else "not running")
        return
    if args.daemon:
        daemonize(pid_file)
    try:
        asyncio.run(run(args.config))
    finally:
        if args.daemon:
            remove_pid(pid_file)
