import argparse
import json
import logging
import os
import queue
import sys

from dataclasses import dataclass
from typing import List, Optional

from graft import __version__
from graft.event import connect_to_event_listener, push_job_result, PrismConfig
from graft.auth import get_auth_token, AuthConfig
from graft.jobs import TaskWatcher

__author__ = "Teague Lasser"
__copyright__ = "Subsequent Corporation"
__license__ = "MIT"


def parse_args(args: List[str]) -> argparse.Namespace:
    """Parse command line parameters"""
    parser = argparse.ArgumentParser(description="Graft Job runner")
    parser.add_argument(
        "--version",
        action="version",
        version=f"graft {__version__}",
    )
    parser.add_argument(
        dest="config", help="Configuration file", type=argparse.FileType("r")
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "--debug",
        dest="loglevel",
        help=argparse.SUPPRESS,
        action="store_const",
        const=logging.DEBUG,
    )

    return parser.parse_args(args)


def setup_logging(loglevel: int):
    """Setup basic logging"""
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


@dataclass
class Config:
    prism: PrismConfig
    auth: Optional[AuthConfig]
    namespace: str

    def __init__(self, prism, namespace, auth=None):
        self.prism = PrismConfig(**prism)
        self.namespace = namespace
        self.auth = None if auth is None else AuthConfig(**auth)


def main(args: List[str]):
    args = parse_args(args)
    setup_logging(args.loglevel)

    config = json.load(args.config)
    config = Config(**config)

    if config.auth:
        client_secret = os.getenv("GRAFT_CLIENT_SECRET")
        if client_secret is None:
            raise ValueError("Empty client secret; set GRAFT_CLIENT_SECRET")
        token = get_auth_token(config.auth, client_secret=client_secret)
    else:
        token = None

    task_queue = queue.Queue()
    sources = config.prism.event_sources
    client = connect_to_event_listener(config.prism, token=token, queue=task_queue)
    watcher = TaskWatcher(config.namespace, task_queue)
    watcher.start()

    try:
        while watcher.is_alive():
            try:
                result = watcher.results.get(timeout=watcher.QUEUE_POLL_RATE)
            except queue.Empty:
                continue
            push_job_result(client, result, sources)
    finally:
        print("Performing job cleanup...")
        task_queue.put(None)
        watcher.join()
        while not watcher.results.empty():
            result = watcher.results.get_nowait()
            push_job_result(client, result, sources)


def cli():
    """Calls :func:`main` from the command line."""
    main(sys.argv[1:])


if __name__ == "__main__":
    cli()
