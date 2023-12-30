import logging
import json
from dataclasses import asdict, dataclass
from queue import Queue
from typing import List, Optional

from prism import Client, Wavelet
from .jobs import Job

_log = logging.getLogger(__name__)


@dataclass
class PrismConfig:
    addr: str
    event_sources: List[str]
    event_sinks: List[str]


def push_job_result(client: Client, result: Job, sources: List[str]):
    result = asdict(result)
    result["completion_time"] = result["completion_time"].isoformat()
    json_bytes = json.dumps(result).encode('utf-8')
    for source in sources:
        client.emit(source, json_bytes)


@dataclass
class Task:
    name: str
    container: str


def connect_to_event_listener(config: PrismConfig,
                              token: Optional[str],
                              queue: Queue):
    def event_handler(wavelet: Wavelet):
        for photon in wavelet.photons:
            task_data = json.loads(photon.payload)
            try:
                task = Task(**task_data)
                queue.put(task)
            except TypeError:
                _log.warning(
                    "Could not make Task out of incoming data, got: %s",
                    task_data)

    client = Client(f"ws://{config.addr}", event_handler)
    for event_source in config.event_sources:
        client.add_beam(event_source)
    for event_sink in config.event_sinks:
        client.subscribe(event_sink)
    return client
