import base64
import json
import logging

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


def encode_b64(data: str) -> str:
    """Do the string dance to appease the byte-encoded gods"""
    return base64.standard_b64encode(data.encode('utf-8')).decode('utf-8')


def push_job_result(client: Client, result: Job, sources: List[str]):
    result = asdict(result)
    result["completion_time"] = result["completion_time"].isoformat()
    result["job_log"] = (
        encode_b64(result["job_log"])
        if result["job_log"] is not None else None
    )
    json_text = json.dumps(result)
    _log.info("Job result: %s", json_text)
    json_bytes = json_text.encode('utf-8')
    for source in sources:
        client.emit(source, json_bytes)


def connect_to_event_listener(config: PrismConfig,
                              token: Optional[str],
                              queue: Queue):
    def event_handler(wavelet: Wavelet):
        for photon in wavelet.photons:
            data = json.loads(photon.payload)
            _log.info("Job task: %s", data)
            queue.put(data)

    client = Client(f"ws://{config.addr}", event_handler)
    for event_source in config.event_sources:
        client.add_beam(event_source)
    for event_sink in config.event_sinks:
        client.subscribe(event_sink)
    return client
