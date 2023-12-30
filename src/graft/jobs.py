import logging

from datetime import datetime
from dataclasses import dataclass
from queue import Queue, Empty
from threading import Thread
from typing import Dict, Optional

from kubernetes import config, client
from kubernetes.client.rest import ApiException

_log = logging.getLogger(__name__)


def create_job_config(name, container):
    """Create a job from the default template."""
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": "main",
                        "image": container,
                    }],
                    "restartPolicy": "Never"
                }
            }
        }
    }


class TaskWatcher(Thread):
    QUEUE_POLL_RATE = 1.0

    def __init__(self, namespace, queue):
        super().__init__()
        self.namespace = namespace
        self._queue = queue
        self.results = Queue()
        self._jobs = []

    def run(self):
        # Default namespace is considered to be dev mode
        if self.namespace == "default":
            config.load_kube_config()
        else:
            config.load_incluster_config()
        api = client.BatchV1Api()
        while True:
            try:
                next_task = self._queue.get(timeout=self.QUEUE_POLL_RATE)
            except Empty:
                self.reap_jobs(api)
                continue

            # Check guard value for quit
            if next_task is None:
                return
            self.start_job(api, next_task)
            self.reap_jobs(api)

    def reap_jobs(self, api: client.BatchV1Api):
        new_jobs = []
        jobs = list_active_jobs(api, self.namespace)
        for job_name in self._jobs:
            job = jobs.pop(job_name)
            if job is None:
                continue
            if job.completion_time is not None:
                self.results.put(job)
                self.delete_job(api, job_name)
            else:
                new_jobs.append(job_name)

        if jobs:
            _log.warn("Namespace has other jobs. They will be deleted: %s",
                      list(jobs.keys()))
            for job in jobs.values():
                self.delete_job(api, job.job_name)

        self._jobs = new_jobs

    def delete_job(self, api: client.BatchV1Api, job_name: str):
        delete_immediate(api, job_name, self.namespace)

    def start_job(self, api, next_task):
        job_name = create_job(api,
                              self.namespace,
                              next_task.name,
                              next_task.container)
        self._jobs.append(job_name)


@dataclass
class Job:
    job_name: str
    completion_time: Optional[datetime]
    succeeded: Optional[bool]
    job_log: Optional[str]


def list_active_jobs(api: client.BatchV1Api, namespace: str) -> Dict[str, Job]:
    """List all active jobs in the namespace."""
    jobs_api_response = api.list_namespaced_job(namespace)
    jobs = {}
    for api_job in jobs_api_response.items:
        name = api_job.metadata.name
        completion_time = api_job.status.completion_time
        if completion_time is not None:
            succeeded = api_job.status.succeeded > 0
            job_log = get_job_log(api, name, namespace)
        else:
            succeeded = None
            job_log = None
        jobs[name] = Job(name, completion_time, succeeded, job_log)
    return jobs


def get_job_log(api: client.BatchV1Api,
                job_name: str,
                namespace: str) -> Optional[str]:
    """Find the log of the latest pod in the job."""
    core_api = client.CoreV1Api(api_client=api.api_client)
    selector = 'job-name={}'.format(job_name)
    pods = core_api.list_namespaced_pod(namespace=namespace,
                                        label_selector=selector)
    if not pods.items:
        return None

    # Get the logs of the last created pod
    last_pod = sorted(pods.items,
                      key=lambda pod: pod.metadata.creation_timestamp,
                      reverse=True)[0]
    log = core_api.read_namespaced_pod_log(name=last_pod.metadata.name,
                                           namespace=namespace)
    return log


def delete_immediate(api, job_name, namespace):
    """Delete a job with no grace period"""
    try:
        api.delete_namespaced_job(job_name,
                                  namespace,
                                  grace_period_seconds=0)
    except ApiException as e:
        _log.error("Delete failed: %s", e)


def create_job(api: client.BatchV1Api,
               namespace: str,
               name: str,
               container: str):
    manifest = create_job_config(name, container)
    api_response = api.create_namespaced_job(
        body=manifest,
        namespace=namespace
    )
    return api_response.metadata.name
