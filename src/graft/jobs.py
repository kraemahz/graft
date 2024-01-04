import logging

from datetime import datetime
from dataclasses import dataclass
from queue import Queue, Empty
from threading import Thread
from typing import Dict, Optional, List

from kubernetes import config, client
from kubernetes.client.rest import ApiException

_log = logging.getLogger(__name__)


@dataclass
class Job:
    job_name: str
    completion_time: Optional[datetime]
    succeeded: Optional[bool]
    job_log: Optional[str]


@dataclass
class Task:
    name: str
    container: str
    prompt: str


@dataclass
class ConfigMount:
    name: str
    namespace: str
    payload: str
    filename: str = "conf.yaml"

    @property
    def definition(self):
        """Create a ConfigMap definition."""
        return {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": self.namespace,
                "name": self.name,
            },
            "data": {self.filename: self.payload}
        }

    @property
    def mount_name(self):
        return f"config-map-{self.name}"

    def mount_path(self, app_name: str):
        return f"/etc/{app_name}/{self.filename}"

    def create_configmap(self, core_api: client.CoreV1Api):
        """Insert the ConfigMap into k8s."""
        return core_api.create_namespaced_config_map(
            namespace=self.namespace,
            body=self.definition
        )

    def volume_definition(self):
        return {
            "name": self.mount_name,
            "configMap": {"name": self.name}
        }

    def volume_mount(self, app_name: str):
        return {
            "name": self.mount_name,
            "mountPath": self.mount_path(app_name),
            "subPath": self.filename
        }


@dataclass
class Project:
    name: str
    git_address: str
    branch: str
    known_hosts_config_map: str
    private_key_secret: str
    external_env: List[Dict[str, Dict]]
    args: List[str]

    @classmethod
    def default(cls, **project_data):
        project_name = project_data["name"]

        if not project_data.get("known_hosts_config_map"):
            project_data["known_hosts_config_map"] = "github-known-hosts"

        if not project_data.get("private_key_secret"):
            project_data["private_key_secret"] = f"{project_name}-rsa-key"

        return cls(**project_data)


@dataclass
class JobMount:
    name: str
    namespace: str
    container: str

    def definition(self,
                   config_mount: ConfigMount,
                   project: Project):
        """Create a Job config from the default template."""

        volume_mounts = [{
            "name": "private-key",
            "readOnly": True,
            "mountPath": "/root/.ssh/id_rsa",
            "subPath": "id_rsa"
        }, {
            "name": "known-hosts",
            "mountPath": "/root/.ssh/known_hosts",
            "subPath": "known_hosts"
        }, {
            "name": "code",
            "mountPath": "/code",
        }] + config_mount.volume_mount

        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": self.name,
                "namespace": self.namespace,
            },
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "metadata": {
                        "labels": {
                            "app": project.name
                        }
                    },
                    "spec": {
                        "restartPolicy": "Never",
                        "initContainers": [{
                            "name": "checkout",
                            "image": self.container,
                            "imagePullPolicy": "IfNotPresent",
                            "command": [
                                "/bin/bash/",
                                "-c",
                                f"git clone {project.git_address} -- ."
                                f" && git checkout -b {project.branch}"
                            ],
                            "env": project.external_env,
                            "volumeMounts": volume_mounts,
                        }],
                        "containers": [{
                            "name": "main",
                            "image": self.container,
                            "imagePullPolicy": "IfNotPresent",
                            "args": [config.mount_path] + project.args,
                            "env": project.external_env,
                            "volumeMounts": volume_mounts,
                        }],
                        "volumes": [{
                            "name": "code",
                            "emptyDir": {"sizeLimit": "1G"}
                        }, {
                            "name": "known-hosts",
                            "configMap": {
                                "name": project.known_hosts_config_map
                            }
                        }, {
                            "name": "private-key",
                            "secret": {
                                "secretName": project.private_key_secret,
                                "defaultMode": "0400"
                            }
                        }] + config_mount.volume_definition(),
                    }
                }
            }
        }

    def create_job(self,
                   batch_api: client.BatchV1Api,
                   config_mount: ConfigMount,
                   project: Project):
        """Insert the ConfigMap into k8s."""
        manifest = self.definition(config_mount, project)
        return batch_api.create_namespaced_job(
            body=manifest,
            namespace=self.namespace
        )


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
                data = self._queue.get(timeout=self.QUEUE_POLL_RATE)
            except Empty:
                self.reap_jobs(api)
                continue

            try:
                task_data = data["task"]
                task = Task(**task_data)

                project_data = data["project"]
                project = Project.default(**project_data)
            except TypeError:
                _log.error(
                    "Could not make Task + Project out of data, got: %s", data)
                continue
            except KeyError:
                _log.error("Got an invalid container entry: %s", data)
                continue

            # Check guard value for quit
            if task is None:
                return
            self.start_job(api, project, task)
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

    def start_job(self,
                  api: client.BatchV1Api,
                  project: Project,
                  task: Task):
        job_name = create_job(api, self.namespace, project, task)
        self._jobs.append(job_name)


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
    core_api = client.CoreV1Api(api_client=api.api_client)
    try:
        api.delete_namespaced_job(job_name,
                                  namespace,
                                  grace_period_seconds=0)
        core_api.delete_namespaced_config_map(
            job_name,
            namespace,
            grace_period_seconds=0)
    except ApiException as e:
        _log.error("Delete failed: %s", e)


def create_job(api: client.BatchV1Api,
               namespace: str,
               project: Project,
               task: Task):
    core_api = client.CoreV1Api(api_client=api.api_client)
    config_mount = ConfigMount(task.name, namespace, task.prompt)
    config_mount.create_configmap(core_api)
    job_mount = JobMount(task.name, namespace, task.container)
    api_response = job_mount.create_job(api, config_mount, project)

    return api_response.metadata.name
