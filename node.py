import json
import logging
from enum import IntEnum, Enum

import requests

from settings import *


class JobStatus(IntEnum):
    CREATED = 0
    PENDING = 1
    RUNNING = 2
    FINISHED = 3


class TaskStatus(Enum):
    READY = 'ready'
    RUNNING = 'running'
    PAUSED = 'paused'
    FINISHED = 'finished'


class NodeStatus(IntEnum):
    ONLINE = 0
    OFFLINE = 1
    DISABLED = 2


class Node:
    def __init__(self, node_id: int, server_ip: str, server_port: int, username: str, password: str, status: int):
        self.node_id = node_id
        self.server_ip = server_ip
        self.server_port = server_port
        self.username = username
        self.password = password
        self.status = NodeStatus(status)

    def __repr__(self):
        return f'[Node_{self.node_id}: {self.status}]'

    def api_url_prefix(self):
        return f'http://{self.server_ip}:{self.server_port}/'

    def daemon_status(self):
        url = self.api_url_prefix() + 'daemonstatus.json'
        response = requests.get(url, auth=(self.username, self.password), timeout=3)
        return response.json()
        # { "status": "ok", "running": "0", "pending": "0", "finished": "0", "node_name": "node-name" }

    def poll_status(self):
        try:
            return self.daemon_status()
        except requests.exceptions.RequestException as _e:
            update_node_status(self.node_id, NodeStatus.OFFLINE)
            return f'{self} no response'

    def add_version(self, project_name: str, project_version: str, egg_path: str):
        url = self.api_url_prefix() + 'addversion.json'
        data = {'project': project_name, 'version': project_version}
        egg = open(egg_path, 'rb')
        response = requests.post(url, files={'egg': egg}, data=data, auth=(
            self.username, self.password))
        egg.close()
        return response.json()
        # {"status": "ok", "spiders": 3}

    def schedule(self, project_name: str, spider_name: str, setting, job_id, version, arg):
        data = {
            'project': project_name,
            'spider': spider_name,
        }
        if setting is not None:
            data['setting'] = setting.split(';')
        if job_id is not None:
            data['jobid'] = job_id
        if version is not None:
            data['_version'] = version
        if arg is not None:
            for k in arg.keys():
                data[k] = arg[k]
        url = self.api_url_prefix() + 'schedule.json'
        response = requests.post(url, data, auth=(self.username, self.password))
        return response.json()
        # {"status": "ok", "jobid": "6487ec79947edab326d6db28a2d86511e8247444"}

    def cancel(self, project_name: str, job_id: str):
        url = self.api_url_prefix() + 'cancel.json'
        data = {'project': project_name, 'job': job_id}
        response = requests.post(url, data, auth=(self.username, self.password))
        return response.json()
        # {"status": "ok", "prevstate": "running"}

    def list_projects(self):
        url = self.api_url_prefix() + 'listprojects.json'
        response = requests.get(url, auth=(self.username, self.password))
        return response.json()
        # {"status": "ok", "projects": ["myproject", "otherproject"]}

    def list_versions(self, project_name: str):
        url = self.api_url_prefix() + 'listversions.json'
        params = {
            'project': project_name
        }
        response = requests.get(url, params=params, auth=(self.username, self.password))
        return response.json()
        # {"status": "ok", "versions": ["r99", "r156"]}

    def list_spiders(self, project_name: str, project_version):
        url = self.api_url_prefix() + 'listspiders.json'
        params = {
            'project': project_name
        }
        if project_version is not None:
            params['_version'] = project_version
        response = requests.get(url, params=params, auth=(self.username, self.password))
        return response.json()
        # {"status": "ok", "spiders": ["spider1", "spider2", "spider3"]}

    def list_jobs(self, project_name: str):
        url = self.api_url_prefix() + 'listjobs.json'
        params = {
            'project': project_name
        }
        response = requests.get(url, params=params, auth=(self.username, self.password))
        return response.json()
        # {"status": "ok",
        #  "pending": [{"id": "78391cc0fcaf11e1b0090800272a6d06", "spider": "spider1"}],
        #  "running": [{"id": "422e608f9f28cef127b3d5ef93fe9399", "spider": "spider2", "start_time": "2012-09-12 10:14:03.594664"}],
        #  "finished": [{"id": "2f16646cfcaf11e1b0090800272a6d06", "spider": "spider3", "start_time": "2012-09-12 10:14:03.594664", "end_time": "2012-09-12 10:24:03.594664"}]}

    def delete_version(self, project_name: str, project_version: str):
        url = self.api_url_prefix() + 'delversion.json'
        data = {'project': project_name, 'version': project_version}
        response = requests.post(url, data=data, auth=(self.username, self.password))
        return response.json()
        # {"status": "ok"}

    def delete_project(self, project_name: str):
        url = self.api_url_prefix() + 'delproject.json'
        data = {'project': project_name}
        response = requests.post(url, data=data, auth=(self.username, self.password))
        return response.json()
        # {"status": "ok"}

    def add_job(self, project_name: str, spider_name: str, setting, job_id: str, task_id: int, arg):
        dict_arg = json.loads(arg)
        if setting is None:
            setting = ''
        if setting != '':
            setting += ';'
        setting += f'MONGO_URL={MONGO_URL};SPIDER_NAME={spider_name};TASK_ID={task_id};JOB_ID={job_id}'
        r = self.schedule(project_name, spider_name, setting, job_id, None, dict_arg)
        status = r['status']
        if status != 'ok':
            logging.error(r)
            raise Exception('node schedule failed')

    def cancel_job(self, project_name: str, job_id: str) -> JobStatus:
        r = self.cancel(project_name, job_id)
        status = r['prevstate']
        if status == 'running':
            return JobStatus.RUNNING
        elif status == 'pending':
            return JobStatus.PENDING
        elif status == 'finished':
            return JobStatus.FINISHED
        else:
            logging.error(r)
            raise Exception('unknown previous status')


def pick_node(online_nodes) -> Node:
    min_pending = 0
    picked_node = None
    for node in online_nodes:
        try:
            status = node.daemon_status()
            pending = int(status['pending'])
            # pick the node with least pending job
            if picked_node is None or min_pending > pending:
                picked_node = node
                min_pending = pending
        except requests.exceptions.RequestException as _e:
            update_node_status(node.node_id, NodeStatus.OFFLINE)
            # disable not reachable node
    if picked_node is None:
        raise Exception("no available node")
    return picked_node


class Job:
    def __init__(self, job_id: str, project_name: str, spider_name: str, setting, arg: str, node_id: int,
                 status: JobStatus, task_status: TaskStatus, task_id: int):
        self.job_id = job_id
        self.project_name = project_name
        self.spider_name = spider_name
        self.setting = setting
        self.arg = arg
        self.node_id = node_id
        self.status = status
        self.task_status = task_status
        self.task_id = task_id

    def __repr__(self):
        return f'[Job_{self.job_id}]'

    def update_status(self, status: JobStatus):
        update_job_status(self.job_id, status)

    def update_node(self, node_id: int):
        update_job_node(self.job_id, node_id)


def poll_pending_or_running_jobs():
    jobs = fetch_job_by_status(JobStatus.PENDING)
    jobs += fetch_job_by_status(JobStatus.RUNNING)
    logging.info(f'fetched pending or running jobs: {jobs}')
    projects = set()
    for job in jobs:
        projects.add(job.project_name)
    nodes = fetch_online_nodes()
    job_status = dict()
    for project_name in projects:
        for node in nodes:
            try:
                r = node.list_jobs(project_name)
                if r['status'] != 'ok':
                    logging.error(f'node.list_jobs not ok {r}')
                for j in r['pending']:
                    job_status[j['id']] = JobStatus.PENDING
                for j in r['running']:
                    job_status[j['id']] = JobStatus.RUNNING
                for j in r['finished']:
                    job_status[j['id']] = JobStatus.FINISHED
            except requests.exceptions.RequestException as _e:
                update_node_status(node.node_id, NodeStatus.OFFLINE)
    for job in jobs:
        if job.job_id in job_status.keys():
            job.update_status(job_status[job.job_id])
        else:
            # job missing (possibly node failed)
            job.update_status(JobStatus.CREATED)


def poll_created_jobs():
    jobs = fetch_job_by_status(JobStatus.CREATED)
    logging.info(f'fetched created jobs: {jobs}')
    for job in jobs[:10]:
        if job.task_status is not TaskStatus.RUNNING and job.task_status is not TaskStatus.READY:
            logging.info(f'{job} task status {job.task_status} not ready or running')
            continue
        node = pick_node(fetch_online_nodes())
        logging.info(f'pick {node} for job {job}')
        node.add_job(job.project_name, job.spider_name, job.setting, job.job_id, job.task_id, job.arg)
        job.update_status(JobStatus.PENDING)
        job.update_node(node.node_id)


def fetch_node_by_id(node_id) -> Node:
    r = requests.get(f'{DJANGO_API}node/get/{node_id}/').json()
    return Node(r['id'], r['ip'], r['port'], r['username'], r['password'], r['status'])


def fetch_online_nodes():
    node_list = []
    nodes = requests.get(f'{DJANGO_API}node/list-online/').json()
    for r in nodes:
        node_list.append(Node(r['id'], r['ip'], r['port'], r['username'], r['password'], r['status']))
    logging.info(f'fetched online nodes: {node_list}')
    return node_list


def update_node_status(node_id: int, status: NodeStatus):
    requests.get(f'{DJANGO_API}node/set-status/{node_id}/{status}/')
    logging.info(f'[Node_{node_id}] status -> {status}')


def fetch_job_by_status(status: JobStatus):
    job_list = []
    jobs = requests.get(f'{DJANGO_API}job/list/{status}/').json()
    for r in jobs:
        job_list.append(
            Job(r['id'],
                r['project_name'],
                r['spider_name'],
                r['settings'],
                r['args'],
                r['node_id'],
                r['status'],
                TaskStatus(r['task_status']),
                r['task_id']))
    return job_list


def update_job_status(job_id: str, status: JobStatus):
    requests.get(f'{DJANGO_API}job/set-status/{job_id}/{status}/')
    logging.info(f'[job_{job_id}] status -> {status}')


def update_job_node(job_id: str, node_id: int):
    requests.get(f'{DJANGO_API}job/set-node/{job_id}/{node_id}/')
    logging.info(f'[job_{job_id}] node -> {node_id}')
