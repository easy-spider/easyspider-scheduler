import json
import logging
from enum import IntEnum, Enum

import mysql.connector
import requests

from scheduler.settings import *


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
        response = requests.get(url, auth=(self.username, self.password))
        return response.json()
        # { "status": "ok", "running": "0", "pending": "0", "finished": "0", "node_name": "node-name" }

    def add_version(self, project_name: str, project_version: str, egg_path: str):
        url = self.api_url_prefix() + 'addversion.json'
        data = {'project': project_name, 'version': project_version}
        response = requests.post(url, files={'egg': open(egg_path, 'rb')}, data=data, auth=(
            self.username, self.password))
        return response.json()
        # {"status": "ok", "spiders": 3}

    def schedule(self, project_name: str, spider_name: str, setting, job_id, version, arg):
        data = {
            'project': project_name,
            'spider': spider_name,
        }
        if setting is not None:
            data['setting'] = setting
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

    def add_job(self, project_name: str, spider_name: str, setting, job_id: str, arg):
        dict_arg = json.load(arg)
        r = self.schedule(project_name, spider_name, setting, job_id, None, dict_arg)
        status = r['status']
        if status != 'ok':
            raise Exception('node schedule failed')

    def cancel_job(self, project_name: str, job_id: str) -> JobStatus:
        r = self.cancel(project_name, job_id)
        if r['status'] != 'ok':
            raise Exception('cancel failed')
        status = r['prevstate']
        if status == 'running':
            return JobStatus.RUNNING
        elif status == 'pending':
            return JobStatus.PENDING
        elif status == 'finished':
            return JobStatus.FINISHED
        else:
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
    def __init__(self, job_id: str, project_name: str, spider_name: str, setting: str, arg: str, node_id: int,
                 status: JobStatus, task_status: TaskStatus):
        self.job_id = job_id
        self.project_name = project_name
        self.spider_name = spider_name
        self.setting = setting
        self.arg = arg
        self.node_id = node_id
        self.status = status
        self.task_status = task_status

    def update_status(self, status: JobStatus):
        update_job_status(self.job_id, status)

    def update_node(self, node_id: int):
        update_job_node(self.job_id, node_id)


def poll_pending_or_running_jobs():
    jobs = fetch_job_by_status(JobStatus.PENDING)
    jobs += fetch_job_by_status(JobStatus.RUNNING)
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
    for job in jobs:
        if job.task_status is not TaskStatus.RUNNING or job.task_status is not TaskStatus.READY:
            continue
        node = pick_node(fetch_online_nodes())
        node.add_job(job.project_name, job.spider_name, job.setting, job.job_id, job.arg)
        job.update_status(JobStatus.PENDING)


def fetch_node_by_id(node_id) -> Node:
    conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                   host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DATABASE)
    cursor = conn.cursor()
    cursor.execute(f'SELECT id,ip,port,username,password,status FROM easyspider_node WHERE id=%s', (node_id))
    node_id, server_ip, server_port, username, password, status = cursor.fetchone()
    cursor.close()
    conn.close()
    return Node(node_id, server_ip, server_port, username, password, status)


def fetch_online_nodes():
    node_list = []
    conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                   host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DATABASE)
    cursor = conn.cursor()
    cursor.execute(
        f'SELECT id,ip,port,username,password,status FROM easyspider_node WHERE status={int(NodeStatus.ONLINE)}')
    r = cursor.fetchall()
    for node_id, server_ip, server_port, username, password, status in r:
        node_list.append(Node(node_id, server_ip, server_port, username, password, status))
    cursor.close()
    conn.close()
    logging.info(f'Fetched online nodes {node_list}')
    return node_list


def update_node_status(node_id: int, status: NodeStatus):
    conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                   host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DATABASE)
    cursor = conn.cursor()
    cursor.execute('UPDATE easyspider_node SET status=%s WHERE id=%s', (int(status), node_id))
    cursor.close()
    conn.commit()
    conn.close()
    logging.info(f'Node_{node_id} status -> {status}')


def fetch_job_by_status(status: JobStatus):
    jobs = []
    conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                   host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DATABASE)
    cursor = conn.cursor()
    cursor.execute(f"""
        select
           job.id as id,
           site.project_name as project_name,
           template.spider_name as spider_name,
           site.settings as settings,
           job.args as args,
           job.node_id as node_id,
           job.status as status,
           task.status as task_status
        from easyspider_job job
        left join easyspider_node node on job.node_id = node.id
        left join easyspider_task task on job.task_id = task.id
        left join easyspider_template template on task.template_id = template.id
        left join easyspider_sitetemplates site on template.site_templates_id = site.id
        where job.status={int(status)} 
    """)
    r = cursor.fetchall()
    for job_id, project, spider, setting, arg, node, status, ts in r:
        jobs.append(Job(job_id, project, spider, setting, arg, node, status, TaskStatus(ts)))
    cursor.close()
    conn.close()
    return jobs


def update_job_status(job_id: str, status: JobStatus):
    conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                   host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DATABASE)
    cursor = conn.cursor()
    cursor.execute('UPDATE easyspider_job SET status=%s WHERE id=%s', (int(status), job_id))
    cursor.close()
    conn.commit()
    conn.close()
    logging.info(f'Node_{job_id} status -> {status}')


def update_job_node(job_id: str, node_id: int):
    conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                   host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DATABASE)
    cursor = conn.cursor()
    cursor.execute('UPDATE easyspider_job SET node_id=%s WHERE id=%s', (node_id, job_id))
    cursor.close()
    conn.commit()
    conn.close()
    logging.info(f'Node_{job_id} node -> {node_id}')
