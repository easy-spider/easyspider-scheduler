from enum import IntEnum

import mysql.connector
import requests

from settings import *


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
        print(response.json())
        # { "status": "ok", "running": "0", "pending": "0", "finished": "0", "node_name": "node-name" }

    def add_version(self, project_name: str, project_version: str, egg_path: str):
        url = self.api_url_prefix() + 'addversion.json'
        data = {'project': project_name, 'version': project_version}
        response = requests.post(url, files={'egg': open(egg_path, 'rb')}, data=data, auth=(
            self.username, self.password))
        print(response.json())
        # {"status": "ok", "spiders": 3}

    def schedule(self, project_name: str, spider_name: str, setting: str, job_id: str, version: str, arg):
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
        for k, v in arg:
            data[k] = v
        url = self.api_url_prefix() + 'schedule.json'
        response = requests.post(url, data, auth=(self.username, self.password))
        print(response.json())
        # {"status": "ok", "jobid": "6487ec79947edab326d6db28a2d86511e8247444"}

    def cancel(self, project_name: str, job_id: str):
        url = self.api_url_prefix() + 'cancel.json'
        data = {'project': project_name, 'job': job_id}
        response = requests.post(url, data, auth=(self.username, self.password))
        print(response.json())
        # {"status": "ok", "prevstate": "running"}

    def list_projects(self):
        url = self.api_url_prefix() + 'listprojects.json'
        response = requests.get(url, auth=(self.username, self.password))
        print(response.json())
        # {"status": "ok", "projects": ["myproject", "otherproject"]}

    def list_versions(self, project_name: str):
        url = self.api_url_prefix() + 'listversions.json'
        data = {
            'project': project_name
        }
        response = requests.get(url, data=data, auth=(self.username, self.password))
        print(response.json())
        # {"status": "ok", "versions": ["r99", "r156"]}

    def list_spiders(self, project_name: str, project_version):
        url = self.api_url_prefix() + 'listspiders.json'
        data = {
            'project': project_name
        }
        if project_version is not None:
            data['_version'] = project_version
        response = requests.get(url, data=data, auth=(self.username, self.password))
        print(response.json())
        # {"status": "ok", "spiders": ["spider1", "spider2", "spider3"]}

    def list_jobs(self, project_name: str):
        url = self.api_url_prefix() + 'listjobs.json'
        data = {
            'project': project_name
        }
        response = requests.get(url, data=data, auth=(self.username, self.password))
        print(response.json())
        # {"status": "ok",
        #  "pending": [{"id": "78391cc0fcaf11e1b0090800272a6d06", "spider": "spider1"}],
        #  "running": [{"id": "422e608f9f28cef127b3d5ef93fe9399", "spider": "spider2", "start_time": "2012-09-12 10:14:03.594664"}],
        #  "finished": [{"id": "2f16646cfcaf11e1b0090800272a6d06", "spider": "spider3", "start_time": "2012-09-12 10:14:03.594664", "end_time": "2012-09-12 10:24:03.594664"}]}

    def delete_version(self, project_name: str, project_version: str):
        url = self.api_url_prefix() + 'delversion.json'
        data = {'project': project_name, 'version': project_version}
        response = requests.post(url, data=data, auth=(self.username, self.password))
        print(response.json())
        # {"status": "ok"}

    def delete_project(self, project_name: str):
        url = self.api_url_prefix() + 'delproject.json'
        data = {'project': project_name}
        response = requests.post(url, data=data, auth=(self.username, self.password))
        print(response.json())
        # {"status": "ok"}


def fetch_node_list():
    node_list = []
    conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                   host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DATABASE)
    cursor = conn.cursor()
    cursor.execute('SELECT id,ip,port,username,password,status FROM `node`')
    r = cursor.fetchall()
    for node_id, server_ip, server_port, username, password, status in r:
        node_list.append(Node(node_id, server_ip, server_port, username, password, status))
    cursor.close()
    conn.close()
    return node_list


def update_node_status(node_id: int, status: NodeStatus):
    conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                   host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DATABASE)
    cursor = conn.cursor()
    cursor.execute('UPDATE `node` SET status=%s WHERE id=%s', (int(status), node_id))
    cursor.close()
    conn.commit()
    conn.close()
