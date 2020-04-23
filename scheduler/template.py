import logging

import requests

from scheduler.node import *


def add_template(project_name: str, egg_path: str, version: str):
    """
    :param project_name: template name to be deployed
    :param egg_path: absolute path to egg file on this machine
    :param version: new version (generate before calling this function)
    """
    nodes = fetch_online_nodes()
    for node in nodes:
        try:
            r = node.add_version(project_name, version, egg_path)
            logging.info(f'add template {r}')
        except requests.exceptions.RequestException as _e:
            update_node_status(node.node_id, NodeStatus.OFFLINE)


def delete_template(project_name: str):
    """
    :param project_name: template name to be de-deployed
    """
    nodes = fetch_online_nodes()
    for node in nodes:
        try:
            r = node.delete_project(project_name)
            logging.info(f'del template {r}')
        except requests.exceptions.RequestException as _e:
            update_node_status(node.node_id, NodeStatus.OFFLINE)
