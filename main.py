from node import *

if __name__ == '__main__':
    node_list = fetch_node_list()
    print(node_list)
    for node in node_list:
        node.daemon_status()
        node.list_projects()
    update_node_status(1, NodeStatus.ONLINE)
