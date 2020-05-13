import time

from node import *

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.StreamHandler()]
    )
    while True:
        try:
            nodes = fetch_online_nodes()
            for node in nodes:
                logging.info(node.poll_status())
            poll_pending_or_running_jobs()
            poll_created_jobs()
        finally:
            time.sleep(5)
