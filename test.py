import os
import shutil
from http.server import BaseHTTPRequestHandler, HTTPServer
import subprocess
from threading import Thread
from unittest import TestCase

import requests

from node import Node, NodeStatus, JobStatus, Job, TaskStatus


class MockServerRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Process an HTTP GET request and return a response with an HTTP 200 status.
        self.send_response(requests.codes.ok)
        self.end_headers()
        return


class Test(TestCase):
    scrapyd = None

    @classmethod
    def setUpClass(cls) -> None:
        if os.path.exists('test_scrapyd'):
            shutil.rmtree('test_scrapyd')
        os.mkdir('test_scrapyd')
        cls.scrapyd = subprocess.Popen('scrapyd', cwd='test_scrapyd')
        cls.mock_server_port = 8000
        cls.mock_server = HTTPServer(('127.0.0.1', cls.mock_server_port), MockServerRequestHandler)
        cls.mock_server_thread = Thread(target=cls.mock_server.serve_forever)
        cls.mock_server_thread.setDaemon(True)
        cls.mock_server_thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.scrapyd.kill()

    def test_api_url_prefix(self):
        n1 = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        self.assertEqual(n1.api_url_prefix(), 'http://127.0.0.1:6800/')
        n2 = Node(0, 'example.com', 6800, '', '', NodeStatus.ONLINE)
        self.assertEqual(n2.api_url_prefix(), 'http://example.com:6800/')
        pass

    def test_poll_status(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        status = node.poll_status()
        print(status)
        print(status['status'])
        self.assertEqual(status['status'], 'ok')
        self.assertEqual(status['pending'], 0)
        self.assertEqual(status['running'], 0)
        self.assertEqual(status['finished'], 0)
        bad_node = Node(0, '127.0.0.1', 1234, '', '', NodeStatus.ONLINE)
        status = bad_node.poll_status()
        self.assertEqual(status, '[Node_0: 0] no response')
        pass

    def test_add_version(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        r = node.add_version('dummy', 'r1', 'test.egg')
        self.assertEqual(r['status'], 'ok')
        self.assertEqual(r['spiders'], 1)
        self.assertTrue(os.path.exists('test_scrapyd/eggs/dummy/r1.egg'))
        pass

    def test_schedule(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        node.add_version('dummy', 'r1', 'test.egg')
        r = node.schedule('dummy', 'test', setting=None, job_id=None, version=None, arg=None)
        self.assertEqual(r['status'], 'ok')
        r = node.schedule('dummy', 'test', setting='setting=1', job_id='uuid', version='r1', arg={'k': 'v'})
        self.assertEqual(r['status'], 'ok')
        self.assertEqual(r['jobid'], 'uuid')
        pass

    def test_cancel(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        node.add_version('dummy', 'r1', 'test.egg')
        r = node.schedule('dummy', 'test', setting=None, job_id=None, version=None, arg=None)
        job_id = r['jobid']
        r = node.cancel('dummy', job_id)
        self.assertEqual(r['status'], 'ok')
        pass

    def test_list_projects(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        node.add_version('dummy', 'r1', 'test.egg')
        r = node.list_projects()
        self.assertEqual(r['status'], 'ok')
        self.assertEqual(r['projects'], ['dummy'])
        pass

    def test_list_versions(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        node.add_version('dummy', 'r1', 'test.egg')
        node.add_version('dummy', 'r2', 'test.egg')
        r = node.list_versions('dummy')
        self.assertEqual(r['status'], 'ok')
        self.assertEqual(r['versions'], ['r1', 'r2'])
        pass

    def test_list_spiders(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        node.add_version('dummy', 'r1', 'test.egg')
        r = node.list_spiders('dummy', 'r1')
        self.assertEqual(r['status'], 'ok')
        self.assertEqual(r['spiders'], ['test'])
        pass

    def test_list_jobs(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        node.add_version('dummy', 'r1', 'test.egg')
        r = node.list_jobs('dummy')
        self.assertEqual(r['status'], 'ok')
        pass

    def test_delete_version(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        node.add_version('dummy', 'r1', 'test.egg')
        node.add_version('dummy', 'r2', 'test.egg')
        r = node.delete_version('dummy', 'r1')
        self.assertEqual(r['status'], 'ok')
        r = node.list_versions('dummy')
        self.assertEqual(r['status'], 'ok')
        self.assertEqual(r['versions'], ['r2'])
        pass

    def test_delete_project(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        node.add_version('dummy', 'r1', 'test.egg')
        r = node.delete_project('dummy')
        self.assertEqual(r['status'], 'ok')
        r = node.list_projects()
        self.assertEqual(r['projects'], [])
        pass

    def test_add_job(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        node.add_version('dummy', 'r1', 'test.egg')
        node.add_job('dummy', 'test', '', 'uuid', 12, '{"k":"v"}')
        node.add_job('dummy', 'test', None, 'uuid', 13, '{"k":"v"}')
        node.add_job('dummy', 'test', 'SETTING=1', 'uuid', 14, '{"k":"v"}')
        try:
            node.add_job('dummy2', 'test', 'SETTING=1', 'uuid', 14, '{"k":"v"}')
        except:
            pass
        pass

    def test_cancel_job(self):
        node = Node(0, '127.0.0.1', 6800, '', '', NodeStatus.ONLINE)
        node.add_version('dummy', 'r1', 'test.egg')
        try:
            node.cancel_job('dummy', 'not_exists')
        except:
            pass
        node.add_job('dummy', 'test', '', 'uuid1', 12, '{"k":"v"}')
        r = node.cancel_job('dummy', 'uuid1')
        self.assertEqual(r, JobStatus.PENDING)
        # Note: mocked scrapyd cannot run test spider properly
        # sleep(2)
        # r = node.cancel_job('dummy', 'uuid2')
        # self.assertEqual(r, JobStatus.RUNNING, 'running')
        # node.add_job('dummy', 'test', '', 'uuid3', 12, '{"k":"v"}')
        # sleep(15)
        # r = node.cancel_job('dummy', 'uuid3')
        # self.assertEqual(r, JobStatus.FINISHED, 'finished')
        pass

    def test_job_print(self):
        job = Job('id', 'dummy', 'test', None, '{}', 0, JobStatus.PENDING, TaskStatus.RUNNING, 1)
        self.assertEqual(str(job), '[Job_id]')
        pass

    def test_update_status(self):
        job = Job('id', 'dummy', 'test', None, '{}', 0, JobStatus.PENDING, TaskStatus.RUNNING, 1)
        job.update_status(JobStatus.FINISHED)
        pass

    def test_update_node(self):
        job = Job('id', 'dummy', 'test', None, '{}', 0, JobStatus.PENDING, TaskStatus.RUNNING, 1)
        job.update_node(1)
        pass

