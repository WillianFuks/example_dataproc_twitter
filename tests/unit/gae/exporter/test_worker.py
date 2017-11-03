#MIT License
#
#Copyright (c) 2017 Willian Fuks
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.


import json
import webtest
import unittest
import mock
import gae.exporter.worker as worker
from gae.exporter.worker import app
from gae.exporter import utils


class TestWorkerService(unittest.TestCase):
    test_app = webtest.TestApp(app)


    @staticmethod
    def load_mock_config():
        data = open('tests/unit/data/gae/exporter/test_config.json').read()
        return json.loads(data)


    @mock.patch('gae.exporter.worker.request')
    @mock.patch('gae.exporter.utils.uuid')
    @mock.patch('gae.exporter.worker.bq_service')
    def test_export(self, con_mock, uuid_mock, request_mock):
        uuid_mock.uuid4.return_value = 'name'
        request_mock.form.get.return_value = '20171010'
        worker.config = self.load_mock_config()
        query_job_body = utils.load_query_job_body("20171010", **worker.config)

        extract_job_body = utils.load_extract_job_body("20171010", **worker.config)

        job_mock = mock.Mock()
        con_mock.execute_job.return_value = 'job'
        response = self.test_app.post("/queue_export?date=20171010")

        con_mock.execute_job.assert_any_call(*['project123',
                                              query_job_body])
        con_mock.execute_job.assert_any_call(*['project123',
                                              extract_job_body])
        self.assertEqual(response.status_int, 200)

