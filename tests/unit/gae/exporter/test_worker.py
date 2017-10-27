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


import webtest
import unittest
import mock
from gae.exporter.worker import app


class TestWorkerService(unittest.TestCase):
    test_app = webtest.TestApp(app)

    @mock.patch('gae.exporter.utils.uuid')
    @mock.patch('gae.exporter.worker.bq_service')
    @mock.patch('gae.exporter.worker.utils.load_config')
    def test_export(self, utils_mock, con_mock, uuid_mock):
        uuid_mock.uuid4.return_value = 'name'
        query_job_body = {'configuration': {'query':
            {'query': 'select 1 from project123.dataset_id.table_id',
             'writeDisposition': 'WRITE_TRUNCATE',
             'destinationTable': {'projectId': 'project123',
                 'tableId': 'table_id',
                 'datasetId': 'dataset_id'},
             'maximumBytesBilled': 100000000000,
             'useLegacySql': False}},
             'jobReference': {'projectId': 'project123',
             'jobId': 'name'}}

        extract_job_body = {'configuration': {'extract':
            {'destinationFormat': 'CSV',
             'destinationUris': ['output'],
             'sourceTable': {'projectId': 'project123',
                 'tableId': 'table_id',
                 'datasetId': 'dataset_id'},
             'compression': 'GZIP'}},
             'jobReference': {'projectId': 'project123',
                 'jobId': 'name'}}

        utils_mock.return_value = ['project123',
            'dataset_id', 'table_id',
            'tests/unit/data/gae/exporter/test_query.sql',
            'output']
        job_mock = mock.Mock()
        con_mock.execute_job.return_value = 'job'
        response = self.test_app.post("/queue_export")
        con_mock.execute_job.assert_any_call(*['project123',
                                              query_job_body])
        con_mock.execute_job.assert_any_call(*['project123',
                                              extract_job_body])
        self.assertEqual(response.status_int, 200)


