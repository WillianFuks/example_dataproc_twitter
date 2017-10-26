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


import unittest
import mock
import datetime

import gae.exporter.utils as utils


class TestUtils(unittest.TestCase):

    def test_get_yesterday_date(self):
        expected = datetime.datetime.now() + datetime.timedelta(days=-1)
        result = utils.get_yesterday_date()
        self.assertEqual(expected.date(), result.date())

    @mock.patch('gae.exporter.utils.uuid')
    def test_load_query_job_body(self, uuid_mock):
        expected = {'jobReference': {
                'projectId': 'project123',
                'jobId': 'name'
                },
            'configuration': {
                'query': {
                    'destinationTable': {
                        'datasetId': 'dataset_id',
                        'tableId': 'table_id',
                        'projectId': 'project123'
                         },
                    'maximumBytesBilled': 100000000000,
                    'query': "select 1",
                    'useLegacySql': False,
                    'writeDisposition': 'WRITE_TRUNCATE'
                    }
                }
            }
        uuid_mock.uuid4.return_value = 'name'

        result = utils.load_query_job_body("select 1",
                                           "project123",
                                           "dataset_id",
                                           "table_id")

        self.assertEqual(expected, result)


    def test_load_query(self):
        expected = "select 1 from project123.dataset_id.table_id"
        result = utils.load_query(
            'tests/unit/data/gae/exporter/test_query.sql',
            'project123',
            'dataset_id',
            'table_id')
        self.assertEqual(expected, result)


    @mock.patch('gae.exporter.utils.uuid')
    def test_load_extract_job_body(self, uuid_mock):
        uuid_mock.uuid4.return_value = 'name'
        expected = {
            'jobReference': {
                'projectId': 'project123',
                'jobId': 'name'
            },
            'configuration': {
                'extract': {
                    'sourceTable': {
                        'projectId': 'project123',
                        'datasetId': 'dataset_id',
                        'tableId': 'table_id',
                    },
                    'destinationUris': ['gs://bucket/folder'],
                    'destinationFormat': 'CSV',
                    'compression': 'GZIP'
                }
            }
        }
 
        result = utils.load_extract_job_body('project123',
                                             'gs://bucket/folder',
                                             'dataset_id',
                                             'table_id')
        self.assertEqual(expected, result)


    def test_load_config(self):
        config = {'project_id': 'project123',
                  'dataset_id': 'dataset_id',
                  'table_id': 'table_id',
                  'query_path': 'path/to/query.sql',
                  'output': 'output'}
    
        expected = ['project123', 'dataset_id', 'table_id',
                    'path/to/query.sql', 'output']
        result = utils.load_config(config)
        self.assertEqual(expected, result)
