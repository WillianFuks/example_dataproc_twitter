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

    @staticmethod
    def load_mock_config():
        return {"jobs":{
                "query_job": {
                    "source": {
                        "table_id": "source_table",
                        "dataset_id": "source_dataset",
                        "project_id": "project123",
                        "query_path": ("tests/unit/data/gae/"
                                       "exporter/test_query.sql")
                     },
                     "destination": {
                        "table_id": "dest_table",
                        "dataset_id": "dest_dataset",
                        "project_id": "project123"
                     }

                },
                "extract_job": {
                    "table_id": "extract_table",
                    "dataset_id": "extract_dataset",
                    "project_id": "project123",
                    "output": "output/result.gz"
                }
            }
           }


    def test_get_yesterday_date(self):
        expected = datetime.datetime.now() + datetime.timedelta(days=-1)
        result = utils.get_yesterday_date()
        self.assertEqual(expected.date(), result.date())


    @mock.patch('gae.exporter.utils.uuid')
    def test_load_query_job_body(self, uuid_mock):
        query_str = ("SELECT 1 FROM `project123.source_dataset.source_table` "
                     "WHERE date={}")

        expected = {'jobReference': {
                'projectId': 'project123',
                'jobId': 'name'
                },
            'configuration': {
                'query': {
                    'destinationTable': {
                        'datasetId': 'dest_dataset',
                        'tableId': 'dest_table',
                        'projectId': 'project123'
                         },
                    'maximumBytesBilled': 100000000000,
                    'query': "",
                    'useLegacySql': False,
                    'writeDisposition': 'WRITE_TRUNCATE'
                    }
                }
            }
        uuid_mock.uuid4.return_value = 'name'
        result = utils.load_query_job_body(None, **self.load_mock_config())
        expected['configuration']['query']['query'] = query_str.format(
            utils.get_yesterday_date().strftime("%Y%m%d"))
        print expected['configuration']['query']['query']
        print
        print
        print result['configuration']['query']['query']
        self.assertEqual(expected, result)

        result = utils.load_query_job_body("20171010",
            **self.load_mock_config())
        expected['configuration']['query']['query'] = query_str.format(
            utils.get_yesterday_date().strftime("20171010"))
        self.assertEqual(expected, result)




    def test_load_query(self):
        expected = ("SELECT 1 FROM `project123.source_dataset.source_table`"
            " WHERE date={}")
        result = utils.load_query(None, **self.load_mock_config())
        self.assertEqual(expected.format(
            utils.get_yesterday_date().strftime("%Y%m%d")), result)

        result = utils.load_query("20171010", **self.load_mock_config())
        self.assertEqual(expected.format("20171010"), result)



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
