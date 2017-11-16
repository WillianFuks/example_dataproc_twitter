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
import unittest
import mock
import datetime

import gae.exporter.utils as utils

class TestUtils(unittest.TestCase):
    @staticmethod
    def load_mock_config():
        data = (open('tests/unit/data/gae/exporter/test_config.json')
                .read().replace("config = ", ""))
        return json.loads(data)


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
        dest = 'gs://bucket/output/{}/result.gz'
        expected = {
            'jobReference': {
                'projectId': 'project123',
                'jobId': 'name'
            },
            'configuration': {
                'extract': {
                    'sourceTable': {
                        'projectId': 'project123',
                        'datasetId': 'extract_dataset',
                        'tableId': 'extract_table',
                    },
                    'destinationUris': [],
                    'destinationFormat': 'CSV',
                    'compression': 'GZIP'
                }
            }
        }
 
        result = utils.load_extract_job_body(None, **self.load_mock_config())
        expected['configuration']['extract']['destinationUris'] = [
            dest.format(utils.get_yesterday_date().strftime("%Y-%m-%d"))]
        self.assertEqual(expected, result)

        result = utils.load_extract_job_body("20171010", **self.load_mock_config())
        expected['configuration']['extract']['destinationUris'] = [
            dest.format("2017-10-10")]


    def test_format_date(self):
        result = utils.format_date("20171010")
        expected = "2017-10-10"
        self.assertEqual(result, expected)


    def test_process_url_date(self):
        expected = None
        result = utils.process_url_date({})
        self.assertEqual(expected, result)

        expected = "20171010"
        result = utils.process_url_date({"date": "20171010"})
        self.assertEqual(expected, result)

        with self.assertRaises(ValueError):
            utils.process_url_date({"date": "2017-10-10"})

