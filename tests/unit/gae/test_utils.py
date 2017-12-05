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
import os
import shutil
from collections import Counter

from base import BaseTests


class TestUtils(unittest.TestCase, BaseTests):
    def setUp(self):
        self.prepare_environ()    

    def tearDown(self):
        self.clean_environ()

    @staticmethod
    def load_mock_config():
        data = (open('tests/unit/data/gae/test_config.json')
                .read().replace("config = ", ""))
        return json.loads(data)

    def test_get_yesterday_date(self):
        expected = datetime.datetime.now() + datetime.timedelta(days=-1)
        result = self.utils.get_yesterday_date()
        self.assertEqual(expected.date(), result.date())

    @mock.patch('gae.utils.uuid')
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
        result = self.utils.load_query_job_body(None, **self.load_mock_config())
        expected['configuration']['query']['query'] = query_str.format(
            self.utils.get_yesterday_date().strftime("%Y%m%d"))
        self.assertEqual(expected, result)

        result = self.utils.load_query_job_body("20171010",
            **self.load_mock_config())
        expected['configuration']['query']['query'] = query_str.format(
            self.utils.get_yesterday_date().strftime("20171010"))
        self.assertEqual(expected, result)

    def test_load_query(self):
        expected = ("SELECT 1 FROM `project123.source_dataset.source_table`"
            " WHERE date={}")
        result = self.utils.load_query(None, **self.load_mock_config())
        self.assertEqual(expected.format(
            self.utils.get_yesterday_date().strftime("%Y%m%d")), result)

        result = self.utils.load_query("20171010", **self.load_mock_config())
        self.assertEqual(expected.format("20171010"), result)

    @mock.patch('gae.utils.uuid')
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
        result = self.utils.load_extract_job_body(
            None, **self.load_mock_config())
        expected['configuration']['extract']['destinationUris'] = [
            dest.format(self.utils.get_yesterday_date().strftime("%Y-%m-%d"))]
        self.assertEqual(expected, result)

        result = self.utils.load_extract_job_body("20171010",
            **self.load_mock_config())
        expected['configuration']['extract']['destinationUris'] = [
            dest.format("2017-10-10")]

    def test_format_date(self):
        result = self.utils.format_date("20171010")
        expected = "2017-10-10"
        self.assertEqual(result, expected)

    def test_process_url_date(self):
        expected = None
        result = self.utils.process_url_date(None)
        self.assertEqual(expected, result)

        expected = "20171010"
        result = self.utils.process_url_date("20171010")
        self.assertEqual(expected, result)

        with self.assertRaises(ValueError):
            self.utils.process_url_date("2017-10-10")

    def test_process_recommendations(self):
        class Entity(object):
            @property
            def items(self):
                return self._items

            @property
            def scores(self):
                return self._scores

            @property
            def key(self):
                class foo(object): pass
                f = foo()
                f.__setattr__('id', lambda: self._id)
                return f

            def __init__(self, items, scores, id):
                self._items = items
                self._scores = scores
                self._id = id

            def __repr__(self):
                return """Entity(id={}, items={}, scores={})""".format(
                    self._id, self.items, self.scores)

        entities = [Entity(['sku1', 'sku2'], [0.9, 0.8], 'sku0'),
                    Entity(['sku0', 'sku2'], [0.8, 0.5], 'sku1')]
        scores = {'sku0': 0.5, 'sku1': 2}
        result = self.utils.process_recommendations(entities, scores) 
        expected = {'result': [{'item': 'sku0', 'score': 1.6},
                       {'item': 'sku2', 'score': 1.4},
                       {'item': 'sku1', 'score': 0.45}]}
        self.assertTrue(expected, result)



class TestSkuModel(unittest.TestCase, BaseTests):
    def setUp(self):
        self.prepare_environ()    

    def tearDown(self):
        self.clean_environ()

    @mock.patch('gae.utils.config')
    def test_cto(self, config_mock):
        items = ['sku0', 'sku1']
        scores = [0.9, 0.8]
        config_mock.__getitem__.return_value = {"kind": "test_kind"} 
        sku = self.utils.SkuModel(items=items, scores=scores, id='sku0')
        self.assertEqual(sku.items, items)
        self.assertEqual(sku.scores, scores)
        self.assertEqual(sku.key.id(), 'sku0')
        self.assertEqual(sku.key.kind(), 'test_kind')
