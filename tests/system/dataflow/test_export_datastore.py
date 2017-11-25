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


import os
import mock
import unittest
import dataflow.export_datastore as exporter
from dataflow.config import config as config2
import google.cloud.datastore as ds


class TestExportDatastore(unittest.TestCase):
    @mock.patch("dataflow.config.config")
    def test_main(self, config_mock):
        kind = 'unittest-example-dataproc'
        config2['input'] = 'tests/system/data/dataflow/*.json.gz'
        config2['similarities_cap'] = 5
        config2['kind'] = kind
        config_mock.items.return_value = config2.items()
        expected = {'sku0': {'items': ['sku8', 'sku7', 'sku6', 'sku5', 'sku4'],
            'scores': [0.8, 0.7, 0.6, 0.5, 0.4]},
                    'sku1': {'items': ['sku0', 'sku2', 'sku3', 'sku4', 'sku5'],
            'scores': [0.8, 0.7, 0.6, 0.5, 0.4]},
                    'sku2': {'items': ['sku2', 'sku0'],
            'scores': [0.7, 0.2]}}
        dsc = ds.Client()
        keys = map(lambda x: dsc.key(kind, x), ['sku0', 'sku1', 'sku2'])

        exporter.main()
        ds_keys = dsc.get_multi(keys)
        for key in ds_keys:
            name = key.key.name
            self.assertEqual(expected[name]['items'], key['items'])
            self.assertEqual(expected[name]['scores'], key['scores'])

        dsc.delete_multi(keys)
        key0 = dsc.get(keys[0])
        self.assertEqual(key0, None)
