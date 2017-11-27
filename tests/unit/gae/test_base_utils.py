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
import base_utils


class TestBaseUtils(unittest.TestCase):
    def test_process_input_items(self):
        inp = {'browsed': 'sku0'}
        result = base_utils.process_input_items(inp)
        self.assertEqual(result, Counter({'sku0': 0.5}))

        inp = {'basket': 'sku0'}
        result = base_utils.process_input_items(inp)
        self.assertEqual(result, Counter({'sku0': 2}))

        inp = {'purchased': 'sku0'}
        result = base_utils.process_input_items(inp)
        self.assertEqual(result, Counter({'sku0': 6}))

        inp = {'purchased': 'sku0', 'browsed': 'sku0,sku1'}
        result = base_utils.process_input_items(inp)
        self.assertEqual(result, Counter({'sku0': 6.5, 'sku1': 0.5}))

        inp = {'purchased': 'sku0', 'browsed': 'sku0,sku1',
            'basket': 'sku0,sku2'}
        result = base_utils.process_input_items(inp)
        self.assertEqual(result, Counter(
            {'sku0': 8.5, 'sku2': 2.0, 'sku1': 0.5}))

        inp = {}
        result = base_utils.process_input_items(inp)
        self.assertEqual(result, Counter())

    def test_cy_process_recommendations(self):
        entities = [{"id": "sku0",
                     "items": ["sku1", "sku2"],
                     "scores": [0.6, 0.4]},
                    {"id": "sku1",
                     "items": ["sku0", "sku2"],
                     "scores": [0.8, 0.1]}]
        weights = {"sku0": 0.5, "sku1": 2.}

        result = base_utils.cy_process_recommendations(entities, weights)
        self.assertEqual(result, {'result': [{'item': 'sku0', 'score': 1.6},
            {'item': 'sku2', 'score': 0.4}, {'item': 'sku1', 'score': 0.3}]})

        result = base_utils.cy_process_recommendations(entities, weights, 1)
        self.assertEqual(result, {'result': [{'item': 'sku0', 'score': 1.6}]})
