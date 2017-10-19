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

import re
import unittest

from google.cloud.bigquery import Client


bc = Client()


class TestQueriesResults(unittest.TestCase):
    def test_queries(self):
        query = open(
            "gae/exporter/queries/customers_interactions.sql").read().strip()
        simulated_data = open(
            "tests/system/data/gae/test_query_customers.sql").read().strip()
        query = re.sub(r"`.*`", simulated_data, query)
        pattern = r"WHERE TRUE\n"
        query = re.sub(pattern, pattern + '#', query)

        job = bc.run_sync_query(query)
        job.use_legacy_sql = False

        job.run()

        # orders by user_id and then by sku
        result = sorted(list(job.fetch_data()), key=lambda x: (x[0], x[1]))
        
        expected = [(u'1', u'sku0', 1),
                    (u'1', u'sku0', 2),
                    (u'1', u'sku0', 3),
                    (u'1', u'sku1', 1),
                    (u'1', u'sku3', 1),
                    (u'2', u'sku0', 1),
                    (u'3', u'sku0', 3),
                    (u'3', u'sku1', 3)]

        self.assertEqual(result, expected)

