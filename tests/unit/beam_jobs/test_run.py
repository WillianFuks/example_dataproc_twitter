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


import sys
sys.path.append('.')
import os

print [os.path.abspath(e) for e in sys.path]
import unittest
import beam_jobs.run as module_run

class Test_Run(unittest.TestCase):


    @staticmethod
    def load_args():
        return ['--table_id=ga_sessions_*',
                '--dataset_id=12345',
                '--project_id=company',
                '--output=output.csv',
                '--query_path=./tests/data/queries/test_customers_query.sql',
                '--date=20171010',
                '--runner=DirectRunner']


    def test_process_args(self):
        args = self.load_args()

        known_args, pipeline_options = module_run.process_args(args)
        self.assertEqual(known_args.table_id, 'ga_sessions_*')
        self.assertEqual(known_args.dataset_id, '12345')
        self.assertEqual(known_args.project_id, 'company')
        self.assertEqual(known_args.output, 'output.csv')
        self.assertEqual(known_args.query_path, './tests/data/queries/test_customers_query.sql')
        self.assertEqual(known_args.date, '20171010')
        self.assertEqual(pipeline_options.display_data()['runner'], 'DirectRunner') 
        self.assertEqual(pipeline_options.display_data()['project'], 'company') 


    def test_build_query(self):
        args = self.load_args()
        known_args, _ = module_run.process_args(args)
        query = module_run.build_query(known_args)
        expected = """#standardSQL
                   SELECT
                     table_id,
                     dataset_id,
                     project_id,
                     date_
                   FROM(
                     SELECT
                       'ga_sessions_*' AS table_id,
                       '12345' AS dataset_id,
                       'company' AS project_id,
                       '20171010' AS date_
                     )
                     """

        self.assertEqual(expected.replace(' ', ''), query.replace(' ', '')) 

