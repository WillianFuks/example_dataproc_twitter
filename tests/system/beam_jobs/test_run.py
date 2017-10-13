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
import ast
import sys
sys.path.append('.')

import unittest
import beam_jobs.run as module_run

class Test_Run(unittest.TestCase):


    @staticmethod
    def load_args():
        return ['--table_id=ga_sessions_*',
                '--dataset_id=12345',
                '--project_id=dafiti-analytics',
                '--output=output.csv',
                '--query_path=./tests/data/queries/test_customers_query.sql',
                '--date=20171010',
                '--runner=DirectRunner']


    def test_main(self):
        sys.argv = ['foo'] + self.load_args()
        module_run.main()
        expected = {u'dataset_id': u'12345',
                    u'project_id': u'dafiti-analytics',
                    u'table_id': u'ga_sessions_*',
                    u'date_': u'20171010'}

        file_ = 'output.csv-00000-of-00001'
        result = ast.literal_eval(open(file_).read())

        self.assertEqual(expected, result)
        os.remove(file_)
        self.assertFalse(os.path.isfile(file_))
