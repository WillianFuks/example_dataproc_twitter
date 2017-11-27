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
import os
import mock
import unittest


class TestJobsFactory(unittest.TestCase):
    @staticmethod
    def _get_target_klass(): 
        from factory import JobsFactory


        return JobsFactory


    @mock.patch('gae.main.jobs_factory')
    def test_factor_job(self, factory_mock):
        klass = self._get_target_klass()()
        with self.assertRaises(TypeError):
            klass.factor_job('invalid_name') 

        exporter = klass.factor_job('export_customers_from_bq') 
        self.assertEqual(exporter.__name__, 'SchedulerJob')
        
        exporter = klass.factor_job('run_dimsum') 
        self.assertEqual(exporter.__name__, 'SchedulerJob')
