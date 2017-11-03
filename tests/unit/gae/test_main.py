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

import webtest
from google.appengine.ext import testbed
from werkzeug.datastructures import ImmutableMultiDict
from gae.main import app


class TestMainService(unittest.TestCase):
    test_app = webtest.TestApp(app)

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()


    def tearDown(self):
        self.testbed.deactivate()


    @mock.patch('gae.main.jobs_factory')
    def test_run_job(self, factory_mock):
        job_mock = mock.Mock()
        factory_mock.factor_job.return_value = mock.Mock(return_value=job_mock)
        response = self.test_app.get('/run_job/job_name_test/')
        factory_mock.factor_job.assert_called_once_with(*['job_name_test'])
        job_mock.run.assert_called_once()
        self.assertEqual(response.status_int, 200)

        url = '/run_job/job_name_test/?url=url&target=target'
        response = self.test_app.get(url)
        expected = ImmutableMultiDict([('url', 'url'), ('target', 'target')])
        job_mock.run.assert_called_with(expected)
        self.assertEqual(response.status_int, 200)

