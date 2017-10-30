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
import webtest
import googleapiclient
import unittest
from google.appengine.ext import testbed
from gae.exporter.main import app, process_url_date

from google.appengine.api import taskqueue

class TestExporterService(unittest.TestCase):
    test_app = webtest.TestApp(app)

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub('./gae/exporter/')
        self.taskqueue_stub = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)


    def tearDown(self):
        self.testbed.deactivate()


    def test_added_to_queue(self):
        response = self.test_app.get("/export_customers?date=20171010")
        self.assertEqual(response.status_int, 200)

        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(1, len(tasks)) 
        self.assertEqual(tasks[0].url, '/queue_export')


    def test_process_url_date(self):
        expected = None
        result = process_url_date({})
        self.assertEqual(expected, result)

        expected = "20171010"
        result = process_url_date({"date": "20171010"})
        self.assertEqual(expected, result)

        with self.assertRaises(ValueError):
            process_url_date({"date": "2017-10-10"})
    
 
