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
from collections import namedtuple

from google.appengine.ext import testbed


class TestSchedulerJob(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub('./gae/')
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)


    def tearDown(self):
        self.testbed.deactivate()


    @staticmethod
    def _get_target_klass(): 
        from scheduler import SchedulerJob 


        return SchedulerJob


    def test_run(self):
        klass = self._get_target_klass()()

        with self.assertRaises(ValueError):
            klass.run({})
        
        args = {'url': '/url', 'target': 'target'} 
        klass.run(args)
        task = self.taskqueue_stub.get_filtered_tasks()[0]
        self.assertEqual(task.url, '/url')
        self.assertTrue(task.target is not None)       
 
        args['params'] = {'date': 'date'}
        klass.run(args)
        task = self.taskqueue_stub.get_filtered_tasks()[-1]
        self.assertEqual(task.url, '/url')
        self.assertTrue(task.target is not None)       
        self.assertTrue(task.target is not None)       
        self.assertEqual(task.payload, 'params=date') 


    def test___str__(self):
        klass = self._get_target_klass()()
        self.assertEqual(str(klass), "No task has been enqueued so far")
        args = namedtuple("task", ["name", "eta"])
        task_mock = args("1", "2") 
        klass.task = task_mock
        self.assertEqual(str(klass), "Task 1 enqued, ETA 2")
        
        
        
        



