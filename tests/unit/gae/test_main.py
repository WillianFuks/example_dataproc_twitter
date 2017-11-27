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
import json

import webtest
from google.appengine.ext import testbed, ndb
from werkzeug.datastructures import ImmutableMultiDict
from base import BaseTests


class TestMainService(unittest.TestCase, BaseTests):
    test_app = None
    def setUp(self):
        self.prepare_environ() 
        from gae.main import app
        self.test_app = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.clean_environ()
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

    def test_make_reco(self):
        def round_result(result):
            for i in range(len(result['result'])):
                result['result'][i]['score'] = round(
                    result['result'][i]['score'], 3)
        SkuModel = self.utils.SkuModel
        sku1 = SkuModel(id='sku0', items=['sku1', 'sku2'],
            scores=[0.9, 0.8])        
        sku2 = SkuModel(id='sku1', items=['sku0', 'sku2'],
            scores=[0.8, 0.5])
        ndb.put_multi([sku1, sku2])
        result = self.test_app.get('/make_recommendation?browsed=0,1')
        self.assertEqual(result.json, []) 

        result = self.test_app.get('/make_recommendation?browsed=sku0,sku1')
        self.assertEqual(result.json, {'result': [
            {'item': u'sku2', 'score': 0.65},
            {'item': u'sku1', 'score': 0.45},
            {'item': u'sku0', 'score': 0.4}]})

        result = self.test_app.get(
            '/make_recommendation?browsed=sku0&basket=sku1')
        self.assertEqual(result.json, {'result': [
            {'item': u'sku0', 'score': 1.6},
            {'item': u'sku2', 'score': 1.4},
            {'item': u'sku1', 'score': 0.45}]})

        result = self.test_app.get(
            '/make_recommendation?browsed=sku0&purchased=sku1').json
        round_result(result)
        self.assertEqual(result, {'result': [
            {'item': u'sku0', 'score': 4.8},
            {'item': u'sku2', 'score': 3.4},
            {'item': u'sku1', 'score': 0.45}]}) 

        result = self.test_app.get(
            '/make_recommendation?basket=sku1').json
        self.assertEqual(result, {'result': [{'item': u'sku0', 'score': 1.6},
            {'item': u'sku2', 'score': 1.0}]})
    
        result = self.test_app.get(
            '/make_recommendation?purchased=sku0').json
        round_result(result)
        self.assertEqual(result, {'result': [{'item': u'sku1', 'score': 5.4},
            {'item': u'sku2', 'score': 4.8}]})

        result = self.test_app.get(
            '/make_recommendation?purchased=sku0&basket=sku1&browsed=sku0')
        result = result.json
        round_result(result)
        self.assertEqual(result, {'result': [{'item': u'sku2', 'score': 6.2},
            {'item': u'sku1', 'score': 5.85},
            {'item': u'sku0', 'score': 1.6}]})

        result = self.test_app.get(
        '/make_recommendation?purchased=sku0&basket=sku1&browsed=sku0&n=1')
        result = result.json
        round_result(result)
        self.assertEqual(result, {'result': [
            {'item': 'sku2', 'score': 6.2}]})
