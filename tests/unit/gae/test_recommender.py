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
from base import BaseTests


class TestRecommenderService(unittest.TestCase, BaseTests):
    test_app = None
    def setUp(self):
        self.prepare_environ()
        from gae.recommender import app
        self.test_app = webtest.TestApp(app)        

    def tearDown(self):
        self.clean_environ()

    @mock.patch('gae.recommender.config')
    @mock.patch('gae.recommender.time')
    @mock.patch('gae.recommender.Con.get_ds_client')
    def test_make_reco(self, ds_mock, time_mock, config_mock):
        config_mock.__getitem__.return_value = {"kind": "test"}
        ds_keys_mock = mock.Mock()
        ds_mock.return_value = ds_keys_mock
        class Entity(object):
            def __init__(self, id, items, scores):
                self.items = items
                self.scores = scores
                self.key = self
                self.name = id
            def get(self, key):
                return self.__dict__[key]
 
        def round_result(result):
            for i in range(len(result['result'])):
                result['result'][i]['score'] = round(
                    result['result'][i]['score'], 3)

        time_mock.time.side_effect = [0, 1]
        ds_keys_mock.get_keys.return_value = []
        response = self.test_app.get(
            '/make_recommendation?browsed=sku0,sku1').json
        expected = {'elapsed_time': 1, 'results': []}
        self.assertEqual(response, expected)

        time_mock.time.side_effect = [0, 1]
        e1 = Entity('sku0', ['sku1', 'sku2'], [0.6, 0.4])
        e2 = Entity('sku1', ['sku0', 'sku2'], [0.8, 0.1])
        ds_keys_mock.get_keys.return_value = [e1, e2]
        response = self.test_app.get(
            '/make_recommendation?browsed=sku0,sku1').json
        expected = {"elapsed_time": 1, "result": [
                      {"item": "sku0", "score": 0.4}, 
                      {"item": "sku1", "score": 0.3}, 
                      {"item": "sku2", "score": 0.25}]}
        self.assertEqual(response, expected)
        mock_call = list([e for e in ds_keys_mock.get_keys.call_args][0])
        mock_call = [mock_call[0], sorted(mock_call[1])]
        self.assertEqual(['test', ['sku0', 'sku1']], mock_call) 

        time_mock.time.side_effect = [0, 1]
        response = self.test_app.get('/make_recommendation?browsed=sku0,sku1'
            '&basket=sku0').json
        expected = {'elapsed_time': 1, 'result': [
            {'item': 'sku1', 'score': 1.5},
            {'item': 'sku2', 'score': 1.05},
            {'item': 'sku0', 'score': 0.4}]}
        self.assertEqual(response, expected)

        time_mock.time.side_effect = [0, 1]
        response = self.test_app.get('/make_recommendation?browsed=sku0,sku1'
            '&basket=sku0&purchased=sku1').json
        expected = {'elapsed_time': 1, 'result': [
            {'item': 'sku0', 'score': 5.2}, {'item': 'sku2', 'score': 1.65},
            {'item': 'sku1', 'score': 1.5}]}
        self.assertEqual(response, expected)

        time_mock.time.side_effect = [0, 1]
        ds_keys_mock.get_keys.return_value = [e1]
        response = self.test_app.get('/make_recommendation?browsed=sku0').json
        expected = {'elapsed_time': 1, 'result': [
            {'item': 'sku1', 'score': 0.3}, {'item': 'sku2', 'score': 0.2}]}
        self.assertEqual(response, expected)

        time_mock.time.side_effect = [0, 1]
        response = self.test_app.get('/make_recommendation?basket=sku0').json
        expected = {'elapsed_time': 1, 'result': [
            {'item': 'sku1', 'score': 1.2}, {'item': 'sku2', 'score': 0.8}]}
        self.assertEqual(response, expected)

        time_mock.time.side_effect = [0, 1]
        response = self.test_app.get(
            '/make_recommendation?purchased=sku0').json
        round_result(response)
        expected = {'elapsed_time': 1, 'result': [
            {'item': 'sku1', 'score': 3.6}, {'item': 'sku2', 'score': 2.4}]}
        self.assertEqual(response, expected)
