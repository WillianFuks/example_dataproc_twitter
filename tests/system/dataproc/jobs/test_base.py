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
import unittest
import datetime
import json
import mock
import shutil
from collections import namedtuple

import pytest
import pyspark
import pyspark.sql.types as stypes
import dataproc.jobs.base as base_job
from pyspark.sql import Row
from base_fixture import BaseTest


class TestUnitBaseDataprocJob(unittest.TestCase, BaseTest):
    @staticmethod
    def get_target_klass():
        return base_job.JobsBase

    def test_process_base_sysargs(self):
        klass = self.get_target_klass()()
        args = ['--days_init=1',
                '--days_end=2',
                '--source_uri=source_uri',
                '--inter_uri=inter_uri',
                '--force=no',
                '--neighbor_uri=neighbor_uri']
        result = klass.process_base_sysargs(args)
        self.assertEqual(result.days_end, 2)
        self.assertEqual(result.days_init, 1)
        self.assertEqual(result.source_uri, 'source_uri')
        self.assertEqual(result.inter_uri, 'inter_uri')
        self.assertEqual(result.force, 'no')
        self.assertEqual(result.neighbor_uri, 'neighbor_uri')
        self.assertEqual(result.threshold, None)
        self.assertEqual(result.w_browse, 0.5)
        self.assertEqual(result.w_basket, 2.)
        self.assertEqual(result.w_purchase, 6.)
    
    def test_load_users_schema(self):
        klass = self.get_target_klass()()
        expected = stypes.StructType(fields=[
        	stypes.StructField("user", stypes.StringType()),
        	 stypes.StructField('interactions', stypes.ArrayType(
        	  stypes.StructType(fields=[stypes.StructField('item', 
        	   stypes.StringType()), stypes.StructField('score', 
        	    stypes.FloatType())])))])
        result = klass.load_users_schema()
        self.assertEqual(result, expected)
   
    @mock.patch('dataproc.jobs.base.datetime') 
    def test_get_formatted_date(self, dt_mock):
        klass = self.get_target_klass()()
        dt_mock.datetime.now.return_value = datetime.datetime(2017, 10, 10)
        dt_mock.timedelta = datetime.timedelta
        result = klass.get_formatted_date(1)
        expected = "2017-10-09"
        self.assertEqual(result, expected)

    def test_aggregate_skus(self):
        klass = self.get_target_klass()()
        row = ['0', [('sku0', 1), ('sku1', 0.5), ('sku0', 0.5)]]
        expected = ['0', [('sku0', 1.5), ('sku1', 0.5)]]
        result = list(klass.aggregate_skus(row))[0]
        self.assertEqual(expected[0], result[0])
        self.assertEqual(expected[1], sorted(result[1], key=lambda x: x[0]))

    def test_load_neighbor_schema(self):
        klass = self.get_target_klass()()
        result = klass.load_neighbor_schema()
        expected = stypes.StructType(fields=[
                stypes.StructField("item", stypes.StringType()),
                 stypes.StructField("similarity_items", stypes.ArrayType(
                  stypes.StructType(fields=[
                   stypes.StructField("item", stypes.StringType()),
                    stypes.StructField("similarity", stypes.FloatType())])))])
        self.assertEqual(expected, result)

class TestSystemBaseDataprocJob(BaseTest):
    _base_path = "tests/system/data/dataproc/jobs/train/{}/"
    @staticmethod
    def get_target_klass():
        return base_job.JobsBase

    @classmethod
    def setup_class(cls):
        cls.build_data(cls._base_path) 

    def teardown_class(cls):
        inter_path = "tests/system/data/dataproc/jobs/inter/"
        if os.path.isdir(inter_path):
            shutil.rmtree(inter_path)
        assert(os.path.isdir(inter_path) == False)
        cls.delete_data(cls._base_path) 

    def test_transform_data_force_no(self, spark_context):
        klass = self.get_target_klass()()
        source_uri = "tests/system/data/dataproc/jobs/train/{}/"
        inter_uri = "tests/system/data/dataproc/jobs/inter/{}/"
        args = klass.process_base_sysargs(['--days_init=2', '--days_end=1',
            '--source_uri={}'.format(source_uri),
            '--inter_uri={}'.format(inter_uri), '--force=no', 
            '--neighbor_uri=neighbor_uri'])
        klass.transform_data(spark_context, args)
        expected = {'2': {'0': [{'item': 'sku0', 'score': 6.0}],
                          '3': [{'item': 'sku0', 'score': 0.5}],
                          '2': [{'item': 'sku0', 'score': 0.5}, 
                                {'item': 'sku1', 'score': 0.5}]},
                    '1': {'1': [{'item': 'sku0', 'score': 0.5},
                                {'item': 'sku1', 'score': 8.5}],
                          '0': [{'item': 'sku0', 'score': 1.0},
                                {'item': 'sku1', 'score': 2.0}]}}
        for i in range(1, 3):
            date_str = klass.get_formatted_date(i)
            result = [json.loads(e) for e in 
                spark_context.textFile(inter_uri.format(date_str)).collect()]
            for row in result:
                assert(expected[str(i)][row['user']] ==
                    sorted(row['interactions'], key=lambda x: x['item']))

    def test_transform_data_force_yes(self, spark_context):
        klass = self.get_target_klass()()
        source_uri = "tests/system/data/dataproc/jobs/train/{}/"
        inter_uri = "tests/system/data/dataproc/jobs/inter/{}/"
        args = klass.process_base_sysargs(['--days_init=2', '--days_end=1',
            '--source_uri={}'.format(source_uri),
            '--inter_uri={}'.format(inter_uri), '--force=no', 
            '--neighbor_uri=neighbor_uri'])        
        klass.transform_data(spark_context, args)
        args = klass.process_base_sysargs(['--days_init=2', '--days_end=1',
            '--source_uri={}'.format(source_uri),
            '--inter_uri={}'.format(inter_uri), '--force=yes', 
            '--neighbor_uri=neighbor_uri'])
        expected = {'2': {'0': [{'item': 'sku0', 'score': 6.0}],
                          '3': [{'item': 'sku0', 'score': 0.5}],
                          '2': [{'item': 'sku0', 'score': 0.5}, 
                                {'item': 'sku1', 'score': 0.5}]},
                    '1': {'1': [{'item': 'sku0', 'score': 0.5},
                                {'item': 'sku1', 'score': 8.5}],
                          '0': [{'item': 'sku0', 'score': 1.0},
                                {'item': 'sku1', 'score': 2.0}]}}
        for i in range(1, 3):
            date_str = klass.get_formatted_date(i)
            result = [json.loads(e) for e in 
                spark_context.textFile(inter_uri.format(date_str)).collect()]
            for row in result:
                assert(expected[str(i)][row['user']] == 
                    sorted(row['interactions'], key=lambda x: x['item']))

    def test_save_neighbor_matrix(self, spark_context):
        klass = self.get_target_klass()()
        data = spark_context.parallelize([(('sku0', 'sku1'), 1.),
                                   (('sku0', 'sku2'), 0.5),
                                   (('sku1', 'sku2'), 0.5)])    
        neighbor_uri = 'tests/system/data/dataproc/jobs/results/'
        klass.save_neighbor_matrix(neighbor_uri, data)
        result = [json.loads(e) for e in 
                    spark_context.textFile(neighbor_uri).collect()]
        expected = {'sku0': [{"item": "sku1", "similarity": 1.0},
                          {"item": "sku2", "similarity": 0.5}],
                    'sku2': [{"item": "sku0", "similarity": 0.5},
                             {"item": "sku1", "similarity": 0.5}],
                    'sku1': [{"item": "sku0", "similarity": 1.0},
                             {"item": "sku2", "similarity": 0.5}]} 
        for row in result:
            assert(expected[row['item']] == row['similarity_items'])
        shutil.rmtree(neighbor_uri)
        assert(os.path.isdir(neighbor_uri) == False)
