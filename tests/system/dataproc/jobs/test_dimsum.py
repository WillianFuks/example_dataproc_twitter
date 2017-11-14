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
import numpy as np
import pyspark
import pyspark.sql.types as stypes
import dataproc.jobs.dimsum as dimsum
from pyspark.sql import Row
from base import BaseTest


class TestSystemBaseDataprocJob(BaseTest):
    _base_path = "tests/system/data/dataproc/jobs/train/dimsum/{}/"
    @staticmethod
    def get_target_klass():
        return dimsum.DimSumJob


    @classmethod
    def setup_class(cls):
        cls.build_data(cls._base_path) 


    def teardown_class(cls):
        inter_path = "tests/system/data/dataproc/jobs/train/dimsum/inter/"
        if os.path.isdir(inter_path):
            shutil.rmtree(inter_path)
        assert(os.path.isdir(inter_path) == False)
        cls.delete_data(cls._base_path) 


    def test_transform_data_force_no(self, spark_context):
        klass = self.get_target_klass()()
        source_uri = "tests/system/data/dataproc/jobs/train/dimsum/{}/"
        inter_uri = "tests/system/data/dataproc/jobs/train/dimsum/inter/{}/"
        neighbor_uri = 'tests/system/data/dataproc/jobs/train/dimsum/results/'
        args = klass.process_base_sysargs(['--days_init=2', '--days_end=1',
            '--source_uri={}'.format(source_uri), '--threshold=0', 
            '--inter_uri={}'.format(inter_uri), '--force=no', 
            '--neighbor_uri={}'.format(neighbor_uri)])
        klass.run(spark_context, args)
        a = np.array([[0, 6, 0, 0],
                      [0.5, 0.5, 1, 0.5],
                      [7, 0, 2.5, 6.5],
                      [7, 0, 6, 6]])
        r1 = np.dot(a.T, a)
        d = np.sqrt(r1.diagonal())
        di = np.diag(d)
        idi = np.linalg.inv(di)
        expected = np.dot(idi, np.dot(r1, idi))
        print("THIS IS EXPECTED", expected)
        result = spark_context.textFile(neighbor_uri).collect()
        print("AND THIS IS RESULT", result)
        assert(result == expected)


#    def test_save_neighbor_matrix(self, spark_context):
#        klass = self.get_target_klass()()
#        data = spark_context.parallelize([(('sku0', 'sku1'), 1.),
#                                   (('sku0', 'sku2'), 0.5),
#                                   (('sku1', 'sku2'), 0.5)])    
#        neighbor_uri = 'tests/system/data/dataproc/jobs/results/'
#        klass.save_neighbor_matrix(neighbor_uri, data)
#        result = [json.loads(e) for e in 
#                    spark_context.textFile(neighbor_uri).collect()]
#        expected = {'sku0': [{"item": "sku1", "similarity": 1.0},
#                          {"item": "sku2", "similarity": 0.5}],
#                    'sku2': [{"item": "sku0", "similarity": 0.5},
#                             {"item": "sku1", "similarity": 0.5}],
#                    'sku1': [{"item": "sku0", "similarity": 1.0},
#                             {"item": "sku2", "similarity": 0.5}]} 
#        for row in result:
#            assert(expected[row['item']] == row['similarity_items'])
#        shutil.rmtree(neighbor_uri)
#        assert(os.path.isdir(neighbor_uri) == False)
