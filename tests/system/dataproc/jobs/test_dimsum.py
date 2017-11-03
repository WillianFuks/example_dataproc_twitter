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
import math
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


    def test_run_no_threshold(self, spark_context):
        klass = self.get_target_klass()()
        source_uri = "tests/system/data/dataproc/jobs/train/dimsum/{}/"
        inter_uri = "tests/system/data/dataproc/jobs/train/dimsum/inter/{}/"
        neighbor_uri = 'tests/system/data/dataproc/jobs/train/dimsum/results/'
        args = klass.process_base_sysargs(['--days_init=2', '--days_end=1',
            '--source_uri={}'.format(source_uri),
            '--threshold=0', 
            '--inter_uri={}'.format(inter_uri),
            '--force=no', 
            '--neighbor_uri={}'.format(neighbor_uri)])
        klass.run(spark_context, args)
        a = np.array([[0, 6, 0, 0.5],
                      [0.5, 0.5, 1, 0.5],
                      [7, 0, 2.5, 6.5],
                      [7, 0, 6, 6]])
        r1 = np.dot(a.T, a)
        d = np.sqrt(r1.diagonal())
        di = np.diag(d)
        idi = np.linalg.inv(di)
        expected = np.dot(idi, np.dot(r1, idi))
        print("THIS IS EXPECTED", expected)
        result = [json.loads(e) for e in 
                    spark_context.textFile(neighbor_uri).collect()]
        print("AND THIS IS RESULT", result)
        for row in result:
            for sim in row['similarity_items']:
                i = int(row['item'][-1])
                j = int(sim['item'][-1])
                np.testing.assert_almost_equal(expected[i][j],
                    sim['similarity'])  
                assert(sim['similarity'] > 0.01)
        shutil.rmtree(neighbor_uri)
        assert(os.path.isdir(neighbor_uri) == False)


    def test_run_with_threshold(self, spark_context):
        klass = self.get_target_klass()()
        source_uri = "tests/system/data/dataproc/jobs/train/dimsum/{}/"
        inter_uri = "tests/system/data/dataproc/jobs/train/dimsum/inter/{}/"
        neighbor_uri = 'tests/system/data/dataproc/jobs/train/dimsum/results/'
        a = np.array([[0, 6, 0, 0.5],
                      [0.5, 0.5, 1, 0.5],
                      [7, 0, 2.5, 6.5],
                      [7, 0, 6, 6]])
        # threshold = 10 * log(n) * eps ^ 2 / n since gamma = n / eps^2
        eps = 0.2
        threshold = 10 * math.log(a.shape[1]) * eps ** 2 / a.shape[1]
        args = klass.process_base_sysargs(['--days_init=2', '--days_end=1',
            '--source_uri={}'.format(source_uri),
            '--threshold={}'.format(threshold), 
            '--inter_uri={}'.format(inter_uri),
            '--force=no', 
            '--neighbor_uri={}'.format(neighbor_uri)])
        klass.run(spark_context, args)
        r1 = np.dot(a.T, a)
        d = np.sqrt(r1.diagonal())
        di = np.diag(d)
        idi = np.linalg.inv(di)
        expected = np.dot(idi, np.dot(r1, idi))
        print("THIS IS EXPECTED", expected)
        result = [json.loads(e) for e in 
                    spark_context.textFile(neighbor_uri).collect()]
        print("AND THIS IS RESULT", result)
        correct, total = (0, 0)
        # The following is based on the theorem from
        # https://arxiv.org/pdf/1304.1467.pdf
        # stating:
        # ||DBD - A.TA||/||A.TA|| <= eps with P = 50%
        # where ||.|| is the Frobenius norm of degree 2.
        for row in result:
            for sim in row['similarity_items']:
                i = int(row['item'][-1])
                j = int(sim['item'][-1])
                if ((np.abs(expected[i][j] - sim['similarity']) /
                     expected[i][j]) < eps):
                    correct += 1
                total += 1
                assert(sim['similarity'] > 0.01)
        assert(correct / total >= 0.5)
        shutil.rmtree(neighbor_uri)
        assert(os.path.isdir(neighbor_uri) == False)
