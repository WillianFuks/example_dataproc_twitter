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


import unittest
import mock

import googleapiclient
import google.auth.credentials


class TestGCPFactory(unittest.TestCase):
    @staticmethod
    def _make_credentials():
        return mock.Mock(spec=google.auth.credentials.Credentials)


    @staticmethod
    def _get_target_klass():
        from gae.connector.gcp import GCPService


        return GCPService


    @mock.patch('gae.connector.gcp.app_engine')
    def test_cto_no_credentials(self, app_mock):
        app_mock.Credentials.return_value = self._make_credentials() 
        klass = self._get_target_klass()() 
        self.assertTrue(isinstance(klass._credentials,
             google.auth.credentials.Credentials))
            

    def test_cto_with_credentials(self):
        klass = self._get_target_klass()(self._make_credentials()) 
        self.assertTrue(isinstance(klass._credentials,
             google.auth.credentials.Credentials))


    def test_cto_with_invalid_credentials(self):
        with self.assertRaises(TypeError):
            klass = self._get_target_klass()('invalid credential') 

    
    @mock.patch('gae.connector.gcp.BigQueryService')
    def test_bigquery_property(self, bq_mock):
        bq_mock.return_value = 'con'
        klass = self._get_target_klass()(self._make_credentials()) 
        self.assertTrue(klass._bigquery is None)
        con = klass.bigquery
        self.assertEqual(con, 'con')
        self.assertTrue(klass.bigquery is not None)


    @mock.patch('gae.connector.gcp.DataprocService')
    def test_dataproc_property(self, dp_mock):
        dp_mock.return_value = 'con'
        klass = self._get_target_klass()(self._make_credentials()) 
        self.assertTrue(klass._dataproc is None)
        con = klass.dataproc
        self.assertEqual(con, 'con')
        self.assertTrue(klass.dataproc is not None)


    @mock.patch('gae.connector.gcp.StorageService')
    def test_storage_property(self, st_mock):
        st_mock.return_value = 'con'
        klass = self._get_target_klass()(self._make_credentials()) 
        self.assertTrue(klass._storage is None)
        con = klass.storage
        self.assertEqual(con, 'con')
        self.assertTrue(klass.storage is not None)


    @mock.patch('gae.connector.gcp.DataflowService')
    def test_dataflow_property(self, df_mock):
        df_mock.return_value = 'con'
        klass = self._get_target_klass()(self._make_credentials()) 
        self.assertTrue(klass._dataflow is None)
        con = klass.dataflow
        self.assertEqual(con, 'con')
        self.assertTrue(klass.dataflow is not None)
