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


import json
import unittest
import mock

import googleapiclient
import google.auth.credentials


class TestStorageService(unittest.TestCase):
    @staticmethod
    def _make_credentials():
        return mock.Mock(spec=google.auth.credentials.Credentials)


    @staticmethod
    def _get_target_klass():
        from gae.connector.gcp import StorageService


        return StorageService


    @property
    def config_(self):
        _source_config = 'tests/unit/data/gae/test_config.json' 
        return json.loads(open(_source_config).read().replace( 
            "config = ", ""))


    @mock.patch('gae.connector.storage.disco')
    def test_cto(self, disco_mock):
        mock_cre = self._make_credentials()
        disco_mock.build.return_value = 'con'
        klass = self._get_target_klass()(mock_cre) 
        self.assertEqual(klass.con, 'con')
        disco_mock.build.assert_called_once_with('storage', 'v1',
            credentials=mock_cre)

 
    @mock.patch('gae.connector.storage.open')
    @mock.patch('gae.connector.storage.googleapiclient.http.MediaIoBaseUpload')
    @mock.patch('gae.connector.storage.disco')
    def test_upload_from_filenames(self, disco_mock, media_mock, open_mock):
        config_ = self.config_
        mock_cre = self._make_credentials()
        con_mock = mock.Mock()
        objects_mock = mock.Mock()
        insert_mock = mock.Mock()
        execution_mock = mock.Mock()

        disco_mock.build.return_value = con_mock
        con_mock.objects.return_value = objects_mock
        objects_mock.insert.return_value = insert_mock
        insert_mock.execute.return_value = 'result' 
        media_mock.return_value = 'media_body'

        context_mock = mock.Mock()
        def handler(*args, **kwargs):
            return 
        context_mock.__enter__ = lambda x: 'file' 
        context_mock.__exit__ = handler 
        open_mock.return_value = context_mock 
        
        klass = self._get_target_klass()(mock_cre)
        klass.upload_from_filenames(**config_['jobs']['run_dimsum'][
            'pyspark_job'])

        insert_mock.execute.assert_called_with(num_retries=3)
        objects_mock.insert.assert_any_call(body={'name': 'basename/2.py'},
            bucket='bucket_name', media_body='media_body') 

        objects_mock.insert.assert_any_call(body={'name': 'basename/1.py'},
            bucket='bucket_name', media_body='media_body') 

        media_mock.assert_any_call('file', 'application/octet-stream')      

        
