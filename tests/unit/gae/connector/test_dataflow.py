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


class TestDataflowService(unittest.TestCase):
    @staticmethod
    def _make_credentials():
        return mock.Mock(spec=google.auth.credentials.Credentials)


    @staticmethod
    def _get_target_klass():
        from gae.connector.gcp import DataflowService


        return DataflowService


    @property
    def config_(self):
        _source_config = 'tests/unit/data/gae/test_config.json' 
        return json.loads(open(_source_config).read().replace( 
            "config = ", ""))


    @mock.patch('gae.connector.dataflow.disco')
    def test_cto(self, disco_mock):
        mock_cre = self._make_credentials()
        disco_mock.build.return_value = 'con'
        klass = self._get_target_klass()(mock_cre) 
        self.assertEqual(klass.con, 'con')
        disco_mock.build.assert_called_once_with('dataflow', 'v1b3',
            credentials=mock_cre)


    @mock.patch('gae.connector.dataflow.disco')
    def test_run_template(self, disco_mock):
        mock_cre = self._make_credentials()
        config_ = self.config_
        con_mock = mock.Mock()
        disco_mock.build.return_value = con_mock
        project_mock = mock.Mock()
        template_mock = mock.Mock()
        creation_mock = mock.Mock()
        execution_mock = mock.Mock()

        con_mock.projects.return_value = project_mock
        project_mock.templates.return_value = template_mock
        template_mock.create.return_value = creation_mock
        creation_mock.execute.return_value = 'job'
        
        klass = self._get_target_klass()(mock_cre)
        result = klass.run_template(**config_['jobs']['dataflow_export'])

        template_mock.create.assert_called_once_with(body={
            'environment': {'tempLocation': 'temp_location', 'maxWorkers': 2,
            'machineType': 'type1', 'zone': 'zone-1'}, 'gcsPath': 'gcs_path',
            'jobName': 'job_name'}, projectId='project123')
        
        self.assertEqual(result, 'job')
