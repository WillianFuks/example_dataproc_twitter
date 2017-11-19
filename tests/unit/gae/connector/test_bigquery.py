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
        from gae.connector.gcp import BigQueryService


        return BigQueryService


    @mock.patch('gae.connector.bigquery.disco')
    def test_cto(self, disco_mock):
        mock_cre = self._make_credentials()
        disco_mock.build.return_value = 'con'
        klass = self._get_target_klass()(mock_cre) 
        self.assertEqual(klass.con, 'con')
        disco_mock.build.assert_called_once_with('bigquery', 'v2',
            credentials=mock_cre)


    @mock.patch('gae.connector.bigquery.disco')
    def test_execute_job(self, disco_mock):
        con_mock = mock.Mock()
        disco_mock.build.return_value = con_mock
        klass = self._get_target_klass()('cre') 
        
        job_mock = mock.Mock()
        resource_mock = mock.Mock()
        execute_mock = mock.Mock()
        con_mock.jobs = job_mock
        job_mock.return_value = resource_mock
        resource_mock.insert.return_value = execute_mock

        project_id = 'project123'
        body = {'project_id': 'project123'}

        job = klass.execute_job(project_id, body)
        resource_mock.insert.assert_called_once_with(
            **{'projectId': project_id, 'body': body})
        execute_mock.execute.assert_called_once_with(**{'num_retries': 3})


    @mock.patch('gae.connector.bigquery.time')
    @mock.patch('gae.connector.bigquery.disco')
    def test_poll_job(self, disco_mock, time_mock):
        con_mock = mock.Mock()
        disco_mock.build.return_value = con_mock
        klass = self._get_target_klass()('cre') 

        job_mock = mock.Mock()
        resource_mock = mock.Mock()
        request_mock = mock.Mock()
        job_mock.jobs.return_value = resource_mock
        resource_mock.get.return_value = request_mock
        request_mock.execute.return_value = {"status": {"state": "DONE"}}
        klass.con = job_mock

        job = {"jobReference": {"projectId": "project123", "jobId": "1"}}
        klass.poll_job(job)
        request_mock.execute.assert_called_once_with(**{'num_retries': 3})    


    @mock.patch('gae.connector.bigquery.time')
    @mock.patch('gae.connector.bigquery.disco')
    def test_poll_job_running_time(self, disco_mock, time_mock):
        con_mock = mock.Mock()
        disco_mock.build.return_value = con_mock
        klass = self._get_target_klass()('cre') 

        job_mock = mock.Mock()
        resource_mock = mock.Mock()
        request_mock = mock.Mock()
        job_mock.jobs.return_value = resource_mock
        resource_mock.get.return_value = request_mock
        klass.con = job_mock

        request_mock.execute.side_effect = [{"status": {"state": "RUNNING"}},
            {"status": {"state": "DONE"}}]
        job = {"jobReference": {"projectId": "project123", "jobId": "1"}}
        klass.poll_job(job)
        request_mock.execute.assert_called_with(**{'num_retries': 3})    
        time_mock.sleep.assert_called_once_with(1)


    @mock.patch('gae.connector.bigquery.time')
    @mock.patch('gae.connector.bigquery.disco')
    def test_poll_job(self, disco_mock, time_mock):
        con_mock = mock.Mock()
        disco_mock.build.return_value = con_mock
        klass = self._get_target_klass()('cre') 

        job_mock = mock.Mock()
        resource_mock = mock.Mock()
        request_mock = mock.Mock()
        job_mock.jobs.return_value = resource_mock
        resource_mock.get.return_value = request_mock
        request_mock.execute.return_value = {"status": {"state": "DONE",
                                                        "errorResult": True}}
        klass.con = job_mock

        job = {"jobReference": {"projectId": "project123", "jobId": "1"}}
        with self.assertRaises(RuntimeError):
            klass.poll_job(job)

   
    












