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

from gae.exporter.connector.gcp import GCPService


class TestGCPFactory(unittest.TestCase):

    @staticmethod
    def _make_credentials():
        import google.auth.credentials

        
        return mock.Mock(spec=google.auth.credentials.Credentials)


    def test_service_build_raises_name_error(self):
        with self.assertRaises(ValueError):
            service = GCPService('test')
    

    @mock.patch('gae.exporter.connector.gcp.app_engine')        
    def test_service_builds_successfully_no_credentials(self, app_mock):
        service = GCPService('bigquery')
        app_mock.Credentials.assert_called_once()


    def test_service_builds_successfully_w_credentials(self):
        cre = self._make_credentials()
        service = GCPService('bigquery', cre)
        self.assertTrue(isinstance(service.con,
                         googleapiclient.discovery.Resource))


    def test_execute_job(self):
        cre = self._make_credentials()
        service = GCPService('bigquery', cre)
        job_mock = mock.Mock()
        resource_mock = mock.Mock()
        execute_mock = mock.Mock()
        service.con.jobs = job_mock

        job_mock.return_value = resource_mock
        resource_mock.insert.return_value = execute_mock

        project_id = 'project123'
        body = {'project_id': 'project123'}

        job = service.execute_job(project_id, body)
        resource_mock.insert.assert_called_once_with(**{'projectId': project_id, 'body': body})
        execute_mock.execute.assert_called_once_with(**{'num_retries': 3})


    @mock.patch('gae.exporter.connector.gcp.time')
    def test_poll_job(self, time_mock):
        cre = self._make_credentials()
        service = GCPService('bigquery', cre)
        job_mock = mock.Mock()
        resource_mock = mock.Mock()
        request_mock = mock.Mock()
        job_mock.jobs.return_value = resource_mock
        resource_mock.get.return_value = request_mock
        request_mock.execute.return_value = {"status": {"state": "DONE"}}
        service.con = job_mock

        job = {"jobReference": {"projectId": "project123", "jobId": "1"}}
        service.poll_job(job)
        request_mock.execute.assert_called_once_with(**{'num_retries': 3})


    @mock.patch('gae.exporter.connector.gcp.time')
    def test_poll_job_raises_error(self, time_mock):
        cre = self._make_credentials()
        service = GCPService('bigquery', cre)
        job_mock = mock.Mock()
        resource_mock = mock.Mock()
        request_mock = mock.Mock()
        job_mock.jobs.return_value = resource_mock
        resource_mock.get.return_value = request_mock
        request_mock.execute.return_value = {"status": {"state": "DONE",
                                                        "errorResult": True}}
        service.con = job_mock

        job = {"jobReference": {"projectId": "project123", "jobId": "1"}}
        with self.assertRaises(RuntimeError):
            service.poll_job(job)
 
