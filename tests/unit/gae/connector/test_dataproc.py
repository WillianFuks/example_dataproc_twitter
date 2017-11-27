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


class TestDataprocService(unittest.TestCase):
    @staticmethod
    def _make_credentials():
        return mock.Mock(spec=google.auth.credentials.Credentials)

    @staticmethod
    def _get_target_klass():
        from gae.connector.gcp import DataprocService


        return DataprocService

    @property
    def config_(self):
        _source_config = 'tests/unit/data/gae/test_config.json' 
        return json.loads(open(_source_config).read().replace( 
            "config = ", ""))

    @property
    def job_mock(self):
        return {"name": "foo/project123/bar/region",
                "metadata": {"clusterName": "cluster_name"},
                "reference": {"projectId": "project123",
                              "jobId": "job_id"}}

    @mock.patch('gae.connector.dataproc.disco')
    def test_cto(self, disco_mock):
        mock_cre = self._make_credentials()
        disco_mock.build.return_value = 'con'
        klass = self._get_target_klass()(mock_cre) 
        self.assertEqual(klass.con, 'con')
        disco_mock.build.assert_called_once_with('dataproc', 'v1',
            credentials=mock_cre)

    @mock.patch('gae.connector.dataproc.DataprocService.get_cluster')
    @mock.patch('gae.connector.dataproc.disco')
    def test_build_cluster_raises(self, disco_mock, f_get_cluster_mock):
        f_get_cluster_mock.return_value = {True}
        config_ = self.config_
        mock_cre = self._make_credentials()
        disco_mock.build.return_value = 'con'
        klass = self._get_target_klass()(mock_cre) 
        with self.assertRaises(TypeError):
            klass.build_cluster(**config_['jobs']['run_dimsum'])
    
    @mock.patch('gae.connector.dataproc.DataprocService.wait_cluster_operation')
    @mock.patch('gae.connector.dataproc.DataprocService.get_cluster')
    @mock.patch('gae.connector.dataproc.disco')
    def test_build_cluster(self, disco_mock, f_get_cluster_mock, wait_mock):
        f_get_cluster_mock.return_value = {}
        config_ = self.config_
        mock_cre = self._make_credentials()
        con_mock = mock.Mock()
        project_mock = mock.Mock()
        region_mock = mock.Mock()
        cluster_mock = mock.Mock()
        creation_mock = mock.Mock()

        disco_mock.build.return_value = con_mock
        con_mock.projects.return_value = project_mock
        project_mock.regions.return_value = region_mock
        region_mock.clusters.return_value = cluster_mock
        cluster_mock.create.return_value = creation_mock
        creation_mock.execute.return_value = 'result'
        
        klass = self._get_target_klass()(mock_cre)
        klass.build_cluster(**config_['jobs']['run_dimsum'])

        creation_mock.execute.assert_called_once_with(num_retries=3)        
        wait_mock.assert_called_once_with('result')
        cluster_mock.create.assert_called_once_with(body={
            'clusterName': 'cluster_name', 'projectId': 'project123',
             'config': {'workerConfig': {'machineTypeUri': 'instance-2',
             'numInstances': 2}, 'masterConfig':
             {'machineTypeUri': 'instance-1', 'numInstances': 1},
             'gceClusterConfig':
                 {'zoneUri':
 'https://www.googleapis.com/compute/v1/projects/project123/zones/region-1'}}},
             projectId='project123', region='region')

    @mock.patch('gae.connector.dataproc.time')
    @mock.patch('gae.connector.dataproc.DataprocService.get_cluster')
    @mock.patch('gae.connector.dataproc.disco')
    def test_wait_cluster_operation(self, disco_mock, f_get_cluster_mock,
        time_mock):
        f_get_cluster_mock.return_value = {}
        config_ = self.config_
        mock_cre = self._make_credentials()
        disco_mock.build.return_value = 'con'
        klass = self._get_target_klass()(mock_cre)
 
        f_get_cluster_mock.side_effect = [{'status': {'state': 'CREATING'}},
            {'status': {'state': 'RUNNING'}}]
        job_mock = self.job_mock

        result = klass.wait_cluster_operation(job_mock)
        self.assertEqual(result, {'status': {'state': 'RUNNING'}})
 
        f_get_cluster_mock.assert_called_with('cluster_name',
            'project123', 'region')
        time_mock.sleep.assert_called_with(60)
 
        f_get_cluster_mock.side_effect = [{'status': {'state': 'CREATING'}},
            {'cluster': 'cluster'}]
        result = klass.wait_cluster_operation(job_mock)
        self.assertEqual(result, {'cluster': "cluster"})

        f_get_cluster_mock.return_value = [{'status': {'state': 'ERROR'}}]
        with self.assertRaises(Exception):
            klass.wait_cluster_operation(job_mock)

    @mock.patch('gae.connector.dataproc.time')
    @mock.patch('gae.connector.dataproc.disco')
    def test_wait_for_job(self, disco_mock, time_mock):
        config_ = self.config_
        job_setup = self.job_mock
        mock_cre = self._make_credentials()
        con_mock = mock.Mock()
        disco_mock.build.return_value = con_mock

        project_mock = mock.Mock()
        region_mock = mock.Mock()
        job_mock = mock.Mock()
        execution_mock = mock.Mock()

        con_mock.projects.return_value = project_mock
        project_mock.regions.return_value = region_mock
        region_mock.jobs.return_value = job_mock
        job_mock.get.return_value = execution_mock

        execution_mock.execute.side_effect = [{'status': {'state': 'RUNNING'}},
            {'status': {'state': 'DONE'}}]
        
        klass = self._get_target_klass()(mock_cre) 
        result = klass.wait_for_job(job_setup, 'region')        

        self.assertEqual(result, {'status': {'state': 'DONE'}})
        job_mock.get.assert_called_with(jobId='job_id', projectId='project123',
            region='region')
        execution_mock.execute.assert_called_with(num_retries=3)
        time_mock.sleep.assert_called_with(60)
        execution_mock.execute.return_value = {'status': {'state': 'ERROR'}}

        with self.assertRaises(Exception):
            klass.wait_for_job(job_setup, 'region')

    @mock.patch('gae.connector.dataproc.disco')
    def test_get_cluster(self, disco_mock):
        mock_cre = self._make_credentials()
        con_mock = mock.Mock()
        disco_mock.build.return_value = con_mock

        project_mock = mock.Mock()
        region_mock = mock.Mock()
        cluster_mock = mock.Mock()
        execution_mock = mock.Mock()

        con_mock.projects.return_value = project_mock
        project_mock.regions.return_value = region_mock
        region_mock.clusters.return_value = cluster_mock
        cluster_mock.list.return_value = execution_mock
        execution_mock.execute.return_value = {}
        
        klass = self._get_target_klass()(mock_cre) 
        result = klass.get_cluster('name', 'project123', 'region')        

        self.assertEqual(result, {})
        cluster_mock.list.assert_called_once_with(projectId='project123',
            region='region') 
        execution_mock.execute.assert_called_once_with(num_retries=3)        

        execution_mock.execute.return_value = {'clusters': [{
            'clusterName': 'name'}]}
    
        result = klass.get_cluster('name', 'project123', 'region')        
        self.assertEqual(result, {'clusterName': 'name'})

        result = klass.get_cluster('name2', 'project123', 'region')        
        self.assertEqual(result, {})
        
    @mock.patch('gae.connector.dataproc.DataprocService.wait_cluster_operation')
    @mock.patch('gae.connector.dataproc.disco')
    def test_delete_cluster(self, disco_mock, wait_mock):
        mock_cre = self._make_credentials()
        con_mock = mock.Mock()
        disco_mock.build.return_value = con_mock
        config_ = self.config_

        project_mock = mock.Mock()
        region_mock = mock.Mock()
        cluster_mock = mock.Mock()
        execution_mock = mock.Mock()
        
        con_mock.projects.return_value = project_mock
        project_mock.regions.return_value = region_mock
        region_mock.clusters.return_value = cluster_mock
        cluster_mock.delete.return_value = execution_mock
        execution_mock.execute.return_value = {}

        execution_mock.execute.return_value = 'result'
        
        klass = self._get_target_klass()(mock_cre)
        result = klass.delete_cluster(**config_['jobs']['run_dimsum'])

        cluster_mock.delete.assert_called_once_with(clusterName='cluster_name',
            projectId='project123', region='region')
        execution_mock.execute(num_retries=3)
        wait_mock.assert_called_once_with('result')        

    @mock.patch('gae.connector.dataproc.DataprocService.wait_for_job')
    @mock.patch('gae.connector.dataproc.disco')
    def test_submit_pyspark_job(self, disco_mock, wait_mock):
        config_ = self.config_
        job_setup = self.job_mock
        mock_cre = self._make_credentials()
        con_mock = mock.Mock()
        disco_mock.build.return_value = con_mock

        project_mock = mock.Mock()
        region_mock = mock.Mock()
        job_mock = mock.Mock()
        execution_mock = mock.Mock()

        con_mock.projects.return_value = project_mock
        project_mock.regions.return_value = region_mock
        region_mock.jobs.return_value = job_mock
        job_mock.submit.return_value = execution_mock

        execution_mock.execute.return_value = 'result'
        
        klass = self._get_target_klass()(mock_cre) 
        extended_args = ['--days=3']
        result = klass.submit_pyspark_job(extended_args, **config_['jobs'][
            'run_dimsum'])

        wait_mock.assert_called_once_with('result', 'region') 
        job_mock.submit.assert_called_once_with(body={
            'projectId': 'project123', 'job': {'pysparkJob': {
            'mainPythonFileUri': 'gs://bucket_name/basename/main.py',
            'pythonFileUris': ['gs://bucket_name/basename/1.py',
            'gs://bucket_name/basename/2.py'], 'args': ['--days=3',
            '--source_uri=gs://source/file.gz']}, 'placement': {
            'clusterName': u'cluster_name'}}},
            projectId='project123', region='region')

