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


"""Factory module to build google's apis services such as BigQuery service or
Cloud Storage.
"""

import time

from google.auth import app_engine
import googleapiclient.discovery as disco
import google.auth

class GCPService(object):

    def __init__(self, name, credentials=None):
        """Builds a connector to interact with Google Cloud tools.

        :type name: str
        :param name: name of which service to build, such as 'bigquery'
                     or 'storage'. ``name`` must be available in 
                     `self.available_services()`

        :type credentials: `google.auth.credentials.Credentials`
        :param credentials: certificates to connect to GCP.

        :returns: Resource object to interact with GCP 
        """
        if name not in self.available_services:
            raise ValueError(("'{name}' is not a valid service."
                             "Available services are: {services}").format(
                             name=name,
                             services=",".join(list(
                                 self.available_services))))

        # if no ``credentials`` is sent then assume we are running this code
        # in AppEngine environment
        if not credentials:
            credentials = app_engine.Credentials()
            #credentials, _ = google.auth.default()
        
        self.con = disco.build(name,
                               self.available_services[name],
                               credentials=credentials)
            
    @property
    def available_services(self):
        """Available services that can be used in this project"""
        return {'bigquery': 'v2',
                 'storage': 'v1'} 

    
    def execute_job(self, project_id, body):
        """Executes a job to run in GCP.

        :type project_id: str
        :param projectId: name of project Id to run the job.

        :type body: dict
        :param body: dict that specifies the job configuration
        """
        return self.con.jobs().insert(projectId=project_id,
                                       body=body).execute(num_retries=3)

 
    def poll_job(self, job):
        """Waits for a job to complete.

        :type job: `googleapi.discovery.Resource`
        :param job: any job that has been initiated by the connector.
        """
        request = self.con.jobs().get(
            projectId=job['jobReference']['projectId'],
            jobId=job['jobReference']['jobId'])


        while True:
            result = request.execute(num_retries=3)
            if result['status']['state'] == 'DONE':
                if 'errorResult' in result['status']:
                    raise RuntimeError(result['status']['errorResult'])
                return
            time.sleep(1)
 
