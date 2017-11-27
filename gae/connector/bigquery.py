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


"""BigQuery Service used in googleapiclient to interact with the backend
system"""


import time

import googleapiclient.discovery as disco


class BigQueryService(object):
    """Class to interact with BigQuery's backend using googleapiclient api.

    :type credentials: `google.auth.credentials.Credentials`
    :param credentials: certificates to connect to GCP.
    """
    def __init__(self, credentials):
        self.con = disco.build('bigquery', 'v2', credentials=credentials)

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
