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


"""Dataflow Service to run apache-beam jobs.""" 


import time
from operator import itemgetter

import googleapiclient.discovery as disco


class DataflowService(object):
    """Implements necessary methods to interact with dataflow jobs.

    :type credentials: `google.auth.credentials.Credentials`
    :param credentials: certificates to connect to GCP.
    """
    def __init__(self, credentials):
        self.con = disco.build('dataflow', 'v1b3', credentials=credentials)


    def run_template(self, **kwargs):
        """Runs a templated job. REST API definition can be found here:

        (https://cloud.google.com/dataflow/docs/reference/rest/v1b3/
            projects.templates/create)

        :kwargs:
          :type project_id: str
          :param project_id: project id for where to build the cluster.

          :type zone: str
          :param zone: zone where cluster will be located.

          :type job_name: str
          :param job_name: name of job to run. Notice that if two jobs with
                           the same name are initiated only the first one
                           will succeed to process.

          :type template_location: str
          :param template_location: GCS path where dataflow template is saved.

          :type temp_location: str
          :param temp_location: how many instances to build.
        """
        project_id = kwargs['project_id']
        job_name = kwargs['job_name']
        template_location = kwargs['template_location']
        temp_location = kwargs['temp_location']
        zone = kwargs['zone']
        max_workers = kwargs['max_workers']
        machine_type = kwargs['machine_type']
        body = {
            "jobName": job_name,
            "gcsPath": template_location,
            "environment": {
                "tempLocation": temp_location,
                "zone": zone,
                "maxWorkers": max_workers,
                "machineType": machine_type
            }
        }

        return self.con.projects().templates().create(
            projectId=project_id, body=body).execute(num_retries=3)
