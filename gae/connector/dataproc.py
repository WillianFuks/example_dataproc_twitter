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


"""Dataproc Service used in googleapiclient to build clusters and run jobs""" 


import time

import googleapiclient.discovery as disco


ZONE_URI = 'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'

class DataprocService(object):
    """Implements requests to build clusters, run jobs and clean the system.

    :type credentials: `google.auth.credentials.Credentials`
    :param credentials: certificates to connect to GCP.
    """
    def __init__(self, credentials):
        self.con = disco.build('dataproc', 'v1', credentials=credentials)


    def build_cluster(self, **kwargs):
        """Builds a new dataproc cluster ready to receive jobs.

        Docs available at:

        (https://cloud.google.com/dataproc/docs/reference/rest/v1/
            projects.regions.clusters#Cluster)

        :kwargs:
          :type project_id: str
          :param project_id: project id for where to build the cluster.

          :type zone: str
          :param zone: zone where cluster will be located.

          :type name: str
          :param name: name of cluster to create.

          :type master_type: str
          :param master_type: instance computing type such as ``n1-standard-1``

          :type num_instances: int
          :param num_instances: how many instances to build.

          :type instances_type: str
          :param instances_type: instance computing type for workers.
        """
        if self.get_cluster(name, project_id, region) == {}:
            raise TypeError("Cluster {} already exists".format(name))

        cluster_data = {
            'projectId': project,
            'clusterName': cluster_name,
            'config': {
                'gceClusterConfig': {
                    'zoneUri': zone_uri
                },
                'masterConfig': {
                    'numInstances': 1,
                    'machineTypeUri': 'n1-standard-1'
                },
                'workerConfig': {
                    'numInstances': 2,
                    'machineTypeUri': 'n1-standard-1'
                }
            }
        }  
        result = self.con.project().regions().clusters().create(
            projectId=project_id, region=region,body=cluster_data).execute(num_retries=3)
        return result


    def wait_cluster_creation(self, job, name, project_id, region):
        """Waits for the asynchronous creation of the cluster by constantly
        asking the backend system how is the current job state.

        :type job: dict
        :param job: response object sent by the backend.
        """
        while True:
            cluster_status = self.get_cluster(name, project_id, region)
            if cluster_status.get('status').get('state') == 'ERROR':
                raise Exception(result['status']['details'])
            if cluster.get('status').get('state') == 'RUNNING':
                break
            time.sleep(5)


    def get_cluster(self, name, project_id, region):
        """Gets a specific cluster.

        :type name: str
        :param name: name of cluster to retrieve

        :type project_id: str
        :param project_id: project where cluster is located.
        
        :type region: str
        :param region: which region cluster is located.

        :rtype: dict
        :returns: dict with information of cluster. Empty if finds nothing.
        """
        result = self.con.project().regions().clusters().list(
            projectId=project_id, region=region).execute(num_retries=3)
        return [e for e in result.get('clusters', [{}]) if
            e.get('clusterName', [{}]) == name][0]
        












