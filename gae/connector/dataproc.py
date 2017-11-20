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
from operator import itemgetter

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

          :type cluster_name: str
          :param cluster_name: name of cluster to create.

          :type master_type: str
          :param master_type: instance computing type such as ``n1-standard-1``

          :type num_instances: int
          :param num_instances: how many instances to build.

          :type instances_type: str
          :param instances_type: instance computing type for workers.
        """
        project_id = kwargs['project_id']
        cluster_name = kwargs['cluster_name']
        zone = kwargs['zone']
        region = zone[:-2]
        if self.get_cluster(cluster_name, project_id, region) != {}:
            raise TypeError("Cluster {} already exists".format(cluster_name))
        cluster_data = {
            'projectId': project_id,
            'clusterName': cluster_name,
            'config': {
                'gceClusterConfig': {
                    'zoneUri': ZONE_URI.format(project_id, zone)
                },
                'masterConfig': {
                    'numInstances': 1,
                    'machineTypeUri': kwargs['create_cluster']['master_type'] 
                },
                'workerConfig': {
                    'numInstances': kwargs['create_cluster']['worker_num_instances'],
                    'machineTypeUri': kwargs['create_cluster']['worker_type']
                }
            }
        }  
        result = self.con.projects().regions().clusters().create(
            projectId=project_id, region=region, body=cluster_data).execute(
                num_retries=3)
        self.wait_cluster_operation(result)
        return result


    def wait_cluster_operation(self, job):
        """Waits for the asynchronous operation (either creation or deletion)
        of the cluster by constantly asking the backend system how is the
        current job state.

        :type job: dict
        :param job: response object sent by the backend, it follows the
                    following schema:

        (https://cloud.google.com/dataproc/docs/reference/rest/Shared.Types/
            ListOperationsResponse#Operation)
        """
        mapping = itemgetter(1, 3)
        while True:
            print "IM WAITING CLUSTER OPERATION"
            (project_id, region) = mapping(job['name'].split('/'))
            cluster_name = job['metadata']["clusterName"]
            cluster_status = self.get_cluster(cluster_name, project_id, region)
            if cluster_status['status']['state'] == 'ERROR':
                raise Exception(result['status']['details'])
            if cluster_status['status']['state'] == 'RUNNING':
                break
            # as cluster operations takes longer then we wait more as well
            time.sleep(30)


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
        result = self.con.projects().regions().clusters().list(
            projectId=project_id, region=region).execute(num_retries=3)
        return ([e for e in result.get('clusters', [{}]) if
            e.get('clusterName', [{}]) == name] or [{}])[0]
 

    def delete_cluster(self, **kwargs):
        """Deletes a specific dataproc cluster.

        :kwargs:
          :type project_id: str
          :param project_id: project id where cluster is located.

          :type region: str
          :param region: region where cluster is located.

          :type cluster_name: str
          :param cluster_name: name of cluster to delete

          :rtype: dict
          :returns: dict with resource information for deletion method.
        """
        project_id = kwargs['project_id']
        region = kwargs['zone'][:-2]
        cluster_name = kwargs['cluster_name']  
        result = self.con.projects().regions().clusters().delete(
            projectId=project_id, region=region,
             clusterName=cluster_name).execute(num_retries=3)

        self.wait_cluster_operation(result)
        return result


    def submit_pyspark_job(self, extended_args, **kwargs):
        """Submits a pyspark job to the dataproc cluster.

        :type extended_args: list
        :param extended_args: arguments that can be passed through the URL
                              request, such as ``days_init``, ``days_end``,
                              ``threshold``, ``force`` 

        :kwargs:
          :type project_id:


          :type cluster_name:


          :type zone:


          :type pyspark_job: dict
            :type bucket:

            :type py_files:

            :type main_file:

            :type default_args:
        """
        project_id = kwargs['project_id']
        cluster_name = kwargs['cluster_name']
        bucket = kwargs['pyspark_job']['bucket'] 
        main_file = kwargs['pyspark_job']['main_file']
        args = extended_args + kwargs['pyspark_job']['default_args']
        region = kwargs['zone'][:-2]
        base_uri = 'gs://{}/{}'
        body = {
            'projectId': project_id,
            'job': {
                'placement': {
                    'clusterName': cluster_name
                },
                'pysparkJob': {
                    'mainPythonFileUri': base_uri.format(bucket, main_file),
                    'pythonFileUris': map(lambda x: base_uri.format(bucket, x),
                        [e for e in kwargs['pyspark_job']['py_files'] if e !=
                    kwargs['pyspark_job']['main_file']]), 
                    'args': args  
                }
            }
        } 
        job = self.con.projects().regions().jobs().submit(projectId=project_id,
            region=region, body=body).execute(num_retries=3)
        return job
