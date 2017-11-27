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


"""Storage Service used in googleapiclient to interact with GCS""" 


import time

import googleapiclient.discovery as disco
import googleapiclient.http


class StorageService(object):
    """Implements requests to build clusters, run jobs and clean the system.

    :type credentials: `google.auth.credentials.Credentials`
    :param credentials: certificates to connect to GCP.
    """
    def __init__(self, credentials):
        self.con = disco.build('storage', 'v1', credentials=credentials)

    def upload_from_filenames(self, **kwargs):
        """Walks through a list of filenames and send them to specified bucket.

        :kwargs:
          :type bucket: str
          :param bucket: which bucket to save files.

          :type py_files: str
          :param py_files: list of python files to upload to GCS.

          :type main_file: str
          :param main_file: main file to be executed in dataproc.

        """ 
        for filename in kwargs['py_files']:
            body = {
                'name': filename
            }
            with open(filename, 'rb') as f:
                result = self.con.objects().insert(bucket=kwargs['bucket'],
                    body=body,
                    media_body=googleapiclient.http.MediaIoBaseUpload(f,
                        'application/octet-stream')).execute(num_retries=3)
