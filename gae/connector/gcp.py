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


"""Main class with connectors to different services available in GCP."""


import time

import google.auth.credentials
import googleapiclient.discovery as disco
from google.auth import app_engine
from . import (BigQueryService, DataprocService, StorageService,
    DataflowService)


class GCPService(BigQueryService,
                 DataprocService,
                 StorageService,
                 DataflowService):
    _credentials = None
    _bigquery = None
    _dataproc = None
    _storage = None
    _dataflow = None
    def __init__(self, credentials=None):
        """Builds a connector to interact with Google Cloud tools.

        :type credentials: `google.auth.credentials.Credentials`
        :param credentials: certificates to connect to GCP.

        :raises: TypeError if credentials is not of type
                 google.auth.credentials
        """
        if (credentials is not None and not isinstance(credentials,
            google.auth.credentials.Credentials)):
            raise TypeError("credentials must be of type "
                            "google.auth.credentials") 
        # if no ``credentials`` is sent then assume we are running this
        # code in AppEngine environment
        self._credentials = (app_engine.Credentials() if not credentials else
            credentials)
        #from google.oauth2 import service_account
        #self._credentials = (service_account.Credentials.\
        #    from_service_account_file('./key.json'))            


    @property
    def bigquery(self):
        if not self._bigquery:
            self._bigquery = BigQueryService(self._credentials) 
        return self._bigquery


    @property
    def dataproc(self):
        if not self._dataproc:
            self._dataproc = DataprocService(self._credentials)
        return self._dataproc


    @property
    def storage(self):
        if not self._storage:
            self._storage = StorageService(self._credentials)
        return self._storage

    @property
    def dataflow(self):
        if not self._dataflow:
            self._dataflow = DataflowService(self._credentials)
        return self._dataflow
