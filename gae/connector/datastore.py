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


"""Datastore Client. As this is supposed to be used in Flexible Environment,
we'll be using the official google-cloud API. Notice also that this class
is not used in GCPService as it's speficially designed for Standard Environment
"""


import time
import google.cloud.datastore as ds


class DatastoreService(object):
    """Class to interact with Datastore's backend using google-cloud API.

    :type credentials: `google.oauth.credentials.Credentials` 
    :param credentials: credentials used to authenticate requests in GCP. 
    """
    def __init__(self, credentials):
        self.client = (ds.Client(credentials=credentials) if credentials
            else ds.Client())

    def get_keys(self, kind, keys):
        """Retrieves list of keys from Datastore.
    
        :type kind: str
        :param kind: kind to retrieve keys from DS.

        :type keys: list
        :param keys: list of key names to retrieve data from.

        :rtype: list
        :returns: list with resulting keys retrieved from DS. 
    
        """
        return self.client.get_multi([self.client.key(kind, e) for e in keys])
