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


import os
import re
import gzip
import datetime
import json
import random
import google.cloud.storage as st

from locust import HttpLocust, TaskSet, task
from config import config


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/key.json'
sc = st.Client()


class MarrecoUsers(object):
    users_inters = []
    _allowed_blobs = []
    total_users = 9
    def load_users_interactions(self):
        bucket = sc.bucket(config['bucket'])
        blobs = list(bucket.list_blobs(prefix=config['gcs_prefix']))
        #blobs = []
        self.build_data(bucket, blobs)
 
    def build_url_reco(self):
        idx = random.randint(0, self.total_users - 1) 
        recos = ','.join(self.users_inters[idx])
        return ("/make_recommendation?"
            "browsed={x}&basket={x}&purchased={x}".format(x=recos))
 
    def build_data(self, bucket, blobs_list):
 #       self.users_inters = [[u'NI288SCF02XRL'], [u'MA041SCM31HTK'], [u'TI572SCF35WOU', u'TI572SCF05GQC', u'TI572SCF56JMX', u'TI572SCF37WOS', u'TI572SCF54DHX'], [u'HD124SHM55HVK'], [u'AD464SCM53KZI'], [u'NE184SCF14ZRT'], [u'CA278SHF03VKY', u'CA278SHF07VKU', u'CA278SHF15WDS', u'CA278SHF07CEA', u'CA278SHF06WEB', u'CA278SHF14WDT', u'CA278SHF08VKT', u'CA278SHF84CEX', u'CA278SHF04WED', u'CA278SHF06VKV', u'CA278SHF71CFK', u'CA278SHF82CEZ', u'CA278SHF04VKX'], [u'MO219SHM93GFU'], [u'TU798ACM29ZCW', u'SK554ACM40PIT', u'SP797SAM65LNI', u'SK554ACM23EAE', u'SK554ACM58LIJ']]

        for url in self.allowed_blobs:
            buffer_ = gzip.io.BytesIO()   
            blob = [blob for blob in blobs_list if re.match(
                url + ".*gz", blob.name)][0]
            blob.download_to_file(buffer_)
            buffer_.seek(0)
            with gzip.GzipFile(fileobj=buffer_, mode='rb') as f:
                self.users_inters += [[r['item'] for r in json.loads(
                    e.strip())['interactions']] for e in f.readlines()]

    @property
    def allowed_blobs(self):
        if not self._allowed_blobs:
            today = datetime.datetime.now()
            for i in range(1, config['days_of_data'] + 1):
                self._allowed_blobs.append(config['gcs_prefix'] +
                    (today - datetime.timedelta(days=i)).strftime("%Y-%m-%d") +
                    '/')
        return self._allowed_blobs


m = MarrecoUsers()
m.load_users_interactions()


class MissionSet(TaskSet):
    build_url = m.build_url_reco
    @task
    def target(self):
        #self.client.get("/make_recommendation")
        self.client.get(self.build_url(), name="/make_recommendation")


class Swarm(HttpLocust):
    """Main Locust class to build the swarm that will stress the server."""
    host = "http://localhost:8080"
    min_wait = 1
    max_wait = 1500
    task_set = MissionSet
