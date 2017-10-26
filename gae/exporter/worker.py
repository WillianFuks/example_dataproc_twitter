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


import uuid
import datetime
import google.auth
import gae.exporter.utils as utils
from flask import Flask 
from config import config
from gae.exporter.connector.gcp import GCPService

cre, _ = google.auth.default()

app = Flask(__name__)
bq_service = GCPService('bigquery', cre) 

@app.route("/queue_export", methods=['POST'])
def queue_export():
    (project_id, dataset_id, table_id, query_path,
     output) = utils.load_config(config)

    query = utils.load_query(query_path, project_id, dataset_id, table_id)
    query_job_body = utils.load_query_job_body(query,
                                               project_id,
                                               dataset_id,
                                               table_id)
    job = bq_service.execute_job(project_id, body)
    bq_service.poll_job(job)                
    
    extract_job_body = utils.load_extract_job_body(project_id,
        output.format(date=utils.load_yesterday_date().strftime("%Y-%m-%d"),
        dataset_id,
        table_id)
   return "finished"


