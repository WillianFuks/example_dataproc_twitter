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


import datetime
import utils as utils
from flask import Flask 
from config import config
from connector.gcp import GCPService

app = Flask(__name__)
bq_service = GCPService('bigquery') 


@app.route("/queue_export", methods=['POST'])
def export():
    date = request.args.get('date')
    query = utils.load_query_job_body(date, **config)

    print query_job_body
    print
    print

    job = bq_service.execute_job(project_id, query_job_body)
    bq_service.poll_job(job)
   
    extract_job_body = utils.load_extract_job_body(date, **config)
    print extract_job_body

    bq_service.execute_job(project_id, extract_job_body)
    return "finished"
