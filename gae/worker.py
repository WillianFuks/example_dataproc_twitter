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


import utils
from flask import Flask, request 
from config import config
from connector.gcp import GCPService
from scheduler import SchedulerJob


app = Flask(__name__)
gcp_service = GCPService() 
scheduler = SchedulerJob()


@app.route("/export_customers", methods=['POST'])
def export():
    """Runs a query against BigQuery and export results to a GCS bucket."""
    date = (None if request.form.get('date') == 'None' else
        utils.process_url_date(request.form.get('date')))

    query_job_body = utils.load_query_job_body(date,
        **config)

    job = gcp_service.bigquery.execute_job(config['general']['project_id'],
        query_job_body)

    gcp_service.bigquery.poll_job(job)

    extract_job_body = utils.load_extract_job_body(date, **config)
    gcp_service.bigquery.execute_job(config['general']['project_id'],
        extract_job_body)
    return "finished"


@app.route("/dataproc_dimsum", methods=['POST'])
def dataproc_dimsum():
    """Prepares whole environment to run DIMSUM spark job in Dataproc. After
    the processing is over (this method waits until job is complete) then
    schedules a new call to prepare Datastore with the resulting similarity
    matrix we obtain from the algorithm."""
    try:
        extended_args = request.form.get('extended_args').split(',')
        setup = config['jobs']['run_dimsum']
        job = gcp_service.dataproc.build_cluster(**setup)
        gcp_service.storage.upload_from_filenames(
            **config['jobs']['run_dimsum']['pyspark_job'])
        job = gcp_service.dataproc.submit_pyspark_job(extended_args,
             **config['jobs']['run_dimsum'])
        result = gcp_service.dataproc.delete_cluster(**setup)
        #scheduler.run({'url': '/prepare_datastore',
        #               'target': config['jobs']['dataflow_export'][
        #                            'dataflow_service']})
    except Exception as err:
        print str(err)
    return "finished"


@app.route("/prepare_datastore", methods=['POST'])
def prepare_datastore():
    """With DIMSUM job completed, we run a Dataflow job to get results from 
    GCS and save them properly into Datastore so we have quick access to
    results to build final recommendations for our customers."""
    result = gcp_service.dataflow.run_template(**config['jobs'][
        'dataflow_export']) 
    return "finished"
