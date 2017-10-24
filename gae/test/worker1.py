from flask import Flask, render_template, request
from google.auth import app_engine
import googleapiclient.discovery as disco
import google.auth
import uuid
from config import config
import datetime
from google.appengine.api import taskqueue
import time

yest_date = (datetime.datetime.now() +
                   datetime.timedelta(days=-2))

cre = app_engine.Credentials()
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
cre, _ = google.auth.default()


app = Flask(__name__)

@app.route("/test_worker")
def test():
    return "it's working"

@app.route("/test_queue", methods=['POST'])
def test_queue():
    print 'tou aquii'
    bc = disco.build('bigquery', 'v2', credentials=cre)
    try:
        yest_str = yest_date.strftime("%Y%m%d%S")
        job = bc.jobs().insert(projectId='dafiti-analytics',
             body=load_bq_job(load_query(config['query_path']),
                              config['project_id'],
                              table_id='example_{}'.format(yest_str))).execute(num_retries=3)
    except Exception as err:
        return str(err)   
    print 'ARRIVED HEEEEEEEEEEERE'
    poll_job(bc, job) 
    return "finished"


def poll_job(service, job):
    """Waits for a job to complete."""

    request = service.jobs().get(
        projectId=job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    while True:
        result = request.execute(num_retries=2)

        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                raise RuntimeError(result['status']['errorResult'])
            return
        time.sleep(1)


def load_bq_job(query,
                project_id,
                dataset_id='simona',
                table_id='example_dataproc2'):
    return {'jobReference': {
                'projectId': project_id,
                'jobId': str(uuid.uuid4())
                },
            'configuration': {
                'query': {
                    'destinationTable': {
                        'datasetId': dataset_id,
                        'tableId': table_id,
                        'projectId': project_id
                         },
                    'maximumBytesBilled': 100000000000,
                    'query': query,
                    'useLegacySql': False,
                    'writeDisposition': 'WRITE_TRUNCATE'
                    }
                }
            }

def load_query(query_path):
    yest_str = yest_date.strftime("%Y%m%d")
    result = open(query_path).read().format(project_id=config['project_id'],
        dataset_id=config['dataset_id'],
        table_id=config['table_id'],
        date=yest_str)

    return result


