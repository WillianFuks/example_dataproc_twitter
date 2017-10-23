from flask import Flask, render_template, request
from google.auth import app_engine
import googleapiclient.discovery as disco
import google.auth
import uuid
from config import config
import datetime

yesterday_str = (datetime.datetime.now() +
                 datetime.timedelta(days=-2)).strftime("%Y%m%d")

cre = app_engine.Credentials()
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
cre, _ = google.auth.default()


app = Flask(__name__)

@app.route("/test")
def test():
    return "it's working"

@app.route("/export_customers")
def export_customers():
    bc = disco.build('bigquery', 'v2', credentials=cre)
    try:
        r = bc.jobs().insert(projectId='dafiti-analytics',
                             body=load_bq_job(
                              load_query(config['query_path']),
                              config['project_id'])).execute(num_retries=3)
    except Exception as err:
        return str(err)   
    return 'finished processing query'

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
    result = open(query_path).read().format(project_id=config['project_id'],
                                          dataset_id=config['dataset_id'],
                                          table_id=config['table_id'],
                                          date=yesterday_str)

    print result

    return result
