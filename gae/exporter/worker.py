from flask import Flask, render_template, request
from google.auth import app_engine
import googleapiclient.discovery as disco
import google.auth
import uuid
from config import config
import datetime
import time

yest_date = (datetime.datetime.now() +
              datetime.timedelta(days=-2))

cre = app_engine.Credentials()

app = Flask(__name__)

@app.route("/queue_export", methods=['POST'])
def queue_export():
    try:
        bc = disco.build('bigquery', 'v2', credentials=cre)
        yest_str = yest_date.strftime("%Y%m%d%S")
        job = bc.jobs().insert(projectId='dafiti-analytics',
             body=load_bq_job(load_query(config['query_path']),
                              config['project_id'],
                              table_id='example_dataproc')).execute(num_retries=3)
    except Exception as err:
        return str(err)
    poll_job(bc, job)

    try:
        dest = 'gs://lbanor/dataproc_example/{}/result.gz'
        print 'DEST!!!! ', dest
        job_config = load_extract_job('dafiti-analytics',
            dest.format(yest_date.strftime("%Y-%m-%d")))
        print 'JOB CONFIG: ', job_config
        job = bc.jobs().insert(projectId='dafiti-analytics',
            body=job_config).execute(num_retries=3)

    except Exception as e:
        return str(e)

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

def load_extract_job(project_id,
                     destination,
                     dataset_id='simona',
                     table_id='example_dataproc',
                     format='CSV',
                     compression='GZIP'):
                     
    return {
        'jobReference': {
            'projectId': project_id,
            'jobId': str(uuid.uuid4())
        },
        'configuration': {
            'extract': {
                'sourceTable': {
                    'projectId': project_id,
                    'datasetId': dataset_id,
                    'tableId': table_id,
                },
                'destinationUris': [destination],
                'destinationFormat': format,
                'compression': compression
            }
        }
    }
