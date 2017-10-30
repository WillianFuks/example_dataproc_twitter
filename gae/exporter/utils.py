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


"""General functions to be used throught the exporter modules"""


import datetime
import uuid


def get_yesterday_date():
    """Returns datetime for yesterday value
    :rtype: `datetime.datetime`
    :returns: yesterday's datetime
    """
    return (datetime.datetime.now() +
             datetime.timedelta(days=-1))

def load_query_job_body(date, **kwargs):
    """Returns the body to be used in a query job.

    :type date: str
    :param date: date to set filtering in query results.

    :type kwargs:
      :type source.query_path: str
      :param query: query string to run against BQ.

      :type source.project_id: str
      :param source.project: project where to run query from.

      :type source.dataset_id: str
      :param source.dataset_id: dataset where to run query from.

      :type source.table_id: str
      :param source.table_id: table where to run query from.

      :type destination.table_id: str
      :param destination.table_id: table_id where results should be saved.

      :type destination.dataset_id: str
      :param destination.dataset_id: dataset_id where results should be saved.

      :type destination.project_id: str
      :param destination.project_id: project_id where results should be saved.

    :rtype: dict
    :returns: dict containing body to setup job execution.
    """
    query = load_query(date, **kwargs)
    dest = kwargs['jobs']['query_job']['destination']
    return {'jobReference': {
                'projectId': dest['project_id'],
                'jobId': str(uuid.uuid4())
                },
            'configuration': {
                'query': {
                    'destinationTable': {
                        'datasetId': dest['dataset_id'],
                        'tableId': dest['table_id'],
                        'projectId': dest['project_id']
                         },
                    'maximumBytesBilled': 100000000000,
                    'query': query,
                    'useLegacySql': False,
                    'writeDisposition': 'WRITE_TRUNCATE'
                    }
                }
            }


def load_query(date=None, **kwargs):
    """Reads a query from a source file.

    :type date: str
    :param date: which date to filter on query results.

    :param kwargs:
      :type query_path: str
      :param query_path: location where to find the query file.

      :type source.project_id: str
      :param source.project_id: project id to format query.

      :type source.dataset_id: str
      :param source.dataset_id: dataset_id to format query.

      :type source.table_id: str
      :param source.table_id: table_id to format query.
    """
    source = kwargs['jobs']['query_job']['source']
    date_str = get_yesterday_date().strftime("%Y%m%d") if not date else date
    result = open(source['query_path']).read().format(project_id=source['project_id'],
                                            dataset_id=source['dataset_id'],
                                            table_id=source['table_id'],
                                            date=date_str).strip()
    return result


def load_extract_job_body(date=None, **kwargs):
    """Returns json config to run extract jobs in BigQuery.

    :type date: str
    :param date: used to setup output path in GCS. If None then defaults to 
                 yesterday's date.

    :param kwargs:
      :type project_id: str
    """      

    value = kwargs['jobs']['extract_job']
    date = get_yesterday_date().strftime("%Y-%m-%d") if not date else date
    output = value['output'].format(date=date)

    return {
        'jobReference': {
            'projectId': value['project_id'],
            'jobId': str(uuid.uuid4())
        },
        'configuration': {
            'extract': {
                'sourceTable': {
                    'projectId': value['project_id'],
                    'datasetId': value['dataset_id'],
                    'tableId': value['table_id'],
                },
                'destinationUris': [output],
                'destinationFormat': value['format'],
                'compression': value['compression']
            }
        }
    }
