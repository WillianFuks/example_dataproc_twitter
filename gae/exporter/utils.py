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

def load_query_job_body(query, project_id, dataset_id, table_id):
    """Returns the body to be used in a query job.

    :type query: str
    :param query: query string to run against BQ.

    :type project_id: str
    :param project: project where to save resulting job.

    :type dataset_id: str
    :param dataset_id: dataset where to save resulting job.

    :type table_id: str
    :param table_id: table where to save resulting job.

    :rtype: dict
    :returns: dict containing body to setup job execution.
    """
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


def load_query(query_path, project_id, dataset_id, table_id):
    """Reads a query from a source file.

    :type query_path: str
    :param query_path: location where to find the query file.

    :type project_id: str
    :param project_id: project id to format query.

    :type dataset_id: str
    :param dataset_id: dataset_id to format query.

    :type table_id: str
    :param table_id: table_id to format query.
    """
    yest_str = get_yesterday_date().strftime("%Y%m%d")
    result = open(query_path).read().format(project_id=project_id,
                                            dataset_id=dataset_id,
                                            table_id=table_id,
                                            date=yest_str).strip()
    return result


def load_extract_job_body(project_id,
                          destination,
                          dataset_id,
                          table_id,
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


def load_config(config):
    """Returns values of file config as list for asserting variables.

    :type config: dict
    :param config: config values for setting up jobs

    :rtype: list
    :returns: list of specified variables
    """
    return [config['project_id'],
            config['dataset_id'],
            config['table_id'],
            config['query_path'],
            config['output']]
 
