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

import heapq
import datetime
import uuid
from collections import Counter
import time

import cythonized.c_funcs as c_funcs
from google.appengine.ext import ndb
from config import config


SCORES = {'browsed': 0.5, 'basket': 2., 'purchased': 6.}


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
    dest = kwargs['jobs']['export_customers']['query_job']['destination']
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
    source = kwargs['jobs']['export_customers']['query_job']['source']
    date_str = get_yesterday_date().strftime("%Y%m%d") if not date else date
    result = open(source['query_path']).read().format(
        project_id=source['project_id'], dataset_id=source['dataset_id'],
        table_id=source['table_id'], date=date_str).strip()
    return result

def load_extract_job_body(date=None, **kwargs):
    """Returns json config to run extract jobs in BigQuery.

    :type date: str
    :param date: used to setup output path in GCS. If None then defaults to 
                 yesterday's date. The format expected is "%Y%m%d" and must
                 be casted to "%Y-%m-%d".

    :param kwargs:
      :type project_id: str
    """      
    value = kwargs['jobs']['export_customers']['extract_job']
    date = (get_yesterday_date().strftime("%Y-%m-%d") if not date else
       format_date(date))
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

def format_date(input_date, format="%Y-%m-%d"):
    """Changes input date to a new format.

    :type input_date: str
    :param input_date: date string of format "%Y%m%d".

    :type format: str
    :param format: new format to port input date.

    :rtype: str
    :returns: date string in format `format`.
    """
    return datetime.datetime.strptime(input_date, "%Y%m%d").strftime(
        format)

def process_url_date(date):
    """Gets the variable ``date`` from URL.

    :type date: str 
    :param date: date to process. 

    :raises: `ValueError` if ``date`` is not in format "%Y%m%d" and is
             not null.

    :rtype: str
    :returns: `None` is `date` is empty or a string representation of date
    """
    # if ``date`` is defined then it was sent as parameter in the URL request
    if date:
        try:
            datetime.datetime.strptime(date, "%Y%m%d")
        except ValueError:
            raise
    return date

def process_input_items(args):
    """Process input items to prepare for recommendation.

    :type args: dict
    :param args:
      :type args.browsed: str 
      :param args.browsed: str of items that were navigated by current
                           customer in a format like sku0,sku1,sku2.

      :type args.basket: str 
      :param args.basket: str of items comma separated corresponding to
                          products added to basket.

      :type args.purchased: str
      :param args.purchased: str of items comma separated corresponding to
                             products purchased.

    :rtype: dict
    :returns: dict of each item and total score interaction.  
    """
    return sum([Counter({sku: value * SCORES[k] for sku, value in
        Counter(args[k].split(',')).items()}) or Counter() for k in
        set(SCORES.keys()) & set(args.keys())], Counter())

class SkuModel(ndb.Model):
    @classmethod
    def _get_kind(cls):
        return config['recos']['kind']
    items = ndb.StringProperty(repeated=True)
    scores = ndb.FloatProperty(repeated=True)

def process_recommendations(entities, scores, n=10):
    """Process items and scores from entities retrieved from Datastore, combine
    them up and sorts top n recommendations. 

    :type entities: list of `ndb.Model`
    :param entities: entities retrieved from Datastore, following expected
                     pattern of ndb.Model(key=Key('kind', 'sku0'),
                     items=['sku1', 'sku2'], scores=[0.1, 0.83])

    :type scores: dict
    :param scores: each key corresponds to a sku and the value is the score
                   we observed our customer had with given sku, such as
                   {'sku0': 2.5}.

    :type n: int
    :param n: returns ``n`` largest scores from list of recommendations.

    :rtype: list
    :returns: list with top skus to recommend.
    """
    t0 = time.time()
    r = sum([Counter({e.items[i]: e.scores[i] * scores[e.key.id()]
        for i in range(len(e.items))}) for e in entities], Counter()).items()
    time_build_recos = time.time() - t0
    t0 = time.time()
    heapq.heapify(r)
    return {'result': [{"item": k, "score": v} for k, v in heapq.nlargest(
        n, r, key= lambda x: x[1])], 'statistics2': {'time_build_recos': time_build_recos, 'time_sort_recos': time.time() - t0}}

def cy_process_recommendations(entities, scores, n=10):
    """Uses the Cython implementation to aggregate results and then we use this
    method to sort top n recommendations. This is necessary to improve
    considerably performance to retrive top n results.

    
    :type entities: list of dicts. 
    :param entities: list with entities information retrieved as a dict
                     following the format [{"id": "sku0",
                     "items": ["sku0", "sku1"], "scores": [0.1, 0.2]}]

    :type scores: dict
    :param scores: each key corresponds to a sku and the value is the score
                   we observed our customer had with given sku, such as
                   {'sku0': 2.5}.

    :type n: int
    :param n: returns ``n`` largest scores from list of recommendations.

    :rtype: list
    :returns: list with top skus to recommend.
    """
    r = c_funcs.cy_aggregate_scores(entitie, scores, n)
    heapq.heapify(r)
    return {'result': [{"item": k, "score": v} for k, v in heapq.nlargest(
        n, r, key= lambda x: x[1])]}

 
