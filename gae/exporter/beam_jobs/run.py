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


"""This script runs different beam jobs declared at run time."""


import sys
import argparse
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def process_args(args):
    """Process input runtime parameters.

    :type args: list
    :param args: list with input parameters, like: ['--input=./file.csv']

    :rtype known_args: NamedTuple
    :returns known_args: These are the values we'll be using to build the
                         query and related steps.

    :rtype pipeline_args: list
    :returns pipeline_args: inputs not defined remains in this variable.
    """
    if not args:
        # we only import if it'll be used.
        from config import config


        args = ['--{name}={value}'.format(name=name, value=value) for
                 name, value in config.items()]
        raw_input(args)

    parser = argparse.ArgumentParser()
    parser.add_argument('--table_id',
                        dest='table_id',
                        default='ga_sessions_*',
                        type=str,
                        help=('Table from where to query for customers ',
                              'interactions'))

    parser.add_argument('--dataset_id',
                        dest='dataset_id',
                        type=str,
                        help=('Dataset from where to query for customers ',
                              'interactions'))

    parser.add_argument('--project_id',
                        dest='project_id',
                        type=str,
                        help=('Project ID where jobs will run'))

    parser.add_argument('--output',
                        dest='output',
                        type=str,
                        help=('Path where to save results.'))

    parser.add_argument('--query_path',
                        dest='query_path',
                        type=str,
                        help=('Path where query template is located.'))
                    
    parser.add_argument('--date',
                        dest='date',
                        type=str,
                        default=(datetime.datetime.now() +
                                  datetime.timedelta(
                                   days=-1)).strftime("%Y%m%d"))

    known_args, pipeline_args = parser.parse_known_args(args)
    pipeline_args.extend(['--project=%s' %(known_args.project_id)])
    pipeline_options = PipelineOptions(pipeline_args)

    return known_args, pipeline_options
 
def build_query(args):
    """Builds query to run against BigQuery so to retrieve 
    customers and their interactions with skus.

    :type args: Namedtuple
      :type project_id: str
      :param project_id: project id to be used in query.

      :type dataset_id: str
      :param dataset_id: dataset id to be used in query.

      :type table_id: str
      :param table_id: table_id from where to retrieve data.

      :type date: str
      :param date: date string of format `%Y%m%d`

      :type query_path: str
      :param query_path: where to retrieve query template from.

    :rtype query: str
    :returns: final query to run against BQ.
    """
    return open(args.query_path).read().format(project_id=args.project_id,
                                               dataset_id=args.dataset_id,
                                               table_id=args.table_id,
                                               date=args.date)

def export(argv=None):
    """Exports data from BigQuery to `output` destination.

    :type argv: list
    :param argv: list of input arguments to setup the job, such as 
                 `[--input=input.csv]`. If not specified, then reads input
                 from `config.py` file. This is so we can read config runtime
                 parameters in GAE.
    """
    known_args, pipeline_options = process_args(argv)
    raw_input(known_args)
    query = build_query(known_args)
    print query

    with beam.Pipeline(options=pipeline_options) as p:
        (p | 'Read From BQ' >> beam.io.Read(beam.io.BigQuerySource(query=query,
                                             use_standard_sql=True))
           | 'Write Results' >> beam.io.WriteToText(known_args.output))

if __name__ == '__main__':
    sys.exit(export(sys.argv[1:]))
