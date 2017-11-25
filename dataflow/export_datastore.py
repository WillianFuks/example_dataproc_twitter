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


"""Exports data from specified input to Datastore so we can make the final
recommendations."""


import sys
from operator import itemgetter
import argparse
import datetime
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
from google.cloud.proto.datastore.v1 import entity_pb2
from googledatastore.helper import add_properties, add_key_path


def process_pipe_options():
    """Process options from config file to setup beam job execution.

    :rtype: tuple
    :returns: args to setup the job and the PipelineOptions.
    """
    try:
        from config import config 
    except ImportError:
        raise ImportError("Please create a config file to run this job")

    print "AND CONFIG IS", config
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help=('Input from where to retrieve similarities'
                              ' scores.'))

    parser.add_argument('--kind',
                        dest='kind',
                        help=('kind to descrive datastore entities'))

    parser.add_argument('--project',
                        dest='project',
                        help=('Name of project where Datastore will be used'))

    parser.add_argument('--similarities_cap',
                        dest='sim_cap',
                        type=int,
                        help=('How many items and scores are allowed to be ',
                              'exported to DS'))

    args = ["--{}={}".format(k, v) for k, v in config.items()]
    args, pipe_args = parser.parse_known_args(args)
    pipe_args.extend(['--project={}'.format(args.project)])
    return (args, PipelineOptions(pipe_args))


class EntityWrapper(object):
    """Transform input data into specified Datastore Entity so we can save
    to the database. Input file is like: {"item": "sku0", "similarity_items":
    [{"item": "sku1", "similarity": 0.9}, {"item": "sku2", "similarity": 0.1}]}

    :type kind: str
    :param kind: kind that specifieds Datastore Entity

    :type sim_cap: int
    :param sim_cap: how many similarities scores and items are permitted to
                    be exported to Datastore.
    """
    def __init__(self, kind, sim_cap):
        self._kind = kind
        self._sim_cap = sim_cap


    @property
    def sim_cap(self):
        """property definition to return either value of self._sim_cap or
        -1 if defined value is None"""
        return  self._sim_cap or -1


    def make_entity(self, content):
        """Transform each row from input file to DS Entity.

        :type content: str
        :param content: one line of input file.

        :rtype: `entity_pb2.Entity`
        :returns: Entity created for each line of input file.
        """
        print "sim cap", type(self.sim_cap)
        sku_base, similarities = itemgetter('item', 'similarity_items')(
            json.loads(content))
        entity = entity_pb2.Entity()
        s_sim = [e for e in sorted(similarities,
            key=lambda x: x['similarity'], reverse=True)][:self.sim_cap]
        add_key_path(entity.key, self._kind, sku_base)
        add_properties(entity, {'items': [e['item'] for e in s_sim]},
            exclude_from_indexes = True)
        add_properties(entity, {'scores':
            [e['similarity'] for e in s_sim]},
            exclude_from_indexes=True)
        return entity
 
 
def main():
    args, pipe_args = process_pipe_options()
    with beam.Pipeline(options=pipe_args) as p:
        (p | 'Read Similarities' >> beam.io.ReadFromText(args.input)
           | "Create Entities" >> beam.Map(
               EntityWrapper(args.kind, args.sim_cap).make_entity)
           | "Write to DS" >> WriteToDatastore(args.project))
           #| 'Write Results' >> beam.io.WriteToText(args.kind))

if __name__ == '__main__':
    sys.exit(main())

