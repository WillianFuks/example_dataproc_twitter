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


"""
Base Class for Algorithms in Spark.
"""


import abc
import datetime
import argparse
import operator
from collections import defaultdict

from pyspark.sql import SparkSession
from pyspark.sql import types as stypes
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError


class JobsBase(object):
    """Base Class to run Jobs against Spark"""
    def process_base_sysargs(self, args):
        """Process input arguments sent in sys args. This method works as a
        base parser for all algorithms.

        :type args: list
        :param args: list of arguments like ['--days_init=2', '--days_end=1']
        """
        parser = argparse.ArgumentParser()
    
        parser.add_argument('--days_init',
            dest='days_init',
            type=int,
            help=("Total amount of days to come back in time "
                  "from today's date."))
    
        parser.add_argument('--days_end',
            dest='days_end',
            type=int,
            help=("Total amount of days to come back in time "
                  "from today's date."))
    
        parser.add_argument('--source_uri',
            dest='source_uri',
            type=str,
            help=("URI template from where to read source "
                  "files from."))
    
        parser.add_argument('--inter_uri',
            dest='inter_uri',
            type=str,
            help=('URI for saving intermediary results.'))
    
        parser.add_argument('--threshold',
            dest='threshold',
            type=float,
            default=None,
            help=('Threshold for acceptable similarity relative'
                  ' error.'))
    
        parser.add_argument('--force',
            dest='force',
            type=str,
            help=('If ``yes`` then replace all files with new ones. '
                  'If ``no``, then no replacing happens.'))
    
        parser.add_argument('--neighbor_uri',
            dest='neighbor_uri',
            type=str,
            help=('where to save matrix of skus similarities'))
   
        parser.add_argument('--w_browse',
            dest='w_browse',
            type=float,
            default=0.5,
            help=('weight associated to browsing action score'))

        parser.add_argument('--w_purchase',
            dest='w_purchase',
            type=float,
            default=6.,
            help=('weight associated to purchasing action score'))

        parser.add_argument('--w_basket',
            dest='w_basket',
            type=float,
            default=2.,
            help=('weight associated to adding a product to basket'))

        args = parser.parse_args(args)
        return args

    def transform_data(self, sc, args):
        """Gets input data from GCS, transforms it and saves to intermediary
        resuts so that our algorithms can read and use it properly. 

        :type sc: `pyspark.SparkContext`
        :param sc: spark context where jobs are run against.

        :type args: namedtuple
        :param args: input values to setup job transformation.

          :type args.days_init: int
          :param args.days_init: total days to come back in time relative to
                                 today's date to set at what date the job
                                 should start scanning from.

          :type args.days_end: int
          :param args.days_end: just like ``days_init`` but sets the end limit
                                to when stop reading data.

          :type args.source_uri: str
          :param args.source_uri: URI where our input data is located in GCS.

          :type args.inter_uri: str
          :param args.inter_uri: URI where to save intermediary results.

          :type args.force: str
          :param args.force: if ``yes`` then overwrites intermediary results
                             that may already exist.

          :type args.neighbor_uri: str
          :param neighbor_uri: URI to where save resuts.

          :type args.w_browse: float
          :param args.w_browse: score associated to viewing a product
                                description page.

          :type args.w_basket: float
          :param args.w_basket: score associated to adding a product
                                to the basket.

          :type args.w_purchase: float
          :param args.w_purchase: score associated to buying the product.
        """
        spark = SparkSession(sc)
        for day in range(args.days_init, args.days_end - 1, -1):
            formatted_day = self.get_formatted_date(day)
            source_uri = args.source_uri.format(formatted_day)
            inter_uri = args.inter_uri.format(formatted_day)
            try:
                inter_data = spark.read.json(inter_uri,
                    schema = self.load_users_schema()).first()

                if args.force == 'yes' or not inter_data:
                    self.process_day_input(sc, source_uri, inter_uri, 
                        args, mode='overwrite')
            except (Py4JJavaError, AnalysisException):
                self.process_day_input(sc, source_uri, inter_uri, args)

    @staticmethod
    def load_users_schema():
        """Loads schema with data type [user, [(sku, score), (sku, score)]]

        :rtype: `pyspark.sql.type.StructType`
        :returns: schema speficiation for user -> (sku, score) data.
        """
        return stypes.StructType(fields=[
        	stypes.StructField("user", stypes.StringType()),
        	 stypes.StructField('interactions', stypes.ArrayType(
        	  stypes.StructType(fields=[stypes.StructField('item', 
        	   stypes.StringType()), stypes.StructField('score', 
        	    stypes.FloatType())])))])

    @abc.abstractmethod
    def run(self, sc, args):
        """Main method for each algorithm where results are calculated and
        exported to GCS.
        """
        raise NotImplementedError

    @staticmethod
    def get_formatted_date(day):
        """This method is used mainly to transform the input of ``days``
        into a string of type ``YYYY-MM-DD``

        :type day: int
        :param day: how many days in time to come back from today to make
                    the string transformation.

        :rtype: str
        :returns: formated date of today - day in format %Y-%m-%d
        """
        return (datetime.datetime.now() -
            datetime.timedelta(days=day)).strftime('%Y-%m-%d')

    def process_day_input(self, sc, source_uri, inter_uri, args, mode=None,
            compression='gzip'):
        """Reads data from source input, applies transformations and saves
        results in specified URI.

        :type sc: `pyspark.SparkContext`
        :param sc: spark context to run spark jobs.

        :type source_uri: str
        :param source_uri: Source URI from where to read intpu data.

        :type inter_uri: str
        :param inter_uri: Intermediary URI from where to save processed data.

        :type args: namedtuple
        :param args: arguments to setup job.
          :type args.w_browse: float
          :param args.w_browse: score related to viewing product description.

          :type args.w_basket: float
          :param args.w_basket: score related to adding product to basket.

          :type args.w_purchase: float
          :param args.w_purchase: score related to buying a product.

        :type mode: str
        :param mode: indicates how data should be saved. If ``None`` then
        	     throws error if file already exist. If ``overwrite`` then
        	     deletes previous file and saves a new one.

        :type compression: str
        :param compression: which extension to save the file. Defaults to
                            'gzip'.
        """
        (sc.textFile(source_uri)
         .zipWithIndex()
         .filter(lambda x: x[1] > 0)
         .map(lambda x: x[0].split(','))
         .map(lambda x: (x[0], (x[1], args.w_browse if x[2] == '1' else
             args.w_basket if x[2] == '2' else args.w_purchase)))
         .groupByKey().mapValues(list)
             .flatMap(lambda x: self.aggregate_skus(x))
         .toDF(schema=self.load_users_schema())
         .write.json(inter_uri, compression=compression, mode=mode))

    @staticmethod
    def aggregate_skus(row):
        """Aggregates skus from customers and their respective scores.

        :type row: list
        :param row: list having values [user, (sku, score)]

        :rtype: list
        :returns: `yield` on [user, (sku, sum(score))]
        """
        d = defaultdict(float)
        for inner_row in row[1]:
            d[inner_row[0]] += inner_row[1]
        yield (row[0], list(d.items()))

    def save_neighbor_matrix(self, neighbor_uri, data):
        """Turns similarities into the final neighborhood matrix. The schema
        for saving the matrix is like {sku0: [(sku1, similarity1)...]}

        :type neighbor_uri: str
        :param neighbor_uri: uri for where to save the matrix.
        
        :type data: RDD
        :param data: RDD with data like [((sku0, sku1), similarity)]
        """
        def duplicate_keys(row):
            """Builds the similarities between both the diagonals
            of the similarity matrix. In the DIMSUM algorithm, we just compute
            one of the diagonals. Here we will add the transpose of the matrix
            so Marreco can see all similarities between all skus.
        
            :type row: list
            :param row: data of type [(sku0, sku1), similarity]
        
            :rtype: list:
            :returns: skus and their transposed similarities, such as
                      [sku0, [sku1, s]], [sku1, [sku0, s]]
            """
            yield (row[0][0], [(row[0][1], row[1])])
            yield (row[0][1], [(row[0][0], row[1])])
        
        (data.flatMap(lambda x: duplicate_keys(x))
            .reduceByKey(operator.add)
            .toDF(schema=self.load_neighbor_schema())
            .write.json(neighbor_uri, compression='gzip', mode='overwrite'))

    def load_neighbor_schema(self):
        """Loads neighborhood schema for similarity matrix

        :rtype: `pyspark.sql.types.StructField`
        :returns: schema of type ["key", [("key", "value")]]
        """
        return stypes.StructType(fields=[
                stypes.StructField("item", stypes.StringType()),
                 stypes.StructField("similarity_items", stypes.ArrayType(
                  stypes.StructType(fields=[
                   stypes.StructField("item", stypes.StringType()),
                    stypes.StructField("similarity", stypes.FloatType())])))])
