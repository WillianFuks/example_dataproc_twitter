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


"""Implements a naive approach to the neighborhood approach, i.e, computes
all correlations that there is between all products a given customer
interacted. This leads to quadratic complexity times whose processing time
is prohibitive for large amounts of data"""


import operator
import math
import time

from base import JobsBase
from pyspark.sql import SparkSession
from pyspark.sql import types as stypes

class NaiveJob(JobsBase):
    """Implements the naive approach of the neighborhood algorithm. 'Naive'
    because it computes all correlations between the products each customer
    interacted with, resulting in BigO(nL^2) where L is the maximum value of
    products interacted among all customers.
    """
    def run(self, sc, args):
        """Main method to build the algorithm, its specifications and results.

        :type sc: `pyspark.SparkContext`
        :param sc: context to run spark jobs against.

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
        self.transform_data(sc, args)
        self.build_naive(sc, args)


    def build_naive(self, sc, args):
        """Builds naive approach of neighborhood algorithm whose BigO is 
        proportional to nL^2 where n is the number of users being evaluated
        and L is the highest observed value of interactions between products
        that a given customer had in training data.
        
        :type sc: `pyspark.SparkContext`
        :param sc: spark context to run spark jobs.

        :type args: namedtuple
        :param args:
          :type days_init: int
          :param days_init: which date time that will be used for reading data
        		    with intermediary daily results.
          
          :type args.days_end: int
          :param args.days_end: until what file to read input data.
        
          :type args.inter_uri: str
          :param args.inter_uri: URI where intermediary results should be read from
        
          :type args.neighbor_uri: str
          :param args.neighbor_uri: where to save final marreco matrix (similarity
        		      and user_sku_score matrix).
          :type args.inter_uri: str
          :param args.inter_uri: URI for where to save intermediary results.

          :type args.users_matrix_uri: str
          :param args.users_matrix_uri: URI for where to save matrix of users
                                        and their interacted skus.
        """
        t0 = time.time()
        print('AND NOW THE SHOW BEGINS ')
        spark = SparkSession(sc)
        data = sc.emptyRDD()
        for day in range(args.days_init, args.days_end - 1, -1):
            print('PROCESSING DAY: ', day)
            formatted_day = self.get_formatted_date(day)
            inter_uri = args.inter_uri.format(formatted_day)

            data = data.union(spark.read.json(inter_uri,
                schema=self.load_users_schema()).rdd)

        print('OK DATA IS DONE!')
        data = (data.reduceByKey(operator.add)
            .flatMap(lambda x: self.aggregate_skus(x))
            .filter(lambda x: len(x[1]) > 1 and len(x[1]) <= 100))
        print('AND NOW PREPARING FOR NORMS')
        norms = self._broadcast_norms(sc, data)
        print('NORMS IS COMPUTED!')
        data = (data
            .flatMap(lambda x: self.process_intersections(x, norms))
            .reduceByKey(operator.add))
        print('AND NOW WE SAVE')
        print('\n\nTIME TAKEN: ', time.time() - t0)
        self.save_neighbor_matrix(args.neighbor_uri, data)


    @staticmethod
    def process_intersections(row, norms):
        """Computes the intersections between items and scores. This results 
        in the numerator of the cosine equation, given by:
        sum{i=0 to N}(a_i * b_i) where ``N`` is the length of the vector.

        :type row: `pyspark.RDD`
        :param row: RDD with data like [user, (item, score), (item, score)]

        :type norms: dict
        :param norms: dict whose keys corresponds to items customers
                      interacted and values their respective norm, like:
                      {sku0: 0.11}

        :rtype: tuple
        :returns: all inter-connections between items interacted by each
                  customer normalized by the norm of each item, like so:
                  ((sku0, sku1), 0.3)
        """
        for i in range(len(row[1])):
            for j in range(i + 1, len(row[1])):
                yield ((row[1][i][0], row[1][j][0]),
                    row[1][i][1] * row[1][j][1] / (
                        norms.value[row[1][i][0]] *
                        norms.value[row[1][j][0]]))


    def _broadcast_norms(self, sc, data):
        """Scans through ``data`` computing the norm of each item.

        :type sc: `pyspark.SparkContext`
        :param sc: 

        :type data: `pyspark.RDD`
        :param data: RDD with data of type [user, [(item, score),
                     (item, score)]

        :rtype norms: dict
        :returns norms: dict whose keys are items and values are the computed
                  norm ||.||_2 given ``data``.
        """
        norms = {sku: norm for sku, norm in (data.flatMap(
            lambda x: self._process_scores(x))
            .reduceByKey(operator.add)
            .map(lambda x: (x[0], math.sqrt(x[1])))
            .collect())}
        norms = sc.broadcast(norms)
        return norms


    @staticmethod
    def _process_scores(row):
        """After all user -> score aggregation is done, this method loops
        through each sku for a given user and yields its squared score so
        that we can compute the norm ``||c||`` for each sku column.

        :type row: list
        :param row: list of type [(user, (sku, score))]

        :rtype: tuple
        :returns: tuple of type (sku, (score ** 2))
         """
        for inner_row in row[1]:
            yield (inner_row[0], inner_row[1] ** 2)


