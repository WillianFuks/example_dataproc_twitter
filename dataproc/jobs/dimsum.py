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
import random
import time

from dataproc.jobs.base import JobsBase
from pyspark.sql import SparkSession
from pyspark.sql import types as stypes

class DimSumJob(JobsBase):
    """Implements the DIMSUM algorithm to solve the neighobrhood approach with
    complexity in shuffle size (mapping phase) given by O(nlog(n)L / (s * A))
    where ``L`` is the maximum value of interactions a given customer had with
    items, ``A`` is the smallest value of score present in all interactions and
    ``s`` is a value of threshold that exchanges compute-resources to precision
    in results. A threshold of 0.1 guarantees that values above this level
    converges to real value with 20% relative error. As ``s`` approaches 0
    results gets closer to real value (at the expense of also having
    brute-force complexity).
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

          :type args.threshold: float
          :param args.threshold: threshold on dimsum algorithm for trade-off
                                 between quality and cost.

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
        self.build_dimsum(sc, args)


    def build_dimsum(self, sc, args):
        """Builds dimsum approach for computing cosine similarities between
        columns of a matrix.
        
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
        
        pq_b = self._broadcast_pq(sc, data, args.threshold)
        data = (data.flatMap(lambda x: self._run_DIMSUM(x[1], pq_b))
                   .reduceByKey(operator.add))
        print("\n\nTIME ELAPSED: ", time.time() - t0)
        print('AND NOW WE SAVE')
        self.save_neighbor_matrix(args.neighbor_uri, data)


    def _run_DIMSUM(self, row, pq_b):
        """Implements DIMSUM as describe here:
        
        http://arxiv.org/abs/1304.1467

        :type row: list
        :param row: list with values (user, [(sku, score)...])

        :rtype: list
        :returns: similarities between skus in the form [(sku0, sku1, similarity)]
        """
        for i in range(len(row)):
            if random.random() < pq_b.value[row[i][0]][0]:
                for j in range(i + 1, len(row)):
                    if random.random() < pq_b.value[row[j][0]][0]:
                        value_i = row[i][1] / pq_b.value[row[i][0]][1]
                        value_j = row[j][1] / pq_b.value[row[j][0]][1]
                        key = ((row[i][0], row[j][0]) if row[i][0] < row[j][0]
                               else (row[j][0], row[i][0]))
                        yield (key, value_i * value_j)


    def _broadcast_pq(self, sc, data, threshold):
        """Builds and broadcast probability ``p`` value and factor ``q`` for
        each sku.

        :type data: `spark.RDD`
        :param data: RDD with values (user, (sku, score)).

        :type threshold: float
        :param threshold: all similarities above this value will be guaranteed
                          to converge to real value with relative error ``e``.

        :rtype: broadcasted dict
        :returns: dict sku -> (p, q) where p is defined as ``gamma / ||c||``
                  and ``q = min(gamma, ||c||)``.
        """
        norms = {sku: score for sku, score in
                  (data.flatMap(lambda x: self._process_scores(x)) 
                      .reduceByKey(operator.add) 
                      .map(lambda x: (x[0], math.sqrt(x[1]))) 
                      .collect())}

        gamma = (math.sqrt(10 * math.log(len(norms)) / threshold) if threshold
                  > 1e-6 else math.inf)

        pq_b = sc.broadcast({sku: (gamma / value, min(gamma, value))
                             for sku, value in norms.items()})        
        return pq_b


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
