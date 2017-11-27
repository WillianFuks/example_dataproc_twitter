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
is prohibitive for large amounts of data. The main difference between this
module and `naive.py` is that here we'll work with the concept of Dataframes
and see how much of an improvement (if any) we attain with this approach."""


import operator
import math

from base import JobsBase
from pyspark.sql import SparkSession
from pyspark.sql import types as stypes
from pyspark.sql.context import SQLContext


class DFNaiveJob(JobsBase):
    """Implements the naive approach of the neighborhood algorithm. 'Naive'
    because it computes all correlations between the products each customer
    interacted with, resulting in BigO(nL^2) where L is the maximum value of
    products interacted among all customers. A difference here will be the
    usage of 'dataframes' whose implementation is supposed to improve
    Spark's performance.
    """
    def register_udfs(self, sess, sc):
        """Register UDFs to be used in SQL queries.

        :type sess: `pyspark.sql.SparkSession`
        :param sess: Session used in Spark for SQL queries.

        :type sc: `pyspark.SparkContext`
        :param sc: Spark Context to run Spark jobs.
        """ 
        sess.udf.register("SQUARED", self.squared, returnType=(
            stypes.ArrayType(stypes.StructType(
            fields=[stypes.StructField('sku0', stypes.StringType()),
            stypes.StructField('norm', stypes.FloatType())]))))

        sess.udf.register('INTERSECTIONS',self.process_intersections,
            returnType=stypes.ArrayType(stypes.StructType(fields=[
            stypes.StructField('sku0', stypes.StringType()),
            stypes.StructField('sku1', stypes.StringType()),
            stypes.StructField('cor', stypes.FloatType())])))

    @staticmethod
    def process_intersections(row):
        """Process all intersections between the items a given customer
        interacted with.

        :type row: list
        :param row: customers interactions, like [('sku0', 0.5), ('sku1', 1.)]

        :rtype r: list
        :returns r: list with all intersections of items, such as
                    [('sku0', 'sku1', 0.25), ('sku1', 'sku2', 0.5)] 
        """
        r = []
        for i in range(len(row)):
            for j in range(i + 1, len(row)):
                r.append((row[i][0], row[j][0], row[i][1] * row[j][1]))
        return r

    @staticmethod
    def squared(row):
        """Returns all items with squared score for each customer.

        :type row: list
        :param row: customers interactions, like [('sku0', 0.5), ('sku1', 1.)]

        :rtype: list
        :returns: items and their squared interaction for each customer, such
                  as [('sku0', 0.25), ('sku1', 1)]
        """
        return [(e[0], e[1] ** 2) for e in row]

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
        self.build_df_naive(sc, args)

    def build_df_naive(self, sc, args):
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
        spark = SparkSession(sc)
        data = spark.createDataFrame(sc.emptyRDD(),
            schema=self.load_users_schema())
        for day in range(args.days_init, args.days_end - 1, -1):
            formatted_day = self.get_formatted_date(day)
            inter_uri = args.inter_uri.format(formatted_day)

            data = data.union(spark.read.json(inter_uri,
                schema=self.load_users_schema()))
        
        self.register_udfs(spark, sc)
        data.createOrReplaceTempView('data')
        spark.sql(self.query_norms).createOrReplaceTempView('norms')
        spark.sql(self.query_similarities).createOrReplaceTempView(
            'similarities')
        spark.sql(self.query_results).write.json(args.neighbor_uri,
            compression='gzip', mode='overwrite')

    @property
    def query_norms(self):
        return """SELECT
                    norms.sku0 sku0,
                    SQRT(SUM(norms.norm)) norm
                  FROM(
                    SELECT
                      EXPLODE(SQUARED(interactions)) norms
                    FROM data
                    WHERE SIZE(interactions) BETWEEN 2 AND 20
                  )
                  GROUP BY 1
               """

    @property
    def query_similarities(self):
        return """SELECT
                    a.sku0 sku0,
                    a.sku1 sku1,
                    a.cor / (b.norm * c.norm) similarity
                    FROM(
                        SELECT
                          inter.sku0 sku0,
                          inter.sku1 sku1,
                          SUM(inter.cor) cor
                        FROM(
                          SELECT
                            EXPLODE(INTERSECTIONS(interactions)) inter
                          FROM data
                          WHERE SIZE(interactions) BETWEEN 2 AND 20
                          )
                        GROUP BY 1, 2
                        ) a
                  JOIN (
                    SELECT 
                      sku0,
                      norm
                    FROM norms
                  ) b
                  ON a.sku0 = b.sku0
                  JOIN (
                    SELECT 
                      sku0,
                      norm
                    FROM norms
                  ) c
                  ON a.sku1 = c.sku0
               """

    @property
    def query_results(self):
        return """
            SELECT
               sku0 as item,
               COLLECT_LIST(STRUCT(sku1 as item, similarity)) similarity_items
            FROM(
               SELECT
                 *
               FROM(
                 SELECT
                   sku0,
                   sku1,
                   similarity
                 FROM similarities
                 ) UNION ALL
                 (
                  SELECT
                    sku1 as sku0,
                    sku0 as sku1,
                    similarity
                  FROM similarities
                )
            )
            GROUP BY 1
            """
