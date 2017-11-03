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
        """Gets data from datajet and transforms so that Marreco can read
        and use it properly. Each algorithm shall implement its own strategy
        """
        pass


    @abc.abstractmethod
    def build_marreco(self, sc, args):
        """Main method for each algorithm where results are calculated, such 
        as computing matrix similarities or top selling items.
        """
        pass


    def get_formatted_date(self, day):
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
