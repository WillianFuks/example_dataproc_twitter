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


"""This module contains mainly utilities used in the recommender app. This
is done so we can avoid the importation of ``google.appengine.ext.ndb`` in
flexible environment."""


import heapq
from collections import Counter
import time
import os


try:
    import cythonized.c_funcs as c_funcs
except ImportError:
    # this is done so we can use this module both in standard and flexible
    # environment (as in standard the sandboxed environment will remove 
    # the cython modules from sys.path
    if not os.path.isdir(os.path.join(os.path.dirname(__file__),
        "cythonized")):
        raise ImportError("Module cythonized not found")

SCORES = {'browsed': 0.5, 'basket': 2., 'purchased': 6.}

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
    return dict(sum([Counter({sku: value * SCORES[k] for sku, value in
        Counter(args[k].split(',')).items()}) or Counter() for k in
        set(SCORES.keys()) & set(args.keys())], Counter()))


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
    r = c_funcs.cy_aggregate_scores(entities, scores, n)
    heapq.heapify(r)
    return {'result': [{"item": k, "score": v} for k, v in heapq.nlargest(
        n, r, key= lambda x: x[1])]}
