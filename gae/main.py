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


"""Main module working as entry point for routing different jobs and
making recommendations."""


import utils
import base_utils
from config import config
from flask import Flask, request, jsonify
from factory import JobsFactory
from google.appengine.ext import ndb
import time


app = Flask(__name__)
jobs_factory = JobsFactory()


@app.route("/run_job/<job_name>/")
def run_job(job_name):
    """This method works as a central manager to choose which job to run
    and respective input parameters.

    :type job_name: str
    :param job_name: specifies which job to run.
    """
    scheduler = jobs_factory.factor_job(job_name)()
    return str(scheduler.run(request.args))


@app.route("/make_recommendation")
def make_reco():
    """Makes the final recommendations for customers. Receives as input all
    items a given customer interacted with such as browsed items, added to 
    basket and purchased ones. Returns a list of top selected recommendations.
    """
    t0 = time.time()
    scores = base_utils.process_input_items(request.args)
    keys = map(lambda x: ndb.Key(config['recos']['kind'], x),
        scores.keys())
    entities = [e for e in ndb.get_multi(keys) if e]
    if not entities:
        result = {'results': [], 'statistics':
            {'elapsed_time': time.time() - t0}}
        return jsonify(result)
    results = utils.process_recommendations(entities, scores,
        int(request.args.get('n', 10))) 
    results['statistics'] = {}
    results['statistics']['elapsed_time'] = time.time() - t0 
    return jsonify(results) 
