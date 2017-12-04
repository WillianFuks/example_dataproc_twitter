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


"""Main module to build the Flexible Environment code so we are able to 
make recommendations in a viable time."""


import time
import base_utils

from config import config
from flask import Flask, request, jsonify
from connector.gcp import GCPService


gcp_service = GCPService()
app = Flask(__name__)


@app.route("/make_recommendation")
def make_reco():
    """Makes the final recommendations for customers. Receives as input all
    items a given customer interacted with such as browsed items, added to 
    basket and purchased ones. Returns a list of top selected recommendations.
    """
    try:
        t0 = time.time()
        weights = base_utils.process_input_items(request.args)
        entities = [{"id": e.key.name, "items": e.get('items'),
            "scores": e.get('scores')} for e in
            gcp_service.datastore.get_keys(config['recos']['kind'], 
            weights.keys()) if e]
        if not entities:
            result = {'results': [], 'elapsed_time': time.time() - t0}
            return jsonify(result)
        time_get_entities = time.time() - t0
        t00 = time.time()
        results = base_utils.cy_process_recommendations(entities, weights,
            int(request.args.get('n', 10))) 
 
        time_process_recos = time.time() - t00
        total_time = time.time() - t0    
        results['statistics'] = {}
        results['statistics']['elapsed_time'] = total_time
        results['statistics']['time_get_entities'] = time_get_entities
        results['statistics']['time_process_recos'] = time_process_recos
        return jsonify(results) 
    except Exception as err:
        return str(err)
