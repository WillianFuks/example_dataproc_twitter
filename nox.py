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


import os


def session_unit_gae(session):
    """This session tests only the tests associated to google AppEngine
    folder. To run it, type `nox --session gae`
    """
    session.interpreter = 'python2.7'
    session.virtualenv_dirname = 'unit-gae'

    session.install('-r', 'gae/exporter/requirements.txt')
    session.install('pytest', 'pytest-cov', 'mock')

    if not os.path.isdir('/google-cloud-sdk/platform/google_appengine/'):
        raise RuntimeError("Please install gcloud components for app engine"
                           " in order to simulate an AppEngine environment "
                           " for testing")

    # we set ``gae/exporter`` in PYTHONPATH as well since this becomes
    # the root directory when App Engine starts the wsgi server
    session.env = {'PYTHONPATH': (':/google-cloud-sdk/platform/' 
                                  'google_appengine/:./:.gae/exporter/')}

    session.run(
        'py.test',
        'tests/unit/gae/',
        '--cov=.',
        '--cov-config=.coveragerc',
        '--cov-report=html')

def session_system_gae(session):
    """Runs integration tests. As this runs a real query against BigQuery,
    the environemnt must have ``GOOGLE_APPLICATION_CREDENTIALS`` set pointing
    to ``/key.json`` where the secrets service json must be located.
    """
    session.interpreter = 'python2.7'
    session.virtualenv_dirname = 'system-gae'

    session.install('-r', 'gae/exporter/requirements.txt')
    session.install('google-cloud-bigquery==0.27.0')

    session.install('pytest', 'pytest-cov', 'mock')

    if not os.path.isfile:
        raise RuntimeError("File /key.json not found. Please make sure "
                           "to create this file with the service credentials "
                           "in order to run the integration tests")

    session.env = {'PYTHONPATH': ':./',
                   'GOOGLE_APPLICATION_CREDENTIALS': '/key.json'}

    session.run(
        'py.test',
        'tests/system/gae/')


