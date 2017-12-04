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

import nox


def session_unit_gae(session):
    """This session tests only the tests associated to google AppEngine
    folder. To run it, type `nox --session gae`
    """
    session.interpreter = 'python2.7'
    session.virtualenv_dirname = 'unit-gae'

    session.install('-r', 'tests/unit/data/gae/test_requirements.txt')
    session.install('pytest', 'pytest-cov', 'mock')

    if not os.path.isdir('/google-cloud-sdk/platform/google_appengine/'):
        raise RuntimeError("Please install gcloud components for app engine"
                           " in order to simulate an AppEngine environment "
                           " for testing")

    # we set ``gae/exporter`` in PYTHONPATH as well since this becomes
    # the root directory when App Engine starts the wsgi server
    session.env = {'PYTHONPATH': (':/google-cloud-sdk/platform/' 
        'google_appengine/:./:./gae/:/google-cloud-sdk/platform/'
        'google_appenigne/lib/yaml/lib')}

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

    session.install('-r', 'gae/standard_requirements.txt')
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


@nox.parametrize('py', ['2.7', '3.6'])
def session_system_dataproc(session, py):
    """For testing dataproc jobs the environment must have a spark cluster
    available.
    """
    session.interpreter = 'python{}'.format(py)
    session.virtualenv_dirname = 'system-dataproc-{}'.format(py)

    session.install('pytest', 'pytest-cov', 'mock', 'numpy')

    try:
        import pyspark
    except:
        raise RuntimeError("Please install pyspark and spark clusters to run "
                           "tests")

    # setups environment to be able to see Spark cluster
    session.env = {'PYTHONPATH': (':./'
                   ':/usr/local/spark/python'
                   ':/usr/local/spark/python/lib/py4j-0.10.4-src.zip')}

    session.run(
        'py.test',
        'tests/system/dataproc/',
        '--cov=.',
        '--cov-config=.coveragerc',
        '--cov-report=html')


def session_system_dataflow(session):
    """This test also requires a secret keys located at ``/key.json`` to
    connect to GCP and confirm Dataflow connected successfully to Datastore.
    """
    if not os.path.isfile('./dataflow/config.py'):
        raise RuntimeError(("Please make sure to build the config.py file ",
                            "in dataproc/config.py. You can use the template "
                            "as a guide."))
    session.interpreter = 'python2.7'
    session.virtualenv_dirname = 'system-dataflow'

    session.install('pytest', 'pytest-cov', 'mock', 'apache_beam')
    session.install('apache_beam[gcp]')
    session.install('google-cloud-datastore')
    session.install('six==1.10.0')

    session.env = {'PYTHONPATH': ':./:./dataflow/',
                   'GOOGLE_APPLICATION_CREDENTIALS': '/key.json'}

    session.run(
        'py.test',
        'tests/system/dataflow/test_build_datastore_template.py',
        '--cov=.',
        '--cov-config=.coveragerc',
        '--cov-report=html')
