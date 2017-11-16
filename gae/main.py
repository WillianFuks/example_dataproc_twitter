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


"""Main module working as entry point for routing different jobs."""


from flask import Flask, request
from jobs.factory import JobsFactory

app = Flask(__name__)
jobs_factory = JobsFactory()


@app.route("/run_job/<job_name>/")
def run_job(job_name):
    """This method works as a central manager to choose which job to run
    and respective input parameters.

    :type job_name: str
    :param job_name: specifies which job to run.
    """
    job = jobs_factory.factor_job(job_name)()
    job.run(request.args)
    return str(job)

