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


from scheduler import SchedulerJob


class JobsFactory(object):
    """Builds the specified job for GAE."""
    scheduler = SchedulerJob 
    def factor_job(self, job_name):
        """Selects one of the available jobs.

        :type job_name: str
        :param job_name: name of job to build.
        """
        if job_name in self.available_jobs:
            return self.scheduler 
        else:
            raise TypeError("Please choose a valid job name")

    @property
    def available_jobs(self):
        """Jobs currently defined to be used in GAE.

        :rtype: set
        :returns: set with available jobs that can be used in GAE.    
        """
        return set(['export_customers_from_bq', 'run_dimsum'])
