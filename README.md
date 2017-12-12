# Implementation of a Twitter Based Recommender System Fully Serverless Using GCP
This repository is used as source code for the medium post about implementing a Twitter recommender system using GCP.

We used 5 main tools from Google Cloud Platform: [Dataflow](https://cloud.google.com/dataflow/), [Dataproc](https://cloud.google.com/dataproc/), [AppEngine](https://cloud.google.com/appengine/), [Bigquery](https://cloud.google.com/bigquery/) and [Cloud Storage](https://cloud.google.com/storage/)

## AppEngine (GAE)
Basically it all starts in the `gae` folder. There you will find definitions of yaml files for both of our environments [standard](https://cloud.google.com/appengine/docs/standard/) and [flexible](https://cloud.google.com/appengine/docs/flexible/).
These are the yamls for the standard:
1. [cron.yaml](https://github.com/WillianFuks/example_dataproc_twitter/blob/master/gae/cron.yaml): defines the cron executions and their respective times to run.
2. [queue.yaml](https://github.com/WillianFuks/example_dataproc_twitter/blob/master/gae/queue.yaml): defines the rate at which queued tasks are executed.
3. [main.yaml](https://github.com/WillianFuks/example_dataproc_twitter/blob/master/gae/main.yaml): this is our main service that receives the cron requests and build scheduled tasks that are put into queue. In this project, we worked with [push queues](https://cloud.google.com/appengine/docs/standard/python/taskqueue/push/).
4. [worker.yaml](https://github.com/WillianFuks/example_dataproc_twitter/blob/master/gae/worker.yaml): this service is the one that executes our tasks in background. The cron triggers `main.py` that by turn calls `worker.py` that finally is responsible for sending tasks to the queue.

And finally this is our only service defined in flexible environment since it uses [Cython](http://cython.readthedocs.io/en/latest/) to speed up the computation of recommendations: 
1. [recommender.yaml](https://github.com/WillianFuks/example_dataproc_twitter/blob/master/gae/recommender.yaml): Makes final recommendations for customers.

We have two distinct requirements for GAE, `requirements.txt` is targeted to the flexible deployment. Notice also that we have a `config_template.py` that should be used as a guide to create the file `/gae/config.py`.

Here we have the following available crons:
* Exportage of customers data from BigQuery to Google Cloud Storage (GCS), defined under the route `/export_customers` in `worker.py`
* Creation of Dataproc Cluster, execution of DIMSUM algorithm, deletion of cluster and initializaiton of Dataflow pipeline in route `/dataproc_dimsum` also in `worker.py`.

### Unit Tests for GAE
Running unit testing in this project is quite simple, just install [nox](https://nox.readthedocs.io/en/latest/) by running:

```pip install nox-automation```

And then we have general unit tests and system unit tests (which makes real connections to BigQuery and so on).

To run regular unit tests just run:

```nox -s unit_gae```

And system tests:

```nox -s system_gae```

* Unit test requires [gcloud](https://cloud.google.com/sdk/downloads) to be installed in `/gcloud-sdk/` for simulating AppEngine server locally.
* System test requires a [service key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) with Editor access to BigQuery and GCS. 

## Dataproc
Right after the cron exporting data from BQ to GCS is executed it starts the Dataproc one.
The main subfolder in `/dataproc` is the `/jobs` where we have 3 main jobs:
1. `naive.py`: This is the naive implementation with **O(mL*L)**
2. `df_naive.py`: This was an attempt to implement naive approach but using [Spark Dataframes](https://spark.apache.org/docs/latest/sql-programming-guide.html). It failed ;)...
3. `dimsum.py`: DIMSUM implementation in PySpark following the work of [Rezah Zadeh](https://stanford.edu/~rezab/papers/dimsum.pdf).

There's no config file here as the setup is received as input by the cron job and the config in GAE folder. For instance (as in cron file):

```/run_job/run_dimsum/?url=/dataproc_dimsum&target=worker&extended_args=--days_init=30,--days_end=1,--threshold=0.1&force=no```

Important note: threshold is equivalent to the inverse of "gamma" value discussed in Rezah's paper, it basically asserts a threshold from where everything above this value is guaranteed to converge with relative bound error of 20%.

### Unit Tests for Dataproc
Running unit tests will require a local Spark Cluster; this Docker [image](https://github.com/jupyter/docker-stacks/tree/master/pyspark-notebook) is recommended. Just run it like:

```docker run -it --rm -p 8888:8888 -e GRANT_SUDO=yes --user root jupyter/pyspark-notebook bash```

Once there, you can clone the repository, install nox and run the tests with the command:

```nox -s system_dataproc```

## Dataflow
After Dataproc job is completed a task is schedule through the URL: `/prepare_datastore` which expects a Dataflow template to be available in a path specified in `gae/config.py`.

To create this template, make sure to install [apache-beam](https://beam.apache.org/) by running:

```
pip install apache-beam
pip install apache-beam[gcp]
```

After that, make sure to create a file in `/dataflow/config.py` using `/dataflow/config_template.py` as a guideline with values for your project in GCP.

All this being done just run:

```python build_datastore_template```

and the template will be saved where the config file specified it to.

### Unit Tests for Dataflow

Just make sure to have nox installed and run:

```nox -s system_dataflow```

Haivng a service account is also required.
