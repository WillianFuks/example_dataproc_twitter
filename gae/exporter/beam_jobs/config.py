import datetime


yesterday_str = (datetime.datetime.now() +
                 datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
output_str = "gs://lbanor/dataproc_example/{date}/".format(date=yesterday_str)

config={"table_id": "ga_sessions_*",
        "dataset_id": "40663402",
        "project_id": "dafiti-analytics",
        "runner": "DirectRunner",
        "output": yesterday_str,
        "output": output_str,
        "temp_location": "gs://lbanor/dataproc_example/tmp/",
        "job_name": "example-dataproc",
        "query_path": "../queries/customers_interactions.sql",
        "staging_location": "gs://lbanor/dataproc_example/staging/"
}
