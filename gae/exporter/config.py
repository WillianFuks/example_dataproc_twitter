config={"jobs":{
            "query_job": {
                "source": {
                    "table_id": "ga_sessions_*",
                    "dataset_id": "40663402",
                    "project_id": "dafiti-analytics",
                    "query_path": "queries/customers_interactions.sql"
                 },
                 "destination": {
                    "table_id": "dataproc_example",
                    "dataset_id": "simona",
                    "project_id": "dafiti-analytics"
                 }

            },
            "extract_job": {
                "table_id": "dataproc_example",
                "dataset_id": "simona",
                "project_id": "dafiti-analytics",
                "output": "gs://lbanor/dataproc_example/{date}/result.gz",
                "format": "CSV",
                "compression": "GZIP"
            }
        }
       }
