config={"jobs":{
            "query_job": {
                "source": {
                    "table_id": "table name",
                    "dataset_id": "dataset id",
                    "project_id": "GCP project id",
                    "query_path": "path/to/query/location/query.sql"
                 },
                 "destination": {
                    "table_id": "destination_table_name",
                    "dataset_id": "destination dataset name",
                    "project_id": "GCP project id"
                 }

            },
            "extract_job": {
                "table_id": "table name that'll be extract",
                "dataset_id": "dataset from where to get table",
                "project_id": "GCP project id",
                "output": "Cloud storage URI",
                "format": "CSV",
                "compression": "GZIP"
            }
        },
       "general": {
           "project_id": "Main GCP project id"
       }
      }
