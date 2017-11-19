config = {"jobs":{
             "export_customers": {
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
            "run_dimsum": {
                "create_cluster": {
                    "project_id": "project id",
                    "cluster_name": "name of dataproc cluster to build",
                    "region": "region where to create cluster",
                    "master_type": "computing instance type",
                    "worker_num_instances": "how many workers to create",
                    "worker_type": "computing instance for workers"                
                }
            }
         },
         "general": {
             "project_id": "Main GCP project id"
         }
      }
