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
                 "project_id": "project where to run job",
                 "cluster_name": "cluster where to run job",
                 "zone": "valid zone to run the job",
                 "create_cluster": {
                     "master_type": "GCE computing instance",
                     "worker_num_instances": "# of workers",
                     "worker_type": "GCE computing instance"                
                 },
                 "pyspark_job":{
                     "bucket": "bucket where py files are saved",
                     "py_files": ["python files to feed the job"],
                     "main_file": "name of main py files to run",
                     "default_args": ["general arguments such as where to 
                                      read source files from, intermediary
                                      files, results and so on. It's appended
                                      to arguments also passed as parameters
                                      in the URL request"]

                 }
               },
             "dataflow_export": {
                "dataflow_service": "service name that is responsible for the
                                     dataflow exportage to datastore"   
             }
          },
          "general": {
              "project_id": "mains project id",
              "dataflow_service": "which service "
          }
        }
