config = {"input": "input path to read data from, such as a GSC path",
          "kind": "kind that resolves Entities",
          "similarities_cap": ("total similarities allowed for each key in ",
                               "Datastore. If 1000, then only saves 1000 ",
                               " recommendations for each sku."),
          "runner": "apache beam runner",
          "project": "Google Cloud Project where Datastore is located.",
          "staging_location": "For staging files used by Dataflow.",
          "temp_location": "For temporary files used by Dataflow.",
          "job_name": "Job name used by Dataflow."}







