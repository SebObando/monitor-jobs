# Deployment

1. Go to the monitor-jobs folder from the command line
2. Run docker build -t monitor-jobs-app .
3. Run docker run -p 80:80 monitor-jobs-app

# Usage

1. Do a GET request: 

```
curl http://0.0.0.0:80/execute_handle_warning_state/<build_id>
```

2. Do a POST request:
```
curl \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"organization": <organization>,
       "project": "<project>",
       "stage_ref_name": "<stage_ref_name>",
       "run_ids": "<run_ids>",
       "build_id": "<build_id>",
       "ado_token": "<ado_token>",
       "databricks_base_uri": "<databricks_base_uri>",
       "databricks_token": "<databricks_token">"
       }
   http://0.0.0.0:80/execute_monitor_jobs          
```