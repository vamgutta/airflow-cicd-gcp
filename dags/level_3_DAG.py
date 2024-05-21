# import modules
import airflow
from airflow import DAG
from datetime import datetime,timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

# variables
PROJECT_ID = "bold-streamer-423605-s6"
REGION = "us-central1"
GCS_PYTHON_SCRIPT = "gs://source-bkt-data/beam_job-1.py"
args = {
    "owner" : "developer",
    "start_date" : datetime(2024,5,21),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=2),
    "dataflow_default_options" : {
        "project" : PROJECT_ID,
        "region" : REGION,
        "runner" : "DataflowRunner"
    }
}

# define the dag
with DAG(
    "LEVEL_3_DAG",
    schedule_interval = "30 5 * * *",
    default_args = args
) as dag:
    
# define the tasks
    task_1 = DataFlowPythonOperator(
        task_id = "beam_job_1",
        py_file = GCS_PYTHON_SCRIPT
    )

# define the dep
task_1 