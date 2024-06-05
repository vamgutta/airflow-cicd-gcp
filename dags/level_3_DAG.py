# import modules
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

# variables
GCS_PYTHON_SCRIPT = "gs://source-bkt-data/beam_job-1.py"
PROJECT_ID = "bold-streamer-423605-s6"
REGION = "us-central1"

ARGS = {
    "owner" : "shaik saidhul",
    "start_date" : datetime(2024,6,5),
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
    "level_3_dag",
    schedule_interval = "0 15 * * *",
    deafult_args = ARGS
) as dag:

# define the tasks
    task_1 = DataFlowPythonOperator(
        task_id = "level_3_dag",
        py_file=GCS_PYTHON_SCRIPT
    )

# define the dep
task_1