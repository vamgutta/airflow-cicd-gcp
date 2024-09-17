# import all modules, libraries
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator


# variable section
PROJECT_ID = "western-glazing-431109-b3"
DAGS_BUCKET = "us-central1-composer-demo-29692111-bucket"
GCS_PYTHON_SCRIPT_1 = f"gs://{DAGS_BUCKET}/data/beam-job-1.py" 
GCS_PYTHON_SCRIPT_2 = f"gs://{DAGS_BUCKET}/data/beam-job-2.py" 
GCS_OUTPUT = f"gs://{DAGS_BUCKET}/data/temp/"
REGION = "us-central1"
ARGS = {
    "owner" : "shaik saidhul",
    "start_date" : datetime(2024,9,17),
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
    schedule_interval = "30 17 * * *",
    default_args = ARGS
) as dag :
    
# define the tasks

    task_1 = DataFlowPythonOperator(
        task_id = "beam-job-1",
        py_file = GCS_PYTHON_SCRIPT_1
    )

    task_2 = DataFlowPythonOperator(
        task_id = "beam-job-2",
        py_file = GCS_PYTHON_SCRIPT_2
    )


# define the dependency
(task_1,task_2)