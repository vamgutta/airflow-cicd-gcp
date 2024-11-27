# Import required modules
import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from datetime import timedelta
import logging

# varibale section
PROJECT_ID = "model-arcadia-440702-q1"
REGION = "us-central1"
GCS_BUCKET = "test-bkt-123123123"
BEAM_PY_FILE = "gs://test-bkt-123123123/beam_jobb.py"
DATAFLOW_DEFAULT_OPTIONS = {
    "project": PROJECT_ID,
    "region": REGION,
    "tempLocation": f"gs://{GCS_BUCKET}/temp",
    "stagingLocation": f"gs://{GCS_BUCKET}/staging",
    "runner": "DataflowRunner",
}

ARGS = {
    "owner" : "shaik saidhul",
    "depends_on_past" : False,
    "start_date" : days_ago(1),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=1),
    "email_on_failure": True,
    "email_on_retry": True,
    "email": ["saidhuljohny@gmail.com"],
    "execution_timeout": timedelta(minutes=30),
    "catchup": False,
}

# Define the DAG
with DAG(
    dag_id="LEVEL_5_DAG",
    default_args=ARGS,
    schedule_interval="0 6 * * *",  # Runs daily at 6 AM
    description="DAG to run an Apache Beam job on Dataflow",
    catchup=False,
    max_active_runs=1,
    tags=["dataflow", "beam", "etl", "data_engineering"],
) as dag:
    
    # Task: Submit the Beam job to Dataflow
    submit_beam_job = DataflowCreatePythonJobOperator(
        task_id="run_beam_job",
        py_file=BEAM_PY_FILE,
        job_name="my-beam-job",
        options=DATAFLOW_DEFAULT_OPTIONS,
        location=REGION,
        gcp_conn_id="google_cloud_default",  
        poll_sleep=10,  # Polling interval to check job status
    )