# import all modules
import airflow
from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)

# varibale section
PROJECT_ID = "future-420421"
CLUSTER_NAME = "cluster-aec3"
REGION = "us-east1"
JOB_FILE_URI = "gs://practice-test-0557/job_dataproc.py"
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOB_FILE_URI},
}

args = {
    "owner" : "developer",
    "start_date" : datetime(2024,5,7),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=2)
}

# define the DAG
with DAG(
    "LEVEL_2_DAGg",
    schedule_interval = '30 7 * * *',
    default_args = args
) as dag:
    
# define the Tasks
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )


# define the dependency
start_cluster >> pyspark_task >> stop_cluster