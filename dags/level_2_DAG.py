# import all modules, libraries
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

# variable section
PROJECT_ID = "western-glazing-431109-b3"
REGION = "us-east1"
CLUSTER_NAME = "airflow-prod"
DAGS_BUCKET = "us-central1-composer-demo-29692111-bucket"
GCS_FILE_URI_1 = f"gs://{DAGS_BUCKET}/data/job-1.py"
GCS_FILE_URI_2 = f"gs://{DAGS_BUCKET}/data/job-2.py"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 50},
    },
}
PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_FILE_URI_1},
}
PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_FILE_URI_2},
}

ARGS = {
    "owner" : "shaik saidhul",
    "start_date" : datetime(2024,9,17),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=2)
}

# define the dag
with DAG(
    "level_2_dag",
    schedule_interval = "30 5 * * *",
    default_args = ARGS
) as dag :
    
# define the Tasks

    # create dataproc cluster 
    task_1 = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    # job-1 submission
    task_2 = DataprocSubmitJobOperator(
        task_id="pyspark_task_1", 
        job=PYSPARK_JOB_1, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    # job-2 submission
    task_3 = DataprocSubmitJobOperator(
        task_id="pyspark_task_2", 
        job=PYSPARK_JOB_2, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    # delete the cluster
    task_4 = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

# define dependency
task_1 >> task_2 >> task_3 >> task_4