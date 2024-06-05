# import all modules
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

# variable section
PROJECT_ID = "bold-streamer-423605-s6"
REGION = "us-east1"
CLUSTER_NAME = "dev-cluster"
JOB_FILE_URI = "gs://source-bkt-data/job_dataproc.py"
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOB_FILE_URI},
}
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    }
}


ARGS = {
    "owner" : "shaik saidhul",
    "start_date" : datetime(2024,6,5),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=2)
}


# define the dag
with DAG(
    "level_2_dag",
    schedule_interval = "0 15 * * *",
    default_args = ARGS
) as dag:
    
# define the Tasks

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )
    
# define the dependencies
create_cluster >> pyspark_task >> delete_cluster