# import all modules
import airflow
from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

# varibale section
PROJECT_ID = "caramel-aria-471603-r0"
REGION = "australia-southeast1"
CLUSTER_NAME = "dev-cluster"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 32},
    },
}

GCS_JOB_FILE = "gs://test-bkt-123123123/pyspark_job.py"
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE},
}

ARGS = {
    "owner" : "vamsi krishna",
    "depends_on_past" : False,
    "start_date" : days_ago(1),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=1),
    "email_on_failure": True,
    "email_on_retry": True,
    "email": ["vamsi201988@gmail.com"],
    "execution_timeout": timedelta(minutes=30),
    "catchup": False,
}

# define the dag
with DAG(
    "LEVEL_2_DAG",
    schedule_interval = "0 5 * * *",
    description="DAG to create a Dataproc cluster, run a PySpark job, and delete the cluster",
    default_args = ARGS,
    tags=["dataproc", "pyspark", "etl", "data_engineering"],
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

# define the dependency
create_cluster >> pyspark_task >> delete_cluster
