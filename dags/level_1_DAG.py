# import modules
import airflow
from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# variable section
PROJECT_ID = "bold-streamer-423605-s6"
BUCKET = "source-bkt-data"
DATASET_NAME_1 = "source_dataset"
DATASET_NAME_2 = "insight_dataset"
TABLE_NAME_1 = "emp_raw"
TABLE_NAME_2 = "dep_raw"
TABLE_NAME_3 = "empDep_in"
LOCATION = "US"
QUERY = F"""
CREATE TABLE `{PROJECT_ID}.{DATASET_NAME_2}.{TABLE_NAME_3}` AS
SELECT 
  e.EmployeeID,
  CONCAT(e.FirstName," ",e.LastName) AS FullName,
  e.Email,
  e.Salary,
  e.JoinDate,
  d.DepartmentID,
  d.DepartmentName,
  cast(e.Salary as integer)*0.01 as Tax
FROM 
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_1}` e
LEFT JOIN
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}` d
ON 
    e.DepartmentID = d.DepartmentID
"""


args = {
    "owner" : "developer",
    "start_date" : datetime(2024,5,21),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=2)
}


# define the dag
with DAG(
    "dag_level_1",
    schedule_interval = "30 5 * * *",
    default_args = args
) as dag:
    
# define the tasks
    task_1 = GCSToBigQueryOperator(
        task_id="emp_task",
        bucket=BUCKET,
        source_objects=["employee.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_1}",
        schema_fields=[
            {"name": "EmployeeID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Salary", "type": "STRING", "mode": "NULLABLE"},
            {"name": "JoinDate", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    task_2 = GCSToBigQueryOperator(
        task_id="dep_task",
        bucket=BUCKET,
        source_objects=["departments.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_2}",
        schema_fields=[
            {"name": "DepartmentID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentName", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    task_3 = BigQueryInsertJobOperator(
        task_id="emp_dep_task",
        configuration={
            "query": {
                "query": QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )

# define the dependency
(task_1,task_2) >> task_3