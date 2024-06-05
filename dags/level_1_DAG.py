# import all modules
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# variable section
PROJECT_ID = "bold-streamer-423605-s6"
BUCKET = "source-bkt-data"
DATASET_NAME_1 = "source_dataset"
DATASET_NAME_2 = "insight_dataset"
LOCATION = "US"
TABLE_NAME_1 = "emp_raw"
TABLE_NAME_2 = "dep_raw"
TABLE_NAME_3 = "empDep_in"
QUERY = f"""
CREATE TABLE `{PROJECT_ID}.{DATASET_NAME_2}.{TABLE_NAME_3}` as
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
  e.DepartmentID=d.DepartmentID
"""

ARGS = {
    "owner" : "shaik saidhul",
    "start_date" : datetime(2024,6,5),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=3)
}


# define the dag
with DAG(
    "level_1_dag",
    schedule_interval = "0 15 * * *",
    deafult_args = ARGS
) as dag:

# define the tasks
    
    task_1 = GCSToBigQueryOperator(
        task_id="empTask",
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
        task_id="depTask",
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
        task_id="empDepTask",
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