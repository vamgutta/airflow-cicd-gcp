# import the modules
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# variable section
PROJECT_ID = "western-glazing-431109-b3"
LOCATION = "US"
SOURCE_BUCKET = "source-bucket-airflow-dags"
DATASET_NAME_1 = "raw_ds"
DATASET_NAME_2 = "insight_ds"
TABLE_NAME_1 = "emp_raw"
TABLE_NAME_2 = "dep_raw"
TABLE_NAME_3 = "empDep_in"
ARGS = {
    "owner" : "shaik saidhul",
    "start_date" : datetime(2024,9,17),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=2)
}
INSERT_ROWS_QUERY = f"""
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
INNER JOIN
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}` d
ON 
    e.DepartmentID = d.DepartmentID
"""

# define the dag
with DAG (
    "level_1_dag_bq",
    schedule_interval = "30 5 * * *",
    default_args = ARGS
) as dag:
    
# define the tasks
    task_1 = GCSToBigQueryOperator(
        task_id="emp_task",
        bucket=SOURCE_BUCKET,
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
        task_id="dept_task",
        bucket=SOURCE_BUCKET,
        source_objects=["departments.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_2}",
        schema_fields=[
            {"name": "DepartmentID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentName", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    task_3 = BigQueryInsertJobOperator(
        task_id="empDep_task",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )

# define the dependency
(task_1,task_2) >> task_3