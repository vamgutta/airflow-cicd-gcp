# import all modules
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# varibale section
PROJECT_ID = "sigma-rarity-423909-a7"
BUCKET = "bucket_2_789788945"
DATASET_NAME_1 = "raw_dataset"
DATASET_NAME_2 = "insight_dataset"
TABLE_NAME_1 = "empTable"
TABLE_NAME_2 = "depTable"
TABLE_NAME_3 = "empDepTable"
LOCATION = "US"
INSERT_ROWS_QUERY = f"""
CREATE TABLE `{PROJECT_ID}.{DATASET_NAME_2}.{TABLE_NAME_3}` 
AS
SELECT DISTINCT
	e.EmployeeID,
	CONCAT(e.FirstName," ",e.LastName) as FullName,
	e.Email,
	e.Salary,
	e.JoinDate,
	CAST(e.salary as integer)*0.01 as TaxComputed,
	d.DepartmentID,
	d.DepartmentName	
FROM
	`{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_1}` e
LEFT JOIN
	`{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}` d
ON
	e.DepartmentID=d.DepartmentID
"""
ARGS = {
    "owner" : "developer",
    "start_date" : datetime(2024,7,8),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=2)
}



# define the dag
with DAG(
    "level_1_dag",
    schedule_interval = "30 5 * * *",
    default_args = ARGS
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
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )

# define the dependency
(task_1,task_2) >> task_3