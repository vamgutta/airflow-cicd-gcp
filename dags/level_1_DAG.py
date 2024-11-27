# import all modules
import airflow
from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# variable section
PROJECT_ID = "model-arcadia-440702-q1"
LOCATION = "US"
BUCKET = "random-bucket-1720"
OBJECT_NAME = ["departments.csv"]
DATASET_NAME_1 = "raw_ds"
DATASET_NAME_2 = "insight_ds"
TABLE_NAME_1 = "emp_raw"
TABLE_NAME_2 = "dep_raw"
TABLE_NAME_3 = "empDep_in"
QUERY = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME_2}.{TABLE_NAME_3}` AS
SELECT
    e.EmployeeID,
    CONCAT(e.FirstName,".",e.LastName) AS FullName,
    e.Email,
    e.Salary,
    e.JoinDate,
    d.DepartmentID,
    d.DepartmentName,
    CAST(e.Salary AS INTEGER) * 0.01 as EmpTax
FROM
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_1}` e
LEFT JOIN
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}` d
ON e.DepartmentID = d.DepartmentID
WHERE e.EmployeeID is not null
"""

ARGS = {
    "owner" : "shaik saidhul",
    "depends_on_past" : False,
    "start_date" : days_ago(1),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=1),
    "email_on_failure": True,
    "email_on_retry": True,
    "email": ["saidhuljohny@gmail.com"],
    "execution_timeout" : timedelta(minutes=15)
}

# define the dag
with DAG(
    "LEVEL_1_DAG",
    schedule_interval = "0 5 * * *",
    description="DAG to load data from GCS to BigQuery and create an enriched employee table",
    default_args = ARGS,
    tags = ["gcs", "bq", "etl", "data engineering"],
) as dag:
    
# define the tasks

    load_emp_csv = GCSToBigQueryOperator(
        task_id="load_emp_csv",
        bucket=BUCKET,
        source_objects=["employee.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_1}",
        schema_fields=[
            {"name": "EmployeeID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "Salary", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "JoinDate", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    load_department_data = GCSToBigQueryOperator(
        task_id="load_department_data",
        bucket=BUCKET,
        source_objects=OBJECT_NAME,
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_2}",
        schema_fields=[
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "DepartmentName", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE"
    )

    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )

# define the dependencies
(load_emp_csv,load_department_data) >> insert_query_job