## import all the modules
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLExportInstanceOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

args = { 'owner' : 'developer'}
project_id = "arboreal-avatar-381316"
EXPORT_URI = "gs://datasources_01_02/customers"
SQL_QUERY = "SELECT * from apps_db.customers"
instance_name = "mysqlsource"

export_body = {
    "exportContext": {
        "fileType": "csv",
        "uri": EXPORT_URI,
        "csvExportOptions":{
            "selectQuery": SQL_QUERY
        }
    }
}


## define the DAG
with DAG(
    dag_id = "gcstobq_pipeline",
    start_date = days_ago(1),
    schedule_interval="30 5 * * *",
    default_args = args,
) as dag:
    
    task_1 = CloudSQLExportInstanceOperator(
        task_id = "export_task",
        project_id = project_id,
        body=export_body,
        instance=instance_name
    )

    task_2 = GoogleCloudStorageToBigQueryOperator(
        task_id = "gcs_to_bq",
        bucket = "datasources_01_02",
        source_objects = ["customers"],
        destination_project_dataset_table = "bq_dataset.customers",
        schema_fields = [
            {"name" : "id", "type" : "INTEGER", "mode" : "NULLABLE"},
            {"name" : "name", "type" : "STRING", "mode" : "NULLABLE"},
            {"name" : "email", "type" : "STRING", "mode" : "NULLABLE"},
            {"name" : "city", "type" : "STRING", "mode" : "NULLABLE"},
            {"name" : "state", "type" : "STRING", "mode" : "NULLABLE"},
            {"name" : "zip", "type" : "INTEGER", "mode" : "NULLABLE"}
        ],
        write_disposition='WRITE_TRUNCATE',
        allow_quoted_newlines = True
    )

    task_3 = BigQueryOperator(
        task_id = "bqraw_bqinsight",
        sql = "select count(*) as customer_count from bq_dataset.customers",
        destination_dataset_table = "bq_dataset.customer_insight",
        write_disposition='WRITE_TRUNCATE',
        create_disposition = "CREATE_IF_NEEDED",
        use_legacy_sql = False,
        priority = "BATCH"
    )

    task_1 >> task_2 >> task_3
