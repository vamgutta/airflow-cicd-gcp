from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json

#
JSON_FILE_PATH = 'data/cars.json'
GCS_BUCKET = 'australia-southeast1-core-d-9c7b10ab-bucket'
POSTGRES_CONN_ID = 'pg_conn'
GCS_CONN_ID = 'google_cloud_default'
TABLE_NAME = 'my_cars'

def load_json_to_postgres():
    gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
    file_bytes = gcs_hook.download(bucket_name=GCS_BUCKET, object_name=JSON_FILE_PATH)
    data = json.loads(file_bytes.decode('utf-8'))

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            car_id SERIAL PRIMARY KEY,
            brand VARCHAR(100),
            model VARCHAR(100),
            year INT,
            price NUMERIC
        );
    """)
    conn.commit()

    insert_query = f"""
        INSERT INTO {TABLE_NAME} (car_id, brand, model, year, price)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (car_id) DO NOTHING;
    """
    for record in data:
        cursor.execute(insert_query, (
            record['car_id'],
            record['brand'],
            record['model'],
            record['year'],
            record['price'],
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(data)} records from GCS into {TABLE_NAME}")

with DAG(
    dag_id='GitHub_Sai',
    start_date=datetime(2025, 10, 29),
    schedule_interval=None,  
    catchup=False,
    tags=['gcs', 'postgres', 'json', 'load'],
) as dag:

    load_task = PythonOperator(
        task_id='load_json_to_postgres_task',
        python_callable=load_json_to_postgres,
    )

    load_task

