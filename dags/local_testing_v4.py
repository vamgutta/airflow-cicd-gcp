from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import storage
from datetime import datetime, timedelta, timezone
import requests
import json
import logging
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

API_URL = "https://dummyjson.com/posts"
GCS_BUCKET_NAME = "australia-southeast1-core-d-9c7b10ab-bucket"
POSTGRES_CONN_ID = 'pg_conn'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

logger = logging.getLogger("api_to_gcs_logger")
logger.setLevel(logging.INFO)

def fetch_and_upload_to_gcs(**kwargs):
    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()

        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        gcs_filename = f"api_data/api_data_{timestamp}.json"
        blob = bucket.blob(gcs_filename)
        blob.upload_from_string(json.dumps(data, indent=2), content_type='application/json')

        logger.info(f"Uploaded data to gs://{GCS_BUCKET_NAME}/{gcs_filename}")
        return gcs_filename

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def create_table_if_not_exists(pg_hook):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS posts (
        id INT PRIMARY KEY,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        views INT NOT NULL,
        user_id INT NOT NULL,
        tags JSONB NOT NULL,
        reactions JSONB NOT NULL
    );
    """
    pg_hook.run(create_table_query)
    logger.info("Ensured that table 'posts' exists.")

def load_json_to_postgres(**kwargs):
    ti = kwargs['ti']
    gcs_filename = ti.xcom_pull(task_ids='fetch_and_upload_to_gcs')

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_filename)
    posts_data = json.loads(blob.download_as_text()).get('posts', [])

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    create_table_if_not_exists(pg_hook)

    insert_query = """
        INSERT INTO posts (id, title, body, views, user_id, tags, reactions)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET title = EXCLUDED.title,
            body = EXCLUDED.body,
            views = EXCLUDED.views,
            user_id = EXCLUDED.user_id,
            tags = EXCLUDED.tags,
            reactions = EXCLUDED.reactions;
    """

    for post in posts_data:
        pg_hook.run(insert_query, parameters=(
            post['id'],
            post['title'],
            post['body'],
            post['views'],
            post['userId'], 
            json.dumps(post.get('tags', [])),
            json.dumps(post.get('reactions', {}))
        ))

    logger.info(f"Inserted/Updated {len(posts_data)} posts into Postgres.")

with DAG(
    'api_to_gcs_to_postgres_data_loading_dag',
    default_args=default_args,
    description='Fetch API data, upload to GCS, and load into Postgres',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['api', 'gcs', 'composer', 'postgres'],
) as dag:

    fetch_and_upload_task = PythonOperator(
        task_id='fetch_and_upload_to_gcs',
        python_callable=fetch_and_upload_to_gcs
    )

    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_json_to_postgres
    )

    Portfolio_Success_Mail = EmailOperator(
        task_id='Portfolio_Success_Mail',
        to=['ssurgam@adaqquare.com', 'saikumarsurgam@gmail.com', 'vkgutta@adaequare.com'],
        subject='Airflow - {{ dag.dag_id }} - {{ ds }} Status',
        html_content=(
            '<p>Hi All,<br><br>'
            'The DAG <b>{{ dag.dag_id }}</b> completed successfully.<br>'
            'Today: {{ ds }}<br>'
            'Time: {{ ts_nodash[-6:-4] }}:{{ ts_nodash[-4:-2] }}:{{ ts_nodash[-2:] }}<br><br>'
            'Regards,<br>Saikumar</p>'
        ),
        trigger_rule=TriggerRule.NONE_FAILED
    )

    fetch_and_upload_task >> load_to_postgres_task >> Portfolio_Success_Mail
