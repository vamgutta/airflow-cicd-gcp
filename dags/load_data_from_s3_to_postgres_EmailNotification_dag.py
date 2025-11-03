from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.email import EmailOperator  # type: ignore
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import io

# Default args
default_args = {
    'owner': 'ashutosh',
    'start_date': datetime(2025, 10, 29),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,           # optional - notify if DAG fails
    'email': ['ashutoshsb164@gmail.com']
}

# Constants
S3_CONN_ID = 'aws_conn'
S3_BUCKET = 'adq-raw-data-files'
S3_KEY = 'customers_data.csv'
POSTGRES_CONN_ID = 'pg_conn'
TABLE_NAME = 'customers_data'
SCHEMA_NAME = 'adae_dataset'

def test_postgres_connection():
    """
    Test the connectivity to the Postgres database.
    Uses the PostgresHook to establish a connection and run a simple query (`SELECT 1;`) 
    to verify that the connection works. Prints the result.
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT 1;")
        print(f"Postgres connection test result: {cur.fetchone()}")
    conn.close()

def extract_from_s3(**context):
    """
    Extracts a CSV file from an S3 bucket and returns it as a JSON string via XCom.
    Parameters:
        **context: Airflow context dictionary (provided automatically)
    Returns:
        str: JSON representation of the extracted CSV data.
    """
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    file_content = s3.read_key(key=S3_KEY, bucket_name=S3_BUCKET)
    df = pd.read_csv(io.StringIO(file_content))
    print(f" Extracted {len(df)} rows from S3.")
    return df.to_json()


def transform_data(**context):
    """
    Transforms the extracted data:
        - Combines 'First Name' and 'Last Name' into 'Full Name'
        - Drops 'Phone 2' if it exists
        - Converts 'Subscription Date' to proper datetime objects
    Parameters:
        **context: Airflow context dictionary
    Returns:
        str: Transformed data in JSON format (records oriented).
    """
    df_json = context['ti'].xcom_pull(task_ids='extract_from_s3')
    df = pd.read_json(io.StringIO(df_json))

    if 'First Name' in df.columns and 'Last Name' in df.columns:
        df['Full Name'] = df['First Name'] + ' ' + df['Last Name']
        df.drop(columns=['First Name', 'Last Name'], inplace=True)

    if 'Phone 2' in df.columns:
        df.drop(columns=['Phone 2'], inplace=True)

    if 'Subscription Date' in df.columns:
        if pd.api.types.is_numeric_dtype(df['Subscription Date']):
            df['Subscription Date'] = pd.to_datetime(
                df['Subscription Date'], unit='s', errors='coerce')
        else:
            df['Subscription Date'] = pd.to_datetime(
                df['Subscription Date'], errors='coerce')

    df['Subscription Date'] = df['Subscription Date'].apply(
        lambda x: x.to_pydatetime() if pd.notnull(x) else None)

    print(f"Transformation complete. Columns now: {list(df.columns)}")
    print(df.head())
    return df.to_json(orient='records', date_format='iso')


def load_to_postgres(**context):
    """
    Loads transformed data into the Postgres database.
    - Creates schema if it does not exist
    - Creates table if it does not exist
    - Performs UPSERT (insert or update on conflict)
    Parameters:
        **context: Airflow context dictionary
    Returns:
        dict: Metadata about the loaded data (file name, schema, table, record count).
    """
    df_json = context['ti'].xcom_pull(task_ids='transform_data')
    df = pd.read_json(io.StringIO(df_json), orient='records')

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")
    conn.commit()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            "Customer Id" VARCHAR(50) PRIMARY KEY,
            "Full Name" VARCHAR(200),
            "Company" VARCHAR(150),
            "City" VARCHAR(100),
            "Country" VARCHAR(100),
            "Phone 1" VARCHAR(50),
            "Email" VARCHAR(150),
            "Subscription Date" TIMESTAMP,
            "Website" VARCHAR(150)
        );
    """)
    conn.commit()

    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
                "Customer Id", "Full Name", "Company", "City", "Country",
                "Phone 1", "Email", "Subscription Date", "Website"
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT ("Customer Id") DO UPDATE SET
                "Full Name" = EXCLUDED."Full Name",
                "Company" = EXCLUDED."Company",
                "City" = EXCLUDED."City",
                "Country" = EXCLUDED."Country",
                "Phone 1" = EXCLUDED."Phone 1",
                "Email" = EXCLUDED."Email",
                "Subscription Date" = EXCLUDED."Subscription Date",
                "Website" = EXCLUDED."Website";
        """, (
            row['Customer Id'], row['Full Name'], row['Company'],
            row['City'], row['Country'], row['Phone 1'],
            row['Email'], row['Subscription Date'], row['Website']
        ))
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} rows into {SCHEMA_NAME}.{TABLE_NAME}.")
    # Return metadata to XCom for email task
    return {
        "file_name": S3_KEY,
        "schema_name": SCHEMA_NAME,
        "table_name": TABLE_NAME,
        "record_count": len(df)
    }


def compose_email_body(**context):
    """Generate email body dynamically using XCom data."""
    load_result = context['ti'].xcom_pull(task_ids='load_to_postgres')
    file_name = load_result['file_name']
    schema_name = load_result['schema_name']
    table_name = load_result['table_name']
    record_count = load_result['record_count']

    email_body = f"""
    **DAG Execution Successful!**

    - File Loaded: {file_name}
    - Schema: {schema_name}
    - Table: {table_name}
    - Records Loaded: {record_count}

    DAG ID: load_data_from_s3_to_postgres_dag
    Execution Date: {context['ds']}
    """

    return email_body


# DAG Definition
with DAG(
    dag_id='load_data_from_s3_to_postgres_EmailNotification_dag',
    default_args=default_args,
    description='ETL pipeline: S3 → Transform → Postgres (with email notification)',
    schedule_interval=None,
    catchup=False,
) as dag:

    test_conn = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection,
    )

    extract = PythonOperator(
        task_id='extract_from_s3',
        python_callable=extract_from_s3,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,
    )

    # NEW TASK: Compose email body
    compose_email = PythonOperator(
        task_id='compose_email_body',
        python_callable=compose_email_body,
        provide_context=True,
    )

    # NEW TASK: Send email
    send_email = EmailOperator(
        task_id='send_success_email',
        to='ashutoshsb164@gmail.com',
        subject='Airflow DAG Success: Data Loaded into PostgreSQL',
        html_content="{{ ti.xcom_pull(task_ids='compose_email_body') }}",
    )

    # DAG dependencies
    test_conn >> extract >> transform >> load >> compose_email >> send_email
