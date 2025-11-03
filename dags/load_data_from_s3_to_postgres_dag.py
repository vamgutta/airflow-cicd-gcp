from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import io

# Default args
default_args = {
    'owner': 'ashutosh',
    'start_date': datetime(2025, 10, 29),
    'retries': 2,                     # Number of retries
    'retry_delay': timedelta(minutes=5)  # Delay between retries
}

# Constants
S3_CONN_ID = 'aws_conn'
S3_BUCKET = 'adq-raw-data-files'
S3_KEY = 'customers_data.csv'
POSTGRES_CONN_ID = 'pg_conn'
TABLE_NAME = 'customers_data'
SCHEMA_NAME = 'adae_dataset'


# Task Functions
def test_postgres_connection():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT 1;")
        print(f"Postgres connection test result: {cur.fetchone()}")
    conn.close()


def extract_from_s3(**context):
    """Extract CSV file from S3 and return as JSON string via XCom."""
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    file_content = s3.read_key(key=S3_KEY, bucket_name=S3_BUCKET)
    df = pd.read_csv(io.StringIO(file_content))
    print(f" Extracted {len(df)} rows from S3.")
    return df.to_json()  # Push to XCom


def transform_data(**context):
    df_json = context['ti'].xcom_pull(task_ids='extract_from_s3')
    df = pd.read_json(io.StringIO(df_json))

    # Combine first & last names
    if 'First Name' in df.columns and 'Last Name' in df.columns:
        df['Full Name'] = df['First Name'] + ' ' + df['Last Name']
        df.drop(columns=['First Name', 'Last Name'], inplace=True)

    # Drop 'Phone 2' if exists
    if 'Phone 2' in df.columns:
        df.drop(columns=['Phone 2'], inplace=True)

    # Convert 'Subscription Date' to datetime
    if 'Subscription Date' in df.columns:
        if pd.api.types.is_numeric_dtype(df['Subscription Date']):
            # UNIX timestamp in seconds
            df['Subscription Date'] = pd.to_datetime(df['Subscription Date'], unit='s', errors='coerce')
        else:
            df['Subscription Date'] = pd.to_datetime(df['Subscription Date'], errors='coerce')

    # Ensure all datetime columns are proper Python datetime objects
    df['Subscription Date'] = df['Subscription Date'].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)

    print(f"Transformation complete. Columns now: {list(df.columns)}")
    print(df.head())

    # Push as records JSON
    return df.to_json(orient='records', date_format='iso')



def load_to_postgres(**context):
    """Load transformed data into Postgres."""
    df_json = context['ti'].xcom_pull(task_ids='transform_data')
    df = pd.read_json(io.StringIO(df_json), orient='records')

# No change in cursor.execute placeholders; Python datetime will map correctly


    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Ensure schema
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")
    conn.commit()

    # Create table (no First/Last Name columns now)
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

    # Upsert data
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


# DAG Definition
with DAG(
    dag_id='load_data_from_s3_to_postgres_dag',
    default_args=default_args,
    description='ETL pipeline: S3 → Transform → Postgres (modular version)',
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

    # Define DAG dependencies
    test_conn >> extract >> transform >> load
