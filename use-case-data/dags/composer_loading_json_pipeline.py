from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import bigquery, storage
from datetime import datetime, timedelta
import json
import os

# default arguments for DAG object
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

# DAG definition
dag = DAG('composer_loading_json_pipeline',
          default_args=default_args,
          description='Load JSON files from GCS to BigQuery',
          schedule_interval=timedelta(days=1),
          start_date=datetime(2024, 1, 1),
          catchup=False)

# Define GCS bucket and folder names
BUCKET_NAME = 'airline_inbound_data'
JSON_FOLDER = 'loading/'
PROCESSED_JSON_FOLDER = 'loading/converted/'
ARCHIVE_FOLDER = 'archive/loading/'
BQ_PROJECT_NAME = 'sunlit-analyst-430409-b3'
BQ_DATASET_NAME = 'raw'
BQ_TABLE_NAME = 'loading_data'
PRECORE_PROJECT_NAME = 'sunlit-analyst-430409-b3'
PRECORE_DATASET_NAME = 'precore'
PRECORE_TABLE_NAME = 'loading_data'

# field mapping
field_mapping = {
    "Flight Month/Year": "Flight_Month_Year",
    "Airline": "Airline",
    "Flightnumber": "Flightnumber",
    "Departure Unit": "Departure_Unit",
    "Arrival Unit": "Arrival_Unit",
    "Flight Date": "Flight_Date",
    "Aircraft Type": "Aircraft_Type",
    "Leg": "Leg",
    "Booking Class CBASE": "Booking_Class_CBASE",
    "Bill of material": "Bill_of_material",
    "Price": "Price",
    "Invoice Quantity": "Invoice_Quantity",
    "Sales and  Services Invoice": "Sales_and_Services_Invoice",
    "UAA  Invoice": "UAA_Invoice"
}

# map headers between file and table
def map_fields(json_record):
    mapped_record = {}
    for json_field, bq_field in field_mapping.items():
        mapped_record[bq_field] = json_record.get(json_field, None)
    return mapped_record

# main preprocess method
def preprocess_json_files(bucket_name, input_folder, output_folder):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=input_folder)

    for blob in blobs:
        if blob.name.endswith('.json'):
            print(f"Processing file: {blob.name}")
            json_file_path = f'/tmp/{os.path.basename(blob.name)}'
            blob.download_to_filename(json_file_path)

            with open(json_file_path, 'r', encoding='utf-8') as f:
                records = json.load(f)

            processed_records = [map_fields(record) for record in records]
            processed_json_file_path = f'/tmp/processed_{os.path.basename(blob.name)}'

            with open(processed_json_file_path, 'w', encoding='utf-8') as f:
                for record in processed_records:
                    f.write(json.dumps(record) + '\n')

            processed_blob = bucket.blob(output_folder + os.path.basename(processed_json_file_path))
            processed_blob.upload_from_filename(processed_json_file_path)
            print(f"Uploaded processed file to: {processed_blob.name}")


def preprocess_files_callable(**kwargs):
    preprocess_json_files(BUCKET_NAME, JSON_FOLDER, PROCESSED_JSON_FOLDER)
    print("Preprocessed JSON files and uploaded to GCS.")


def fetch_schema_from_bigquery(project_name, dataset_name, table_name):
    client = bigquery.Client()
    query = f"""
    SELECT column_name, data_type
    FROM `{project_name}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}'
    """
    query_job = client.query(query)
    schema = [{'name': row['column_name'], 'type': row['data_type'], 'mode': 'NULLABLE'} for row in query_job]
    return schema


def get_schema_callable(**kwargs):
    schema = fetch_schema_from_bigquery(BQ_PROJECT_NAME, BQ_DATASET_NAME, BQ_TABLE_NAME)
    kwargs['ti'].xcom_push(key='bq_schema', value=schema)
    print("Fetched BigQuery schema and pushed to XCom.")


preprocess_files_task = PythonOperator(
    task_id='preprocess_json_files',
    python_callable=preprocess_files_callable,
    provide_context=True,
    dag=dag,
)

get_schema_task = PythonOperator(
    task_id='get_bq_schema',
    python_callable=get_schema_callable,
    provide_context=True,
    dag=dag,
)

# custom BQ load method by using gcs to bq operator
def load_to_bq_callable(**kwargs):
    schema = kwargs['ti'].xcom_pull(key='bq_schema', task_ids='get_bq_schema')
    load_to_bq_task = GCSToBigQueryOperator(
        task_id='load_json_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=[f'{PROCESSED_JSON_FOLDER}*.json'],
        destination_project_dataset_table=f'{BQ_PROJECT_NAME}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}',
        schema_fields=schema,
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        autodetect=False,
        dag=dag,
    )
    load_to_bq_task.execute(context=kwargs)
    print("Loading JSON files to BigQuery...")


load_to_bq_task = PythonOperator(
    task_id='load_json_to_bigquery_task',
    python_callable=load_to_bq_callable,
    provide_context=True,
    dag=dag,
)

# archive files after processing (remove from inbound path, move to archive path)
def archive_and_delete_files_callable(**kwargs):
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)

    blobs = bucket.list_blobs(prefix=JSON_FOLDER)
    for blob in blobs:
        if blob.name.endswith('.json'):
            destination_blob_name = f'{ARCHIVE_FOLDER}{os.path.basename(blob.name)}'
            bucket.copy_blob(blob, bucket, destination_blob_name)
            blob.delete()
            print(f"Archived and deleted original file: {blob.name}")

    blobs = bucket.list_blobs(prefix=PROCESSED_JSON_FOLDER)
    for blob in blobs:
        if blob.name.endswith('.json'):
            blob.delete()
            print(f"Deleted processed file: {blob.name}")


archive_and_delete_files_task = PythonOperator(
    task_id='archive_and_delete_files',
    python_callable=archive_and_delete_files_callable,
    provide_context=True,
    dag=dag)

# transform query to clean the data and load to next layer
def load_to_precore_callable(**kwargs):
    client = bigquery.Client()
    query = f"""
    INSERT INTO `{PRECORE_PROJECT_NAME}.{PRECORE_DATASET_NAME}.{PRECORE_TABLE_NAME}`
    SELECT *, CURRENT_TIMESTAMP() as ingestion_time
    FROM `{BQ_PROJECT_NAME}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}` 
    WHERE REGEXP_CONTAINS(Flight_Month_Year, r'^\d{1,2}\d{4}$')
    """
    client.query(query).result()
    print("Loaded data into precore table with an additional timestamp.")


load_to_precore_task = PythonOperator(
    task_id='load_to_precore',
    python_callable=load_to_precore_callable,
    provide_context=True,
    dag=dag)

# Task dependencies
preprocess_files_task >> get_schema_task >> load_to_bq_task >> archive_and_delete_files_task >> load_to_precore_task



