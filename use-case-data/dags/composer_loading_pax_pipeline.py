from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import bigquery, storage
from datetime import datetime, timedelta
import pandas as pd
import os

# default arguments for DAG object
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=2)}

# DAG definition
dag = DAG('composer_loading_pax_pipeline',
         default_args=default_args,
         description='Load UTF-16-LE CSV files from GCS to BigQuery',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 1, 1),
         catchup=False)

# Define GCS bucket and folder names
BUCKET_NAME = 'airline_inbound_data'
PAX_FOLDER = 'pax/'
CONVERTED_FOLDER = 'pax/converted/'
ARCHIVE_FOLDER = 'archive/pax/'
BQ_PROJECT_NAME = 'sunlit-analyst-430409-b3'
BQ_DATASET_NAME = 'raw'
BQ_TABLE_NAME = 'pax_data'
PRECORE_PROJECT_NAME = 'sunlit-analyst-430409-b3'
PRECORE_DATASET_NAME = 'precore'
PRECORE_TABLE_NAME = 'pax_data'


# alternate approach - instead of hard coding schema in the code, fetching directly from BQ table metadata
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


# list all files in bucket and encode to utf-8
def list_and_convert_csv_files(bucket_name, input_folder, converted_folder):
   print("Starting to list and convert CSV files from the GCS bucket...")
   client = storage.Client()
   bucket = client.get_bucket(bucket_name)
   blobs = client.list_blobs(bucket_name, prefix=input_folder)
   csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]

   for csv_file in csv_files:
       print(f"Converting {csv_file} to UTF-8 encoding...")
       blob = bucket.blob(csv_file)
       csv_file_path = f'/tmp/{os.path.basename(csv_file)}'
       blob.download_to_filename(csv_file_path)
       df = pd.read_csv(csv_file_path, encoding='utf-16-le')

       converted_file_path = f'/tmp/{os.path.basename(csv_file)}'
       df.to_csv(converted_file_path, index=False, encoding='utf-8')
       print(f"Converted {csv_file} to UTF-8 CSV at {converted_file_path}")

       converted_file = csv_file.replace(input_folder, converted_folder)
       converted_blob = bucket.blob(converted_file)
       converted_blob.upload_from_filename(converted_file_path)
       print(f"Uploaded converted CSV file to GCS at {converted_file}")

   return csv_files


def list_and_convert_files_callable(**kwargs):
   csv_files = list_and_convert_csv_files(BUCKET_NAME, PAX_FOLDER, CONVERTED_FOLDER)
   kwargs['ti'].xcom_push(key='csv_files', value=csv_files)
   print("Listed and converted all CSV files, and pushed to XCom.")

# custom BQ load method by using gcs to bq operator
def load_to_bq_callable(**kwargs):
   schema = fetch_schema_from_bigquery(BQ_PROJECT_NAME, BQ_DATASET_NAME, BQ_TABLE_NAME)
   load_to_bq_task = GCSToBigQueryOperator(
       task_id='load_csvs_to_bigquery',
       bucket=BUCKET_NAME,
       source_objects=[f'{CONVERTED_FOLDER}*.csv'],
       destination_project_dataset_table=f'{BQ_PROJECT_NAME}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}',
       schema_fields=schema,
       source_format='CSV',
       skip_leading_rows=1,
       write_disposition='WRITE_TRUNCATE',
       autodetect=False,
       dag=dag)
   load_to_bq_task.execute(context=kwargs)
   print("Loading CSV files to BigQuery...")


# archive files after processing (remove from inbound path, move to archive path)
def archive_files_callable(**kwargs):
   client = storage.Client()
   bucket = client.get_bucket(BUCKET_NAME)

   csv_files = kwargs['ti'].xcom_pull(key='csv_files', task_ids='list_and_convert_files')

   for csv_file in csv_files:
       blob = bucket.blob(csv_file)
       destination_blob_name = f'{ARCHIVE_FOLDER}{os.path.basename(blob.name)}'
       bucket.copy_blob(blob, bucket, destination_blob_name)
       blob.delete()
       print(f"Archived and deleted original file: {blob.name}")

   converted_blobs = bucket.list_blobs(prefix=CONVERTED_FOLDER)
   for blob in converted_blobs:
       if blob.name.endswith('.csv'):
           blob.delete()
           print(f"Deleted converted file: {blob.name}")


def load_to_precore_callable(**kwargs):
   client = bigquery.Client()
   query = f"""
   INSERT INTO `{PRECORE_PROJECT_NAME}.{PRECORE_DATASET_NAME}.{PRECORE_TABLE_NAME}`
   SELECT *, CURRENT_TIMESTAMP() as ingestion_time
   FROM `{BQ_PROJECT_NAME}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}` 
   WHERE REGEXP_CONTAINS(Date, r'^\d{2}\.\d{2}\.\d{4}$')
   """
   client.query(query).result()
   print("Loaded data into precore table with an additional timestamp.")


# Task to list and convert CSV files in the GCS folder
list_and_convert_files_task = PythonOperator(
   task_id='list_and_convert_files',
   python_callable=list_and_convert_files_callable,
   provide_context=True,
   dag=dag,
)

# Task to load CSV files to BigQuery
load_to_bq_task = PythonOperator(
   task_id='load_csvs_to_bigquery_task',
   python_callable=load_to_bq_callable,
   provide_context=True,
   dag=dag,
)

# Task to archive original files and delete converted files
archive_files_task = PythonOperator(
   task_id='archive_processed_files',
   python_callable=archive_files_callable,
   provide_context=True,
   dag=dag,
)

# Task to load data into precore table
load_to_precore_task = PythonOperator(
   task_id='load_to_precore',
   python_callable=load_to_precore_callable,
   provide_context=True,
   dag=dag,
)

# Task dependencies
list_and_convert_files_task >> load_to_bq_task >> archive_files_task >> load_to_precore_task



