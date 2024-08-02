from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

default_args = {
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with models.DAG(
    'Sales_Airflow_Pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    start = DummyOperator(task_id='start')

    input_pattern = 'gs://airline_inbound_data/sales/*.xlsx'

    run_dataflow = DataflowCreatePythonJobOperator(
        task_id='sales_dataflow_pipeline',
        location='asia-south2',
        py_file='gs://airline_inbound_data/dataflow_templates/dataflow_sales_pipeline.py',
        dataflow_default_options={
            'project': 'sunlit-analyst-430409-b3',
            'region': 'asia-south2',
            'staging_location': 'gs://airline_inbound_data/staging/',
            'temp_location': 'gs://airline_inbound_data/temp/',
            'runner': 'DataflowRunner',
            'job_name': 'process-sales-files',
        },
        py_requirements=['openpyxl', 'pandas', 'google-cloud-storage', 'apache-beam[gcp]'],
        options={
            'input_pattern': input_pattern,
            'output_table': 'sunlit-analyst-430409-b3:loading.sales_data',
        },
        wait_until_finished=True
    )

    move_files = GCSToGCSOperator(
        task_id='move_files',
        source_bucket='airline_inbound_data',
        source_object='sales/*.xlsx',
        destination_bucket='airline_inbound_data',
        destination_object='archive/',
        move_object=True,
        gcp_conn_id='google_cloud_default',
    )

    end = DummyOperator(task_id='end')

    start >> run_dataflow >> move_files >> end