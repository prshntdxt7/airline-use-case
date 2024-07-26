from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with models.DAG(
    'Loading_Airflow_Pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    input_pattern = 'gs://airline_inbound_data/loading/loading_*.json'

    run_dataflow = DataflowCreatePythonJobOperator(
        task_id='loadingdataflowpipeline',
        py_file='gs://airline_inbound_data/dataflow_templates/dataflow_loading_pipeline.py',
        dataflow_default_options={
            'project': 'sunlit-analyst-430409-b3',
            'region': 'asia-south2',
            'staging_location': 'gs://airline_inbound_data/staging/',
            'temp_location': 'gs://airline_inbound_data/temp/',
            'runner': 'DataflowRunner',
            'job_name': 'load-json-to-bigquery',
        },
        options={
            'input_pattern': input_pattern,
            'output_table': 'sunlit-analyst-430409-b3:loading.loading_json_data',
        }
    )

    query = """
    SELECT
        COUNT(*) AS total_rows
    FROM
        `sunlit-analyst-430409-b3.loading.loading_json_data`
    """

    run_query = BigQueryExecuteQueryOperator(
        task_id='run_bq_query',
        sql=query,
        use_legacy_sql=False
    )

    move_files = GCSToGCSOperator(
        task_id='move_files',
        source_bucket='airline_inbound_data',
        source_object='loading/*.json',
        destination_bucket='airline_inbound_data',
        destination_object='archive/',
        move_object=True,
        gcp_conn_id='google_cloud_default',
    )

    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> run_dataflow >> run_query >> move_files >> end


