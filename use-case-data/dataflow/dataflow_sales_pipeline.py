import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition
import pandas as pd
from google.cloud import bigquery

class ReadExcel(beam.DoFn):
    def __init__(self, file_path, columns):
        self.file_path = file_path
        self.columns = columns

    def process(self, element):
        import pandas as pd
        df = pd.read_excel(self.file_path, header=None, dtype=str)
        df = df.fillna('')
        df.columns = self.columns
        records = df.to_dict(orient='records')
        for record in records:
            yield record

def fetch_bq_columns(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT column_name
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_id}'
    """
    query_job = client.query(query)
    results = query_job.result()
    columns = [row.column_name for row in results]
    return columns

def run_pipeline(input_file_path, output_table_spec):
    project_id = 'sunlit-analyst-430409-b3'
    dataset_id = 'loading'
    table_id = 'sales_data'

    columns = fetch_bq_columns(project_id, dataset_id, table_id)

    pipeline_options = PipelineOptions()
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.job_name = 'sales-pipeline'
    google_cloud_options.temp_location = 'gs://airline_inbound_data/temp'
    google_cloud_options.staging_location = 'gs://airline_inbound_data/staging/'
    google_cloud_options.region = 'asia-south2'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Create' >> beam.Create([None])
         | 'Read Excel' >> beam.ParDo(ReadExcel(input_file_path, columns))
         | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                output_table_spec,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
         )

if __name__ == '__main__':
    input_file_path = 'gs://airline_inbound_data/Sales_Report_March_2019.xlsx'
    output_table_spec = 'sunlit-analyst-430409-b3:loading.sales_data'
    run_pipeline(input_file_path, output_table_spec)



