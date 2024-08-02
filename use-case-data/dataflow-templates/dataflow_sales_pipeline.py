import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import logging

class ListGCSFiles(beam.DoFn):
    def process(self, element):
        from google.cloud import storage
        logging.info('Starting ListGCSFiles DoFn')
        client = storage.Client()
        bucket_name = 'airline_inbound_data'
        bucket = client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix='sales/')
        file_names = []
        for blob in blobs:
            if not blob.name.endswith('/'):
                file_names.append(blob.name)
                logging.info(f'Found file: {blob.name}')
        logging.info(f'File names to be processed: {file_names}')

        if not file_names:
            logging.warning('No files found in the specified GCS bucket and prefix.')

        yield from file_names

class ReadExcel(beam.DoFn):
    def process(self, file_name):
        import pandas as pd
        import io
        from google.cloud import storage
        import openpyxl
        logging.info(f'Starting ReadExcel DoFn for file: {file_name}')
        try:
            client = storage.Client()
            bucket_name = 'airline_inbound_data'
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(file_name)
            content = blob.download_as_bytes()

            logging.info(f'Processing file: {file_name} with size {len(content)} bytes')

            df = pd.read_excel(io.BytesIO(content), header=None, dtype=str)
            df = df.fillna('')
            df.columns = ["Date", "Flight_Number", "Boarded_Y", "Capacity_Physical_Y", "Capacity_Saleable_Y"]
            records = df.to_dict(orient='records')
            logging.info(f'Processed {len(records)} records from file: {file_name}')

            for record in records:
                yield record

        except Exception as e:
            logging.error(f"Error processing file {file_name}: {e}")
            raise

def run():
    table_id = 'sunlit-analyst-430409-b3:loading.sales_data'

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'sunlit-analyst-430409-b3'
    google_cloud_options.job_name = 'sales-pipeline'
    google_cloud_options.temp_location = 'gs://airline_inbound_data/temp'
    google_cloud_options.staging_location = 'gs://airline_inbound_data/staging/'
    google_cloud_options.region = 'asia-south2'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    logging.info('Starting Dataflow pipeline execution')

    with beam.Pipeline(options=options) as p:
        (p
         | 'List GCS Files' >> beam.Create(['gs://airline_inbound_data/sales/'])
         | 'List Files' >> beam.ParDo(ListGCSFiles())
         | 'Read and Process Excel' >> beam.ParDo(ReadExcel())
         | 'Write to BigQuery' >> WriteToBigQuery(
                table_id,
                schema='Date:STRING, Flight_Number:STRING, Boarded_Y:STRING, Capacity_Physical_Y:STRING, Capacity_Saleable_Y:STRING',
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()