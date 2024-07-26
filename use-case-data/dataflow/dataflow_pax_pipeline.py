import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import io
import logging

class ReadCSVAndProcess(beam.DoFn):
    def process(self, element):
        try:
            from google.cloud import storage
            import pandas as pd
            import io

            client = storage.Client()


            bucket_name = 'airline_inbound_data'
            file_name = 'pax_04_2019.csv'
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(file_name)
            content = blob.download_as_bytes()

            df = pd.read_csv(io.BytesIO(content), encoding='utf-16-le')

            df.columns = ["Date", "Flight_Number", "Boarded_Y", "Capacity_Physical_Y", "Capacity_Saleable_Y"]

            df['Date'] = df['Date'].astype(str)
            df['Flight_Number'] = df['Flight_Number'].astype(str)
            df['Boarded_Y'] = df['Boarded_Y'].astype(str)
            df['Capacity_Physical_Y'] = df['Capacity_Physical_Y'].astype(str)
            df['Capacity_Saleable_Y'] = df['Capacity_Saleable_Y'].astype(str)

            records = df.to_dict(orient='records')
            for record in records:
                yield record
        except Exception as e:
            logging.error(f"Error processing file {element}: {e}")
            raise

def run():
    gcs_file_path = 'gs://airline_inbound_data/pax_04_2019.csv'
    table_id = 'sunlit-analyst-430409-b3:loading.pax_data'

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'sunlit-analyst-430409-b3'
    google_cloud_options.job_name = 'pax-pipeline'
    google_cloud_options.temp_location = 'gs://airline_inbound_data/temp'
    google_cloud_options.staging_location = 'gs://airline_inbound_data/staging/'
    google_cloud_options.region = 'asia-south2'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    p = beam.Pipeline(options=options)

    (p
     | 'Create URL' >> beam.Create([gcs_file_path])
     | 'Read and Process CSV' >> beam.ParDo(ReadCSVAndProcess())
     | 'Write to BigQuery' >> WriteToBigQuery(
            table_id,
            schema='Date:STRING, Flight_Number:STRING, Boarded_Y:STRING, Capacity_Physical_Y:STRING, Capacity_Saleable_Y:STRING',
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()



