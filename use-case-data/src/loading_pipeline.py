import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.textio import ReadFromText
import json

# Define the pipeline options
options = PipelineOptions()

# Configure the pipeline options
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'sunlit-analyst-430409-b3'
google_cloud_options.job_name = 'load-json-to-bigquery'
google_cloud_options.staging_location = 'gs://airline_inbound_data/staging/'
google_cloud_options.temp_location = 'gs://airline_inbound_data/temp/'
options.view_as(StandardOptions).runner = 'DirectRunner'

# Define the table schema (all fields as STRING)
table_schema = {
    "fields": [
        {"name": "Flight_Month_Year", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Airline", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Flightnumber", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Departure_Unit", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Arrival_Unit", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Flight_Date", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Aircraft_Type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Leg", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Booking_Class_CBASE", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Bill_of_material", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Price", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Invoice_Quantity", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Sales_and_Services_Invoice", "type": "STRING", "mode": "NULLABLE"},
        {"name": "UAA_Invoice", "type": "STRING", "mode": "NULLABLE"}
    ]
}

# Define field mapping from JSON to BigQuery
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

# Function to map JSON fields to BigQuery fields
def map_fields(json_record):
    mapped_record = {}
    for json_field, bq_field in field_mapping.items():
        mapped_record[bq_field] = json_record.get(json_field, None)
    return mapped_record

# Define the pipeline
with beam.Pipeline(options=options) as p:
    (p
     | 'Read JSON from GCS' >> ReadFromText('gs://airline_inbound_data/03-2019.json')
     | 'Parse JSON' >> beam.FlatMap(lambda x: json.loads(x) if x.strip().startswith('[') else [json.loads(x)])
     | 'Map Fields' >> beam.Map(map_fields)
     | 'Write to BigQuery' >> WriteToBigQuery(
         table='sunlit-analyst-430409-b3:loading.loading_json_data',
         schema=table_schema,
         write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )



