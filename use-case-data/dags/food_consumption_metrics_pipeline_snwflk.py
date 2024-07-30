from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery, storage
import pandas as pd
import snowflake.connector
import configparser
import logging
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('process_food_consumption_metrics',
          default_args=default_args,
          description='A DAG to process food consumption metrics, export to GCS, and load into Snowflake',
          schedule_interval=None,
          start_date=datetime(2024, 7, 30),
          catchup=False)


# 1. Joins the passenger (`pax_data`) and sales (`sales_data`) tables on flight number and date.
# 2. Calculates total passengers, total sold items, and total sales for each flight.
# 3. Computes average sold items and average sales per passenger for each flight.
# 4. Ensures valid flight numbers by excluding null or non-numeric values.
# 5. Inserts the calculated metrics into the `food_consumption_metrics` table, grouped by flight number and flight date.
def run_core_query():
    client = bigquery.Client()
    query = """TRUNCATE TABLE `sunlit-analyst-430409-b3.core.food_consumption_metrics`"""
    client.query(query).result()

    query = """
    INSERT INTO `sunlit-analyst-430409-b3.core.food_consumption_metrics`
    SELECT
      SAFE_CAST(CAST(p.Flight_Number AS FLOAT64) AS INT64) AS FlightNumber,
      PARSE_DATE('%d.%m.%Y', p.Date) AS FlightDate,
      CAST(SUM(CAST(p.Boarded_Y AS INT64)) AS INT64) AS TotalPassengers,
      CAST(SUM(CAST(s.Quantity AS INT64)) AS INT64) AS TotalSoldItems,
      CAST(SUM(CAST(s.BaseCurrencyPrice AS FLOAT64) * CAST(s.Quantity AS INT64)) AS FLOAT64) AS TotalSales,
      AVG(CASE 
            WHEN CAST(p.Boarded_Y AS FLOAT64) > 0 
            THEN CAST(s.Quantity AS FLOAT64) / CAST(p.Boarded_Y AS FLOAT64) 
            ELSE NULL 
          END) AS AverageSoldItemsPerPassenger,
      AVG(CASE 
            WHEN CAST(p.Boarded_Y AS FLOAT64) > 0 
            THEN (CAST(s.BaseCurrencyPrice AS FLOAT64) * CAST(s.Quantity AS FLOAT64)) / CAST(p.Boarded_Y AS FLOAT64) 
            ELSE NULL 
          END) AS AverageSalesPerPassenger
    FROM
      `sunlit-analyst-430409-b3.precore.pax_data` p
    JOIN
      `sunlit-analyst-430409-b3.precore.sales_data` s
    ON
      SAFE_CAST(CAST(p.Flight_Number AS FLOAT64) AS INT64) = SAFE_CAST(CAST(SUBSTR(s.Flight, 3) AS FLOAT64) AS INT64)
      AND PARSE_DATE('%d.%m.%Y', p.Date) = DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', s.FlightDate))
    WHERE
      SAFE_CAST(CAST(p.Flight_Number AS FLOAT64) AS INT64) IS NOT NULL
      AND SAFE_CAST(CAST(SUBSTR(s.Flight, 3) AS FLOAT64) AS INT64) IS NOT NULL
    GROUP BY
      FlightNumber, FlightDate;
    """
    client.query(query).result()


# 1. Exports core table data to GCS staging location in outbound bucket
# 2. Creates files with prefix_[date_part].csv pattern
def export_to_gcs():
    from google.cloud import storage
    import datetime

    client = bigquery.Client()
    bucket_name = 'outbound-world'
    file_name = f'snow-exports_{datetime.datetime.now().strftime("%m%d%Y")}.csv'

    query = "SELECT * FROM `sunlit-analyst-430409-b3.core.food_consumption_metrics`"
    df = client.query(query).to_dataframe()

    local_file_path = '/tmp/metrics.csv'
    df.to_csv(local_file_path, index=False)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'metrics/{file_name}')
    blob.upload_from_filename(local_file_path)


# 1. Downloads rsa private key from cloud storage bucket
# 2. Establishes connection to snowflake account
# 3. Performs data load into FOOD_CONSUMPTION_METRICS metrics table in AIRLINE db
def snowflake_operations(**kwargs):
    import snowflake.connector
    from google.cloud import storage
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

    def get_snwflk_ctx():
        local_rsa_key_path = '/tmp/rsa_key.p8'
        rsa_key_path = 'gs://airline_inbound_data/requirements/rsa_key.p8'
        storage_client = storage.Client()
        bucket = storage_client.bucket('airline_inbound_data')
        blob = bucket.blob('requirements/rsa_key.p8')
        blob.download_to_filename(local_rsa_key_path)

        with open(local_rsa_key_path, "rb") as key:
            p_key = serialization.load_pem_private_key(key.read(), password=None, backend=default_backend())

        pkb = p_key.private_bytes(encoding=serialization.Encoding.DER,
                                  format=serialization.PrivateFormat.PKCS8,
                                  encryption_algorithm=serialization.NoEncryption())

        ctx = snowflake.connector.connect(
            user='VISUALINSIGHTS',
            password='DataMatters123',
            account='qtorzpp-yw14931',
            private_key=pkb,
            warehouse='COMPUTE_WH',
            database='APP_DB',
            schema='APP_SCHEMA')
        return ctx

    conn = get_snwflk_ctx()

    copy_sql = f"""
    COPY INTO AIRLINE.INSIGHTS.FOOD_CONSUMPTION_METRICS
    FROM @GCS_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    PATTERN = '.*snow-exports_.*\\.csv';
    """
    df = pd.read_sql_query(copy_sql, conn)
    print(df)
    conn.close()


# task definition
run_core_query_task = PythonOperator(task_id='run_core_query', python_callable=run_core_query, dag=dag)
export_to_gcs_task = PythonOperator(task_id='export_to_gcs', python_callable=export_to_gcs, dag=dag)
snowflake_operations_task = PythonOperator(task_id='snowflake_operations', python_callable=snowflake_operations, provide_context=True, dag=dag)

#  task dependencies
run_core_query_task >> export_to_gcs_task >> snowflake_operations_task


