from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import logging
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

class BigQueryWriter:
    def __init__(self, project_id, dataset_id, table_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.hook = BigQueryHook(gcp_conn_id='google_cloud_conn')
        self.client = self.hook.get_client(project_id=self.project_id)
        self.table_ref = self.client.dataset(self.dataset_id).table(self.table_id)

    def write_parquet_to_bigquery(self, parquet_file_path):
        try:
            logging.info(f"Writing {parquet_file_path} to BigQuery...")
            # Read the parquet file
            parquet_full_file_path = f"/usr/local/airflow/dags/files/{parquet_file_path}"
            logging.info(f"parquet_full_file_path: {parquet_full_file_path}")
            df = pd.read_parquet(f"/usr/local/airflow/dags/files/{parquet_file_path}")

            # Load the data into BigQuery
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                source_format=bigquery.SourceFormat.PARQUET,
            )

            load_job = self.client.load_table_from_dataframe(
                df, self.table_ref, job_config=job_config
            )

            # Wait for the job to complete
            load_job.result()

            logging.info(f"Loaded {load_job.output_rows} rows into {self.table_id}")

        except Exception as e:
            logging.error(f"Error writing to BigQuery: {str(e)}")
            raise

    def query_table(self, query):
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            return results
        except Exception as e:
            logging.error(f"Error querying BigQuery: {str(e)}")
            raise