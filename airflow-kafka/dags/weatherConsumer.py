from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta
from scripts.cls_kafka_producer_consumer import MyKafkaManager
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from scripts.cls_gcp_bigquery import BigQueryWriter
import logging as logger
import pandas as pd
import os
import time


def make_parquet_file(messages):
    try:
        logger.info(f"start making parquet file")
        df = pd.DataFrame(messages)
        logger.info(f"schema: {df.info()}")
        logger.info(f"df: {df.head(2)}")
        # Generate a new file name with the format customer_year_month_day_hour
        current_time = datetime.now()
        table_name = "weather_data"
        file_name = f"{table_name}_{current_time.year}_{current_time.month:02d}_{current_time.day:02d}_{current_time.hour:02d}_{current_time.minute:02d}_{current_time.second:02d}.parquet"

        # save parquet file to /usr/local/airflow/dags/files
        df.to_parquet(f"/usr/local/airflow/dags/files/{file_name}")
        logger.info(f"parquet file {file_name} created")
        return file_name
    except Exception as e:
        logger.error(f"Error making parquet file: {e}")
        return None



def load_to_S3(parquet_file):
    try:
        s3_hook = S3Hook(aws_conn_id='aws_conn')
        bucket_name = 'ebi-generalpurpose-bucket'
        s3_key = f'p033/weather_data/{parquet_file}'
        local_file_path = f'/usr/local/airflow/dags/files/{parquet_file}'

        # Get all .parquet files in the local directory
        """ parquet_files = [f for f in os.listdir(local_file_path) if f.endswith('.parquet')]
        
        for file in parquet_files:
            logger.info(f"start uploading {file}...") """

        logger.info(f"Uploading {local_file_path} to S3...")
        s3_hook.load_file(
            filename=local_file_path,
            key=s3_key,
            bucket_name=bucket_name,
            replace=False
        )
        logger.info(f"File {parquet_file} uploaded to S3 successfully")
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")

def load_to_bigquery(parquet_file):
    try:
        bigquery_writer = BigQueryWriter(project_id='refreshing-park-427810-b5', dataset_id='dev_dataset', table_id='weather_data')
        bigquery_writer.write_parquet_to_bigquery(parquet_file)
    except Exception as e:
        logger.error(f"Error loading file to BigQuery: {e}")

def clean_local_file():
        local_file_path = f'/usr/local/airflow/dags/files/'
        parquet_files = [f for f in os.listdir(local_file_path) if f.endswith('.parquet')]
        for file in parquet_files:
            os.remove(f"{local_file_path}{file}")
            logger.info(f"File {file} deleted from local directory")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def taskflow_weather_consumer_dag():

    @task()
    def task_start():
        print("Executing task 1")
        
    @task()
    def task_consume_kafka_message():
        try:
            for i in range(1): # or >> while True
                ###########################################################
                # start consuming kafka messages
                ###########################################################
                logger.info("start consuming kafka messages")
                # get 5 last messages from kafka
                kafka_class_instance = MyKafkaManager()
                kafka_class_instance.create_consumer(kafka_class_instance.topic_name)
                messages = kafka_class_instance.consume_messages(timeout_seconds=5)
                logger.info(f"Received messages: {messages}")
                kafka_class_instance.consumer.close()
                logger.info("end consuming kafka messages")

                ###########################################################
                # pack messages to parquet file
                ###########################################################
                parquet_file = make_parquet_file(messages)

                ###########################################################
                # load parquet file to S3
                ###########################################################
                load_to_S3(parquet_file)

                ###########################################################
                # load parquet file to BigQuery
                ###########################################################
                load_to_bigquery(parquet_file)

                ###########################################################
                # clean local file
                ###########################################################
                clean_local_file()

                # sleep 1 minute
                logger.info(f"sleep 1 minute")
                #time.sleep(60)
                logger.info(f"end sleep 1 minute")

        except Exception as e:
            logger.error(f"Error consuming kafka messages: {e}")


    @task()
    def task_end():
        print("Executing task 3")
    


    chain(task_start(), task_consume_kafka_message(), task_end())

taskflow_weather_consumer_dag()