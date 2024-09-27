from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta
from scripts.cls_openWeatherMapApi import OpenWeatherMapAPIClass 
from scripts.cls_kafka_producer_consumer import MyKafkaManager
import json
import logging as logger




@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def taskflow_weather_producer_dag():

    weather_api_instance = OpenWeatherMapAPIClass()
    
    def get_weather_data(city):
        logger.info(f"Getting weather data for {city}")
        # create producer
        kafka_class_instance = MyKafkaManager()
        producer = kafka_class_instance.create_producer()
        
        if producer.bootstrap_connected:  
            # get weather data
            weather_data = weather_api_instance.get_formatted_weather_data(city)

            # Send weather data to Kafka topic
            producer.send(kafka_class_instance.topic_name, weather_data)
            logger.info(f"Sent weather data for {city} to Kafka topic: {kafka_class_instance.topic_name}")
            
            # flush and close producer
            producer.flush()
            producer.close()

    @task()
    def task_start():
        print("Executing task 1")
    
   
    @task()
    def prepare_kafka_topic():
        logger.info("Preparing Kafka topic for weather data")
        # create or get existing topic
        kafka_class_instance = MyKafkaManager()
        kafka_class_instance.create_topic()
        
        logger.info("Kafka topic prepared")

    @task()
    def start_producer():
        logger.info("Starting Kafka producer")
        


    
    
    @task()
    def weather_London():
        City = "London"
        get_weather_data(city=City)

    @task()
    def weather_Stuttgart():
        City = "Stuttgart"
        get_weather_data(city=City)

    @task()
    def weather_Berlin():
        City = "Berlin"
        get_weather_data(city=City)

    @task()
    def weather_another_cities():
        cities = ["Paris", "Rome", "Milan", "Madrid", "Barcelona", "Valencia", "Budapest"]
        for City in cities:
            get_weather_data(city=City)
        
    @task()
    def flush_producer():
        logger.info("Flushing producer")
        # todo: flush and close producer
        logger.info("Flushed producer.")

    

    

    chain(
        task_start(),
        prepare_kafka_topic(),
        start_producer(),
        weather_London(),
        [weather_Stuttgart(), weather_Berlin()],
        weather_another_cities(),   
        flush_producer()
    )


taskflow_weather_producer_dag()
