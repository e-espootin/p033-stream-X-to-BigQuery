from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import logging

class MyKafkaManager:
    def __init__(self, bootstrap_servers=['kafka:9092'], topic_name = "weather_data"    ):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumer = None
        self.admin_client = None
        self.topic_name = topic_name

    def create_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            linger_ms=10
        )
        return self.producer

    

    def create_topic(self):
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)

        try:
            new_topic = NewTopic(name=self.topic_name, num_partitions=1, replication_factor=1)
            self.admin_client.create_topics([new_topic])
            logging.info(f"Topic '{self.topic_name}' created successfully")
        except TopicAlreadyExistsError:
            logging.info(f"Topic '{self.topic_name}' already exists")
        finally:
            self.admin_client.close()

        

    def send_message(self, topic, message):
        if not self.producer:
            self.create_producer()
        future = self.producer.send(topic, message)
        self.producer.flush()
        return future

    def create_consumer(self, group_id=None):
        logging.info(f"Creating consumer for topic: {self.topic_name}")
        self.consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest', #latest, earliest, none
            enable_auto_commit=True, # Automatically commit offsets
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info(f"Consumer for topic: {self.topic_name} created successfully")
        return self.consumer
    
    def consume_messages(self, timeout_seconds=120):
        
        try:
            logging.info(f"Consuming messages from topic: {self.topic_name}")
            current_timestamp = datetime.now().timestamp() 
            
            #
            
            if not self.consumer:
                raise ValueError("Consumer not initialized. Call create_consumer first.")
            
            logging.info(f"start reading messages from consumer...")
            messages = []
            # Poll messages from consumer
    
            #logging.info(f"offsets for times: {self.consumer.beginning_offsets()}")
            #logging.info(f"end offsets: {self.consumer.end_offsets()}")


            
            for msg in self.consumer:
                #logging.info(f"offset position: {self.consumer.position(self.consumer.assignment().pop())}")
                messages.append(msg.value)
                logging.info(f"Received message: {msg.value} from partition: {msg.partition} at offset: {msg.offset}")
                
                """ if self.consumer.position(self.consumer.assignment().pop()) % 100 == 0:
                    self.consumer.commit() # manually commit offsets
                    logging.info(f"Offset committed.") """


                if  (datetime.now().timestamp()  - current_timestamp) > timeout_seconds:
                    logging.info(f"timeout_ms reached, stop consuming messages")
                    break
                

            logging.info(f"Consumed {len(messages)} messages from topic: {self.topic_name}")

            return messages
        
        except Exception as e:
            print(f"Kafka error: {e}")

    

    def close(self):
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()
        if self.admin_client:
            self.admin_client.close()
