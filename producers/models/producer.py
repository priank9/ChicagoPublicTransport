"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"


class Producer:
    """Defines and provides common functionality amongst Producers"""


    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=3,
        num_replicas=1 #only one broker
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas


        #Configure broker properties 
        #https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#avro-producer
        
        self.broker_properties = {
            "bootstrap.servers":BROKER_URL,
            "logger":logger,
            "enable.idempotence":True
        }

        # Create a schema registry client
        schema_registry = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            config=self.broker_properties,
            schema_registry=schema_registry
        )

    #Code to check if topic exists
    def topic_exists(client, topic_name):

        logger.info(f"Topic_exists topic name = {topic_name}")
        topic_fetch = client.list_topics()
        logger.info(f"topic_fetch = {topic_fetch}")

        if topic_fetch.topics.get(topic_name) is not None:
            return True
        else:
            return False

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        
        _topic = self.topic_name
        client = AdminClient({"bootstrap.servers":BROKER_URL})
        logger.info(f"create_topic - Producer Create Topic topic name = {self.topic_name}")
        exists = Producer.topic_exists(client,self.topic_name)

        if not exists:
            futures = client.create_topics(
                [
                    NewTopic(
                        topic = _topic,
                        num_partitions = self.num_partitions,
                        replication_factor = self.num_replicas,
                        config = {
                            "cleanup.policy":"delete",
                            "delete.retention.ms":"100"
                        }
                    )
                ]
            )

            #print(f"futures items: {futures.items}")

            for topic,future in futures.items():
                try:
                    future.result()
                    logger.info(f"Topic {_topic} successfully created")
                except Exception as e:
                    logger.info(f"Failed to create topic - {_topic}: {e}")
      

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        try:
            self.producer.flush()
        except Exception as e:
            logger.info(f"producer close incomplete - skipping - {e}")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
