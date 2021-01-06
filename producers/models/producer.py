"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        bootstap_locations = 'PLAINTEXT://localhost:9092'
        self.broker_properties = {'bootstrap.servers': bootstap_locations  }
        
        # TODO: Configure the AvroProducer
        
        producer_conf = {
            'bootstrap.servers': bootstap_locations,
            'schema.registry.url': 'http://localhost:8081',
            'client.id':"genericProducer"
        }
        self.producer = AvroProducer(producer_conf, 
                                     default_key_schema=key_schema, 
                                     default_value_schema=value_schema,
                                     )
        list_topics_in_kafka = self.producer.list_topics().topics.keys()

        # If the topic does not already exist, try to create it
        if(self.topic_name in list_topics_in_kafka):
            Producer.existing_topics.add(self.topic_name)  
        elif self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
        logger.info("producer attached")
        
    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        
        admin = AdminClient(self.broker_properties)
        topics = [self.topic_name]
        new_topics = [NewTopic(topic, num_partitions=self.num_partitions, replication_factor=self.num_replicas) for topic in topics]
        fs = admin.create_topics(new_topics)

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logger.info("created new topic")
            except Exception as e:
                logger.info(f"topic creation {self.topic_name} kafka integration incomplete  - skipping ")
                logger.info(e)


    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        try:
            self.producer.flush()
        except Exception as e:
            logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
