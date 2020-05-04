"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

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
        self._client = None

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "schema.registry.url": "http://localhost:8081",
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "cleanup.policy": "delete",
            "compression.type": "lz4",
            "delete.retention.ms": "2000",
            "file.delete.delay.ms": "2000",
            "linger.ms": 1000,
            "batch.num.messages": 100,
            "on_delivery": delivery_report
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            {
                "bootstrap.servers": "PLAINTEXT://localhost:9092",
                "schema.registry.url": "http://localhost:8081"},
                default_key_schema=key_schema,
                default_value_schema=value_schema
        )

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

    @property
    def client(self) -> AdminClient:
        if not self._client:
            self._client = AdminClient(
                {"bootstrap.servers": self.broker_properties["bootstrap.servers"]}
            )
        return self._client

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        # TODO: Write code that creates the topic for this producer if it does not already exist on the Kafka Broker
        if self.topic_exists(self.topic_name):
            logger.debug(f"Topic already exists: {self.topic_name}")
            return

        futures = self.client.create_topics(
            [
                NewTopic(
                    self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.debug(f"Topic with name: {topic} created sucessfully")
            except Exception as exc:
                logger.error(f"Failed to create topic with name : {topic}: {exc}")

        #logger.info("topic creation kafka integration incomplete - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def topic_exists(self, topic: str) -> bool:
        """Check if the topic exists in the Kafka"""
        cluster_metadata = self.client.list_topics(timeout=5.0)
        topics = cluster_metadata.topics
        return topic in topics

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # TODO: Write cleanup code for the Producer here
        self.producer.flush()


    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))


def delivery_report(err, msg):
    """Callback on message delivery result"""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()}[{msg.partition()}]")