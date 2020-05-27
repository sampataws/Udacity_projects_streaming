"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests
from Chicago_Transit_Authority.com.sampat.cta.producers.models.connection_config import Connections
from Chicago_Transit_Authority.com.sampat.cta.producers.models.topic_config import CtaTopics



logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = f"{Connections.CONNECT}/connectors"
TOPIC_PREFIX, TABLE_NAME = CtaTopics.STATIONS.rsplit(".", maxsplit=1)
CONNECTOR_NAME = f"jdbc_source_postgres_{TABLE_NAME}"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    logger.info("connector code not completed skipping connector creation")
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "batch.max.rows": 500,
                    "connection.url": Connections.POSTGRES.get("connection.url"),
                    "connection.user": Connections.POSTGRES.get("user"),
                    "connection.password": Connections.POSTGRES.get("password"),
                    "table.whitelist": TABLE_NAME,
                    "mode": "incrementing",
                    "incrementing.column.name": "stop_id",
                    "topic.prefix": f"{TOPIC_PREFIX}.",
                    "poll.interval.ms": 60000,
                },
            }
        ),
    )

    ## Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
