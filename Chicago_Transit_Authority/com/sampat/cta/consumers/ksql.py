"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

from  Chicago_Transit_Authority.com.sampat.cta.consumers import topic_check
from Chicago_Transit_Authority.com.sampat.cta.consumers import connection_config


logger = logging.getLogger(__name__)


KSQL_URL = connection_config.Connections.KSQL
#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name STRING,
    line STRING
) WITH (
    KAFKA_TOPIC='{connection_config.CtaTopics.TURNSTILES}',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE turnstile_summary
    WITH (
        KAFKA_TOPIC='{connection_config.CtaTopics.TURNSTILES_SUMMARY}',
        VALUE_FORMAT='JSON'
    ) AS
    SELECT
        station_id,
        COUNT(*) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
