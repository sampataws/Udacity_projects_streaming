"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from Chicago_Transit_Authority.com.sampat.cta.producers.models.producer import Producer
from Chicago_Transit_Authority.com.sampat.cta.producers.models.turnstile_hardware import TurnstileHardware
from Chicago_Transit_Authority.com.sampat.cta.producers.models.utils import load_schema,format_station_name,RecordSchema
from Chicago_Transit_Authority.com.sampat.cta.producers.models.topic_config import CtaTopics,join_topic_name



logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = load_schema("turnstile_key.json")
    value_schema = load_schema("turnstile_value.json")

    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #

    def __init__(self, station):
        """Create the Turnstile"""
        super().__init__(
            topic_name=CtaTopics.TURNSTILES,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=3,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.debug(
            f"[{timestamp.isoformat()}] Riders count: {num_entries} @ {self.station.name}"
        )
        for _ in range(num_entries):
            self.producer.poll(0)
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    "station_id": self.station.station_id,
                    "station_name": self.station.name,
                    "line": self.station.color.name,
                },
            )
