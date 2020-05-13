"""Contains functionality related to Lines"""
import json
import logging

from Chicago_Transit_Authority.com.sampat.cta.consumers.models import Line
from Chicago_Transit_Authority.com.sampat.cta.consumers import connection_config


logger = logging.getLogger(__name__)


class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""
        topic: str = message.topic()
        if topic.startswith(connection_config.CtaTopics.ARRIVALS_PREFIX) or topic in [
            connection_config.CtaTopics.TURNSTILES,
            connection_config.CtaTopics.STATIONS,
            connection_config.CtaTopics.STATIONS_LINE,
        ]:
            value = message.value()
            if topic == connection_config.CtaTopics.STATIONS_LINE:
                value = json.loads(value)
            if value["line"] == "green":
                self.green_line.process_message(message)
            elif value["line"] == "red":
                self.red_line.process_message(message)
            elif value["line"] == "blue":
                self.blue_line.process_message(message)
            else:
                logger.debug("discarding msg  line with unknown %s", value["line"])
        elif topic == connection_config.CtaTopics.TURNSTILES_SUMMARY:
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
        else:
            logger.info("ignoring messaages for no line %s", message.topic())
