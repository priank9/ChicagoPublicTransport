"""Contains functionality related to Lines"""
import json
import logging

from models import Line


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
        #print(f"{message.topic()} ------ {'cta.station.arrival.events' in message.topic() or 'cta.stations.processed.output' in message.topic()}")
        if "cta.stations.processed.output" in message.topic():
            value = json.loads(message.value())
            print(f"{message.topic()} - {value.get('line')}")   
            if value["line"].lower() == "green":
                self.green_line.process_message(message)
            elif value["line"].lower() == "red":
                self.red_line.process_message(message)
            elif value["line"].lower() == "blue":
                self.blue_line.process_message(message)
            else:
                logger.debug("discarding unknown line msg %s", value["line"])
        elif "cta.station.arrival.events" in message.topic():
            value = message.value()
            if value["line"].lower() == "green":
                self.green_line.process_message(message)
            elif value["line"].lower() == "red":
                self.red_line.process_message(message)
            elif value["line"].lower() == "blue":
                self.blue_line.process_message(message)
            else:
                logger.debug("discarding unknown line msg %s", value["line"])
        elif "TURNSTILE_SUMMARY" == message.topic():
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
        else:
            logger.info("ignoring non-lines message %s", message.topic())
