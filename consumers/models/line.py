"""Contains functionality related to Lines"""
import json
import logging

from models import Station


logger = logging.getLogger(__name__)


class Line:
    """Defines the Line Model"""

    def __init__(self, color):
        """Creates a line"""
        self.color = color
        self.color_code = "0xFFFFFF"
        if self.color == "blue":
            self.color_code = "#1E90FF"
        elif self.color == "red":
            self.color_code = "#DC143C"
        elif self.color == "green":
            self.color_code = "#32CD32"
        self.stations = {}

    def _handle_station(self, value):
        """Adds the station to this Line's data model"""
        print("entered _handle_station")
        if value["line"].lower() != self.color.lower():
            print(f"value[line] - {self.color}")
            return
        self.stations[value["station_id"]] = Station.from_message(value)

    def _handle_arrival(self, message):
        """Updates train locations"""
        print('----- Entered _handle_arrival -------')
        print(self.stations.values())
        value = message.value()
        prev_station_id = value.get("prev_station_id")
        prev_dir = value.get("prev_direction")
        print(f"prev_station_id - {prev_station_id}, prev_dir - {prev_dir}, Condition - {prev_dir is not None and prev_station_id is not None}")
        if prev_dir is not None and prev_station_id is not None:
            prev_station = self.stations.get(prev_station_id)
            print(f"prev_station - {prev_station}")
            if prev_station is not None:
                print('----- Entering handle departure -------')
                prev_station.handle_departure(prev_dir)
            else:
                logger.debug("unable to handle previous station due to missing station")
        else:
            logger.debug(
                "unable to handle previous station due to missing previous info"
            )

        station_id = value.get("station_id")
        station = self.stations.get(station_id)
        if station is None:
            logger.debug("unable to handle message due to missing station")
            return
        print('----- Entering handle arrival -------')
        station.handle_arrival(
            value.get("direction"), value.get("train_id"), value.get("train_status")
        )

    def process_message(self, message):
        """Given a kafka message, extract data"""
        logger.info(f"Message Topic: {message.topic()}")
        # TODO: Based on the message topic, call the appropriate handler.
        if "cta.stations.processed.output" in message.topic(): # Set the conditional correctly to the stations Faust Table
            try:
                logger.info(f"cta.stations.processed.output - process_message -  {message.value()}")
                value = json.loads(message.value())
                self._handle_station(value)
            except Exception as e:
                logger.fatal("bad station? %s, %s", value, e)
        elif "cta.station.arrival.events" in message.topic(): # Set the conditional to the arrival topic
            logger.info(f"cta.station.arrival.events - process_message -  {message.value()}")
            self._handle_arrival(message)
        elif 'TURNSTILE_SUMMARY' in message.topic(): # Set the conditional to the KSQL Turnstile Summary Topic
            json_data = json.loads(message.value())
            logger.info(f"TURNSTILE_SUMMARY - process_message -  {json_data}")
            station_id = json_data.get("STATION_ID")
            station = self.stations.get(station_id)
            if station is None:
                logger.debug("unable to handle message due to missing station")
                return
            station.process_message(json_data)
        else:
            logger.debug(
                "unable to find handler for message from topic %s", message.topic
            )
