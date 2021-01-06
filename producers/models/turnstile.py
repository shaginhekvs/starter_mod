"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )
        self.station_name = station_name
        super().__init__(
            f"{station_name}-{station.station_id}.TurnstileEvents.v1", 
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        for entry in range(num_entries):
            try:
                event_key = {"timestamp": self.time_millis()}
                event_value = {
                "station_id":self.station.station_id,
                "station_name":self.station_name,
                "line":self.station.color.name
                }
                self.producer.produce(topic=self.topic_name,key=event_key,value=event_value)
            except Exception as e:                
                logger.info("turnstile kafka integration incomplete - skipping")
                logger.info(e)

