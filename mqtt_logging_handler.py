# Handler which logs messages to a MQTT broker

import logging
import paho.mqtt.client as mqtt

class MQTTLoggingHandler(logging.Handler):
    def __init__(self, client: mqtt.Client, topic: str):
        super().__init__()
        self.mqtt_client = client
        self.topic = topic

    def emit(self, record):
        log_entry = self.format(record)
        self.mqtt_client.publish(self.topic, log_entry)