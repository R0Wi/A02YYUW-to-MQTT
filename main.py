'''
DFRobot A02YYUW Ultrasonic Sensor to MQTT bridge
See also https://www.dfrobot.com/product-1935.html

Bridges a DFRobot A02YYUW ultrasonic sensor to MQTT: reads distance frames from a serial port, 
validates and filters values, and publishes measurements to configurable MQTT topics. 
Supports periodic publishing and MQTT-triggered single reads, optional MQTT-based logging, 
configurable logging to file/stdout, thread-safe serial access, and graceful shutdown handling.

Copyright 2025 Robin Windey.
'''
from types import FrameType
import serial
import time
import logging
import signal
import sys
import os
import argparse
import paho.mqtt.client as mqtt
from mqtt_logging_handler import MQTTLoggingHandler
import threading

READ_TIMEOUT_SECONDS = 1
READ_INTERVAL_SECONDS = 1
READ_MIN_VALUE_MM = 30
READ_MAX_VALUE_MM = 4500
START_SEQUENCE_BYTE = 0xFF
SERIAL_BAUDRATE = 9600

DEFAULT_LOG_FILE_NAME = os.path.join(sys.path[0], 'A02YYUW-to-MQTT.log')
DEFAULT_LOG_LEVEL = 'INFO'
DEFAULT_LOG_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
DEFAULT_MQTT_LOG_LEVEL = 'ERROR'

DEFAULT_SERIAL_PORT = '/dev/ttyUSB0'
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_PUSH_INTERVAL_SECONDS = 60

def parse_args():
    parser = argparse.ArgumentParser(description='A02YYUW to MQTT - Ultrasonic Sensor Reader')
   
    parser.add_argument('--serial-port', type=str, default=os.getenv('SERIAL_PORT', DEFAULT_SERIAL_PORT), help=f'Serial port to use. Env: `SERIAL_PORT`. Default: {DEFAULT_SERIAL_PORT}')
    parser.add_argument('--mqtt-host', type=str, default=os.getenv('MQTT_HOST', None), help=f'MQTT host. Env: `MQTT_HOST`.', required='MQTT_HOST' not in os.environ)
    parser.add_argument('--mqtt-port', type=int, default=int(os.getenv('MQTT_PORT', str(DEFAULT_MQTT_PORT))), help=f'MQTT port. Env: `MQTT_PORT`. Default: {DEFAULT_MQTT_PORT}')
    parser.add_argument('--mqtt-user', type=str, default=os.getenv('MQTT_USER', None), help='MQTT username (optional). Env: `MQTT_USER`.')
    parser.add_argument('--mqtt-pass', type=str, default=os.getenv('MQTT_PASS', None), help='MQTT password (optional). Env: `MQTT_PASS`.')
    parser.add_argument('--mqtt-value-topic', type=str, default=os.getenv('MQTT_VALUE_TOPIC', None), help=f'MQTT topic to publish distance values to. Env: `MQTT_VALUE_TOPIC`.', required='MQTT_VALUE_TOPIC' not in os.environ)
    parser.add_argument('--mqtt-log-topic', type=str, default=os.getenv('MQTT_LOG_TOPIC', None), help=f'MQTT topic to publish logs to (optional). If set, logs will not only go to the log file but also be published to this MQTT topic. Env: `MQTT_LOG_TOPIC`.')
    parser.add_argument('--mqtt-log-level', type=str, default=os.getenv('MQTT_LOG_LEVEL', DEFAULT_MQTT_LOG_LEVEL), help=f'Log level for MQTT logging handler (DEBUG, INFO, WARNING, ERROR, CRITICAL). Env: `MQTT_LOG_LEVEL`. Default: {DEFAULT_MQTT_LOG_LEVEL}')
    parser.add_argument('--mqtt-read-trigger-topic', type=str, default=os.getenv('MQTT_READ_TRIGGER_TOPIC', None), help=f'MQTT topic to listen for read triggers (optional). Whenever this topic receives a "1" message, a read will be executed and the topic will be set back to "0". Env: `MQTT_READ_TRIGGER_TOPIC`.')
    parser.add_argument('--mqtt-push-interval', type=int, default=int(os.getenv('MQTT_PUSH_INTERVAL_SECONDS', str(DEFAULT_MQTT_PUSH_INTERVAL_SECONDS))), help=f'Interval in seconds to push distance values to MQTT. Env: `MQTT_PUSH_INTERVAL_SECONDS`. Default: {DEFAULT_MQTT_PUSH_INTERVAL_SECONDS}')
    parser.add_argument('--log-file', type=str, default=os.getenv('LOG_FILE', DEFAULT_LOG_FILE_NAME), help=f'Log file name. If set to "stdout", program will log to stdout instead of file. Env: `LOG_FILE`. Default: {DEFAULT_LOG_FILE_NAME}')
    parser.add_argument('--log-level', type=str, default=os.getenv('LOG_LEVEL', DEFAULT_LOG_LEVEL), help=f'Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Env: `LOG_LEVEL`. Default: {DEFAULT_LOG_LEVEL}')
    
    return parser.parse_args()

def get_log_level_enum(log_level: str):
    try:
        return getattr(logging, log_level.upper())
    except AttributeError:
        raise ValueError(f"Invalid log level: {log_level}. Must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL.")

def setup_logging(log_file, log_level):
    logger = logging.getLogger()
    log_level_enum = get_log_level_enum(log_level)
    logger.setLevel(log_level_enum)

    if logger.hasHandlers():
        logger.handlers.clear()

    log_handler = logging.StreamHandler(sys.stdout) if log_file == "stdout" else logging.FileHandler(log_file)
    log_handler.setLevel(log_level_enum)
    log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
    logger.addHandler(log_handler)

class SensorMQTTBridge:
    def __init__(self, serial_port, mqtt_host, mqtt_port, mqtt_value_topic,
                 mqtt_user=None, mqtt_pass=None, mqtt_read_trigger_topic=None,
                 mqtt_log_topic=None, mqtt_log_level=DEFAULT_MQTT_LOG_LEVEL,
                 mqtt_push_interval=DEFAULT_MQTT_PUSH_INTERVAL_SECONDS,
                 read_timeout=READ_TIMEOUT_SECONDS, read_interval=READ_INTERVAL_SECONDS):
        self.serial_port = serial_port
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_user = mqtt_user
        self.mqtt_pass = mqtt_pass
        self.mqtt_value_topic = mqtt_value_topic
        self.mqtt_log_topic = mqtt_log_topic
        self.mqtt_log_level = mqtt_log_level
        self.mqtt_read_trigger_topic = mqtt_read_trigger_topic
        self.mqtt_push_interval = mqtt_push_interval
        self.read_timeout = read_timeout
        self.read_interval = read_interval

        self.serial_lock = threading.Lock()
        self.ser = self._create_serial()
        self.mqtt_client = self._create_mqtt_client()
        self.running = False

    def _create_serial(self) -> serial.Serial:
        logging.debug(f"Setting up serial connection on {self.serial_port}")
        ser = serial.Serial(
            port=self.serial_port,
            baudrate=SERIAL_BAUDRATE,
            timeout=1
        )
        if not ser.is_open:
            logging.debug("Serial connection is not open, opening...")
            ser.open()
            
        return ser
    
    def _create_mqtt_client(self) -> mqtt.Client:
        logging.debug(f"Setting up MQTT client to connect to {self.mqtt_host}:{self.mqtt_port}")
        mqtt_client = mqtt.Client(callback_api_version = mqtt.CallbackAPIVersion.VERSION2)  # type: ignore[attr-defined]
        
        if self.mqtt_user is not None and self.mqtt_user != "":
            mqtt_client.username_pw_set(self.mqtt_user, self.mqtt_pass)
        
        mqtt_client.connect(self.mqtt_host, self.mqtt_port)

        if self.mqtt_read_trigger_topic is not None:
            mqtt_client.subscribe(self.mqtt_read_trigger_topic)
            mqtt_client.message_callback_add(self.mqtt_read_trigger_topic, self._read_trigger_executed)
        
        return mqtt_client
        
    def _setup_mqtt_logging(self, log_level, mqtt_log_topic):
        logger = logging.getLogger()
        log_level_enum = get_log_level_enum(log_level)

        mqtt_handler = MQTTLoggingHandler(self.mqtt_client, mqtt_log_topic)
        mqtt_handler.setLevel(log_level_enum)
        mqtt_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
        logger.addHandler(mqtt_handler)

    def _read_distance(self):
        if self.ser is None:
            raise RuntimeError("Serial port not initialized")
        with self.serial_lock:
            read_start_time = time.time()
            while self.ser.in_waiting < 4:
                time.sleep(0.01)
                if time.time() - read_start_time > self.read_timeout:
                    raise TimeoutError(f"Read timeout after {self.read_timeout}s (no data available).")

            data_buffer = self.ser.read(self.ser.in_waiting)
            length = len(data_buffer)

            if length < 4:
                raise ValueError("Data length is less than 4 bytes")

            # Find last index of 0xFF which starts a sequence of 4 bytes
            last_data_index = -1
            for i in reversed(range(length)):
                if data_buffer[i] == START_SEQUENCE_BYTE and (i + 4) <= length:
                    last_data_index = i
                    break

            if last_data_index == -1:
                raise ValueError(f"No start sequence ({START_SEQUENCE_BYTE}) found in data")

            # Take last 4 bytes from buffer
            data = []
            for i in range(last_data_index, last_data_index + 4):
                data.append(data_buffer[i])

            sum_check = (data[0] + data[1] + data[2]) & 0x00FF
            if sum_check != data[3]:
                raise ValueError(f"Checksum error ({sum_check} != {data[3]} in {data})")
            distance_mm = ((data[1] << 8) + data[2])
            if distance_mm < READ_MIN_VALUE_MM:
                raise ValueError("Below the lower limit")
            elif distance_mm > READ_MAX_VALUE_MM:
                raise ValueError("Above the upper limit")
            else:
                logging.debug(f"Distance: {distance_mm} mm")
                return distance_mm

    def _read_trigger_executed(self, client: mqtt.Client, userdata, message: mqtt.MQTTMessage):
        payload = message.payload.decode()
        if payload == '1':
            logging.debug(f"Received read trigger")
            try:
                read_distance_mm = self._read_distance()
            except ValueError as e:
                logging.error(f"ValueError: {e}")
                return
            except TimeoutError as e:
                logging.error(f"TimeoutError: {e}")
                return

            self._push_mqtt_distance_value(read_distance_mm)
            client.publish(message.topic, "0")

    def _push_mqtt_distance_value(self, read_distance_mm):
        logging.debug(f"Pushing distance {read_distance_mm} mm to MQTT topic {self.mqtt_value_topic}")
        if self.mqtt_client is not None:
            self.mqtt_client.publish(self.mqtt_value_topic, read_distance_mm)
        
    def stop(self):
        self.running = False

    def run(self):
        self.running = True
        try:
            self.mqtt_client.loop_start()
            
            if self.mqtt_log_topic is not None:
                self._setup_mqtt_logging(self.mqtt_log_level, self.mqtt_log_topic)

            logging.info("Starting program.")

            last_mqtt_push_time = 0.0

            while self.running:
                try:
                    read_distance_mm = self._read_distance()
                    if time.time() - last_mqtt_push_time > self.mqtt_push_interval:
                        self._push_mqtt_distance_value(read_distance_mm)
                        last_mqtt_push_time = time.time()
                    time.sleep(self.read_interval)
                except ValueError as e:
                    logging.error(f"ValueError: {e}")
                    time.sleep(1)
                except TimeoutError as e:
                    logging.error(f"TimeoutError: {e}")
                    time.sleep(5)
        except Exception as e:
            logging.exception(e)
        finally:
            if self.ser is not None:
                logging.info("Closing serial connection.")
                try:
                    self.ser.close()
                except Exception:
                    pass
            if self.mqtt_client is not None:
                logging.info("Disconnecting MQTT client.")
                try:
                    self.mqtt_client.disconnect()
                except Exception:
                    pass
        logging.info("Program stopped.")

def main():
    args = parse_args()

    setup_logging(args.log_file, args.log_level)

    bridge = SensorMQTTBridge(
        serial_port=args.serial_port,
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_value_topic=args.mqtt_value_topic,
        mqtt_user=args.mqtt_user,
        mqtt_pass=args.mqtt_pass,
        mqtt_read_trigger_topic=args.mqtt_read_trigger_topic,
        mqtt_log_topic=args.mqtt_log_topic,
        mqtt_push_interval=args.mqtt_push_interval,
    )
    
    def _signal_handler(sig: int, frame: FrameType | None) -> None:
        logging.info('Received signal to stop, exiting...')
        bridge.stop()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    bridge.run()

if __name__ == "__main__":
    main()