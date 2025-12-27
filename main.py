'''
DFRobot A02YYUW Ultrasonic Sensor to MQTT bridge
See also https://www.dfrobot.com/product-1935.html

Bridges a DFRobot A02YYUW ultrasonic sensor to MQTT: reads distance frames from a serial port, 
validates and filters values, and publishes measurements to configurable MQTT topics. 
Supports periodic publishing and MQTT-triggered single reads, optional MQTT-based logging, 
configurable logging to file/stdout, thread-safe serial access, and graceful shutdown handling.

Copyright 2025 Robin Windey.
'''
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

DEFAULT_SERIAL_PORT = '/dev/ttyUSB0'
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_PUSH_INTERVAL_SECONDS = 60

ser = None
serial_lock = threading.Lock()

def parse_args():
    parser = argparse.ArgumentParser(description='A02YYUW to MQTT - Ultrasonic Sensor Reader')
    parser.add_argument('--serial-port', type=str, default=os.getenv('SERIAL_PORT', DEFAULT_SERIAL_PORT), help=f'Serial port to use. Env: `SERIAL_PORT`. Default: {DEFAULT_SERIAL_PORT}', required=True)
    parser.add_argument('--mqtt-host', type=str, default=os.getenv('MQTT_HOST', None), help=f'MQTT host. Env: `MQTT_HOST`.', required=True)
    parser.add_argument('--mqtt-port', type=int, default=int(os.getenv('MQTT_PORT', str(DEFAULT_MQTT_PORT))), help=f'MQTT port. Env: `MQTT_PORT`. Default: {DEFAULT_MQTT_PORT}')
    parser.add_argument('--mqtt-user', type=str, default=os.getenv('MQTT_USER', None), help='MQTT username (optional). Env: `MQTT_USER`.')
    parser.add_argument('--mqtt-pass', type=str, default=os.getenv('MQTT_PASS', None), help='MQTT password (optional). Env: `MQTT_PASS`.')
    parser.add_argument('--mqtt-value-topic', type=str, default=os.getenv('MQTT_VALUE_TOPIC', None), help=f'MQTT topic to publish distance values to. Env: `MQTT_VALUE_TOPIC`.', required=True)
    parser.add_argument('--mqtt-error-topic', type=str, default=os.getenv('MQTT_ERROR_TOPIC', None), help=f'MQTT topic to publish error logs to (optional). Env: `MQTT_ERROR_TOPIC`.', required=False)
    parser.add_argument('--mqtt-read-trigger-topic', type=str, default=os.getenv('MQTT_READ_TRIGGER_TOPIC', None), help=f'MQTT topic to listen for read triggers (optional). Whenever this topic receives a "1" message, a read will be executed and the topic will be set back to "0". Env: `MQTT_READ_TRIGGER_TOPIC`.', required=False)
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

def setup_mqtt_logging(log_level, mqtt_client, mqtt_error_topic):
    logger = logging.getLogger()
    log_level_enum = get_log_level_enum(log_level)

    mqtt_handler = MQTTLoggingHandler(mqtt_client, mqtt_error_topic)
    mqtt_handler.setLevel(log_level_enum)
    mqtt_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
    logger.addHandler(mqtt_handler)

def read_distance():
    global ser
    try:
        serial_lock.acquire()
        read_start_time = time.time()
        while ser.in_waiting < 4:
            time.sleep(0.01)
            if time.time() - read_start_time > READ_TIMEOUT_SECONDS:
                raise TimeoutError(f"Read timeout after {READ_TIMEOUT_SECONDS}s (no data available).")

        data_buffer = ser.read(ser.in_waiting)
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
    finally:
        serial_lock.release()

def setup_serial(serial_port):
    logging.debug(f"Setting up serial connection on {serial_port}")
    ser = serial.Serial(
        port=serial_port,
        baudrate=SERIAL_BAUDRATE,
        timeout=1
    )
    if not ser.is_open:
        logging.debug("Serial connection is not open, opening...")
        ser.open()
    return ser

def read_trigger_executed(mqtt_client: mqtt.Client, userdata, message: mqtt.MQTTMessage, mqtt_value_topic):
    payload = message.payload.decode()
    if payload == '1':
        logging.debug(f"Received read trigger")
        try:
            read_distance_mm = read_distance()
        except ValueError as e:
            logging.error(f"ValueError: {e}")
            return
        except TimeoutError as e:
            logging.error(f"TimeoutError: {e}")
            return

        push_mqtt_value(mqtt_client, read_distance_mm, mqtt_value_topic)
        mqtt_client.publish(message.topic, "0") # Acknowledge read trigger

def setup_mqtt(host, port, mqtt_value_topic, username=None, password=None, mqtt_read_trigger_topic=None):
    mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    if username is not None and username != "":
        mqtt_client.username_pw_set(username, password)
    mqtt_client.connect(host, port)

    if mqtt_read_trigger_topic is not None:
        mqtt_client.subscribe(mqtt_read_trigger_topic)
        mqtt_client.message_callback_add(mqtt_read_trigger_topic,
            lambda client, userdata, message: read_trigger_executed(client, userdata, message, mqtt_value_topic))

    mqtt_client.loop_start()
    return mqtt_client

def push_mqtt_value(mqtt_client, read_distance_mm, mqtt_value_topic):
    logging.debug(f"Pushing distance {read_distance_mm} mm to MQTT topic {mqtt_value_topic}")
    mqtt_client.publish(mqtt_value_topic, read_distance_mm)

def signal_handler(sig, frame):
    global running
    logging.info('Received signal to stop, exiting...')
    running = False

def main():
    global running
    running = True

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    args = parse_args()

    setup_logging(args.log_file, args.log_level)

    try:
        global ser
        ser = setup_serial(args.serial_port)

        mqtt_client = setup_mqtt(args.mqtt_host,
                                 args.mqtt_port,
                                 args.mqtt_value_topic,
                                 args.mqtt_user,
                                 args.mqtt_pass,
                                 args.mqtt_read_trigger_topic)
        
        if args.mqtt_error_topic is not None:
            setup_mqtt_logging(args.log_level, mqtt_client, args.mqtt_error_topic)

        last_mqtt_push_time = 0

        while running:
            try:
                read_distance_mm = read_distance()
                time.sleep(READ_INTERVAL_SECONDS)
                if time.time() - last_mqtt_push_time > args.mqtt_push_interval:
                    push_mqtt_value(mqtt_client, read_distance_mm, args.mqtt_value_topic)
                    last_mqtt_push_time = time.time()
            except ValueError as e:
                logging.error(f"ValueError: {e}")
                time.sleep(1)
            except TimeoutError as e:
                logging.error(f"TimeoutError: {e}")
                time.sleep(5)
    except Exception as e:
        logging.exception(e)
    finally:
        logging.info("Closing serial connection.")
        ser.close()
        logging.info("Disconnecting MQTT client.")
        mqtt_client.disconnect()
    logging.info("Program stopped.")

if __name__ == "__main__":
    main()