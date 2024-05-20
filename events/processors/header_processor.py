"""
This module processes headers from data received from a Kafka topic.
It checks for the presence of expected headers, normalizes them, 
and sends the processed data to a new Kafka topic.

Missing headers are logged as errors.
"""

import json
from config import kafka_config
from config import app_config
from integration.adapters import log_setup
from integration.connectors import kafka_consumer
from integration.connectors import kafka_producer

# Configuring Logs
logging = log_setup.setup_logger('header', 'header_errors.log', 3)

# Logging function to record errors
def log_error(message):
    """
    Logs an error message.

    Args:
        message (str): The error message to log.
    """
    logging.error(message)


def check_headers(data, expected_headers, producer):
    """
    Checks if all expected headers are present in the data and sends normalized data to 
    a Kafka topic.

    Args:
        data (dict): The sensor data as a dictionary.
        expected_headers (list): A list of expected header names.
        producer (KafkaProducer): A Kafka producer instance for sending messages.

    Raises:
        ValueError: If any expected header is missing in the data.
    """

    # Normalizes headers
    normalized_received = {header.replace(" ", "").lower(): value for header, value in data.items()}
    if all(header in normalized_received.keys() for header in expected_headers):
        producer.send(kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED, str(normalized_received)
                      .encode('utf-8'))
        return

    missing = [header for header in expected_headers if header
               not in normalized_received.keys()]
    normalized_received["missing headers"] = missing
    producer.send(kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED, str(normalized_received)
                    .encode('utf-8'))
    raise ValueError(f"Data is missing headers: {missing}")


def process_message(data, producer):
    """
    Processes a single message by checking headers and logging errors if necessary.

    Args:
        data (dict): The sensor data as a dictionary.
        producer (KafkaProducer): A Kafka producer instance for sending messages.
    """

    # Verify if the data contains all expected headers
    try:
        check_headers(data, app_config.EXPECTED_HEADERS, producer)
        #print(f"Data is valid and processing can proceed: {data}")
        return
    except ValueError as data_missing:
        #print(f"{data_missing} in file {data.get('filename')}")
        log_error(f"{data_missing} in file {data.get('filename')}")

def process_messages():
    """
    Continuously consumes messages from the configured Kafka topic, processes them,
    and handles errors.
    """

    consumer = kafka_consumer.get_kafka_consumer(
            topic_name=kafka_config.KAFKA_SENSOR_READINGS_TOPIC,
            broker_url=kafka_config.KAFKA_BROKER,
            group_id=kafka_config.KAFKA_CONSUMER_GROUP)

    producer = kafka_producer.get_kafka_producer(broker_url=kafka_config.KAFKA_BROKER)

    while True:
        try:
            for message in consumer:
                # Deserialize the JSON data from the message
                message_value = message.value.decode('utf-8').strip()

                if not message_value:
                    print("Empty message...")
                    continue
                try:
                    data = json.loads(message_value.replace("'", '"'))
                    process_message(data, producer)
                except json.JSONDecodeError:
                    error_message= f"Error decoding JSON: {message_value}"
                    log_error(error_message)
                except ValueError:
                    pass
                except KeyboardInterrupt:
                    print("Consumer inturrupted by user.")

        except json.JSONDecodeError as e:
            log_error(f"Error decoding JSON message: {e}")
        except KeyError as e:
            log_error(f"Missing key in message: {e}")
        finally:
            producer.close()
            consumer.close()

# Run the processor
if __name__ == "__main__":
    process_messages()
