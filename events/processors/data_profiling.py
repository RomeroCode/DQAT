"""
This module performs data profiling on data received from a Kafka topic. 

It calculates rolling statistics for each sensor parameter, including maximum,
minimum, mode, peak-to-peak, median, quartiles, mean, and variance.

The calculated statistics are then stored in an InfluxDB database.

Dependencies:
- River: A machine learning library for online learning and data streams.
"""


import json
from river import stats
from river import utils
from config import kafka_config
from config import app_config
from integration.adapters import log_setup
from integration.connectors import kafka_consumer
from persistence import load_influxdb


# Configuring Logs
logging = log_setup.setup_logger('Data Profiling', 'data_profiling_errors.log', 3)

# Logging function to record errors
def log_error(message):
    """
    Logs an error message to the specified log file.

    Args:
        message (str): The error message to log.
    """
    logging.error(message)


def create_stats(expected_headers, window_size=50):
    """
    Creates a dictionary of River rolling statistics functions for the given headers.

    Args:
        expected_headers (list): A list of header names for which statistics will be calculated.
        window_size (int, optional): The window size for rolling statistics. Defaults to 50.

    Returns:
        dict: A dictionary where keys are header names and values are dictionaries of rolling 
        statistics functions.
    """
    stats_dict = {}
    for header in expected_headers:
        stats_dict[header] ={
            'maximum': stats.RollingMax(window_size),
            'minimum': stats.RollingMin(window_size),
            'mode': stats.RollingMode(window_size),
            'peak_to_peak': stats.RollingPeakToPeak(window_size),
            'median': stats.RollingQuantile(0.5, window_size),
            'first_quartile': stats.RollingQuantile(0.25, window_size),
            'mean': utils.Rolling(stats.Mean(), window_size),
            'variance': utils.Rolling(stats.Var(), window_size)
        }
    return stats_dict

def evaluate(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED):
    """
    Consumes messages from a Kafka topic, calculates rolling statistics, and writes them 
    to InfluxDB.

    Args:
        topic_name (str, optional): The name of the Kafka topic to consume from. 
                                  Defaults to the configured topic in kafka_config.
    """

    consumer = kafka_consumer.get_kafka_consumer(topic_name=topic_name,
                                                 broker_url=kafka_config.KAFKA_BROKER,
                                                 group_id=kafka_config.KAFKA_DATA_PROFILING_GROUP)

    total_stats_dict = {"Total": create_stats(app_config.NUMERIC_HEADERS)}

    def _process_message(message, total_stats_dict):
        """Helper function to process a single Kafka message."""
        try:
            message_value = message.value.decode("utf-8").strip()
            data = json.loads(message_value.replace("'", '"'))

            if not data:
                print("Empty message...")
                return  # Return early to avoid nesting

            filename = data.get("filename")
            timestamp = data.get("timestamp")
            if filename not in total_stats_dict:
                total_stats_dict[filename] = create_stats(app_config.NUMERIC_HEADERS)

            for parameter, value in data.items():
                if parameter in total_stats_dict[filename]:
                    for _, stat_function in total_stats_dict[filename][parameter].items():
                        try:
                            stat_function.update(float(value))  # Explicitly convert to float
                        except ValueError:
                            log_error(f""" Error converting value '{value}' for parameter
                                      '{parameter}' in file '{filename}' to float.""")
                        load_influxdb.write_profiling(total_stats_dict[filename], filename,
                                                      timestamp)
        except json.JSONDecodeError as e:
            log_error(f"Error decoding JSON message: {e}")
        except KeyError as e:
            log_error(f"Missing key in message: {e}")

    while True:
        try:
            for message in consumer:
                _process_message(message, total_stats_dict)
        except KeyboardInterrupt:
            print("Consumer inturrupted by user.")
            raise
        finally:
            consumer.close()

# Run Data Profiling
if __name__ == "__main__":
    evaluate(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED)
