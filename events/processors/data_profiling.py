import json
from config import kafka_config
from config import app_config
from integration.adapters import log_setup
from integration.connectors import kafka_consumer
from persistence import load_influxdb
from river import stats
from river import utils

# Configuring Logs
logging = log_setup.setup_logger('Data Profiling', 'data_profiling_errors.log', 3)

# Logging function to record errors
def log_error(message):
    logging.error(message)


def create_stats(expected_headers, WINDOW_SIZE=50):
    stats_dict = {}
    for header in expected_headers:
        stats_dict[header] ={
            'maximum': stats.RollingMax(WINDOW_SIZE),
            'minimum': stats.RollingMin(WINDOW_SIZE),
            'mode': stats.RollingMode(WINDOW_SIZE),
            'peak_to_peak': stats.RollingPeakToPeak(WINDOW_SIZE),
            'median': stats.RollingQuantile(0.5, WINDOW_SIZE),
            'first_quartile': stats.RollingQuantile(0.25, WINDOW_SIZE),
            'mean': utils.Rolling(stats.Mean(), WINDOW_SIZE),
            'variance': utils.Rolling(stats.Var(), WINDOW_SIZE)
        }
    return stats_dict

def evaluate(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED):
    """
    Consume messages from a Kafka topic and evaluate data profiling from them.
    """    
   
    consumer = kafka_consumer.get_kafka_consumer(topic_name=topic_name,
                                                 broker_url=kafka_config.KAFKA_BROKER,
                                                 group_id=kafka_config.KAFKA_DATA_PROFILING_GROUP)
    
    total_stats_dict = dict(Total = create_stats(app_config.NUMERIC_HEADERS))
    
    
    while True:
        try:
            for message in consumer:
                # Deserialize the JSON data from the message
                message_value = message.value.decode('utf-8').strip()
                data = json.loads(message_value.replace("'", '"'))
                
                if not data:
                    print("Empty message...")
                    continue
                else:
                    filename = data.get("filename")
                    timestamp = data.get("timestamp")
                    if filename not in total_stats_dict:
                        total_stats_dict[filename] = create_stats(app_config.NUMERIC_HEADERS)
                    for parameter, value in data.items():
                        if parameter in total_stats_dict[filename]:
                            for _, stat_function in total_stats_dict[filename][parameter].items():
                                stat_function.update(value)
                            load_influxdb.write_profiling(total_stats_dict[filename], filename, timestamp)
        except KeyboardInterrupt:
                    print("Consumer inturrupted by user.")
                    raise
        except Exception as e:
            error_message= f"Error loading: {data} because of  {e}"
            log_error(error_message)
        finally:
            consumer.close()
    

# Run Data Profiling
if __name__ == "__main__":
    evaluate(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED)