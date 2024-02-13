from config import influx_config
from config import kafka_config
from integration.connectors import influx_connector
from integration.connectors import kafka_consumer
from integration.adapters import log_setup

# Configuring Logs
logging = log_setup.setup_logger('Load InfluxDB', 'db_errors.log', 3)

# Logging function to record errors
def log_error(message):
    logging.error(message)
    
def write_to_db(topic_name):
    """
    Consume messages from a Kafka topic and process them.
    """
    consumer = kafka_consumer.get_kafka_consumer(topic_name=topic_name,
                                                 broker_url=kafka_config.KAFKA_BROKER,
                                                 group_id=kafka_config.KAFKA_CONSUMER_GROUP)

    
    while True:
        try:
            for message in consumer:
                # Deserialize the JSON data from the message
                message_value = message.value.decode('utf-8').strip()
                
                if not message_value:
                    print("Empty message...")
                    continue
                try:
                    writer = influx_connector.get_influx_writer(url=influx_config.INFLUXDB_URL,
                                                                token=influx_config.INFLUXDB_TOKEN,
                                                                org=influx_config.INFLUXDB_ORG)
                    print(f"Data write: {message_value}")
                    writer.write(bucket=influx_config.INFLUXDB_BUCKET,record=message_value)
                    writer.flush()
                    writer.close()
                except KeyboardInterrupt:
                    print("Consumer inturrupted by user.")
                except Exception as e:
                    error_message= f"Error writing in the DB: {message_value}"
                    log_error(error_message)
        except Exception as e:
            error_message= f"Error decoding JSON: {message_value}"
            log_error(error_message)
        finally:
            consumer.close()
                    
                    
    

# Run the processor
if __name__ == "__main__":
    write_to_db(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_OK)
    
