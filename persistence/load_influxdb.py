import json
from datetime import datetime
from config import influx_config
from config import kafka_config
from integration.connectors import influx_connector
from integration.connectors import kafka_consumer
from integration.adapters import log_setup
from influxdb_client import Point,WritePrecision


# Configuring Logs
logging = log_setup.setup_logger('Load InfluxDB', 'db_errors.log', 3)

# Logging function to record errors
def log_error(message):
    logging.error(message)
    
def write_to_db(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED):
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
                data = json.loads(message_value.replace("'", '"'))

                if not data:
                    print("Empty message...")
                    continue
                try:
                    writer = influx_connector.get_influx_writer(url=influx_config.INFLUXDB_URL,
                                                                token=influx_config.INFLUXDB_TOKEN,
                                                                org=influx_config.INFLUXDB_ORG)
                    
                    #Formating data to write into InfluxDB and adjust timestamp to now
                    point = Point("IoT").tag("filename", 
                                            data['filename']).time(int(datetime.now().timestamp() *1e9), 
                                            WritePrecision.NS)
                    
                        
                    for key, value in data.items():
                        print(f'Key: {key} {type(key)}, Value: {value} {type(value)}')
                        if key not in ['filename', 'timestamp']:
                            if key is '':
                                continue
                            elif key in 'missing headers':
                                point = point.field(key, ','.join(value))
                            else:
                                point = point.field(key, value)

                    print(f"Data write: {point}")
                    
                    writer.write(bucket=influx_config.INFLUXDB_BUCKET,org=influx_config.INFLUXDB_ORG,
                                 record=point)
                    writer.close()
                except KeyboardInterrupt:
                    print("Consumer inturrupted by user.")
                    raise
                except Exception as e:
                    error_message= f"Error writing in the DB: {data} because of {e}"
                    log_error(error_message)
        except KeyboardInterrupt:
                    print("Consumer inturrupted by user.")
                    raise
        except Exception as e:
            error_message= f"Error loading: {data} because of  {e}"
            log_error(error_message)
        finally:
            consumer.close()
                    
                    
    

# Run the processor
if __name__ == "__main__":
    write_to_db(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED)
    
