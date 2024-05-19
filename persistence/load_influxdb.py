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
                                            data['filename']).time(data['timestamp'],WritePrecision.NS)
                    
                        
                    for key, value in data.items():
                        #print(f'Key: {key} {type(key)}, Value: {value} {type(value)}')
                        if key not in ['filename', 'timestamp']:
                            if key == '':
                                continue
                            elif key in 'missing headers':
                                point = point.field(key, ','.join(value))
                            else:
                                point = point.field(key, value)

                    #print(f"Data write: {point}")
                    
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


def write_profiling(stats_dict, filename, timestamp):
    try:
        writer = influx_connector.get_influx_writer(url=influx_config.INFLUXDB_URL,
                                                                token=influx_config.INFLUXDB_TOKEN,
                                                                org=influx_config.INFLUXDB_ORG)
                    
        #Formating data to write into InfluxDB and adjust timestamp to now
        for parameter, stats in stats_dict.items():
            point = (Point("Data Profiling")
                    .tag("filename", filename)
                    .tag("parameter", parameter)
                    .field("maximum", stats.get("maximum").get())
                    .field("minimum", stats.get("minimum").get())
                    .field("mode", stats.get("mode").get())
                    .field("peak_to_peak", stats.get("peak_to_peak").get())
                    .field("median", stats.get("median").get())
                    .field("first_quartile", stats.get("first_quartile").get())
                    .field("mean", stats.get("mean").get())
                    .field("variance", stats.get("variance").get())
                    .time(timestamp, WritePrecision.NS)
                )
            writer.write(bucket=influx_config.INFLUXDB_BUCKET,org=influx_config.INFLUXDB_ORG,
                                 record=point)
            writer.close()
    except Exception as e:
        error_message= f"Error writing in the DB: {stats_dict} because of {e}"
        log_error(error_message)                  

def write_anomaly(data, score):
    try:
        writer = influx_connector.get_influx_writer(url=influx_config.INFLUXDB_URL,
                                                    token=influx_config.INFLUXDB_TOKEN,
                                                    org=influx_config.INFLUXDB_ORG)
        point = Point("Anomaly").tag("filename", 
                    data['filename']).time(data['timestamp'], 
                    WritePrecision.NS)
        
        for key, value in data.items():
            if key not in ['filename', 'timestamp']:
                if key == '':
                    continue
                elif key in 'missing headers':
                    point = point.field(key, ','.join(value))
                else:
                    point = point.field(key, value)
        point = point.field("score", score)
        point = point.tag("score", score)
        writer.write(bucket=influx_config.INFLUXDB_BUCKET,org=influx_config.INFLUXDB_ORG,
                    record=point)
        writer.close()
                                
    except Exception as e:
        error_message= f"Error writing in the DB: {data} because of {e}"
        log_error(error_message)       

# Run the processor
if __name__ == "__main__":
    write_to_db(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED)
    
