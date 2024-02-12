import json
from integration.connectors import kafka_consumer
from integration.connectors import kafka_producer
from config import kafka_config
from config import app_config
from integration.adapters import log_setup

# Configuring Logs
logging = log_setup.setup_logger('header', 'header_errors.log', 3)

# Logging function to record errors
def log_error(message):
    logging.error(message)
    

def check_headers(data, expected_headers, producer):
    """
    Check if all the expected headers are present in the data.
    """
    
    # Normalizes headers
    normalized_received = {header.replace(" ", "").lower(): value for header, value in data.items()}
    if all(header in normalized_received.keys() for header in expected_headers):
        producer.send(kafka_config.KAFKA_SENSOR_HEADERS_OK, str(normalized_received).encode('utf-8'))
        return True
    else:
        missing = [header for header in expected_headers if header not in normalized_received.keys()] 
        normalized_received["missing headers"] = missing
        producer.send(kafka_config.KAFKA_SENSOR_MISSING_HEADERS, str(normalized_received).encode('utf-8'))
        raise ValueError(f"Data is missing headers: {missing}")
        

def process_message(data, producer):
    """
    Process a single message consumed from Kafka.
    """
    
    # Verify if the data contains all expected headers
    try:
        check_headers(data, app_config.EXPECTED_HEADERS, producer)
        print(f"Data is valid and processing can proceed: {data}")
        return True
    except ValueError as data_missing:
        print(f"{data_missing} in file {data.get('filename')}")
        log_error(f"{data_missing} in file {data.get('filename')}")
         

def process_messages():
    """
    Consume messages from a Kafka topic and process them.
    """
    consumer = kafka_consumer.get_kafka_consumer(topic_name=kafka_config.KAFKA_SENSOR_READINGS_TOPIC,
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
        except Exception as e:
            error_message= f"An error occurred trying to consume topic: {e}"
            print(error_message)
            log_error(error_message)
        finally:
            consumer.close()

# Run the consumer process
if __name__ == "__main__":
    process_messages()
    
