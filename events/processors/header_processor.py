import json
from integration.connectors import kafka_consumer
from config import kafka_config
from integration.adapters import log_setup

# Configuring Logs
logging = log_setup.setup_logger('header', 'header_errors.log', 3)


# Expected headers in the data
expected_headers = [
    'filename', 'timestamp', 'entry_id', 'Temperature (C)', 'Turbidity(NTU)',
    'Dissolved Oxygen(g/ml)', 'PH', 'Ammonia(g/ml)', 'Nitrate(g/ml)',
    'Population', 'Fish_Length(cm)', 'Fish_Weight(g)'
]

# Logging function to record errors
def log_error(message):
    logging.error(message)
    

def check_headers(data, expected_headers):
    """
    Check if all the expected headers are present in the data.
    """
    
    # Normalizes headers
    normalized_received_headers = {header.replace(" ", "").lower() for header in data.keys()}

    for header in normalized_received_headers:
        print(f"Header:{header}")
    return all(header in data for header in expected_headers)

def process_message(data):
    """
    Process a single message consumed from Kafka.
    """
    
    # Verify if the data contains all expected headers
    if not check_headers(data, expected_headers):
        print(f"Data is missing headers: {data}")
        # Handle missing headers
        error_message = f"Data is missing headers: {data}"
        log_error(error_message)
    else:
        # If headers are correct, process the data
        print(f"Data is valid and processing can proceed: {data}")
        # Implement your data processing logic here

def consume_messages():
    """
    Consume messages from a Kafka topic and process them.
    """
    consumer = kafka_consumer.get_kafka_consumer(topic_name=kafka_config.KAFKA_SENSOR_READINGS_TOPIC,
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
                    data = json.loads(message_value.replace("'", '"'))
                    process_message(data)
                except json.JSONDecodeError:
                    error_message= f"Error decoding JSON: {message_value}"
                    log_error(error_message)
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
    consume_messages()
    
