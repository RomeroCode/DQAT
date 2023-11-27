import os
import csv
import signal
import time
import sys
from concurrent.futures import ThreadPoolExecutor
from dateutil import parser
from integration.connectors import kafka_producer
from config import kafka_config
from integration.adapters import log_setup


# Logging configuration
logging = log_setup.setup_logger('producer', 'producer_errors.log', 3)

# Flag to control execution state
shutdown_requested = False

# Handler to catch interruption signal
def signal_handler(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    print("Shutdown signal received.")
    
# Configure handler to signal SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, signal_handler)

# Logging function to record errors
def log_error(message):
    logging.error(message)

# Function to read data from CSV and produce events to Kafka
def produce_data(file_path):
    producer = kafka_producer.get_kafka_producer(broker_url=kafka_config.KAFKA_BROKER)
    global shutdown_requested

    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        previous_timestamp = None

        for row in reader:
            if shutdown_requested:
                    break
            try:
                timestamp_str = row['created_at']

                # Parse timestamp in different formats
                current_timestamp = parser.parse(timestamp_str)

                # Calculate time difference between events
                if previous_timestamp:
                    time_difference = (current_timestamp - previous_timestamp).total_seconds()
                    # Adjust the waiting time
                    sleep_time = 0
                    while sleep_time < time_difference and not shutdown_requested:
                        time.sleep(min(0.1, time_difference - sleep_time))
                        sleep_time += 0.1
                        
                if shutdown_requested:
                    # Send message to Kafka before stops
                    shutdown_message = "Processing was stopped for: " + file_path
                    producer.send(kafka_config.KAFKA_SENSOR_READINGS_TOPIC, shutdown_message.encode('utf-8'))
                    print(shutdown_message)
                    return
                
                # Update previous timestamp
                previous_timestamp = current_timestamp

                # Get the filename without the extension
                filename = os.path.splitext(os.path.basename(file_path))[0]

                # Dynamic header CSV
                event_data = {'filename': filename, 'timestamp': current_timestamp.strftime("%Y-%m-%d %H:%M:%S")}
                for key, value in row.items():
                    if key != 'created_at':
                        if (type(value) == str):
                            cleaned_value = value.replace(',', '')
                        
                        # Try convert to float
                            try:
                                event_data[key] = float(cleaned_value)

                            except ValueError:
                                event_data[key] = cleaned_value
                        
                # Convert the data to a string and produce to Kafka topic
                message = str(event_data).encode('utf-8')
                producer.send(kafka_config.KAFKA_SENSOR_READINGS_TOPIC, message)

                print(f"Event produced: {event_data}")
                
                

            except Exception as e:
                error_message = f"Error processing the file {file_path}: {e}"
                print(error_message)
                log_error(error_message)

    producer.close()

# Function to process each file using ThreadPoolExecutor
def process_files(directory):
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(produce_data, os.path.join(directory, filename)): filename for filename in os.listdir(directory) if filename.endswith(".csv")}
        #futures = executor.submit(produce_data, '../../data/IoTPond8.csv')
        # Wait for all futures to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                error_message = f"Error processing the file {futures[future]}: {e}"
                print(error_message)
                log_error(error_message)

# Example usage
if __name__ == "__main__":
    data_directory = "../../data"
    process_files(data_directory)
    sys.exit()
