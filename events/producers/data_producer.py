import os
import csv
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
from dateutil import parser
from datetime import datetime

# Logging configuration
log_directory = "../../monitoring/logs/"
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, "error_log.txt")
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename=log_file_path, level=logging.ERROR, format=log_format, datefmt='%Y-%m-%d %H:%M:%S')

# Kafka settings
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'sensor_readings'

# Logging function to record errors
def log_error(message):
    logging.error(message)

# Function to read data from CSV and produce events to Kafka
def produce_data(file_path):
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        previous_timestamp = None

        for row in reader:
            try:
                timestamp_str = row['created_at']

                # Parse timestamp in different formats
                current_timestamp = parser.parse(timestamp_str)

                # Calculate time difference between events
                if previous_timestamp:
                    time_difference = (current_timestamp - previous_timestamp).total_seconds()
                    # Adjust the waiting time
                    time.sleep(time_difference)

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
                producer.send(KAFKA_TOPIC, message)

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
