import os
import csv
import time
import threading
from kafka import KafkaProducer

# Kafka settings
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'sensor_readings'

# Function to read data from CSV and produce events to Kafka
def produce_data(file_path):
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        previous_timestamp = None

        for row in reader:
            current_timestamp = row['created_at']
            
            # Calculate time difference between events
            if previous_timestamp:
                time_difference = (
                    time.mktime(time.strptime(current_timestamp, "%Y-%m-%d %H:%M:%S CET")) -
                    time.mktime(time.strptime(previous_timestamp, "%Y-%m-%d %H:%M:%S CET"))
                )
                # Adjust the waiting time
                time.sleep(time_difference)

            # Update previous timestamp
            previous_timestamp = current_timestamp

            # Get the filename without the extension
            filename = os.path.splitext(os.path.basename(file_path))[0]

            # Adapt as necessary for the fields in your CSV
            event_data = {
                'filename' : filename,
                'timestamp': current_timestamp,
                'entry_id': row['entry_id'],
                'temperature': float(row['Temperature (C)']),
                'turbidity': float(row['Turbidity(NTU)']),
                'ammonia': float(row['Ammonia(g/ml)']),
                'nitrate': float(row['Nitrate(g/ml)']),
                'ph': float(row['PH']),
                'do': float(row['Dissolved Oxygen(g/ml)']),
                'population': row['Population'],
                'fish_length': float(row['Fish_Length(cm)']),
                'fish_weight': float(row['Fish_Weight(g)'])
            }

            # Convert the data to a string and produce to Kafka topic
            message = str(event_data).encode('utf-8')
            producer.send(KAFKA_TOPIC, message)

            print(f"Event produced: {event_data}")

    producer.close()

# Function to process each file in a separate thread
def process_file(file_path):
    print(f"Processing file: {file_path}")
    produce_data(file_path)

# Function to iterate over all files in a directory and start a thread for each file
def process_files(directory):
    threads = []
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            thread = threading.Thread(target=process_file, args=(file_path,))
            threads.append(thread)
            thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

# Example usage
if __name__ == "__main__":
    data_directory = "../../data"
    process_files(data_directory)
