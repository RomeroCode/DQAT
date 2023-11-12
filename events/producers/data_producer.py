# data_producer.py

import csv
import time
import random
from kafka import KafkaProducer

# Kafka settings
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'sensor_readings'

# Function to read data from CSV and produce events to Kafka
def produce_data(file_path):
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Simulate a small delay to mimic real-time data
            time.sleep(random.uniform(0.1, 0.5))

            # Adapt as needed for the fields in your CSV
            event_data = {
                'timestamp': row['created_at'],
                'temperature': float(row['Temp']),
                'turbidity': float(row['Turbidity']),
                'ammonia': float(row['Ammonia']),
                'nitrate': float(row['Nitrate']),
                'ph': float(row['PH']),
                'do': float(row['DO']),
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'elevation': row['elevation'],
                'status': row['status']
            }

            # Convert the data to a string and produce to Kafka topic
            message = str(event_data).encode('utf-8')
            producer.send(KAFKA_TOPIC, message)

            print(f"Event produced: {event_data}")

    producer.close()

# Example usage
if __name__ == "__main__":
    file_path = "/home/marcos_romero/QualityTool/data/IoTPond1.csv"
    produce_data(file_path)
