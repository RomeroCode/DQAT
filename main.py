"""
This module is responsible for processing IoT sensor data for anomaly detection. 
It utilizes a thread pool executor for concurrent processing of multiple sensor data directories.

The module performs the following tasks:

1. Header Processing: Extracts and standardizes the header information from the sensor data files.
2. Data Profiling: Analyzes the statistical properties of the sensor data to gain insights.
3. Anomaly Detection: Applies a Half-Space Trees anomaly detection algorithm 
to identify unusual patterns in the sensor data.
4. Persistence: Stores the processed sensor data and anomaly detection results in an 
InfluxDB database.

Example usage:
python3 main.py
"""


import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from events.producers import data_producer
from events.processors import anomaly, header_processor, data_profiling
from persistence import load_influxdb



def process_all_sensors(base_directory):
    """
    Processes all sensor data directories found within the specified base directory.
    Args:
    base_directory (str): The path to the base directory containing the sensor data directories.
    """
    with ThreadPoolExecutor(max_workers=20) as executor:
        # Submit the header processor task once for all directories
        executor.submit(header_processor.process_messages)
        executor.submit(load_influxdb.write_to_db)
        executor.submit(data_profiling.evaluate)
        executor.submit(anomaly.evaluate)

        # Create a list to hold all the future objects
        futures = []

        # Iterate over all subdirectories in the base directory
        for sensor_name in os.listdir(base_directory):
            sensor_directory = os.path.join(base_directory, sensor_name)

            # Check if it's a directory (not a file)
            if os.path.isdir(sensor_directory):
                print(f"Scheduling processing for sensor: {sensor_name}")

                # Submit the processing task for each sensor directory
                future = executor.submit(data_producer.process_files, sensor_directory)
                futures.append(future)

        try:
            # Wait for all submitted tasks to complete
            for future in as_completed(futures):
                future.result()
        except KeyboardInterrupt:
            print("Interrupt received, shutting down...")
            executor.shutdown(wait=False)  # This will immediately stop the executor
            sys.exit(1)


if __name__ == "__main__":
    BASE_DATA_DIRECTORY = "data/ai4i"  # Path to the base directory where sensor data are stored
    process_all_sensors(BASE_DATA_DIRECTORY)
