import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from events.producers import data_producer
from events.processors import header_processor
from persistence import load_influxdb

def process_all_sensors(base_directory):
    with ThreadPoolExecutor() as executor:
        
        # Submit the header processor task once for all directories
        executor.submit(header_processor.process_messages)
        executor.submit(load_influxdb.write_to_db)

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

# Example usage
if __name__ == "__main__":
    BASE_DATA_DIRECTORY = "data/sensors"  # Path to the base directory where sensor data are stored
    process_all_sensors(BASE_DATA_DIRECTORY)
