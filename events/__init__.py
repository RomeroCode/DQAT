import os
import sys

from concurrent.futures import ThreadPoolExecutor, as_completed
from producers import data_producer

def process_all_sensors(base_directory):
    with ThreadPoolExecutor() as executor:
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
    base_data_directory = "../data/sensors"  # Path to the base directory where sensor data are stored
    process_all_sensors(base_data_directory)
