from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

def get_influx_writer(url, token, org):
    
    try:
        client = InfluxDBClient(url, token, org)
        return client.write_api(write_options=SYNCHRONOUS)
    except Exception as e:
        print(f"Failed to create InfluxDB writer: {e}")
        raise