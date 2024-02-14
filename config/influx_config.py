INFLUXDB_URL = 'http://localhost:8086'
INFLUXDB_ORG = 'ufabc'
INFLUXDB_BUCKET = 'IoT'

try:
    with open('config/secrets/.TOKEN_INFLUX', 'r') as file:
        token = file.read().strip()
    INFLUXDB_TOKEN = token
except IOError as e:
    print(f"Failed to get token: {e}")
    INFLUXDB_TOKEN = None