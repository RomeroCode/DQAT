"""
This module implements an anomaly detection system for IoT sensor data using the River library. 
It consumes messages from a Kafka topic, preprocesses the data, and applies the HalfSpaceTrees 
algorithm to detect anomalies in real-time.

Features:
- Consumes sensor data from a Kafka topic.
- Handles missing and invalid data with error logging.
- Preprocesses the data using MinMaxScaler.
- Detects anomalies using HalfSpaceTrees.
- Stores detected anomalies in InfluxDB.

Dependencies:
- River: A machine learning library for online learning and data streams.
- NumPy: A library for numerical operations in Python.
- Kafka: A distributed streaming platform.
- InfluxDB: A time series database.
"""

import json
import numpy as np
from river import anomaly
from river import preprocessing
from config import kafka_config
from config import app_config
from integration.adapters import log_setup
from integration.connectors import kafka_consumer
from persistence import load_influxdb


# Configuring Logs
logging = log_setup.setup_logger('Anomaly Detection', 'anomaly_errors.log', 3)

# Logging function to record errors
def log_error(message):
    """
    Logs an error message.

    Args:
        message (str): The error message to be logged.
    """
    logging.error(message)


def evaluate(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED):
    """
    Consumes messages from a Kafka topic, preprocesses the data, and evaluates anomalies.

    Args:
        topic_name (str, optional): The name of the Kafka topic to consume from
                                  Defaults to the configured topic in kafka_config.
    """
    float_columns = app_config.NUMERIC_HEADERS

    consumer = kafka_consumer.get_kafka_consumer(
        topic_name=topic_name,
        broker_url=kafka_config.KAFKA_BROKER,
        group_id=kafka_config.KAFKA_ANOMALY_GROUP
    )

    model = anomaly.HalfSpaceTrees(seed=42,window_size=100)
    scaler = preprocessing.MinMaxScaler()

    scores = []

    try:
        for message in consumer:
            message_value = message.value.decode('utf-8').strip()
            data = json.loads(message_value.replace("'", '"'))

            if not data:
                print("Empty message...")
                continue

            for col in float_columns:
                # Converter e validar os dados
                if col in data:  # Verificar se a coluna existe no dicionário
                    try:
                        data[col] = float(data[col])
                    except (ValueError, KeyError):
                        log_error(f"Error converting or missing value for column '{col}': {data}")
                        continue  # Ignorar a mensagem se houver erro de conversão ou valor ausente
                else:
                    #Lidar com a coluna ausente (por exemplo, definir um valor padrão)
                    log_error(f"Missing header '{col}': {data}, filled with 0.0")
                    data[col] = 0.0

            # Remover colunas desnecessárias
            filtered_data = {col: data[col] for col in float_columns}

            #print(filtered_data)
            # Calcular pontuação de anomalia e atualizar modelo
            x_scaled = scaler.learn_one(filtered_data).transform_one(filtered_data)
            score = model.score_one(x_scaled)  # Pontuação de anomalia
            scores.append(score)
            model.learn_one(x_scaled)

            # Calcular limiar dinâmico e verificar anomalia
            threshold = np.percentile(scores, 95) if scores else 0
            if score > threshold:
                print(f"Anomalia detectada: {data}, score: {score}")
                load_influxdb.write_anomaly(data, score)


    except KeyboardInterrupt:
        print("Consumer interrupted by user.")
    finally:
        consumer.close()

# Run Anomaly Detection
if __name__ == "__main__":
    evaluate(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED)
