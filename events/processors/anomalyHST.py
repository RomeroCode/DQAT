import json
from config import kafka_config
from config import app_config
from integration.adapters import log_setup
from integration.connectors import kafka_consumer
from persistence import load_influxdb
from river import anomaly
from river import preprocessing
from river import utils
import numpy as np

# Configuring Logs
logging = log_setup.setup_logger('Anomaly Detection', 'anomaly_errors.log', 3)

# Logging function to record errors
def log_error(message):
    logging.error(message)


def evaluate(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED):
    """
    Consume messages from a Kafka topic and evaluate anomaly from them.
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
