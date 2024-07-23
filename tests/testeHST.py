import json
import numpy as np
from river import anomaly
from river import preprocessing
#from integration.adapters import log_setup
#from persistence import load_influxdb
from ucimlrepo import fetch_ucirepo


ai4i_2020_dataset = fetch_ucirepo(id=601)

# Configuring Logs
#logging = log_setup.setup_logger('Anomaly Detection HST test', 'anomaly_errors_hst.log', 3)

"""
# Logging function to record errors
def log_error(message):

    Logs an error message.

    Args:
        message (str): The error message to be logged.

    logging.error(message)
"""

""" 
def evaluate(dataframe):
    scores_hst = []
    scores_svm = []
    model_hst = anomaly.HalfSpaceTrees(seed=42,window_size=50)
    scaler_minmax = preprocessing.MinMaxScaler()
    model_svm = anomaly.OneClassSVM(nu=0.1)
    scaler_standard = preprocessing.StandardScaler()
    
    #helping function for anomaly detector
    def _anomaly_evaluate(model, scaler, data, scores):
        x_scaled = scaler.learn_one(data).transform_one(data)
        score = model.score_one(x_scaled)  # Anomaly Score
        scores.append(score)
        model.learn_one(x_scaled)
        return model, scaler, score, scores

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
            
            model_hst, scaler_minmax, score_hst, scores_hst = _anomaly_evaluate(model_hst,
                                                                                scaler_minmax,
                                                                                filtered_data,
                                                                                scores_hst)
            model_svm, scaler_standard, score_svm, scores_svm = _anomaly_evaluate(model_svm,
                                                                                  scaler_standard,
                                                                                  filtered_data,
                                                                                  scores_svm)

            #print(score_hst, scores_hst)
            # Calcular limiar dinâmico e verificar anomalia
            threshold_hst = np.percentile(scores_hst, 95) if scores_hst else 0
            if score_hst > threshold_hst:
                print(f"Anomaly HST detected: {data}, score: {score_hst}")
                load_influxdb.write_anomaly(data, score_hst, method="HST")

            
            if score_svm > 0.1:
                print(f"Anomaly SVM detected: {data}, score: {score_svm}")
                load_influxdb.write_anomaly(data, score_svm, method="SVM")


    except KeyboardInterrupt:
        print("Consumer interrupted by user.")
    finally:
        consumer.close() """

# Run Anomaly Detection
if __name__ == "__main__":
    # data (as pandas dataframes) 
    X = ai4i_2020_dataset.data.features 
    y = ai4i_2020_dataset.data.targets 
    
    # metadata 
    #print(statlog_shuttle.metadata) 
    print(X)
    print(y)
    # variable information 
    #print(statlog_shuttle.variables) 
    #evaluate(topic_name=kafka_config.KAFKA_SENSOR_HEADERS_NORMALIZED)
