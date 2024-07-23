from river import stream, preprocessing, anomaly
import numpy as np

data_stream = stream.iter_csv('../../data/sensors/Pond12/IoTPond12.csv')

# Modelo OneClassSVM
model = anomaly.OneClassSVM(nu=0.1)  
scaler = preprocessing.StandardScaler()

float_columns = ['Temperature (C)', 'Turbidity(NTU)', 'Dissolved Oxygen(g/ml)',
                 'PH', 'Ammonia(g/ml)', 'Nitrate(g/ml)', 'Population', 
                 'Fish_Length(cm)', 'Fish_Weight(g)']

ignore_columns = ['created_at', 'entry_id']

scores = []

anomalias = 0

for x, y in data_stream:
    # Converter colunas para float (com tratamento de exceção)
    for col in float_columns:
        try:
            # Converter para float, ignorando valores vazios
            if x[col] != '':
                x[col] = float(x[col])
            else:
                # Substituir valores vazios por um valor padrão (ex: 0.0)
                x[col] = 0.0
        except (ValueError, KeyError):
            pass

    # Remover colunas (com tratamento de exceção)
    for col in ignore_columns:
        x.pop(col, None)

    x_scaled = scaler.learn_one(x).transform_one(x)
    score = model.score_one(x_scaled)
    scores.append(score)
    model.learn_one(x_scaled)

    # Calcular limiar dinâmico (por exemplo, 95º percentil)
    threshold = np.percentile(scores, 95)
    #print(x)
    if score > threshold:
        # Obter os pesos do modelo e suas importâncias relativas
        weights = model.weights
        # Converter os valores dos pesos para uma lista antes de aplicar np.abs
        total_weight = sum(np.abs(list(weights.values())))  
        importances = {col: np.abs(weight) / total_weight for col, weight in weights.items()}
        anomalias = anomalias + 1
        #print(f"Anomalia detectada: {x}")
        #for col, importance in importances.items():
            #print(f"  - {col}: {importance:.2%}")

print(anomalias)
        