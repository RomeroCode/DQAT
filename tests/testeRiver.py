import numpy as np
import pandas as pd
from river import stream, preprocessing, anomaly, compose
import os

class Data:
    """A helper class to load various data.

    Args:
        data_base_path (str): Base path that contains the data files.
    """

    def __init__(self, data_base_path="data"):
        self.data_base_path = data_base_path
        
def get_data(self, data_file):
        """Loads the data given the path.

        Args:
            data_file: Path of the data.

        Returns:
            X (np.array of shape (num_instances, num_features)): Feature vectors.
            y (np.array of shape (num_instances,)): Labels.
        """
        data_path = os.path.join(self.data_base_path, data_file)

        if ".mat" in data_file:
            from scipy.io import loadmat

            f = loadmat(data_path)

            X = f['X']
            y = f['y'].ravel()
        else:
            X = self._load_via_txt(data_path)

            y = X[:, -1].ravel()
            X = X[:, :-1]

        return X, y
    
def evaluate(model, window_size):
    data = Data("data")
    X_all, y_all = get_data(data,"cardio.mat")
    data_stream = stream.iter_array(X_all,y_all)
    
    if model == 'HST':
        model = compose.Pipeline(
            preprocessing.MinMaxScaler(),            
            anomaly.HalfSpaceTrees(n_trees=25, height=15, window_size=window_size, seed=42)
        )
    if model == 'SVM':
        model = compose.Pipeline(
            preprocessing.StandardScaler(),
            anomaly.OneClassSVM(nu=window_size)
        )
    
    if model == 'LOF':
        model = anomaly.LocalOutlierFactor(n_neighbors=30)
        train = 0
        for X, y in data_stream:
            if train < window_size:
                model.learn_one(X)
                train = train + 1
            else:
                break
        
    scores = []
    tp = 0
    fp = 0
    tn = 0
    fn = 0
    
    # Iterate over examples.
    for X, y in data_stream:
        score = model.score_one(X)
        
        if model != 'LOF':
            model.learn_one(X)
        
        scores.append(score)
        
        # Dynamicly build the threshold
        threshold = np.percentile(scores, 80) if scores else 0

        # Considering negative as anomalous data
        if score > threshold:
            if y == 1:
                tn = tn + 1
            else:
                fn = fn + 1
        else:
            if y == 1:
                fp = fp + 1
            else:
                tp = tp + 1
                
        if (tp + tn + fn + fp) > 0:
            accuracy = (tp + tn) / (tp + tn + fn + fp)
        else:
            accuracy = 0
        accuracy_result = f'{accuracy * 100:.2f}'
        
        if (tn + fn) > 0:
            precision = (tn) / (tn + fn)
        else: 
            precision = 0
        precision_result = f'{precision * 100:.2f}'
        
        if (tn + fp) > 0:
            recall = (tn) / (tn + fp)
        else:
            recall = 0
        recall_result = f'{recall * 100:.2f}'
        
        if precision + recall > 0:
            f1_score = f'{(2 * (precision * recall) / (precision + recall)) * 100:.2f}'
        else:
            f1_score = 0
        
    print(f'{window_size},{accuracy_result},{precision_result},{recall_result},{f1_score}')            
    return tp, fn, fp, tn    
        
if __name__ == '__main__':
    tp, fn, fp, tn = evaluate('HST',1600)
    print(f'TP: {tp}, FN: {fn}')
    print(f'FP: {fp}, TN: {tn}')