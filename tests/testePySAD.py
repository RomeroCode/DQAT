from pysad.models import IForestASD, HalfSpaceTrees
from pysad.utils import ArrayStreamer
from pysad.utils import Data, get_minmax_array
import numpy as np


def evaluate(model, window_size):
    data = Data("data")
    X_all, y_all = data.get_data("arrhythmia.mat")
    X_min, X_max = get_minmax_array(X_all)

    iterator = ArrayStreamer(shuffle=False)  # Create streamer to simulate streaming data.
    
    if model == 'IFASD':
        model = IForestASD(window_size=window_size)
    if model == 'HST':
        model = HalfSpaceTrees(X_min, X_max, window_size=window_size, num_trees=25, max_depth=15)
        

    scores = []
    tp = 0
    fp = 0
    tn = 0
    fn = 0
    
    # Iterate over examples.
    for X, y in iterator.iter(X_all, y_all):  
        model.fit_partial(X)
        score = model.score_partial(X)
        if np.isnan(score):
            continue
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
    tp, fn, fp, tn = evaluate(30)
    print(f'TP: {tp}, FN: {fn}')
    print(f'FP: {fp}, TN: {tn}')
    
    