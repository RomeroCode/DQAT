import multiprocessing
import testeRiver


if __name__ == '__main__':
    processos = []
    for i in range(40): 
        window_size = (i+1)*10
        p = multiprocessing.Process(target=testeRiver.evaluate, args=(['SVM',window_size]))
        processos.append(p)
        p.start()
    
    print('window_size,accuracy,precision,recall,f1_score')
    
    for p in processos:
        p.join()
    