import multiprocessing
import testePySAD


if __name__ == '__main__':
    processos = []
    for i in range(20): 
        window_size = (i+1)*100
        p = multiprocessing.Process(target=testePySAD.evaluate, args=(['HST',window_size]))
        processos.append(p)
        p.start()
    
    print('window_size,accuracy,precision,recall,f1_score')
    
    for p in processos:
        p.join()
    