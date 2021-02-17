import numpy as np
import pandas as pd
import time
from tqdm import tqdm
from joblib import Parallel, delayed
import pickle
from collections import defaultdict
import argparse
import os
print(f'CPU cores: {os.cpu_count()}')


def parallel_csv2chunkarray(f_name: str, chunksize: int, usecols: list) -> list:
    reader = pd.read_csv(f_name, chunksize=chunksize, usecols=usecols)
    # results_list = Parallel(n_jobs=-1)([delayed(np.array)(df) for df in tqdm(reader)])
    results_list = []
    for i, df in tqdm(enumerate(reader)):
        results_list.append(np.array(df))
        # if i == 1:
        #     break 
    return results_list


def conjuction(data: np.ndarray) -> list:
    return [(data[i,0], data[i,2], data[i,4]) for i in range(len(data))] # 必要な要素をタプルで展開


def parallel_conjuction(ndarray_list: list) -> list:
    results_list = Parallel(n_jobs=-1)([delayed(conjuction)(d) for d in tqdm(ndarray_list)])
    return [item for result in results_list for item in result]
    

def main():
    # setting
    f_name = '../iFAERS_LMTbin_INDI_delisrwithINDILMT.csv'
    chunksize = 10000000
    # chunksize = 10000
    usecols = list(np.arange(6))
    # Processing
    print('-'*100)
    print('Now loading... Please wait. ')
    DATA = parallel_csv2chunkarray(f_name, chunksize, usecols)
    print('Successfully completed')
    print('-'*100)
    print('Now conjucting data. Please wait. ')
    conjuction_results = parallel_conjuction(DATA)
    del DATA
    print('Successfully completed')
    print('-'*100)
    print('Getting IDs. ')
    Id_Dict = defaultdict(lambda: len(Id_Dict))
    ID = [Id_Dict[text] for text in tqdm(conjuction_results)]
    print(f'N_elements {len(Id_Dict)}')
    del conjuction_results
    print('Successfully completed')
    # save results
    with open('Id_Dict.pkl', 'wb') as f:
        pickle.dump(dict(Id_Dict), f)
    del Id_Dict
    with open('ID.pkl', 'wb') as f:
        pickle.dump(ID, f)
    
    # del Id_Dict
    # del conjuction_results
    # # Data expansion
    # print('-'*100)
    # print('Data expansion. ')
    # ISR = [isr for d in DATA for isr in d[:,0]]
    # TERMEN = [termen for d in DATA for termen in d[:,2]]
    # PT = [pt for d in DATA for pt in d[:,4]]
    # INDI = [indi for d in DATA for indi in d[:,5]]
    # with open('INDI.pkl', 'wb') as f:
    #     pickle.dump(INDI, f)

    # del DATA
    # print('All session was successfully completed.')


if __name__ == '__main__':
    main()