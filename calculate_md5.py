from folderstats import *
import multiprocessing as mp
from datetime import datetime
import humanize
import numpy as np


cores=16
splits=50

#Read in entire folderstats dataframe
file="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/07.13.2020/chaijin_folderstats.txt"
df=pd.read_csv(file,sep='\t')

#Spit the pandas dataframe into n parts with roughly the same cumulative file size

a = (np.random.random(10000000)*1000).astype(int)

def calculate_hash_wrapper(file, size):
    
    startTime = datetime.now()
    md5=calculate_hash(file) 
    print(file, "\t",md5, "\t", size, (datetime.now() - startTime).total_seconds())
    
    return md5
    
with mp.Pool(mp.cpu_count()) as pool:
    df['md5'] = pool.starmap(calculate_hash_wrapper, zip(df['absolute_path'],df['size']))
    
    
def equisum_partition(arr,p):
    ac = arr.cumsum()

    #sum of the entire array
    partsum = ac[-1]//p 

    #generates the cumulative sums of each part
    cumpartsums = np.array(range(1,p))*partsum

    #finds the indices where the cumulative sums are sandwiched
    inds = np.searchsorted(ac,cumpartsums) 

    #split into approximately equal-sum arrays
    parts = np.split(arr,inds)

    return parts
