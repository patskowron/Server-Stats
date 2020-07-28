from pebble import ProcessPool
from concurrent.futures import TimeoutError
from folderstats import *
import multiprocessing as mp
import pandas as pd
import sys

#The input and output arguements
input_file=sys.argv[1]
output_file=sys.argv[2]
cores=int(sys.argv[3])
verbose=True
timeout_sec=3600

input_file="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/07.13.2020/inode_md5sum_splits/clus1_md5_filelist.txt"
output_file="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/07.13.2020/inode_md5sum_splits/clus1_md5_filelist.md5"
cores=8

df=pd.read_csv(input_file, sep='\t')

df=df.iloc[:1000,]

def calculate_hash_wrapper(args):
    """Parallel Processing Wrapper for the calculate_hash function"""
    file, inode=args
    md5=calculate_hash(file, hash_name="md5") 
    return (inode, md5)

with ProcessPool(max_workers=cores) as pool:
    
    input_data=list(zip(df['absolute_path'], df['inode']))
    
    #Future object yeilds results in the same order that they were submitted
    future = pool.map(calculate_hash_wrapper, input_data, timeout=1)
    iterator = future.result()
    with open(output_file, 'w') as out_file: 
        idx=0
        while True:
            try:
                inode, md5 =next(iterator)
                a=out_file.write("{}\t{}\n".format(inode, md5))
            except StopIteration:
                break
            except TimeoutError:
                print("sfd")
                #out_file.write("{}\t{}\n".format(input_data[idx][1], "TimeoutError"))
            finally:
                idx += 1
 

# if verbose == True:
#    with mp.Pool(cores) as pool:
#        df['md5'] = pool.starmap(calculate_hash_wrapper, zip(df['absolute_path'], df['size'], #df['inode']))       
#else:
#    with mp.Pool(cores) as pool:
#        df['md5'] = pool.starmap(calculate_hash, zip(df['absolute_path']))

#Out to specidied file
#df.to_csv(output_file, sep="\t", index=False)

