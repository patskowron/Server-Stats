from pebble import ProcessPool
from concurrent.futures import TimeoutError
from folderstats import *
import pandas as pd
import sys

#Wrapper function for the parallel processing calls
def calculate_hash_wrapper(args):
    """Parallel Processing Wrapper for the calculate_hash function"""
    file, inode=args
    md5=calculate_hash(file, hash_name="md5") 
    return (inode, md5)

#The input and output arguements
input_file=sys.argv[1]
output_file=sys.argv[2]
cores=int(sys.argv[3])

#Global variables
timeout_sec=1200

#input_file="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/07.13.2020/inode_md5sum_splits/clus1_md5_filelist.txt"
#output_file="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/07.13.2020/inode_md5sum_splits/clus1_md5_filelist.md5"
#cores=8

#Open the ionde list with pandas
print("Reading " + input_file)
df=pd.read_csv(input_file, sep='\t')

print("Starting Parallel Process pool...")
df=pd.read_csv(input_file, sep='\t')
with ProcessPool(max_workers=cores) as pool:
    
    input_data=list(zip(df['absolute_path'], df['inode']))
    
    #Future object yeilds results iterator in the same order that they were submitted
    future = pool.map(calculate_hash_wrapper, input_data, timeout=timeout_sec)
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
                a=out_file.write("{}\t{}\n".format(input_data[idx][1], "TimeoutError"))
            finally:
                idx += 1
 

