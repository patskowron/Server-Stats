from folderstats import *
import multiprocessing as mp
import pandas as pd
import os 
import sys


#The input and output arguements
input_file=sys.argv[1]
output_file=sys.argv[2]
cores=sys.argv[3]


#input_file="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/07.13.2020/inode_md5sum_splits/clus1_md5_filelist.txt"
#output_file="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/07.13.2020/inode_md5sum_splits/clus1_md5_filelist.md5"
#cores=16

df=pd.read_csv(input_file, sep='\t')

#df=df.iloc[:1000,]

with mp.Pool(cores) as pool:
    df['md5'] = pool.starmap(calculate_hash, zip(df['absolute_path']))

#Out to specidied file
df.to_csv(output_file, sep="\t", index=False)

