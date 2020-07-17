from folderstats import *
from qsub import *
from datetime import datetime
import numpy as np
from pathlib import Path
import pandas as pd
import humanize


#Global paths
src_dir="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Scripts/" #Where the scripts are stored

#Parameters
work_dir=sys.argv[1]
work_dir="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/07.13.2020/"

#Parallization parameters
cores=16
splits=50

#Combine all the outputed dataframes together 
df_list=[]    
for f_name in os.listdir(work_dir):
    if f_name.endswith('_folderstats.txt'):
        print(f_name)
        df_list.append( pd.read_csv(os.path.join(work_dir, f_name), sep='\t'))
    
df = pd.concat(df_list)

#Extract the inodes keep only the first file location
unique_inodes=df.drop_duplicates(subset="inode", keep="first", inplace=False)

#Shuffle the dataframe 
unique_inodes = unique_inodes.sample(frac=1).reset_index(drop=True)

#Assign clusters 1:num_clus of approximately equal size of data
unique_inodes["split"], approx_size=equisum_partition(
    unique_inodes["size"].to_numpy(copy=True), splits, ignore=unique_inodes["folder"].to_numpy(copy=True))

#Output the split list of unique file inodes and create qsub script to run parellelized md5 hashing
out_dir=os.path.join(work_dir,"inode_md5sum_splits")
Path(out_dir).mkdir(parents=True, exist_ok=True)
for i in range(1,splits+1):
    
    #Split the dataframe and keep only the inode and file path
    out=unique_inodes.loc[unique_inodes["split"]==i ,
                          ["split", "absolute_path","size", "inode"]]
    
    #Write md5 list on disk
    outfileprefix=os.path.join(out_dir, "clus{}_filelist".format(i))
    out.to_csv(outfileprefix + "txt", sep="\t", index=False)
    
    #Creating md5 script for qsub submission
    cmd="python {}run_md5_parallized.py {}.txt {}.md5 {}".format(src_dir, outfileprefix, outfileprefix, cores)
    #rint(outfilename)
    #rint(cmd)
    
    q_print([cmd], t=24, vmem=20, mem=20, out = outfileprefix + "_qsub.sh",
            environment="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Env/bin/activate")
    

