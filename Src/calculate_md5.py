from folderstats import *
from qsub import *
import numpy as np
from pathlib import Path
import pandas as pd
import sys

#Global paths
src_dir="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Scripts" #Where the scripts are stored

#Parameters
#work_dir=sys.argv[1]
#splits=sys.argv[2]
#cores=sys.argv[3]

work_dir="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/11.02.2020/"
splits=300
cores=16

#Combine all the outputed dataframes together 
set_dtype={"id": np.float64, "size": np.float64, "folder": bool, "num_files" : np.float64, "depth" : np.float64, "parent" : np.float64, "uid" : np.float64, "inode" : np.float64, "num_hard_links" : np.float64}
print("Loading all the file information...")
df_list=[]    
for f_name in os.listdir(os.path.join(work_dir,"folderstats")):
    if f_name.endswith('_folderstats.txt'):
        print(f_name)
        df_list.append( pd.read_csv(os.path.join(work_dir, "folderstats", f_name), sep='\t', dtype=set_dtype))
df = pd.concat(df_list)

#write the combined dataframe to file
#write the combined dataframe to file
df.to_csv(os.path.join(work_dir,"folderstats","folderstats_combined.txt"), sep="\t", index=False)

#Extract the inodes keep only the first file location
print("Finding unique inodes and spliting into groups...")
unique_inodes=df.drop_duplicates(subset="inode", keep="first", inplace=False)

#Shuffle the dataframe 
unique_inodes = unique_inodes.sample(frac=1).reset_index(drop=True)

#Assign clusters 1:num_clus of approximately equal size of data
unique_inodes["split"], approx_size=equisum_partition(
    unique_inodes["size"].to_numpy(copy=True), splits, ignore=unique_inodes["folder"].to_numpy(copy=True))

#Output the split list of unique file inodes and create qsub script to run parellelized md5 hashing
print("Creating qsub script files...")
out_dir=os.path.join(work_dir,"md5sums")
Path(out_dir).mkdir(parents=True, exist_ok=True)
for i in range(1,splits+1):
    
    #Split the dataframe and keep only the inode and file path
    out=unique_inodes.loc[unique_inodes["split"]==i ,
                          ["split", "absolute_path","size", "inode"]]
    
    #Write md5 list on disk
    outfileprefix=os.path.join(out_dir, "clus{}_filelist".format(i))
    out.to_csv(outfileprefix + ".txt", sep="\t", index=False)
    
    #Creating md5 script for qsub submission
    cmd="python {}/run_md5_parallized.py {}.txt {}.md5 {}".format(src_dir, outfileprefix, outfileprefix, cores)
    #rint(outfilename)
    #rint(cmd)
    
    q_write([cmd], t=24, node='1:ppn=16', vmem=50, mem=50, out = outfileprefix,
            environment="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Env/bin/activate")
    


    
find=pd.to_numeric(unique_inodes["size"], errors='coerce')
unique_inodes=unique_inodes[~find.isnull()]


unique_inodes["folder"] = (unique_inodes["folder"] == True)
