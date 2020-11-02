import os
from folderstats import *
from qsub import *

#Listing all the top level directories in the mdtaylor folder
top_level_dir="/hpf/largeprojects/mdtaylor"
directories=[d for d in os.listdir(top_level_dir) if os.path.isdir(os.path.join(top_level_dir, d))]

#Further split these large directories in the top level directory
split_again=["hsuzuki"]
directories=[i for i in directories if i not in split_again]

for folder in split_again:
    split_dir=os.path.join(top_level_dir,folder)
    split_folders=[os.path.join(folder,d) for d in os.listdir(split_dir) if os.path.isdir(os.path.join(split_dir, d))]
    directories= directories + split_folders   

# For each top level directory I want to aggregate all the file names and proporties in parallel
# This might be best acheived with a qsub script submission on each top level directory
output_dir="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/07.17.2020/folderstats"
src_dir="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Scripts/"

for d in directories:
    
    d_full=os.path.join(top_level_dir, d)
    out_file=os.path.join(output_dir,d.replace("/","-")) + "_folderstats"
    
    cmd="python {}run_aggregator.py {} {}.txt".format(src_dir,d_full, out_file)
    
    q_write([cmd], t=48, vmem=40, mem=40, out = out_file, environment="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Env/bin/activate")
                    
                