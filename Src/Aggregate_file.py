# --------------------------------------------------------
# Aggregate All File Information in Project Directory
# --------------------------------------------------------
# This script looks in the lab project root directory and identifies all the folders. It can further find folders in the second level with the split_again variable.
# Afterwards, for each folder in this, it submits a job on the cluster to recurivily write and save information on every file and folder. 
# This version does the job but its slow. Luckily it only needs to be used once in a while. 

# Known Issues:
# - It assumes that each folder in the top level is about the same size which is not true
# - hsuzuki sub folders are very complex and require a large walltime (48h)

# Wishlist:
# - use a config file rather then hardcode the file paths and names
# - Rather than submitting a job for each folder try to create a single job which parallizes the file querying in each folder
# - Save the file information as sql database rather then a large flat file

print("Loading Packages...")
import os
from folderstats import *
from qsub import *


#All the top level directories in the mdtaylor folder
print("Parsing folder names for qsub script submission...")
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
output_dir="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/11.02.2020/folderstats"
src_dir="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Scripts/"

#Create the results directory if it doesn't already exisit
try:
    os.makedirs(output_dir)
except OSError:
    print ("Creation of the directory %s failed" % output_dir)
else:
    print ("Successfully created the directory %s" % output_dir)

#Create and write a qsub submission script for each directory
print("Creating qsub job scripts...")
for d in directories:
    
    d_full=os.path.join(top_level_dir, d)
    out_file=os.path.join(output_dir,d.replace("/","-")) + "_folderstats"
    
    cmd="python {}run_aggregator.py {} {}.txt".format(src_dir,d_full, out_file)
    
    #Using the qsub lirbary to create job scripts. These need to be submitted manually afterward
    q_write([cmd], t=48, vmem=40, mem=40, out = out_file, environment="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Env/bin/activate")
                    
                