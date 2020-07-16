##

import os
from folderstats import *
from qsub import *


testing_functions()


#Listing all the top level directories in the mdtaylor folder
top_level_dir="/hpf/largeprojects/mdtaylor"
directories=[d for d in os.listdir(top_level_dir) if os.path.isdir(os.path.join(top_level_dir, d))]


# For each top level directory I want to aggregate all the file names and proporties in parallel
# This might be best acheived with a qsub script submission on each top level directory
output_dir="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/07.13.2020"
src_dir="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Scripts/"

for d in directories:

    d_full=os.path.join(top_level_dir, d)
    out_file=os.path.join(output_dir,d) + "_folderstats"
    
    cmd="python {}run_aggregator.py {} {}.txt".format(src_dir,d_full, out_file)
    
    q_write([cmd], t=24, vmem=20, mem=20, out = out_file, environment="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Env/bin/activate")
                    
                