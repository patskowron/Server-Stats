import pandas as pd
import sys
import os

#Work Directory
work_dir="/hpf/largeprojects/mdtaylor/patryks/Server_Police/Data/07.28.2020/"

#Combine all the files md5 together
print("Loading all the file information...")
df_list=[]    
for f_name in os.listdir(os.path.join(work_dir,"md5sums")):
    if f_name.endswith('_filelist.md5'):
        df_list.append( pd.read_csv(os.path.join(work_dir, "md5sums", f_name), sep='\t', names=["inode","md5sum"]))
        print(f_name)
inodes = pd.concat(df_list, ignore_index =True)

#Load the combined folderstats file
df=pd.read_csv(os.path.join(work_dir,"folderstats","folderstats_combined.txt"), sep="\t")

#Merge with folderstats combined dataframe
merged=pd.merge(df, inodes, on="inode")

#Remove columns which are not needed to save space
merged.drop(columns=["id","absolute_path","atime","mtime","ctime","depth","num_files","parent","permissions"], axis=1, inplace=True)


#Write to file
merged.to_csv(os.path.join(work_dir,"md5sums","folderstats_combined_md5sums.txt"), sep="\t")

#Write as compressed file
merged.to_csv(os.path.join(work_dir,"md5sums","folderstats_combined_md5sums.txt.gz"), sep="\t", compression="gzip")