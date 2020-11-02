from folderstats import *
import os 
import sys

#The input and output arguements
folderpath=sys.argv[1]
df_output=sys.argv[2]

#Some simple checks of the input
if not os.path.isdir(folderpath):
    print('Filepath is not a folder : ', args.folderpath)
    exit(-1)


#Run the file proporties scanner for the inputted folder and all subdirectories
print("Running Folderstats")
df = folderstats(folderpath,
                 hash_name=None,
                 microseconds=False,
                 absolute_paths=True,
                 ignore_hidden=True,
                 parent=True,
                 verbose=False)


#Out to specidied file
df.to_csv(df_output, sep="\t", index=False)


