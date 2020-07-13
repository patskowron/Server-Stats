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


#Cases where this program crashes:
#1) When there are issues with the read permissions:
#'/hpf/largeprojects/mdtaylor/osaulnier/PROJECTS/single_cell/G3_Cell_ranger_v3/snv/bams/PS17_3376_possorted_genome_bam_dedup_cigar.bam'

#folderpath="/hpf/largeprojects/mdtaylor/osaulnier/PROJECTS/single_cell/G3_Cell_ranger_v3/snv/bams/"

#2) When softlinks are broken and file cannot be accessed 
#'/hpf/largeprojects/mdtaylor/igv_mount/sorted_aligned_MDT1367P_filtered_subreads.bam'
#'/hpf/largeprojects/mdtaylor/jiaozhang/projects/pfa/all_PFA_data/SingleCell_G2/SC_RNA_COUNTER_CS/SC_RNA_COUNTER/SORT_BY_POS/fork0/files/output.bam.bai'

folderpath="/hpf/largeprojects/mdtaylor/igv_mount/sorted_aligned_MDT1367P_filtered_subreads.bam"
folderpath="/hpf/largeprojects/mdtaylor/jiaozhang/projects/pfa/all_PFA_data/SingleCell_G2/SC_RNA_COUNTER_CS/SC_RNA_COUNTER/SORT_BY_POS/fork0/files/"
folderpath="/hpf/largeprojects/mdtaylor/jiaozhang/projects/pfa/all_PFA_data/SingleCell_G2/SC_RNA_COUNTER_CS/SC_RNA_COUNTER/SORT_BY_POS/fork0/"
#Some questions to answer
# - If I folder does not have read permissions can I still call a os.stat() on the folder itself - YES
#folderpath="/hpf/largeprojects/vijaylab"
# - How about for files?