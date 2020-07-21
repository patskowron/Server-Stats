import os
import stat
import hashlib
import pandas as pd
from datetime import datetime
import numpy as np

def testing_functions():
    print ("Working fine!!")

def calculate_hash(filepath, hash_name="md5"):
    """Calculate the hash of a file. The available hashes are given by the hashlib module. The available hashes can be listed with hashlib.algorithms_available."""
    hash_name = hash_name.lower()
    if not hasattr(hashlib, hash_name):
        raise Exception('Hash algorithm not available : {}'\
            .format(hash_name))
    try:
        with open(filepath, 'rb') as f:
            checksum = getattr(hashlib, hash_name)()
            for chunk in iter(lambda: f.read(4096), b''):
                checksum.update(chunk)
            return checksum.hexdigest()
    except IsADirectoryError:
        return np.nan
    except PermissionError:
        return "PermissionError"
    except FileNotFoundError:
        return "FileNotFoundError"

def _recursive_folderstats(folderpath, items=None, hash_name=None,
                           ignore_hidden=False, depth=0, idx=1, parent_idx=0,
                           verbose=False):
    """Helper function that recursively collects folder statistics and returns current id, foldersize and number of files traversed."""
    items = items if items is not None else []
    foldersize, num_files = 0, 0
    current_idx = idx

    #Making sure that the folder is accesible before checking its contexts
    if os.access(folderpath, os.R_OK): 
        for f in os.listdir(folderpath):
            if ignore_hidden and f.startswith('.'):
                continue

            filepath = os.path.join(folderpath, f)
            idx += 1
            
            #Try to read the filepath 
            try:   
            
                stats = os.stat(filepath)
                foldersize += stats.st_size
                 
                #If its a directory call the recursive function again
                if os.path.isdir(filepath):
                    if verbose:
                        print('FOLDER : {}'.format(filepath))

                    idx, items, _foldersize, _num_files = _recursive_folderstats(
                        filepath, items, hash_name,
                        ignore_hidden, depth + 1, idx, current_idx, verbose)
                    foldersize += _foldersize
                    num_files += _num_files
                #If its a file append information to array
                else:
                    filename, extension = os.path.splitext(f)
                    extension = extension[1:] if extension else None
                    item = [idx, filepath, f, extension, stats.st_size,
                            stats.st_atime, stats.st_mtime, stats.st_ctime,
                            False, None, depth, current_idx, stats.st_uid, stats.st_ino, 
                            stats.st_nlink, stat.filemode(stats.st_mode)]
                    if hash_name:
                        item.append(calculate_hash(filepath, hash_name))
                    items.append(item)
                    num_files += 1
            
            #If the file does not have read permissions keep the name but set all proporties to None
            except Exception:
                print("File is not readable:",filepath)    
                filename, extension = os.path.splitext(f)
                extension = extension[1:] if extension else None
                item = [idx, filepath, f, extension, None,
                        0, 0, 0, False, None, None, 
                        None, None, None, None, None]
                items.append(item)
                num_files += 1
    else:
        print("Cannot enter folder:",folderpath)

    #Properties of the original input folder
    #It is here because it needs to keep tally of the number and size or all files in it  
    stats = os.stat(folderpath)
    foldername = os.path.basename(folderpath)
    item = [current_idx, folderpath, foldername, None, foldersize,
            stats.st_atime, stats.st_mtime, stats.st_ctime,
            True, num_files, depth, parent_idx, stats.st_uid, stats.st_ino, 
            stats.st_nlink, stat.filemode(stats.st_mode)]
    if hash_name:
        item.append(None)
    items.append(item)

    return idx, items, foldersize, num_files


def folderstats(folderpath, hash_name=None, microseconds=False,
                absolute_paths=False, ignore_hidden=False, parent=True,
                verbose=False):
    """Function that returns a Pandas dataframe from the folders and files from a selected folder."""
    
    columns = ['id', 'path', 'name', 'extension', 'size',
               'atime', 'mtime', 'ctime',
               'folder', 'num_files', 'depth', 'parent', 'uid', 'inode', 'num_hard_links', 'permissions']
    if hash_name:
        hash_name = hash_name.lower()
        columns.append(hash_name)

    idx, items, foldersize, num_files = _recursive_folderstats(
        folderpath,
        hash_name=hash_name,
        ignore_hidden=ignore_hidden,
        verbose=verbose)
    df = pd.DataFrame(items, columns=columns)

    for col in ['atime', 'mtime', 'ctime']:
        df[col] = df[col].apply(
            lambda d: datetime.fromtimestamp(d) if microseconds else \
                datetime.fromtimestamp(d).replace(microsecond=0))

    if absolute_paths:
        df.insert(1, 'absolute_path', df['path'].apply(
            lambda p: os.path.abspath(p)))

    if not parent:
        df.drop(columns=['id', 'parent'], inplace=True)

    return df


def equisum_partition(arr,p, ignore):
    
    #MUST RANDOMIZE INPUT ARRAY FIRST

    #arr and ignore have the same length, ignore is type bolean and specifies /
    # if the elements in arr should be included in calculation. 
    arr[ignore]=0
    
    #cumulative sum of data as you iterate throught the array
    ac = np.nancumsum(arr)

    #sum of the entire array decided by the number of clusters requested
    partsum = ac[-1]//p 

    #generates the cumulative sums of each part
    cumpartsums = np.array(range(1,p))*partsum

    #finds the indices where the cumulative sums are sandwiched
    inds = np.searchsorted(ac,cumpartsums) 

    #return a array with labels 1:p for each of the corresponding clusters
    parts=np.zeros(arr.shape[0])
    start=0
    cluster=1
    for idx in np.concatenate((inds,arr.shape[0]),axis=None):
        parts[start:idx]=cluster
        cluster+=1
        start=idx

    return parts, partsum


def calculate_hash_wrapper(file, size):
    
    startTime = datetime.now()
    md5=calculate_hash(file) 
    print(file, "\t",md5, "\t", size, (datetime.now() - startTime).total_seconds())
    
    return md5