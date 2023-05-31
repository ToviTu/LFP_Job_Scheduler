import pandas as pd
import pickle as pk
import glob
import re

RAW_DIR = '/media'
PARTIAL_JOB_FILE_PATTERN = r'[A-Z]{3}\d{5}_'

def search_file(pattern: str, path: str) -> list:
    search_result = []

    def recursive_search(depth:int, path:str, result:list) -> None:
        cur_stuff = glob.glob(path + "/*")
        if depth == 3 or not cur_stuff: return
        if re.search(pattern, path) != None: result.append(path)
        for inner_folder in cur_stuff: recursive_search(depth+1, inner_folder, result)
    
    recursive_search(0, path+'/bs00*r', search_result)
    return search_result

all_filez = search_file(PARTIAL_JOB_FILE_PATTERN, RAW_DIR)
df = pd.DataFrame({'RestartDate':all_filez})
df['Animal'] = df['RestartDate'].str.extract(r'([A-Z]{3}\d{5})')
df.set_index('Animal')

with open('/hlabhome/wg-mjames/Job_Scheduler/LFP_Job_Scheduler/temp/filez_info.cache', 'wb') as f:
    pk.dump(df, f)