import multiprocessing
import pandas as pd
import glob
import numpy as np

monitering_list = glob.glob('/srv/project_data/EMR/sr_data/monitering/*')
merged_data = pd.DataFrame()

def merge_data_from_file(file):
    intra_op = pd.read_excel(file, sheet_name=None)
    merged_df = pd.concat(intra_op.values(), ignore_index=True)
    return merged_df

# Create a multiprocessing pool
pool = multiprocessing.Pool(4)

# Iterate over each Excel file and apply multiprocessing
results = pool.map(merge_data_from_file, monitering_list)

# Close the multiprocessing pool
pool.close()

# Combine the results into a single DataFrame
merged_data = pd.concat(results, ignore_index=True)

path = "/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.npz"
np.savez(path, merged_data)