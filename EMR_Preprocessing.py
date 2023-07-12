def column_arrange(pat_info):
    pat_info = pat_info.rename(columns=pat_info.iloc[0])
    pat_info = pat_info.drop([0])
    pat_info = pat_info.reset_index(drop=True)
    return pat_info