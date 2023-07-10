def column_arrange(pat_info):
    pat_info.columns = pat_info.loc[0]
    pat_info = pat_info.drop([0])
    pat_info = pat_info.reset_index(drop=True)
    return pat_info