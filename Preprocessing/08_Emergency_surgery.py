import pandas as pd

def column_arrange(pat_info):
    pat_info = pat_info.rename(columns=pat_info.iloc[0])
    pat_info = pat_info.drop([0])
    pat_info = pat_info.reset_index(drop=True)
    return pat_info

preprocessing_path = '/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/'
original_path = '/srv/project_data/EMR/original_data/'

surgery1 = pd.read_excel(original_path+"211015_ANS_이상욱_2021-1352_1차(3.14).xlsx", sheet_name='2-1(2)')
surgery2 = pd.read_excel(original_path+"221017_ANS_이상욱_2021-1352_(EMR)_1차(11.2).xlsx", sheet_name='2-1(2)')

basic_pat = pd.read_excel(preprocessing_path+"00_basic_patients.xlsx")

surgery1 = column_arrange(surgery1)
surgery2 = column_arrange(surgery2)

test1 = surgery1[['마취기록작성번호', '[기본]마취과적 신체 상태 분류-E']]
test2 = surgery2[['마취기록작성번호', '[기본]마취과적 신체 상태 분류-E']]

surgery = pd.concat([test1, test2])

surgery['마취기록작성번호'] = surgery['마취기록작성번호'].astype(int)
surgery = surgery[pd.isnull(surgery['[기본]마취과적 신체 상태 분류-E'])== False]
surgery = surgery.drop_duplicates('마취기록작성번호')

merge_pat = pd.merge(basic_pat, surgery, how='left', on=['마취기록작성번호'])

merge_pat.to_excel(preprocessing_path+"00_basic_emergency.xlsx", index=False)