import pandas as pd
import numpy as np

method_anesthesia1 = pd.read_excel('/srv/project_data/EMR/original_data/20220426_ANS_이상욱_2021-1352_(3.서식_마취방법,인계사항)_4차(7.12).xlsx')
method_anesthesia2 = pd.read_excel('/srv/project_data/EMR/original_data/22_1018_ANS_이상욱_2021-1352_(3.서식_마취방법,인계사항)_2차(12.22).xlsx')
middle_pat = pd.read_excel('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/middle_pat.xlsx')

def EMR_column_arrange(pat_info):
    pat_info = pat_info.rename(columns=pat_info.iloc[0])
    pat_info = pat_info.drop([0])
    pat_info = pat_info.reset_index(drop=True)
    return pat_info

method_anesthesia1 = EMR_column_arrange(method_anesthesia1)

anesthesia1 = method_anesthesia1[['마취기록작성번호', '마취방법 - General', '마취방법 - Spinal', '마취방법 - Epidural', '마취방법 - Peripheral nerve block', '마취방법 - MAC']]
anesthesia2 = method_anesthesia2[['마취기록작성번호', '마취방법 - General', '마취방법 - Spinal', '마취방법 - Epidural', '마취방법 - Peripheral nerve block', '마취방법 - MAC']]

method_anesthesia = pd.concat([anesthesia1, anesthesia2]).reset_index(drop=True)
method_anesthesia['마취기록작성번호'] = method_anesthesia['마취기록작성번호'].astype(int)

anes_num = []
anes_append = anes_num.append
method = []
method_append = method.append

status = 0
for idx, data in  enumerate(method_anesthesia.iloc()):
    status = 1
    for key in data.keys():
        if key == '마취기록작성번호':
            anes_append(data[key])
        elif not pd.isnull(data[key]):
            status = 0
            method_append(data[key])
    if status == 1:
        method_append(np.nan)

anes_method = pd.DataFrame({'마취기록작성번호': anes_num, '마취방법': method})
print(len(anes_method))
middle_18 = middle_pat[middle_pat['수술나이'] >= 18]
test = pd.merge(middle_18, anes_method, how='inner', on=['마취기록작성번호'])

anes_method.to_csv('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/04_anesthesia_induction.csv', index=False, encoding='utf-8-sig')

