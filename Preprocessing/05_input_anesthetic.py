import pandas as pd
import numpy as np

event1 = pd.read_excel('/srv/project_data/EMR/original_data/20220426_ANS_이상욱_2021-1352_(4.EVENT)_5차(7.15).xlsx')
event2 = pd.read_excel('/srv/project_data/EMR/original_data/22_1018_ANS_이상욱_2021-1352_(4.Event)_2차(12.22).xlsx')

gas1 = pd.read_excel('/srv/project_data/EMR/original_data/20211015_ANS_이상욱_2021-1352_(Gas,Drug,Input,Output)_1_1차.xlsx')
gas2 = pd.read_excel('/srv/project_data/EMR/original_data/20211015_ANS_이상욱_2021-1352_(Gas,Drug,Input,Output)_2_1차.xlsx')
gas3 = pd.read_excel('/srv/project_data/EMR/original_data/22_1017_ANS_이상욱_2021-1352_(Gas,Drug,Input,Output)_1차(11.17).xlsx', sheet_name=None)
gas3 = pd.concat([gas3['1'], gas3['2']])

del gas1['RN']
del gas2['RN']

gas_drug_info = pd.concat([gas1, gas2, gas3]).reset_index(drop=True)

del event1['수술일자']
del event2['마취기록이벤트순서']

event2 = event2.rename(columns={'마취기록이벤트기록시간': '마취기록이벤트기록일시'})

event_info = pd.concat([event1, event2]).reset_index(drop=True)


event_info['마취기록이벤트내용_lower'] = event_info['마취기록이벤트내용'].str.lower()
event_info['마취기록이벤트내용_lower'] = event_info['마취기록이벤트내용_lower'].str.replace(' ', '')


anes_num = []
info_str = []
anes = []
induction = []
for i in event_info['마취기록작성번호'].unique():
    anesthetic = 0
    temp_op = event_info[event_info['마취기록작성번호'] == i].copy()
    temp_induction = temp_op[(temp_op['마취기록이벤트내용_lower'].str.contains('preoxygenation')) & (temp_op['마취기록이벤트내용_lower'].str.contains('propofol'))].copy()
    if len(temp_induction) == 0:
        temp_induction = temp_op[(temp_op['마취기록이벤트내용_barlower'].str.contains(r'(propofol[0-9])'))]
    elif len(temp_induction) == 0 :
        temp_induction = temp_op[temp_op['마취기록이벤트내용_lower'].str.contains('midazolam')].copy()
        if len(temp_induction) == 0:
            anesthetic = 1
    else :
        induction.append('propofol')
    if len(temp_induction) == 0 :
        anes_num.append(i)
        info_str.append(str(temp_op['마취기록이벤트내용'].to_numpy())[1:-1])
    else:
        anes.append(i)
        if anesthetic == 1:
            induction.append('midazolam')

df = pd.DataFrame({'anes_num':anes_num, 'info_str':info_str})
df.to_excel('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/05_debug_2.xlsx', index=False)



event_info['마취기록이벤트내용_barlower'] = event_info['마취기록이벤트내용_lower'].str.replace(' ', '')
event_info['마취기록이벤트내용_bracket'] = event_info['마취기록이벤트내용_barlower'].str.replace('(', '')
event_info['마취기록이벤트내용_bracket_ver2'] = event_info['마취기록이벤트내용_bracket'].str.replace(')', '')

test = event_info['마취기록작성번호']

# len(event_info[(event_info['마취기록이벤트내용_barlower'].str.contains(r'(propofol[0-9])'))])
# len(event_info[(event_info['마취기록이벤트내용_barlower'].str.contains('propofol ('))])
# len(event_info[(event_info['마취기록이벤트내용_barlower'].str.contains('midazolam'))])
# len(event_info[(event_info['마취기록이벤트내용_barlower'].str.contains('pentothal'))])
# len(event_info[(event_info['마취기록이벤트내용_barlower'].str.contains('pentotal'))])
#
# len(event_info[(event_info['마취기록이벤트내용_bracket'].str.contains(r'(propofol[0-9])'))])
# len(event_info[(event_info['마취기록이벤트내용_bracket'].str.contains('midazolam'))])
# len(event_info[(event_info['마취기록이벤트내용_bracket'].str.contains('pentothal'))])
# len(event_info[(event_info['마취기록이벤트내용_bracket'].str.contains('pentotal'))])


anes_num = np.array([])
anes_num = np.append(anes_num, event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket'].str.contains(r'(propofol[0-9])'))].unique())
anes_num = np.append(anes_num, event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket'].str.contains('midazolam'))].unique())
anes_num = np.append(anes_num, event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket'].str.contains('pentothal'))].unique())
anes_num = np.append(anes_num, event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket'].str.contains('pentotal'))].unique())

len(pd.Series(anes_num).unique())

basic_patients = pd.read_excel('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/00_basic_patients.xlsx')
# basic_patients = pd.read_excel('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/middle_pat.xlsx')
method_anesthesia = pd.read_csv('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/method_anesthesia.csv')
method_anesthesia = method_anesthesia[method_anesthesia['마취방법'] == 'General']

basic = pd.merge(basic_patients, method_anesthesia, how='left', on=['마취기록작성번호'])
basic_pat = basic[pd.isnull(basic['마취방법'])== False]

basic_index = basic_pat['마취기록작성번호'].to_numpy()
propofol = np.zeros(len(basic_index))
midazolam = np.zeros(len(basic_index))
pentotal = np.zeros(len(basic_index))

for i in event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket_ver2'].str.contains(r'((fresofol|freefol|propofol)[0-9])'))].unique():
    temp_idx = np.where(basic_index == i)[0]
    if len(temp_idx) == 0 :
        continue
    temp_idx = temp_idx[0]
    propofol[temp_idx] = 1
for i in event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket_ver2'].str.contains('midazolam'))].unique():
    temp_idx = np.where(basic_index == i)[0]
    if len(temp_idx) == 0 :
        continue
    temp_idx = temp_idx[0]
    midazolam[temp_idx] = 1
for i in event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket_ver2'].str.contains(r'((pentothal|pentotal))'))].unique():
    temp_idx = np.where(basic_index == i)[0]
    if len(temp_idx) == 0 :
        continue
    temp_idx = temp_idx[0]
    pentotal[temp_idx] = 1

anes_induction = pd.DataFrame({'마취기록작성번호': basic_index, 'propofol':propofol, 'midazolam':midazolam, 'pentotal':pentotal})
anes_induction.to_excel('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/05_anesthetic.xlsx', index=False)

len(anes_induction[(anes_induction['propofol'] != 0) | (anes_induction['midazolam'] != 0) | (anes_induction['pentotal'] != 0)])

aa = list(anes_induction['마취기록작성번호'][(anes_induction['propofol'] == 0) & (anes_induction['midazolam'] == 0) & (anes_induction['pentotal'] == 0)])

bb = event_info[event_info['마취기록작성번호'].isin(aa)].copy()
bb = bb[['마취기록작성번호', '마취기록이벤트내용', '마취기록이벤트내용_bracket']]
bb.to_csv('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/05_False_data.csv', index=False, encoding='utf-8-sig')

len(bb['마취기록작성번호'].unique())

test1 = bb['마취기록작성번호'].unique()
len(bb['마취기록작성번호'][(bb['마취기록이벤트내용_bracket'].str.contains('propofol')) | (bb['마취기록이벤트내용_bracket'].str.contains('freefol'))].unique())


import re
pattern1 = 'fresofol150mg'
pattern2 = 'fresofol50mg'

re.findall(r'fresofol[0-9]{1,3}mg', pattern2)


len(event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket_ver2'].str.contains(r'((fresofol|freefol|propofol)[0-9]{1,3}mg)'))].unique())
len(event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket_ver2'].str.contains(r'((fresofol|freefol|propofol)[0-9]{0,3}mg)'))].unique())