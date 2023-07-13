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

event_info['마취기록이벤트내용_barlower'] = event_info['마취기록이벤트내용_lower'].str.replace(' ', '')
event_info['마취기록이벤트내용_bracket'] = event_info['마취기록이벤트내용_barlower'].str.replace('(', '')
event_info['마취기록이벤트내용_bracket_ver2'] = event_info['마취기록이벤트내용_bracket'].str.replace(')', '')

anes_num = np.array([])
anes_num = np.append(anes_num, event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket'].str.contains(r'(propofol[0-9])'))].unique())
anes_num = np.append(anes_num, event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket'].str.contains('midazolam'))].unique())
anes_num = np.append(anes_num, event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket'].str.contains('pentothal'))].unique())
anes_num = np.append(anes_num, event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket'].str.contains('pentotal'))].unique())

basic_patients = pd.read_excel('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/00_basic_patients.xlsx')
method_anesthesia = pd.read_csv('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/method_anesthesia.csv')
method_anesthesia = method_anesthesia[method_anesthesia['마취방법'] == 'General']

basic = pd.merge(basic_patients, method_anesthesia, how='left', on=['마취기록작성번호'])
basic_pat = basic[pd.isnull(basic['마취방법'])== False]
print(f"General 마취 제외 전: {len(basic)}, 제외 후: {len(basic_pat)}")

basic_index = basic_pat['마취기록작성번호'].to_numpy()
propofol = np.zeros(len(basic_index))
midazolam = np.zeros(len(basic_index))
pentotal = np.zeros(len(basic_index))

for i in event_info['마취기록작성번호'][(event_info['마취기록이벤트내용_bracket_ver2'].str.contains(r'((fresofol|freefol|propofol)([0-9]|mg))'))].unique():
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
# anes_induction.to_excel('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/05_anesthetic.xlsx', index=False)
anes_induction = anes_induction[(anes_induction['propofol'] != 0) | (anes_induction['midazolam'] != 0) | (anes_induction['pentotal'] != 0)]
true_anesthetic = anes_induction[(anes_induction['propofol'] != 0) | (anes_induction['midazolam'] != 0) | (anes_induction['pentotal'] != 0)].astype(int)
true_anesthetic.to_csv('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/05_anesthetic.csv', index=False, encoding='utf-8-sig')

###################################################################################################################################################################

# len(anes_induction) - len(anes_induction[(anes_induction['propofol'] != 0) | (anes_induction['midazolam'] != 0) | (anes_induction['pentotal'] != 0)])
#
# aa = list(anes_induction['마취기록작성번호'][(anes_induction['propofol'] == 0) & (anes_induction['midazolam'] == 0) & (anes_induction['pentotal'] == 0)])
#
# bb = event_info[event_info['마취기록작성번호'].isin(aa)].copy()
# bb = bb[['마취기록작성번호', '마취기록이벤트내용', '마취기록이벤트내용_bracket']]
# bb.to_csv('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/05_False_data.csv', index=False, encoding='utf-8-sig')
#
# test1 = bb['마취기록작성번호'].unique()
#
# temp_cc = bb['마취기록작성번호'][(bb['마취기록이벤트내용_bracket_ver2'].str.contains('vima'))].unique()
# len(temp_cc)
# len(bb['마취기록작성번호'].unique())
# cc = bb[bb['마취기록작성번호'].isin(temp_cc) == False].copy()
# len(cc['마취기록작성번호'].unique())
#
# cc = bb['마취기록작성번호'][(bb['마취기록이벤트내용_bracket'].str.contains('vima') == False)]
# cc.to_csv('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/05_False_data_exceptVIMA.csv', index=False, encoding='utf-8-sig')

###################################################################################################################################################################

# 유지제 코딩
anes_list = anes_induction['마취기록작성번호'].unique()
maintenance = event_info[event_info['마취기록작성번호'].isin(anes_list)]

test = maintenance[maintenance['마취기록이벤트내용_bracket_ver2'].str.contains(r'([0-9]%(propofol|freefol|fresfol))|des|sev')]
print(len(test['마취기록작성번호'].unique()))
false_data = maintenance[maintenance['마취기록작성번호'].isin(test['마취기록작성번호'].unique()) == False]
false_data = false_data[['마취기록작성번호', '마취기록이벤트내용', '마취기록이벤트내용_bracket_ver2']]
false_data.to_csv('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/05_False_data_maintenace.csv', index=False, encoding='utf-8-sig')

gas_drug_info = gas_drug_info.rename(columns={"마취기록작성번호 ": "마취기록작성번호"})
gas_drug_info = gas_drug_info[gas_drug_info['연속/일회구분명'] != '일회성']

maintenance_gas = gas_drug_info[gas_drug_info['입력항목명'].str.contains(r'(Propofol|propofol)|Sevoflurane|Desflurane')]
maintenance_gas = maintenance_gas[maintenance_gas['마취기록작성번호'].isin(anes_induction['마취기록작성번호'].unique())]
len(maintenance_gas['마취기록작성번호'].unique())
len(anes_induction['마취기록작성번호'].unique())
83466 - 83380