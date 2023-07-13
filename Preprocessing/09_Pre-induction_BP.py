import pandas as pd
import numpy as np

def column_arrange(pat_info):
    pat_info = pat_info.rename(columns=pat_info.iloc[0])
    pat_info = pat_info.drop([0])
    pat_info = pat_info.reset_index(drop=True)
    return pat_info

preprocessing_path = '/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/'
original_path = '/srv/project_data/EMR/original_data/'

anes_19 = pd.read_excel('/srv/project_data/EMR/original_data/20220426_ANS_이상욱_2021-1352_(1.마취기본)_3차(7.7).xlsx')
anes_20 = pd.read_excel('/srv/project_data/EMR/original_data/22_1018_ANS_이상욱_2021-1352_(1.마취기본)_2차(12.22).xlsx')

anes_19 = column_arrange(anes_19)
anes_20.columns = anes_20.keys().str.replace("'", "")
anes_20.rename(columns= {'(마취기록)마취소요시간(분)':'(마취기록) 마취소요시간(분)'}, inplace=True)
anes_20.rename(columns= {'(마취기록) ASA점수코드':'(마취기록) ASA점수'}, inplace=True)

anes = pd.concat([anes_19, anes_20])
anes['마취기록작성번호'] = anes['마취기록작성번호'].astype(int)
anes = anes.drop_duplicates('마취기록작성번호')

moni_anes = anes[['마취기록작성번호', '(마취기록)마취시작']]
moni_anes['(마취기록)마취시작'] = pd.to_datetime(moni_anes['(마취기록)마취시작'])

path = "/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.npz"
aa = np.load(path, allow_pickle=True)
column_names = ['RN', '병원등록번호', '마취기록작성번호', '수술일자', '수술일정번호', '모니터링명', '모니터링약어명', '모니터링기록일시', '측정값']
data_array = aa['arr_0']
df = pd.DataFrame(data_array, columns=column_names)

df['모니터링기록일시'] = pd.to_datetime(df['모니터링기록일시'])
df['마취기록작성번호'] = df['마취기록작성번호'].astype(int)
df = df[
    (df['모니터링약어명'] == 'SBP') |
    (df['모니터링약어명'] == 'DBP') |
    (df['모니터링약어명'] == 'MBP')
    ]
df['측정값'] = df['측정값'].astype(float)

basic_pat = pd.read_excel(preprocessing_path+'00_basic_emergency.xlsx')
anes['(마취기록)마취시작'] = pd.to_datetime(anes['(마취기록)마취시작'])

sbp = []
dbp = []
mbp = []
anes_num = []
for i in basic_pat['마취기록작성번호']:
    anes_time = anes['(마취기록)마취시작'][anes['마취기록작성번호'] == i].reset_index(drop=True)[0]
    temp_df = df[(df['마취기록작성번호'] == i) & (df['모니터링기록일시'].between(anes_time - pd.Timedelta("15 min"), anes_time + pd.Timedelta("10 min")))]
    df_sbp = temp_df['측정값'][temp_df['모니터링약어명'] == 'SBP'].reset_index(drop=True)
    df_dbp = temp_df['측정값'][temp_df['모니터링약어명'] == 'DBP'].reset_index(drop=True)
    # df_mbp = temp_df['측정값'][temp_df['모니터링약어명'] == 'MBP'].reset_index(drop=True)
    if len(df_sbp) == 0 or len(df_dbp) == 0:
        continue
    sbp.append(df_sbp[0])
    dbp.append(df_dbp[0])
    anes_num.append(i)

df = pd.DataFrame({"마취기록작성번호": anes_num, 'Pre-induction_SBP': sbp, 'Pre-induction_DBP': dbp})
df['Pre-induction_MBP'] = (df['Pre-induction_SBP'] + (2*df['Pre-induction_DBP'])) /3
df = df.astype(int)

df.to_csv('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/09_Pre-induction_BP.csv', index=False, encoding='utf-8-sig')