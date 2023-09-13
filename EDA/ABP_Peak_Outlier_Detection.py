import pandas as pd
import numpy as np
import time
import pyarrow.parquet as pq
import seaborn as sns
import matplotlib.pyplot as plt

### numpy로 읽기 너무 오래 걸ㄹ려래무
# st = time.time()
# print(st)
#
# path = "/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.npz"
# aa = np.load(path, allow_pickle=True)
# column_names = ['RN', '병원등록번호', '마취기록작성번호', '수술일자', '수술일정번호', '모니터링명', '모니터링약어명', '모니터링기록일시', '측정값']
# # column_names = ['RN', 'pat_num', 'anes_num', 'op_date', 'op_num', 'monitor_name', 'abbreviation_name', 'measurement_date', 'measure_value']
# data_array = aa['arr_0']
# df = pd.DataFrame(data_array, columns=column_names)
#
# print(time.time()-st)
#
# df['마취기록작성번호'] = df['마취기록작성번호'].astype(int)
# df['모니터링기록일시'] = pd.to_datetime(df['모니터링기록일시'])
# temp_df = df[['마취기록작성번호', '모니터링약어명', '모니터링기록일시', '측정값']]
# temp_df.to_parquet("/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.parquet", engine="pyarrow")

df_pq = pd.read_parquet("/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.parquet", engine='pyarrow')
abp_df = df_pq[df_pq['모니터링약어명'].str.contains('ABP')]
temp_mbp = abp_df[abp_df['모니터링약어명'].str.contains('M')]

anes_19 = pd.read_excel('/srv/project_data/EMR/original_data/20220426_ANS_이상욱_2021-1352_(1.마취기본)_3차(7.7).xlsx', engine='openpyxl')
anes_20 = pd.read_excel('/srv/project_data/EMR/original_data/22_1018_ANS_이상욱_2021-1352_(1.마취기본)_2차(12.22).xlsx', engine='openpyxl')

anes_19.columns = anes_19.iloc[0]
anes_19 = anes_19.drop([0]).reset_index(drop=True)
anes_20.columns = anes_20.keys().str.replace("'", "")

anes_20.rename(columns= {'(마취기록)마취소요시간(분)':'(마취기록) 마취소요시간(분)'}, inplace=True)
anes_20.rename(columns= {'(마취기록) ASA점수코드':'(마취기록) ASA점수'}, inplace=True)
anes_20 = anes_20.drop(columns=['(마취기록) 최종확인일시', '성별'])

anes = pd.concat([anes_19, anes_20])

anes['마취기록작성번호'] = anes['마취기록작성번호'].astype(int)
anes['(마취기록)마취시작'] = pd.to_datetime(anes['(마취기록)마취시작'], format='%Y-%m-%d %H:%M')
anes['(마취기록)수술시작'] = pd.to_datetime(anes['(마취기록)수술시작'], format='%Y-%m-%d %H:%M')

sns.set_style(
    {
        "axes.edgecolor": "0",
        "ytick.color": "0",
        "xtick.color": "black",
        "xtick.direction": "in",
        "ytick.direction": "in",
        "text.color": "black",
        "axes.spines.right": False,
        "axes.spines.top": False,
        "axes.facecolor": "white",
        "axes.grid": False,
        "patch.edgecolor": "black",
        'font.family': ['sans-serif']
    }
)
sns.set_palette("Set2")


for idx, anes_info in enumerate(anes.iloc):
    temp_pq = temp_mbp[temp_mbp['마취기록작성번호'] == anes_info['마취기록작성번호']].reset_index(drop=True)
    if temp_pq.empty:
        continue
    temp_pq['측정값'] = temp_pq['측정값'].astype(int)
    temp_hypotension = np.where(temp_pq['측정값'].values[0:3] < 60)[0]
    if temp_hypotension.size == 0:
        continue
    else:
        temp_outliar = np.where((np.diff(temp_pq['측정값'].values[0:3]) > 30) | (np.diff(temp_pq['측정값'].values[0:3]) < -30))[0]
        if temp_outliar.size == 0 :
            continue
    mt_ts = (temp_pq['모니터링기록일시'].values.astype('int') / 10 ** 9).astype('int32')
    temp_ts = []
    for i in mt_ts:
        if len(temp_ts) == 0:
            temp_ts.append(0)
            temp_num = i
            continue
        temp_result = int((i - temp_num) / 60)
        temp_ts.append(temp_result)
    fig, ax = plt.subplots(figsize=(12, 8))
    plt.plot(temp_ts, temp_pq['측정값'], 'b', linewidth=2, label="ABP_M")
    # plt.ylim([30,170])
    plt.axhline(60, color='red', label="Hypotension threshold")
    # plt.axvline(tnew.min(), color='black', linestyle='dashed')
    # plt.axvline(70, color='black', linestyle='dashed')
    # plt.xlim([-2, 82])
    plt.ylim([20, 140])
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.show()
    if idx >= 100:
        break
plt.close()

temp_hypotension = np.where(temp_pq['측정값'].values[0:3] < 60)[0]
if temp_hypotension.size == 0:
    continue
else:
    break

temp_hypotension

temp_outliar


