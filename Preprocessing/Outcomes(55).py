import pandas as pd
import glob
from scipy import interpolate
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

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
sns.dark_palette("#69d", reverse=True, as_cmap=True)
# sns.color_palette("light:b", as_cmap=True)
sns.color_palette("vlag", as_cmap=True)
sns.color_palette("coolwarm", as_cmap=True)

addr = '/srv/project_data/EMR/original_data/'
sr_list = glob.glob(addr+"*1.마취기본*.xlsx")

anes_19 = pd.read_excel('/srv/project_data/EMR/original_data/20220426_ANS_이상욱_2021-1352_(1.마취기본)_3차(7.7).xlsx')
anes_20 = pd.read_excel('/srv/project_data/EMR/original_data/22_1018_ANS_이상욱_2021-1352_(1.마취기본)_2차(12.22).xlsx')

anes_19.columns = anes_19.iloc[0]
anes_19 = anes_19.drop([0]).reset_index(drop=True)
anes_20.columns = anes_20.keys().str.replace("'", "")

anes_20.rename(columns= {'(마취기록)마취소요시간(분)':'(마취기록) 마취소요시간(분)'}, inplace=True)
anes_20.rename(columns= {'(마취기록) ASA점수코드':'(마취기록) ASA점수'}, inplace=True)
anes_20 = anes_20.drop(columns=['(마취기록) 최종확인일시', '성별'])

anes = pd.concat([anes_19, anes_20])

anes['마취기록작성번호'] = anes['마취기록작성번호'].astype(int)
anes['(마취기록)마취시작'] = pd.to_datetime(anes['(마취기록)마취시작'], format = '%Y-%m-%d %H:%M')
anes['(마취기록)수술시작'] = pd.to_datetime(anes['(마취기록)수술시작'], format = '%Y-%m-%d %H:%M')

path = "/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.npz"
aa = np.load(path, allow_pickle=True)
column_names = ['RN', '병원등록번호', '마취기록작성번호', '수술일자', '수술일정번호', '모니터링명', '모니터링약어명', '모니터링기록일시', '측정값']
data_array = aa['arr_0']
df = pd.DataFrame(data_array, columns=column_names)
df['마취기록작성번호'] = df['마취기록작성번호'].astype(int)
len(df['마취기록작성번호'].unique())
bp_df = df[df['모니터링약어명'].str.contains('BP')]
len(bp_df['마취기록작성번호'].unique())
bp_df['모니터링기록일시'] = pd.to_datetime(bp_df['모니터링기록일시'])
bp_df['측정값'] = bp_df['측정값'].astype(float)
abp_df = df[df['모니터링약어명'].str.contains('ABP')]
len(abp_df['마취기록작성번호'].unique())
nibp_df = df[df['모니터링약어명'].str.contains('ABP') == False]
len(nibp_df['마취기록작성번호'].unique())

# df_pq = pd.read_parquet("/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.parquet", engine='pyarrow')
# abp_df = df_pq[df_pq['모니터링약어명'].str.contains('ABP')]
# abp_df['측정값'] = abp_df['측정값'].astype(float)

df['모니터링명'].unique()

test = df[df['모니터링약어명'].str.contains('ABP')]

len(test['마취기록작성번호'].unique())

df[df['모니터링명'] == 'Pulmonary Artery Pressure(systolic)']

test['모니터링명'].unique()
test['모니터링약어명'].unique()
len(df['마취기록작성번호'].unique())

nisbp_df = nibp_df[nibp_df['모니터링약어명'].str.contains('S')]
nisbp_df = nisbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
nidbp_df = nibp_df[nibp_df['모니터링약어명'].str.contains('D')]
nidbp_df = nidbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
nimbp_df = nibp_df[nibp_df['모니터링약어명'].str.contains('M')]
nimbp_df = nimbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])

nibp_df = pd.merge(nisbp_df, nidbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
nibp_df.rename(columns={'측정값_x':'sbp', '측정값_y':'dbp'}, inplace=True)
nibp_df = pd.merge(nibp_df, nimbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
nibp_df.rename(columns={'측정값':'mbp'}, inplace=True)

print(nibp_df.keys())
nibp_df = nibp_df[['마취기록작성번호', '모니터링기록일시', 'sbp', 'mbp', 'dbp']]

sbp_df = abp_df[abp_df['모니터링약어명'].str.contains('S')]
sbp_df = sbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
dbp_df = abp_df[abp_df['모니터링약어명'].str.contains('D')]
dbp_df = dbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
mbp_df = abp_df[abp_df['모니터링약어명'].str.contains('M')]
mbp_df = mbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])

abp_df = pd.merge(sbp_df, dbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
abp_df.rename(columns={'측정값_x':'sbp', '측정값_y':'dbp'}, inplace=True)
abp_df = pd.merge(abp_df, mbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
abp_df.rename(columns={'측정값':'mbp'}, inplace=True)


print(abp_df.keys())
abp_df = abp_df[['마취기록작성번호', '모니터링기록일시', 'sbp', 'mbp', 'dbp']]
abp_df = pd.concat([nibp_df, abp_df])
abp_df.drop(abp_df[pd.isnull(abp_df['sbp'])].index, inplace=True)
abp_df.drop(abp_df[abp_df['sbp'].str.contains('^[a-zA-Z]*$', None) == True].index, inplace=True)
abp_df = abp_df[abp_df['sbp'].str.contains('^[0-9\.]*$', None) == True]
abp_df = abp_df[abp_df['dbp'].str.contains('^[0-9\.]*$', None) == True]
abp_df = abp_df[abp_df['mbp'].str.contains('^[0-9\.]*$', None) == True]
abp_df.drop(abp_df[abp_df['sbp'] == '.'].index, inplace=True)
abp_df.drop(abp_df[abp_df['mbp'] == '.'].index, inplace=True)
abp_df.drop(abp_df[abp_df['mbp'] == '5..6'].index, inplace=True)
abp_df = abp_df.reset_index(drop=True)
abp_df['sbp'] = abp_df['sbp'].astype(float)
abp_df['dbp'] = abp_df['dbp'].astype(float)
abp_df['mbp'] = abp_df['mbp'].astype(float)

abp_df = abp_df[((abp_df['sbp'] < 300) & (abp_df['sbp'] > 20) & (abp_df['sbp'] > abp_df['dbp']+5)) & ((abp_df['dbp'] > 5) & (abp_df['dbp'] < 225))]
abp_df = abp_df[(abp_df['mbp'] < abp_df['sbp']) & (abp_df['mbp'] > abp_df['dbp'])]

len(abp_df)
## case별로 각각 전처리되고 전처기 과정이 기록이 되도록

test = abp_df[((abp_df['sbp'] < 300) & (abp_df['sbp'] > 20) & (abp_df['sbp'] > abp_df['dbp']+5)) & ((abp_df['dbp'] > 5) & (abp_df['dbp'] < 225))]
test = abp_df[(abp_df['mbp'] < abp_df['sbp']) & (abp_df['mbp'] > abp_df['dbp'])]

len(test['마취기록작성번호'].unique())

# outliar_index = abp_df[((abp_df['모니터링약어명'].str.contains('S')) & ((abp_df['측정값'] >= 300) | (abp_df['측정값'] <= 20))) | ((abp_df['모니터링약어명'].str.contains('D')) & ((abp_df['측정값'] <= 5) | (abp_df['측정값'] >= 225)))]
# del_idx = abp_df[(abp_df['마취기록작성번호'].isin(outliar_index['마취기록작성번호'])) & (abp_df['모니터링기록일시'].isin(outliar_index['모니터링기록일시']))].index
# abp_df.drop(del_idx, axis=0, inplace=True)

fig, ax = plt.subplots(figsize=(12, 8))
plt.hist(abp_df['mbp'], label="ABP_M", bins=40)
sns.distplot(nibp_df['mbp'], hist=True, kde=True, color = 'blue',
             hist_kws={'edgecolor':'black'}, bins=40)
plt.xticks(fontsize=20)
plt.yticks(fontsize=20)
plt.title("ABP_M+NIMBP", fontsize=28)
plt.ylabel('Density', fontsize=20)
plt.xlabel('MBP', fontsize=20)
# current_values = plt.gca().get_yticks()
# plt.gca().set_yticklabels(['{:,.0f}'.format(x) for x in current_values])
plt.show()



anesnum_info = []
hpi_info = []
hpicount_info = []
mean_max = []
mean_mean = []
mean_min = []
hpi_max = []
hpi_min = []
hpi_mean = []
exclusion_incision_missing_count = 0
exclusion_incision_over_count = 0
area_info = []
real_area_info = []

error_anes_num = []
error_info = []
abp_df.keys()
count = 0
for anes_num, anes_time, surgery_time in zip(anes['마취기록작성번호'], anes['(마취기록)마취시작'], anes['(마취기록)수술시작']):
    count += 1
    pat_data = abp_df[(abp_df['마취기록작성번호'] == anes_num) & (abp_df['모니터링기록일시'] >= anes_time) & (abp_df['모니터링기록일시'] <= surgery_time)].reset_index(drop=True)
    pat_data = pat_data.sort_values(by='모니터링기록일시', ascending=True).reset_index(drop=True)
    if len(pat_data) <= 2 :
        error_anes_num.append(anes_num)
        if pd.isnull(anes_time) and pd.isnull(surgery_time):
            error_info.append("마취시작, 수술시작 시간 없음")
            continue
        elif pd.isnull(surgery_time):
            error_info.append("수술시작 시간 없음")
            continue
        error_info.append("해당 환자 모니터링 데이터 없음")
        continue
    pat_data['minute'] = (pat_data['모니터링기록일시'] - pat_data['모니터링기록일시'].min()).dt.total_seconds() / 60
    pat_data['minute'] = pat_data['minute'].astype(int)
    time_array = pat_data['minute'].to_numpy()
    hpi = pat_data[pat_data['mbp'] < 55]
    hpi_count = len(hpi)
    mbp_stat = pat_data.groupby('마취기록작성번호')
    anesnum_info.append(anes_num)
    mean_max.append(mbp_stat.max('mbp')['mbp'].reset_index().iloc[0]['mbp'])
    mean_min.append(mbp_stat.min('mbp')['mbp'].reset_index().iloc[0]['mbp'])
    mean_mean.append(mbp_stat.mean('mbp')['mbp'].round(1).astype(float).reset_index().iloc[0]['mbp'])
    if hpi_count >= 1:
        hpi_info.append(1)
        time_diff = np.diff(pat_data['minute'].values)
        time_diff = np.append(np.array([time_diff[0]]), time_diff)
        hpi['hpi_diff'] = 60 - hpi['mbp']
        hpi_area = int((hpi['hpi_diff'].values * time_diff[hpi.index]).sum())
        hpi_time = time_diff[hpi.index].sum()
        hpicount_info.append(hpi_time)
        hpi_stat = hpi.groupby('마취기록작성번호')
        hpi_max.append(hpi_stat.max('mbp')['mbp'].reset_index().iloc[0]['mbp'])
        hpi_min.append(hpi_stat.min('mbp')['mbp'].reset_index().iloc[0]['mbp'])
        hpi_mean.append(hpi_stat.mean('mbp')['mbp'].round(1).astype(float).reset_index().iloc[0]['mbp'])
        area_info.append(hpi_area)
    else:
        hpi_info.append(0)
        hpicount_info.append(0)
        hpi_max.append(None)
        hpi_min.append(None)
        hpi_mean.append(None)
        area_info.append(None)
    if count % 1000 == 0:
        print(count)
        test = pd.DataFrame({'anes_num': anesnum_info, 'Hypotension': hpi_info, 'Hypotension_Area': area_info, 'Hypotension_Time(minute)': hpicount_info, 'MBP_Max': mean_max, 'MBP_Mean': mean_mean, 'MBP_Min': mean_min})
        test.to_excel('/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/55_outcomes.xlsx', index=False, sheet_name='HPI')
#     # print(len(test))
test = pd.DataFrame({'anes_num': anesnum_info, 'Hypotension': hpi_info, 'Hypotension_Area': area_info,'Hypotension_Time(minute)': hpicount_info, 'MBP_Max': mean_max, 'MBP_Mean': mean_mean, 'MBP_Min': mean_min})
test.to_excel('/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/55_temp_outcomes.xlsx', index=False, sheet_name='HPI')
# test.to_csv('/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/temp_outcomes_csv_ver.csv', index=False)

len(abp_df['마취기록작성번호'].unique())


import os
import pandas as pd
path = '/srv/project_data/EMR/monitering/'
file_list = os.listdir(path)

for file in file_list:
    temp = pd.read_excel(path+file)
    temp['마취기록작성번호'] = temp['마취기록작성번호'].astype(int)
    print(len(temp['마취기록작성번호'].unique()))
    temp_abp = temp[temp['모니터링약어명'].str.contains('ABP')]
    print(len(temp_abp['마취기록작성번호'].unique()))
    print("")


