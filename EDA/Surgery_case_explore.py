import pandas as pd
import numpy as np
import EMRLibJY

op_df = EMRLibJY.read_intra_operation_data()
surgery_df = EMRLibJY.read_surgery_info()
patient_df = EMRLibJY.read_PIH_patient_info()

len(op_df['마취기록작성번호'].unique())
len(surgery_df['마취기록작성번호'].unique())
len(patient_df['마취기록작성번호'].unique())

len(surgery_df[surgery_df['마취기록작성번호'].isin(patient_df['마취기록작성번호'])])
test = op_df[op_df['마취기록작성번호'].isin(patient_df['마취기록작성번호'])]
testnot = op_df[op_df['마취기록작성번호'].isin(patient_df['마취기록작성번호']) == False]
testnot = op_df[op_df['마취기록작성번호'].isin(surgery_df['마취기록작성번호']) == False]
len(test['마취기록작성번호'].unique())
len(testnot['마취기록작성번호'].unique())

## 1. 환자번호로 groupby --> 환자당 마취기록작성번호가 평균 몇개인지 민 맥스
pat_group = patient_df.groupby('병원등록번호')
pat_count = pat_group['마취기록작성번호'].agg(['count'])
pat_count['count'].min()
pat_count['count'].max()
pat_count['count'].mean()
np.quantile(pat_count['count'].to_numpy(), [0, 0.25, 0.5, 0.75, 1.0])

import matplotlib.pyplot as plt
plt.figure(figsize=(12,8))
plt.hist(pat_count['count'], bins=40)
plt.show()

## 2. 마취기록작성번호 groupby --> ABP 카운트를 출력 --> 이상값을 걸러서 다시 ABP 카운트 각각 출력 (걸러진거 안걸러진거) --> MBP 이상 값 처리 기준에서 다시 ABP 카운트 각각 출력
# test = op_df[op_df['마취기록작성번호'].isin(patient_df['마취기록작성번호'])]
# test.reset_index(drop=True, inplace=True)
# test = test[test['모니터링약어명'].str.contains('BP')]
# print(test['모니터링약어명'].unique())
# mtest = test[(test['모니터링약어명'].str.contains('ABP')) & (test['모니터링약어명'].str.contains('M'))]
# abp_group = mtest.groupby('마취기록작성번호')
# mbp_count = abp_group['측정값'].agg(['count'])
# mbp_count['count'].min()
# mbp_count['count'].max()
# mbp_count['count'].mean()
# np.quantile(mbp_count['count'].to_numpy(), [0, 0.25, 0.5, 0.75, 1.0]).astype(int)

path = "/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.npz"
aa = np.load(path, allow_pickle=True)
column_names = ['RN', '병원등록번호', '마취기록작성번호', '수술일자', '수술일정번호', '모니터링명', '모니터링약어명', '모니터링기록일시', '측정값']
data_array = aa['arr_0']
df = pd.DataFrame(data_array, columns=column_names)
df['마취기록작성번호'] = df['마취기록작성번호'].astype(int)
bp_df = df[df['모니터링약어명'].str.contains('BP')]

test[(test['모니터링기록일시'] >= '2020-07-09 11:41:00') & (test['모니터링기록일시'] <= '2020-07-09 11:53:00')]

#& (test['모니터링기록일시'] <= surgery_df['(마취기록)수술시작'][surgery_df['마취기록작성번호'] == 20200700093024]))

bp_df['모니터링기록일시'] = pd.to_datetime(bp_df['모니터링기록일시'])
bp_df['측정값'] = bp_df['측정값'].astype(float)
bp_df['모니터링약어명'].unique()
abp_df = df[df['모니터링약어명'].str.contains('ABP')]
nibp_df = df[df['모니터링약어명'].str.contains('ABP') == False]
print(f"BP를 측정한 수술 건수: {len(df['마취기록작성번호'].unique())} \nBP row 갯수 {len(df['마취기록작성번호'])}")
print(f"BP를 측정한 수술 건수: {len(bp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(bp_df['마취기록작성번호'])}")
test = bp_df[bp_df['마취기록작성번호'].isin(patient_df['마취기록작성번호'])]
print(f"BP를 측정한 수술 건수: {len(test['마취기록작성번호'].unique())} \nBP row 갯수 {len(test['마취기록작성번호'])}")
nibp_df = test[test['모니터링약어명'].str.contains('ABP') == False]
abp_df = test[test['모니터링약어명'].str.contains('ABP')]

len(nibp_df['마취기록작성번호'].unique())
len(abp_df['마취기록작성번호'].unique())
len(nibp_df['마취기록작성번호'][nibp_df['마취기록작성번호'].isin(abp_df['마취기록작성번호'].unique()) == False].unique())

#54987건 = NIBP는 있으나 ABP가 없음


nisbp_df = nibp_df[nibp_df['모니터링약어명'].str.contains('S')]
nisbp_df = nisbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
print(f"BP를 측정한 수술 건수: {len(nisbp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(nisbp_df['마취기록작성번호'])}")
nidbp_df = nibp_df[nibp_df['모니터링약어명'].str.contains('D')]
nidbp_df = nidbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
print(f"BP를 측정한 수술 건수: {len(nidbp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(nidbp_df['마취기록작성번호'])}")
nimbp_df = nibp_df[nibp_df['모니터링약어명'].str.contains('M')]
nimbp_df = nimbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
print(f"BP를 측정한 수술 건수: {len(nimbp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(nimbp_df['마취기록작성번호'])}")

plt.figure(figsize=(12,8))
plt.hist(nisbp_df['측정값'], bins=100)
plt.show()
plt.figure(figsize=(12,8))
plt.hist(nidbp_df['측정값'], bins=100)
plt.show()
plt.figure(figsize=(12,8))
plt.hist(nimbp_df['측정값'], bins=100)
plt.show()

plt.figure(figsize=(12,8))
plt.hist(nisbp_df['측정값'][nisbp_df['마취기록작성번호'].isin(nimbp_df['마취기록작성번호']) == False], bins=100)
plt.show()


nibp_df = pd.merge(nisbp_df, nidbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
nibp_df.rename(columns={'측정값_x':'sbp', '측정값_y':'dbp'}, inplace=True)
nibp_df = pd.merge(nibp_df, nimbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
nibp_df.rename(columns={'측정값':'mbp'}, inplace=True)

nibp_df = nibp_df[['병원등록번호','마취기록작성번호', '모니터링기록일시', 'sbp', 'mbp', 'dbp']]
print(f"BP를 측정한 수술 건수: {len(nibp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(nibp_df['마취기록작성번호'])}")

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
print(f"BP를 측정한 수술 건수: {len(abp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(abp_df['마취기록작성번호'])}")


print(abp_df.keys())
abp_df = abp_df[['마취기록작성번호', '모니터링기록일시', 'sbp', 'mbp', 'dbp']]
abp_df = pd.concat([nibp_df, abp_df])
print(f"BP를 측정한 수술 건수: {len(abp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(abp_df['마취기록작성번호'])}")
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














########

test1 = pd.read_excel('/srv/project_data/EMR/original_data/211015_ANS_이상욱_2021-1352_1차(3.14).xlsx', sheet_name=None)
test2 = pd.read_excel('/srv/project_data/EMR/original_data/221017_ANS_이상욱_2021-1352_(EMR)_1차(11.2).xlsx', sheet_name=None)

test1['대상환자&1'].rename(columns=test1['대상환자&1'].iloc[0], inplace=True)
test1['대상환자&1'].drop([0], inplace=True)
test1['대상환자&1'].reset_index(drop=True, inplace=True)

test2['0&1'].rename(columns=test2['0&1'].iloc[0], inplace=True)
test2['0&1'].drop([0], inplace=True)
test2['0&1'].reset_index(drop=True, inplace=True)

test1['대상환자&1']['마취기록작성번호'] = test1['대상환자&1']['마취기록작성번호'].astype(int)
test2['0&1']['마취기록작성번호'] = test2['0&1']['마취기록작성번호'].astype(int)

temp1 = test1['대상환자&1'][['마취기록작성번호', '병원등록번호']]
temp2 = test2['0&1'][['마취기록작성번호', '병원등록번호']]

a = pd.concat([temp1,temp2])

len(a['마취기록작성번호'].unique())
