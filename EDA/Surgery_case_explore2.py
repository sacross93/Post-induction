import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import EMRLibJY

op_df = EMRLibJY.read_intra_operation_data()
surgery_df = EMRLibJY.read_surgery_info()
patient_df = EMRLibJY.read_PIH_patient_info()

path = "/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.npz"
aa = np.load(path, allow_pickle=True)
column_names = ['RN', '병원등록번호', '마취기록작성번호', '수술일자', '수술일정번호', '모니터링명', '모니터링약어명', '모니터링기록일시', '측정값']
data_array = aa['arr_0']
df = pd.DataFrame(data_array, columns=column_names)
df['마취기록작성번호'] = df['마취기록작성번호'].astype(int)
bp_df = df[df['모니터링약어명'].str.contains('BP')]
len(bp_df['마취기록작성번호'].unique())
len(bp_df)

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

nibp_df = pd.merge(nisbp_df, nidbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
nibp_df.rename(columns={'측정값_x':'sbp', '측정값_y':'dbp'}, inplace=True)
nibp_df = pd.merge(nibp_df, nimbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
nibp_df.rename(columns={'측정값':'mbp'}, inplace=True)


nibp_df = nibp_df[['병원등록번호','마취기록작성번호', '모니터링기록일시', 'sbp', 'mbp', 'dbp']]
print(f"BP를 측정한 수술 건수: {len(nibp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(nibp_df['마취기록작성번호'])}")

sbp_df = abp_df[abp_df['모니터링약어명'].str.contains('S')]
sbp_df = sbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
print(f"BP를 측정한 수술 건수: {len(sbp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(sbp_df['마취기록작성번호'])}")
dbp_df = abp_df[abp_df['모니터링약어명'].str.contains('D')]
dbp_df = dbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
print(f"BP를 측정한 수술 건수: {len(dbp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(dbp_df['마취기록작성번호'])}")
mbp_df = abp_df[abp_df['모니터링약어명'].str.contains('M')]
mbp_df = mbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
print(f"BP를 측정한 수술 건수: {len(mbp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(mbp_df['마취기록작성번호'])}")

abp_df = pd.merge(sbp_df, dbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
abp_df.rename(columns={'측정값_x':'sbp', '측정값_y':'dbp'}, inplace=True)
abp_df = pd.merge(abp_df, mbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
abp_df.rename(columns={'측정값':'mbp'}, inplace=True)
print(f"BP를 측정한 수술 건수: {len(abp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(abp_df['마취기록작성번호'])}")
aa = pd.merge(abp_df, nibp_df, how='outer', on=['마취기록작성번호'])
len(aa['마취기록작성번호'].unique())


print(abp_df.keys())
abp_df = abp_df[['마취기록작성번호', '모니터링기록일시', 'sbp', 'mbp', 'dbp']]
abp_df = pd.concat([nibp_df, abp_df])
old_abp = abp_df.copy()
print(f"BP를 측정한 수술 건수: {len(abp_df['마취기록작성번호'].unique())} \nBP row 갯수 {len(abp_df['마취기록작성번호'])}")

abp_df = abp_df[((abp_df['sbp'] < 300) & (abp_df['sbp'] > 20) & (abp_df['sbp'] > abp_df['dbp']+5)) & ((abp_df['dbp'] > 5) & (abp_df['dbp'] < 225))]


nibp_df = nibp_df[((nibp_df['sbp'] < 300) & (nibp_df['sbp'] > 20) & (nibp_df['sbp'] > nibp_df['dbp']+5)) & ((nibp_df['dbp'] > 5) & (nibp_df['dbp'] < 225))]
nibp_df = nibp_df[(nibp_df['mbp'] < nibp_df['sbp']) & (nibp_df['mbp'] > nibp_df['dbp'])]
nibp_df['cal_mbp'] = round(((nibp_df['dbp'] * 2) + nibp_df['sbp']) / 3,2)
nibp_df = nibp_df[['마취기록작성번호', '모니터링기록일시', 'sbp', 'mbp', 'dbp', 'cal_mbp']]


import matplotlib.pyplot as plt

nibp_df.keys()
for idx, anes_num in enumerate(nibp_df['마취기록작성번호'].unique()):
    temp = nibp_df[nibp_df['마취기록작성번호']==anes_num]
    temp = temp.sort_values(by='모니터링기록일시', ascending=True).reset_index(drop=True)
    temp['minute'] = (temp['모니터링기록일시'] - temp['모니터링기록일시'].min()).dt.total_seconds() / 60
    plt.figure(figsize=(12, 8))
    plt.plot(temp['minute'], temp['sbp'], label='sbp', marker='o')
    plt.plot(temp['minute'], temp['dbp'], label='dbp', marker='o')
    plt.plot(temp['minute'], temp['mbp'], label='mbp', marker='o')
    plt.plot(temp['minute'], temp['cal_mbp'], label='cal_mbp', marker='o')
    plt.axhline(60, color='red', linestyle='-')
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.ylim(0,240)
    plt.legend()
    plt.show()
    if idx >= 5:
        break

nibp_df['mbp'].max()

plt.figure(figsize=(12,8))
plt.plot(nibp_df['mbp'], nibp_df['cal_mbp'] - nibp_df['mbp'], 'o', markersize=2, alpha=0.1)
plt.xlim(0,200)
plt.ylim(-100,100)
plt.show()

test = nibp_df[nibp_df['mbp'] < 60]
plt.figure(figsize=(12,8))
plt.hist(test[['cal_mbp'] - test['mbp']])
plt.plot()
plt.show()