import pandas as pd
import glob
from scipy import interpolate
import numpy as np
import matplotlib.pyplot as plt

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

## ABP만 가져올 수 있도록 전처리
df['모니터링기록일시'] = pd.to_datetime(df['모니터링기록일시'])
df = df[(df['모니터링약어명'].str.contains('ABP') & df['모니터링약어명'].str.contains('_M')) | (df['모니터링약어명'].str.contains('MBP'))]
df['측정값'] = df['측정값'].astype(float)
ABP_data = df[(df['측정값'] >= 20) & (df['측정값'] <= 150)]
ABP_data['모니터링기록일시'] = pd.to_datetime(ABP_data['모니터링기록일시'])

count = 0
for anes_num, anes_time, surgery_time in zip(anes['마취기록작성번호'], anes['(마취기록)마취시작'], anes['(마취기록)수술시작']):
    count += 1
    pat_data = ABP_data[(ABP_data['마취기록작성번호'] == anes_num) & (ABP_data['모니터링기록일시'] >= anes_time) & (ABP_data['모니터링기록일시'] <= surgery_time)].reset_index(drop=True)
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
    hpi = pat_data[pat_data['측정값'] < 60]
    hpi_count = len(hpi)
    mbp_stat = pat_data.groupby('마취기록작성번호')
    anesnum_info.append(anes_num)
    mean_max.append(mbp_stat.max('측정값')['측정값'].reset_index().iloc[0]['측정값'])
    mean_min.append(mbp_stat.min('측정값')['측정값'].reset_index().iloc[0]['측정값'])
    mean_mean.append(mbp_stat.mean('측정값')['측정값'].round(1).astype(float).reset_index().iloc[0]['측정값'])
    if hpi_count >= 1:
        t = pat_data['minute'].to_numpy()
        v = pat_data['측정값'].to_numpy()
        dt = np.append([5], np.diff(t))
        f = interpolate.interp1d(t, v, kind='linear')
        total_time = t[-1]
        area_dt = 0.1
        tnew = np.arange(total_time / area_dt + 1) * area_dt
        vnew = f(tnew)
        ind = np.where(vnew < 60)[0]
        hvnew = vnew[ind]
        # vind = round(np.sum(60-v[vind])*5,2)
        # area = round(np.sum(60-hvnew)*0.1,2)
        area = np.sum(60 - hvnew) * 0.1
        if area == 0:
            area = (60 - v.min()) * 0.1
            print(f"{anes_num}: area 계산 오류 {anes_time}, {surgery_time}, fixed: {area}")
        hpi_info.append(1)
        if len(tnew[ind]) * 6 < 60:
            hpi_count = 1
        else:
            hpi_count = len(tnew[ind]) * 6 // 60
        hpicount_info.append(hpi_count)
        hpi_stat = hpi.groupby('마취기록작성번호')
        hpi_max.append(hpi_stat.max('측정값')['측정값'].reset_index().iloc[0]['측정값'])
        hpi_min.append(hpi_stat.min('측정값')['측정값'].reset_index().iloc[0]['측정값'])
        hpi_mean.append(hpi_stat.mean('측정값')['측정값'].round(1).astype(float).reset_index().iloc[0]['측정값'])
        area_info.append(area)
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
        test.to_excel('/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/temp_outcomes.xlsx', index=False, sheet_name='HPI')
    # print(len(test))
test = pd.DataFrame({'anes_num': anesnum_info, 'Hypotension': hpi_info, 'Hypotension_Area': area_info,'Hypotension_Time(minute)': hpicount_info, 'MBP_Max': mean_max, 'MBP_Mean': mean_mean, 'MBP_Min': mean_min})
test.to_excel('/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/20230623_temp_outcomes.xlsx', index=False, sheet_name='HPI')
test.to_csv('/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/temp_outcomes_csv_ver.csv', index=False)

################ 면적/시간 추가
import pandas as pd

outcomes = pd.read_csv('/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/temp_outcomes_csv_ver.csv')

outcomes['area/time'] = outcomes['Hypotension_Area'] / outcomes['Hypotension_Time(minute)']
outcomes.to_csv('/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/Post-induction_outcomes.csv', index=False)
