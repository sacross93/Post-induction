import pandas as pd
import glob

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

## 모니터링 데이터 주소 및 파일 명 전부 불러오기
monitering_list = glob.glob('/srv/project_data/EMR/sr_data/monitering/*')

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

for moni_sr in monitering_list:
    intra_op = pd.read_excel(moni_sr, sheet_name=None)
    for intra_data in intra_op.keys():
        ABP_data = intra_op[intra_data]
        ABP_data = ABP_data[(ABP_data['모니터링약어명'].str.contains('ABP') & ABP_data['모니터링약어명'].str.contains('_M')) | (ABP_data['모니터링약어명'].str.contains('MBP'))]
        ABP_data['측정값'] = ABP_data['측정값'].astype(float)
        ABP_data['모니터링기록일시'] = pd.to_datetime(ABP_data['모니터링기록일시'])
        ABP_data = ABP_data[(ABP_data['측정값'] >= 20) & (ABP_data['측정값'] <= 150)]
        for anes_num in ABP_data['마취기록작성번호'].unique():
            anes_info = anes[(anes['마취기록작성번호'] == anes_num)]
            pat_data = ABP_data[(ABP_data['마취기록작성번호'] == anes_info['마취기록작성번호'].iloc[0]) & (
                       ABP_data['모니터링기록일시'] >= anes_info['(마취기록)마취시작'].iloc[0]) & (
                       ABP_data['모니터링기록일시'] <= anes_info['(마취기록)수술시작'].iloc[0])]
            if len(pat_data) == 0:
                print(f"마취시작시간: {anes_info['(마취기록)마취시작'].iloc[0]}, 수술시작시간: {anes_info['(마취기록)수술시작'].iloc[0]}, 첫 모니터링시간: {ABP_data[ABP_data['마취기록작성번호'] == anes_info['마취기록작성번호'].iloc[0]].iloc[0]['모니터링기록일시']}")
                if len(anes_info[pd.isnull(anes_info['(마취기록)수술시작'])]) > 0:
                    exclusion_incision_missing_count += 1
                else:
                    exclusion_incision_over_count += 1
                print(f"수술시작시간 x: {exclusion_incision_missing_count}, 사이 시간 기록 x: {exclusion_incision_over_count}, 테스트: {len(anes_info[pd.isnull(anes_info['(마취기록)수술시작'])])}")
                continue
            hpi = pat_data[pat_data['측정값'] < 60]
            hpi_count = len(hpi)
            mbp_stat = pat_data.groupby('마취기록작성번호')
            anesnum_info.append(anes_num)
            mean_max.append(mbp_stat.max('측정값')['측정값'].reset_index().iloc[0]['측정값'])
            mean_min.append(mbp_stat.min('측정값')['측정값'].reset_index().iloc[0]['측정값'])
            mean_mean.append(mbp_stat.mean('측정값')['측정값'].round(1).astype(float).reset_index().iloc[0]['측정값'])
            if hpi_count >= 1:
                hpi_info.append(1)
                hpicount_info.append(hpi_count)
                hpi_stat = hpi.groupby('마취기록작성번호')
                hpi_max.append(hpi_stat.max('측정값')['측정값'].reset_index().iloc[0]['측정값'])
                hpi_min.append(hpi_stat.min('측정값')['측정값'].reset_index().iloc[0]['측정값'])
                hpi_mean.append(hpi_stat.mean('측정값')['측정값'].round(1).astype(float).reset_index().iloc[0]['측정값'])
            else :
                hpi_info.append(0)
                hpicount_info.append(0)
                hpi_max.append(None)
                hpi_min.append(None)
                hpi_mean.append(None)
        test = pd.DataFrame({'마취기록작성번호': anesnum_info, 'HPI': hpi_info, 'HPI_Count': hpicount_info, 'MBP_Max': mean_max, 'MBP_Mean': mean_mean, 'MBP_Min': mean_min})
        test.to_excel('/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/temp_outcomes.xlsx', index=False)

### duration 알고리즘 시작
            #     for i in hpi.index:

test2 = pd.DataFrame({'마취기록작성번호': anesnum_info, 'HPI': hpi_info, 'HPI_Count': hpicount_info, 'MBP_Max': mean_max, 'MBP_Mean': mean_mean, 'MBP_Min': mean_min,
                      'HPI_Max':hpi_max, 'HPI_Mean':hpi_mean, 'HPI_Min':hpi_min})
test2.to_excel('/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/20230518_temp_outcomes.xlsx', index=False)

ABP_data[ABP_data['마취기록작성번호'] == anes_info['마취기록작성번호'].iloc[0]].iloc[0]['모니터링기록일시']

ABP_data[(ABP_data['모니터링약어명'].str.contains('ABP') & ABP_data['모니터링약어명'].str.contains('_M')) | (ABP_data['모니터링약어명'].str.contains('MBP'))]