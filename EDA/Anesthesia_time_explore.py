import pandas as pd

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
anes['(마취기록)마취시작'] = pd.to_datetime(anes['(마취기록)마취시작'], format = 'mixed')
anes['(마취기록)수술시작'] = pd.to_datetime(anes['(마취기록)수술시작'], format = 'mixed')

test = (anes['(마취기록)수술시작'] - anes['(마취기록)마취시작']).dt.seconds/60




anes_test = anes[['마취기록작성번호',  '(마취기록)마취시작', '(마취기록)Intubation', '(마취기록)수술시작',
       '(마취기록)수술종료', '(마취기록)Extubation', '(마취기록)마취종료']]


anes_test[(anes['(마취기록)수술시작'] - anes['(마취기록)마취시작']).dt.seconds/60 >= 100]