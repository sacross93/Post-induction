import numpy as np
import pandas as pd

preprocessing_path = '/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/'

############################################## READ
## lab 데이터, 환자기본 정보, 응급수술 여부
basic_pat = pd.read_excel(preprocessing_path+'00_basic_emergency.xlsx')

## General 수술 여부
method_general = pd.read_csv(preprocessing_path+'04_anesthesia_induction.csv')
method_general = method_general[method_general['마취방법'] == 'General']

## 마취 유도제 정보
anesthesia_induction = pd.read_csv(preprocessing_path+'05_anesthetic.csv')

## 마취 유지제 정보
maintenance_gas = pd.read_csv(preprocessing_path+'05_maintenance_gas.csv')

## 병동 혈압 정보
ward_bp = pd.read_excel(preprocessing_path+'06_Ward_BP.xlsx')

## 프리 인덕셕 혈압 정보
preinduction_bp = pd.read_csv(preprocessing_path+'09_Pre-induction_BP.csv')

############################################## MERGE
## 이미 18세 이상 필터링은 되어있음
print(len(basic_pat[basic_pat['Age']>=18]), len(basic_pat))

## general 수술이 아닌 것 제외하고 수술 추가
basic_method = pd.merge(basic_pat, method_general, how='inner', on=['마취기록작성번호'])

## 마취 유도제 추가
basic_induction = pd.merge(basic_method, anesthesia_induction, how='inner', on=['마취기록작성번호'])

## 마취 유지제 추가
test = pd.get_dummies(maintenance_gas, columns=['입력항목명'])
test_group = test.groupby('마취기록작성번호')
test_max = test_group.max().reset_index()
test_max[(test_max['입력항목명_1% Propofol'] == 1) & (test_max['마취기록작성번호'] == 20200400074557)]

for i in test.keys():
    print(test[i][test['마취기록작성번호'] == 20200400074557])
test.keys()

propofol = np.zeros(len(test_max))
test_idx = test_max['마취기록작성번호'].unique()
for i in test_max['마취기록작성번호'][(test_max['입력항목명_1% Propofol'] == 1) | (test_max['입력항목명_2% Propofol'] == 1) | (test_max['입력항목명_2% Propofol 1000mg'] == 1) | (test_max['입력항목명_2% Propofol(mcg/mL)'] == 1)].unique():
    temp_idx = np.where(test_idx == i)[0]
    if len(temp_idx) == 0 :
        continue
    temp_idx = temp_idx[0]
    propofol[temp_idx] = 1
test_max['입력항목명_Propofol'] = propofol
test_max.columns = test_max.keys().str.replace("입력항목명_","마취유지제_")
del test_max['마취유지제_1% Propofol'], test_max['마취유지제_2% Propofol'], test_max['마취유지제_2% Propofol 1000mg'], test_max['마취유지제_2% Propofol(mcg/mL)']


basic_anesthesia = pd.merge(basic_induction, test_max, how='inner', on=['마취기록작성번호'])

## 병동 혈압 추가
basic_ward = pd.merge(basic_anesthesia, ward_bp, how='inner', on=['마취기록작성번호'])

## 프리인덕션 혈압 추가
basic_BP = pd.merge(basic_ward, preinduction_bp, how='inner', on=['마취기록작성번호'])

## 최종본 저장
basic_BP.to_excel(preprocessing_path+'Post-induction_data_set_ver2.xlsx', index=False)

## 진단코드 더미 읽기
diagnosis = pd.read_csv(preprocessing_path+'07_Diagnosis_dummies.csv')

## 진단코드가 현재 데이터셋에 얼마나 적합한지 확인하기
basic_BP[basic_BP['마취기록작성번호'].isin(diagnosis['마취기록작성번호'].unique())]
## 100% 적합




