import pandas as pd
import EMR_Preprocessing as emr

## 병동 혈압
preprocessing_path = 'C:/Users/user/Dropbox/김진영_SignalHouse/0_마취기록지/Post-induction/input_preprocessing/'
original_path = 'C:/Users/user/Dropbox/김진영_SignalHouse/0_마취기록지/original_data/'

basic_data = pd.read_excel(preprocessing_path+'00_basic_patients.xlsx')

ward_pat1 = pd.read_excel(original_path+'211015_ANS_이상욱_2021-1352_(EMR-자료5 보완)_2차(6.23).xlsx', sheet_name='5(수술일-1일~수술일)')
ward_pat2 = pd.read_excel(original_path+'221017_ANS_이상욱_2021-1352_(EMR)_1차(11.2).xlsx', sheet_name='5')

ward_pat1 = EMR_column_arrange(ward_pat1)
ward_pat2 = EMR_column_arrange(ward_pat2)

ward_temp1 = ward_pat1[['마취기록작성번호', '작성일자', '작성시각', '수축기혈압값', '이완기혈압값']].copy()
ward_temp2 = ward_pat2[['마취기록작성번호', '작성일자', '작성시각', '수축기혈압값', '이완기혈압값']].copy()

ward_pat = pd.concat([ward_temp1, ward_temp2])
ward_group = ward_pat.groupby(['마취기록작성번호'])
ward_group_mean = ward_group.mean().reset_index(drop=False)
print(f"병동혈압 전처리 전 환자 수: len(ward_group_mean)")
del ward_group_mean
print(f"병동 혈압 측정 N수: {len(ward_pat)}, 혈압을 측정하지 않아 제거 된 N수: {len(ward_pat) - len(ward_pat[(ward_pat['수축기혈압값'].isnull() == False) & (ward_pat['이완기혈압값'].isnull() == False)])}")
ward_pat = ward_pat[(ward_pat['수축기혈압값'].isnull() == False) & (ward_pat['이완기혈압값'].isnull() == False)]
print(f"남은 N수: {len(ward_pat)}")

ward_pat['작성일자'] = pd.to_datetime(ward_pat['작성일자'], format='%Y%m%d')
ward_pat['병원등록번호'] = ward_pat['마취기록작성번호'].astype(int)
ward_pat['WARD_SBP'] = ward_pat['수축기혈압값'].astype(int)
ward_pat['WARD_DBP'] = ward_pat['이완기혈압값'].astype(int)
print(f"혈압 측정이 잘못 된 N수: {len(ward_pat) - len(ward_pat[ward_pat['WARD_SBP'] > ward_pat['WARD_DBP']])} (SBP가 DBP보다 작은 경우)")
ward_pat = ward_pat[ward_pat['WARD_SBP'] > ward_pat['WARD_DBP']]
print(f"남은 N수: {len(ward_pat)}")
ward_pat['WARD_MBP'] = (ward_pat['WARD_SBP'] + (2*ward_pat['WARD_DBP'])) / 3
ward_pat['WARD_MBP'] = ward_pat['WARD_MBP'].astype(int)

ward_group = ward_pat.groupby(['마취기록작성번호'])
ward_group_mean = ward_group.mean().reset_index(drop=False)
ward_group_mean = ward_group_mean[['마취기록작성번호', 'WARD_SBP', 'WARD_DBP', 'WARD_MBP']].astype(int)
print(f"병동혈압 전처리 후 환자 수: {len(ward_group_mean)}")

basic_18 = basic_pat[basic_pat['수술나이'] >= 18]
middle_pat = pd.merge(basic_18, ward_group_mean, how='inner', on=['마취기록작성번호'])
print(f"제거 전: {len(basic_18)}, 병동 혈압 비정상으로 6066명 제거: {len(middle_pat)}")