import pandas as pd
import pyspark

conf = pyspark.SparkConf() \
    .setAppName("spark-sql") \
    .set("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java-8.0.33.jar")
sc = pyspark.SparkContext(conf=conf)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("spark-sql") \
    .config("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java-8.0.17.jar") \
    .config("spark.executor.memory", "64g") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2") \
    .config("spark.driver.memory", "32g") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .getOrCreate()

nurse_data = pd.read_excel(
    '/srv/project_data/EMR/original_data/20230221(11.17)_ANS_이상욱_2021-1352_(간호정보조사지)_1차(2.27).xlsx')
spark_df = spark.createDataFrame(nurse_data)
# spark_df.printSchema()
# spark_df.show()

basic_data1 = pd.read_excel('/srv/project_data/EMR/original_data/211015_ANS_이상욱_2021-1352_1차(3.14).xlsx',
                            sheet_name=None)
basic_data2 = pd.read_excel('/srv/project_data/EMR/original_data/221017_ANS_이상욱_2021-1352_(EMR)_1차(11.2).xlsx',
                            sheet_name=None)

print(len(basic_data2), len(basic_data1))

basic_pat1 = basic_data1['대상환자&1'].copy()
basic_pat2 = basic_data2['0&1'].copy()

def EMR_column_arrange(pat_info):
    pat_info.columns = pat_info.loc[0]
    pat_info = pat_info.drop([0])
    pat_info = pat_info.reset_index(drop=True)
    return pat_info

basic_pat1 = EMR_column_arrange(basic_pat1)
basic_pat1 = basic_pat1.rename(columns = {'수술일나이':'수술나이', '내원구분코드':'내원구분'})
basic_pat2 = EMR_column_arrange(basic_pat2)

basic_pat = pd.concat([basic_pat1, basic_pat2])
basic_pat['마취기록작성번호'] = basic_pat['마취기록작성번호'].astype(int)
basic_pat['수술나이'] = basic_pat['수술나이'].astype(int)

## 18세 이상 환자
print(f"전처리 전 환자 숫자: {len(basic_pat)}, 18세 이상 환자 숫자: {len(basic_pat[basic_pat['수술나이'] >= 18])}, 제거 된 숫자: {len(basic_pat) - len(basic_pat[basic_pat['수술나이'] >= 18])}")
basic_pat = basic_pat[basic_pat['수술나이'] >= 18]

## 병동 혈압
ward_pat1 = pd.read_excel('/srv/project_data/EMR/original_data/211015_ANS_이상욱_2021-1352_(EMR-자료5 보완)_2차(6.23).xlsx', sheet_name='5(수술일-1일~수술일)')
ward_pat2 = basic_data2['5'].copy()

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
print(f"병동혈압 전처리 후 환자 수: len(ward_group_mean)")

middle_pat = pd.merge(basic_pat, ward_group_mean, how='inner', on=['마취기록작성번호'])
print(f"제거 전: {len(basic_pat)}, 병동 혈압 비정상으로 6066명 제거: {len(middle_pat)}")

import time
middle_pat.to_excel("/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/middle_pat2.xlsx", index=False)
middle_pat.to_csv("/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/middle_pat2.csv", index=False, encoding='utf-8-sig')


############################################################# infomation
lab_list = ['ASA status', '성별', '나이', 'BMI', 'preop_egfr_ce(ZF0103)', 'preop_egfr_md(ZF0104)', 'preop_wbc ',
            'preop_rbc ', 'preop_mg(BB0017)', 'preop_na(BB0013)', 'preop_alp(BC0014)',
            'preop_hb ', 'preop_hct ', 'preop_ck ', 'preop_glu ', 'preop_bun ', 'preop_rdw(AC0123)',
            'preop_mpv(AC0160)', 'preop_pdw(AC0124)', 'preop_alb ', 'preop_cr',
            'preop_plt', 'preop_pt_sec ', 'preop_pt_inr ', 'preop_aptt ', 'preop_ck_mb ', 'preop_k ', 'preop_cl ',
            'preop_ca ', 'preop_glu ', 'preop_ast ', 'preop_alt ',
            'preop_tp ', 'preop_ua ', 'preop_bilirubin ', 'preop_crp ']