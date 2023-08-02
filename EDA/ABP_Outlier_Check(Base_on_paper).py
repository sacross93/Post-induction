#참고 논문: Intraoperative Hypotension Is Associated With Adverse Clinical Outcomes After Noncardiac Surgery
import pandas as pd
import numpy as np


path = "/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.npz"
aa = np.load(path, allow_pickle=True)
column_names = ['RN', '병원등록번호', '마취기록작성번호', '수술일자', '수술일정번호', '모니터링명', '모니터링약어명', '모니터링기록일시', '측정값']
# column_names = ['RN', 'pat_num', 'anes_num', 'op_date', 'op_num', 'monitor_name', 'abbreviation_name', 'measurement_date', 'measure_value']
data_array = aa['arr_0']
df = pd.DataFrame(data_array, columns=column_names)

test = df[df['모니터링약어명'].str.contains('ABP')]
test['모니터링기록일시'] = pd.to_datetime(test['모니터링기록일시'])
test['측정값'] = test['측정값'].astype(float)
test['마취기록작성번호'] = test['마취기록작성번호'].astype(int)

test_SBP = test[test['모니터링약어명'].str.contains(r'ABP[0-9]_S')]
test_SBP = test_SBP[test_SBP['측정값'].between(20, 300, inclusive='neither') == False]

test_DBP = test[test['모니터링약어명'].str.contains(r'ABP[0-9]_D')]
test_DBP = test_SBP[test_SBP['측정값'].between(5, 225, inclusive='neither') == False]

ABP_False = pd.concat([test_SBP, test_DBP])

test_ABP = test[test['모니터링약어명'].str.contains(r'ABP[0-9]_')]

False_idx = np.array([])
for False_data in ABP_False.iloc:
    temp_data = test_ABP[(test_ABP['마취기록작성번호'] == False_data['마취기록작성번호']) & (test_ABP['모니터링기록일시'] == False_data['모니터링기록일시'])]
    # if len(temp_data) != 0:
    #     False_idx = np.concatenate([False_idx, temp_data.index.to_numpy()], axis=0)
    if len(temp_data) != 3:
        print(temp_data)
        break
False_idx = False_idx.astype(int)

len(test_ABP) - len(test_ABP[test_ABP.index.isin(False_idx) == False])

exception_ABP = test_ABP[test_ABP.index.isin(False_idx) == False]

aa = pd.Series(False_idx)
aa[aa.duplicated()]
aa.drop_duplicates()

aa[aa==77312464]
aa[aa==77312416]
aa[aa==77312440]
tt = test_ABP['모니터링기록일시'][test_ABP.index == 77312464].iloc[0]
nn = test_ABP['마취기록작성번호'][test_ABP.index == 77312464].iloc[0]
test_ABP[(test_ABP['모니터링기록일시'] == tt) & (test_ABP['마취기록작성번호'] == nn)]


## Spark 테스트
import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
os.environ['SPARK_LOCAL_IP'] = '192.168.134.193'
import pyspark.pandas as ps
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
    .config("spark.executor.memory", "8g") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .config("spark.executor.instances", "10") \
    .getOrCreate()

spark_test = test_ABP[['마취기록작성번호', '모니터링약어명', '모니터링기록일시', '측정값']]
spark_df = spark.createDataFrame(spark_test)
num_partitions = 10
spark_df = spark_df.repartition(num_partitions)

# spark_df.select('마취기록작성번호').limit(5).show()
# spark_df.filter(spark_df.측정값 >= 300).limit(3).show()

spark_df.cache()

spark_df.limit(5).show()


## Polars 테스트
import polars as pl

test_polars = pl.from_numpy(data = data_array, schema = column_names)
polars_test = test_ABP[['마취기록작성번호', '모니터링약어명', '모니터링기록일시', '측정값']]
polars_df = pl.from_pandas(polars_test)
polars_df.filter((pl.col('측정값') > 300) | (pl.col('측정값') < 5))
pl_df.filter(
    (pl.col('모니터링약어명').str.contains('S')) & (pl.col('측정값') >)
)
