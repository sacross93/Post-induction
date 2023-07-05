import pandas as pd
import pyspark

conf = pyspark.SparkConf()\
        .setAppName("spark-sql")\
        .set("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java-8.0.33.jar")
sc = pyspark.SparkContext(conf=conf)

from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .master("local") \
        .appName("spark-sql")\
        .config("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java-8.0.17.jar")\
        .config("spark.executor.memory", "64g")\
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2") \
        .config("spark.driver.memory", "32g")\
        .config("spark.sql.debug.maxToStringFields", "100")\
        .getOrCreate()

pd.read_excel('/srv/project_data/EMR/original_data/20230221(11.17)_ANS_이상욱_2021-1352_(간호정보조사지)_1차(2.27).xlsx')

df = spark.read.format("com.crealytics.spark.excel") \
    .option("useHeader", "true") \
    .load("/srv/project_data/EMR/original_data/20210929_ANS_이상욱_2021-1352_4차(11.12).xlsx")

df.show()



df = pd.read_excel('/srv/project_data/EMR/original_data/20210929_ANS_이상욱_2021-1352_4차(11.12).xlsx', sheet_name=None, inferSchema='')
sdf = spark.createDataFrame(df)