# Mesto gde se najcesce desava prekrsaj za svaku opstinu po godinama (2015-2019)

import os
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf().setAppName("batch").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

HDFS_NAMENODE = os.environ['CORE_CONF_fs_defaultFS']

df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ",") \
    .load(HDFS_NAMENODE + "/data/NYPD_Complaint_Data_Historic.csv") \

func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())

df = df.filter(col('BORO_NM').isNotNull())
df = df.filter(col('PREM_TYP_DESC').isNotNull())
df = df.filter(col('CMPLNT_FR_DT').isNotNull())

df = df.withColumn('CMPLNT_FR_DT', func(col('CMPLNT_FR_DT')))
df = df.filter(year("CMPLNT_FR_DT") >= lit(2015))

df.groupBy(col('BORO_NM').alias("borough"), col('PREM_TYP_DESC').alias('place'), year(col('CMPLNT_FR_DT')).alias("year")).count().orderBy('count', ascending=False).groupBy('borough').pivot('year').agg(first('place')).show(truncate=False)


