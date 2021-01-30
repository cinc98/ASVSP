# Prosecno, najduze, najkrace vreme trajanja uvidjaja

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

df = df.filter(col('CMPLNT_FR_DT').isNotNull())
df = df.filter(col('CMPLNT_FR_TM').isNotNull())
df = df.withColumn('CMPLNT_FR_DT', func(col('CMPLNT_FR_DT')))

df = df.filter(col('CMPLNT_TO_DT').isNotNull())
df = df.filter(col('CMPLNT_TO_TM').isNotNull())
df = df.withColumn('CMPLNT_TO_DT', func(col('CMPLNT_TO_DT')))
df = df.filter(year("CMPLNT_FR_DT") >= lit(2000))

df = df.withColumn('start_datetime', concat(col('CMPLNT_FR_DT'),lit(' '), col('CMPLNT_FR_TM')))
df = df.withColumn('end_datetime', concat(col('CMPLNT_TO_DT'),lit(' '), col('CMPLNT_TO_TM')))

df = df.withColumn('start_datetime',to_timestamp(df.start_datetime, 'yyyy-MM-dd HH:mm:ss'))
df = df.withColumn('end_datetime', to_timestamp(df.end_datetime, 'yyyy-MM-dd HH:mm:ss'))

diff_secs_col = col("end_datetime").cast("long") - col("start_datetime").cast("long")

df = df.withColumn("diff_mins", diff_secs_col/ 60 )
df = df.filter(col('diff_mins') > 0)

df = df.agg(max("diff_mins").alias('max (years)'), min("diff_mins").alias('min (minutes)'), avg("diff_mins").alias('avg (days)'))
df = df.withColumn("max (years)", round(col('max (years)')/ 525600,2))
df = df.withColumn("avg (days)", round(col('avg (days)')/ 1440,2))

df.show()
