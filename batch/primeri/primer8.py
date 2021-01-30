# Stopa kriminala u odnosu na proslu godinu

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

schema = StructType([ \
    StructField("id",IntegerType(),True), \
    StructField("name",StringType(),True), \
    StructField("address",StringType(),True), \
    StructField("officer", StringType(), True), \
  ])

df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ",") \
    .load(HDFS_NAMENODE + "/data/NYPD_Complaint_Data_Historic.csv") \

df_json = spark.read \
    .format("json") \
    .option("schema", schema) \
    .load(HDFS_NAMENODE + "/data/new_york_precincts.json") \

func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())

df = df.filter(col('CMPLNT_FR_DT').isNotNull())
df = df.filter(col('ADDR_PCT_CD').isNotNull())
df = df.filter(col('ADDR_PCT_CD') > 0)

df = df.withColumn('CMPLNT_FR_DT', func(col('CMPLNT_FR_DT')))
df = df.filter(year("CMPLNT_FR_DT") >= lit(2018))

df = df.groupBy(col('ADDR_PCT_CD').alias('precinct'), year(col('CMPLNT_FR_DT')).alias("year")).count().groupBy(col('precinct')).pivot('year').agg(first('count')).orderBy('precinct', ascending=True)

df = df.withColumn('criminal_rate %',round(col('2019')*100/col('2018')-100,2))

df.join(df_json, df["precinct"] == df_json["id"]).drop('precinct','id','address').show(df.count(),False)