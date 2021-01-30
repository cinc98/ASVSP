# Odnos ukupnoog stanovništva i prekršaja po regiji

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
    StructField("population",IntegerType(),True), \
    StructField("name",StringType(),True), \
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
    .load(HDFS_NAMENODE + "/data/new_york_boroughs.json") \

df = df.filter(col('BORO_NM').isNotNull())
df = df.groupBy(col('BORO_NM').alias("borough")).agg(count(lit(1)).alias("offenses"))

df = df.join(df_json, df["borough"] == df_json["name"]).drop('name')

df = df.withColumn('ratio',round((col('offenses')-col('population'))/col('offenses')*100,2))

df.show()
