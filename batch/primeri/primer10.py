# Rasa, pol i broj godina osumljicenog i zrtve

import os
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window



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

df = df.filter(col('SUSP_RACE').isNotNull())
df = df.filter(col('SUSP_AGE_GROUP').isNotNull())
df = df.filter(col('SUSP_SEX').isNotNull())
df = df.filter(col('VIC_RACE').isNotNull())
df = df.filter(col('VIC_AGE_GROUP').isNotNull())
df = df.filter(col('VIC_SEX').isNotNull())


s_age = df.groupBy(col('SUSP_AGE_GROUP').alias('suspect_age')).count().orderBy('count', ascending=False).drop('count').limit(1)
s_race = df.groupBy(col('SUSP_RACE').alias('suspect_race')).count().orderBy('count', ascending=False).drop('count').limit(1)
s_sex = df.groupBy(col('SUSP_SEX').alias('suspect_sex')).count().orderBy('count', ascending=False).drop('count').limit(1)

v_age = df.groupBy(col('VIC_AGE_GROUP').alias('victim_age')).count().orderBy('count', ascending=False).drop('count').limit(1)
v_race = df.groupBy(col('VIC_RACE').alias('victim_race')).count().orderBy('count', ascending=False).drop('count').limit(1)
v_sex = df.groupBy(col('VIC_SEX').alias('victim_sex')).count().orderBy('count', ascending=False).drop('count').limit(1)


s_age=s_age.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
s_race=s_race.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
s_sex=s_sex.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))

s_age = s_age.join(s_race, on=["row_index"])
s_age = s_age.join(s_sex, on=["row_index"])

v_age=v_age.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
v_race=v_race.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
v_sex=v_sex.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))

v_age = v_age.join(v_race, on=["row_index"])
v_age = v_age.join(v_sex, on=["row_index"])

s_age = s_age.join(v_age, on=["row_index"]).drop('row_index')

s_age.show()