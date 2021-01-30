# Ukupan broj prekrsaja po godini od 2000 godine

import os
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf().setAppName("batch").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

HDFS_NAMENODE = os.environ['CORE_CONF_fs_defaultFS']



# schemaString = "id date_of_occurrence time_of_occurrence ending_date_of_occurrence ending_time_of_occurrence precinct date_reported_to_police offense_code offense_desc internal_code internal_desc completed offense_level borough location premises_desc jurisdiction_code_desc jurisdiction_code parks hadevelpt housing x y suspects_age suspects_race suspects_sex district lat lon lat_lon patrol station victim_age victim_race victim sex"
# fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
# schema = StructType(fields)

df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ",") \
    .load(HDFS_NAMENODE + "/data/NYPD_Complaint_Data_Historic.csv") \


func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())

df = df.filter(col('CMPLNT_FR_DT').isNotNull())
df = df.withColumn('CMPLNT_FR_DT', func(col('CMPLNT_FR_DT')))
df = df.filter(year("CMPLNT_FR_DT") >= lit(2000))

df.groupBy(year(col('CMPLNT_FR_DT')).alias("year")).count().orderBy('year', ascending=False).show(df.count(),False)


# columns_to_drop=['PARKS_NM','HADEVELOPT','HOUSING_PSA','X_COORD_CD','Y_COORD_CD','TRANSIT_DISTRICT','Latitude','Longitude','Lat_Lon','STATION_NAME']
# df.drop('PARKS_NM','HADEVELOPT','HOUSING_PSA','X_COORD_CD','Y_COORD_CD','TRANSIT_DISTRICT','Latitude','Longitude','Lat_Lon','STATION_NAME').show()

# df.show()
# df.printSchema()