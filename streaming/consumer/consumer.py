r"""
 Run the example
    `$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 $SPARK_HOME/primeri/kafka_wordcount.py zoo1:2181 subreddit-politics subreddit-worldnews`
"""
from __future__ import print_function

import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)



sc = SparkContext(appName="SparkStreamingKafkaWordCount")
quiet_logs(sc)
ssc = StreamingContext(sc, 3)

ssc.checkpoint("stateful_checkpoint_direcory")

zooKeeper, topic1, topic2 = sys.argv[1:]
kvs = KafkaUtils.createStream(ssc, zooKeeper, "spark-streaming-consumer", {topic1: 1, topic2: 1})

lines = kvs.map(lambda x: json.loads(x[1]))

notification = lines.filter(lambda x: x['VIC_AGE_GROUP']=='<18' )\
                    .filter(lambda x: x['OFNS_DESC']=='RAPE' )\
                    .map(lambda x: "UZAS U '%s'! Nepoznata osoba silovala maloletno lice! '%s' " % (x['BORO_NM'], x['CMPLNT_NUM']))

notification1 = lines.filter(lambda x: x['VIC_AGE_GROUP']=='18-24' )\
                    .filter(lambda x: x['VIC_SEX']=='F' )\
                    .filter(lambda x: x['SUSP_SEX']=='M' )\
                    .filter(lambda x: x['SUSP_AGE_GROUP']=='45-64' )\
                    .filter(lambda x: x['OFNS_DESC']=='SEX CRIMES' )\
                    .map(lambda x: "SOK! U '%s' stariji muskarac seksualno zlostavljao devojku! '%s' " % (x['BORO_NM'], x['CMPLNT_NUM']))

notification2 = lines.filter(lambda x: x['SUSP_AGE_GROUP']=='<18' )\
                    .filter(lambda x: x['OFNS_DESC']=='DANGEROUS DRUGS' )\
                    .map(lambda x: "U '%s' uhapsen maloletnik sa drogom '%s'" % (x['BORO_NM'], x['CMPLNT_NUM']))

notification3 = lines.filter(lambda x: x['BORO_NM']=='MANHATTAN' )\
                    .filter(lambda x: x['OFNS_DESC']=='ROBBERY' )\
                    .map(lambda x: "Pljacka U '%s' '%s'" % (x['BORO_NM'], x['CMPLNT_NUM']))

notification.pprint()
notification1.pprint()
notification2.pprint()
notification3.pprint()


ssc.start()
ssc.awaitTermination()