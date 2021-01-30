#!/usr/bin/python3

import os
import time
import praw
from kafka import KafkaProducer
import kafka.errors
import csv, json

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "nycrimes"


while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

def create_json(header, row):
    r = {}
    for i,c in enumerate(header):
        r[c]=row[i]

    return json.dumps(r)


with open('csv1.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    header = None
    for i,row in enumerate(csv_reader):
        if i == 0:
            header = row
        else:
            producer.send(TOPIC, key=bytes(str(row[0]), 'utf-8'), value=bytes(create_json(header,row), 'utf-8'))
            time.sleep(0.5)