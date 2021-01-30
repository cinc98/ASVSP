# Projekat iz predmeta Arhitekture sistema velikih skupova podataka

Pokretanje:

docker-compose up

Pokretanje primera:

docker exec -it spark-master1 bash

spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 spark/primeri/consumer.py zoo1:2181 nycrimes nycrimes1