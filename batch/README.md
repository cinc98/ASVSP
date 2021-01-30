# Projekat iz predmeta Arhitekture sistema velikih skupova podataka

Pokretanje:

docker-compose up


Postavljanje podataka na hdfs:

docker exec -it namenode bash

hdfs dfs -put /hadoop-data/ /data


Pokretanje primera:

docker exec -it spark-master bash

cd home/batch/

/spark/bin/spark-submit naziv_skripte.py
