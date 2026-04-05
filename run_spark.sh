#!/bin/bash
set -e # Exit immediately if a command fails

docker cp ./spark/star.py spark-master:/app/star.py
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  star.py

docker cp ./spark/datamarts.py spark-master:/app/datamarts.py
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  datamarts.py


