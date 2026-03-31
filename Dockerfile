FROM apache/spark:latest

USER root

RUN pip install --no-cache-dir pyspark

WORKDIR /opt/spark/work-dir

USER root
