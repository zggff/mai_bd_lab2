FROM apache/spark:3.5.8-python3

USER root

RUN apt-get update && apt-get install -y maven

RUN pip install --no-cache-dir neopyter jupyterlab pyspark pandas cassandra-driver

COPY pom.xml .
RUN mvn dependency:copy-dependencies -DoutputDirectory=/opt/spark/jars

ENV PYSPARK_PYTHON=python3
ENV SPARK_DRIVER_MEMORY=2g
ENV SPARK_EXECUTOR_MEMORY=2g
ENV SPARK_EXECUTOR_MEMORY_OVERHEAD=1g


WORKDIR /app
