FROM apache/spark:3.5.8-python3

USER root

RUN pip install --no-cache-dir neopyter jupyterlab pyspark
RUN pip install --no-cache-dir pandas

RUN apt-get update && apt-get install -y maven

COPY pom.xml .
RUN mvn dependency:copy-dependencies -DoutputDirectory=/opt/spark/jars

RUN pip install --no-cache-dir cassandra-driver

ENV PYSPARK_PYTHON=python3

WORKDIR /app
