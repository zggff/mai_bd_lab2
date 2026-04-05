FROM apache/spark:3.5.8-python3

USER root

RUN apt-get update && apt-get install -y maven

RUN pip install --no-cache-dir neopyter jupyterlab pyspark==3.5.8 pandas cassandra-driver

COPY pom.xml .
RUN mvn dependency:copy-dependencies -DoutputDirectory=/opt/spark/jars

WORKDIR /app
