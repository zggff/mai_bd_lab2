FROM apache/spark:4.0.2-python3

USER root

RUN pip install --no-cache-dir neopyter jupyterlab pyspark
RUN pip install --no-cache-dir pandas

RUN curl -L -o /opt/spark/jars/postgresql-42.7.2.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.2/postgresql-42.7.2.jar
RUN curl -L -o /opt/spark/jars/clickhouse-spark-runtime-4.0_2.13-0.10.0.jar \
    https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-4.0_2.13/0.10.0/clickhouse-spark-runtime-4.0_2.13-0.10.0.jar
RUN curl -L -o /opt/spark/jars/clickhouse-jdbc-all-0.9.8.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc-all/0.9.8/clickhouse-jdbc-all-0.9.8.jar

ENV PYSPARK_PYTHON=python3

WORKDIR /app
