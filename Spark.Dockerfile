FROM apache/spark:latest

USER root

RUN pip install --no-cache-dir pyspark

RUN echo '#!/bin/sh\nspark-submit --packages org.postgresql:postgresql:42.7.2,com.clickhouse:clickhouse-jdbc:0.6.0 "$@"' > /usr/local/bin/my-spark-submit \
 && chmod +x /usr/local/bin/my-spark-submit

WORKDIR /opt/spark/work-dir

USER root
