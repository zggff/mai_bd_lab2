# %%
from pyspark.sql import SparkSession
import pyspark.pandas as ps

# %%
spark = SparkSession.builder.appName("postgres").getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/mydatabase"
properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=jdbc_url, table="raw_data", properties=properties)

# %%
df.toPandas()
