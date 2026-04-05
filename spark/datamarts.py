from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])  # Replace with your Cassandra host IP(s)
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS my_keyspace
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.shutdown()
cluster.shutdown()

spark = SparkSession.builder \
    .appName("datamart") \
    .config("spark.sql.catalog.casscatalog", "com.datastax.spark.connector.datasource.CassandraCatalog") \
    .config("spark.sql.catalog.casscatalog.spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()



def read_db(table_name):
    return spark.read.jdbc(url="jdbc:postgresql://postgres:5432/mydatabase",
                           table=table_name,
                           properties={
                               "user": "user",
                               "password": "password",
                               "driver": "org.postgresql.Driver"
                           })


def write_clickhouse(df, table_name, order_by):
    df.write \
        .format("clickhouse") \
        .option("host", "clickhouse") \
        .option("port", "8123") \
        .option("database", "default") \
        .option("user", "user") \
        .option("password", "password") \
        .option("table", table_name) \
        .option("order_by", order_by) \
        .mode("overwrite") \
        .save()


def write_cassandra(df, table_name, partition_by, use_existing):
    if not use_existing:
        df = df.withColumn("_id", F.monotonically_increasing_id())
        partition_by = "_id"
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .mode("append") \
        .partitionBy(partition_by) \
        .saveAsTable(f"casscatalog.my_keyspace.{table_name}")


def write_mongodb(df, table_name):
    df.write \
        .format("mongodb") \
        .option("connection.uri", "mongodb://user:password@mongodb:27017") \
        .option("database", "mydatabase") \
        .option("collection", table_name) \
        .mode("overwrite") \
        .save()


def write_to_db(df, table_name, main_col, use_existing=False):
    write_clickhouse(df, table_name, main_col)
    write_cassandra(df, table_name, main_col, use_existing)
    write_mongodb(df, table_name)


p = read_db("dim_products").alias("p")
c = read_db("dim_customers").alias("c")
s = read_db("dim_sellers").alias("s")
sp = read_db("dim_suppliers").alias("sp")
st = read_db("dim_stores").alias("st")
f = read_db("fact_sales").alias("f")

report_products = f.join(p,f.product_id == p.id) \
            .withColumn("product_name",
                        F.concat(F.col("brand"), F.lit(": "), F.col("name"))) \
            .groupBy(F.col("product_name"), p.category) \
            .agg(
                 F.sum(f.sale_quantity).alias("total_quantity_sold"),
                 F.sum(f.sale_total_price).alias("total_revenue"),
                 F.avg(p.rating).alias("average_rating"),
                 F.sum(p.reviews).alias("total_reviews"))
write_to_db(report_products, "report_products", "product_name")


report_customers = f.join(c, c.id == f.customer_id) \
            .withColumn("customer_name",
                        F.concat(F.col("c.first_name"),
                                 F.lit(" "),
                                 F.col("c.last_name"))) \
            .groupBy(F.col("c.id").alias("customer_id"),
                     F.col("customer_name"),
                     c.country) \
            .agg(
                F.sum(f.sale_total_price).alias("total_spent"),
                (F.sum(f.sale_total_price) / F.count(f.sale_id)).alias("average_order_value"))
write_to_db(report_customers, "report_customers", "customer_id", True)


report_time = f.withColumn("sale_month", F.date_trunc("month", "sale_date")) \
               .withColumn("sale_year", F.year("sale_date")) \
               .groupBy("sale_year", "sale_month") \
               .agg(
                    F.count(f.sale_id).alias("order_count"),
                    F.sum(f.sale_total_price).alias("monthly_revenue"),
                    F.avg(f.sale_total_price).alias("avg_order_size")
                )
write_to_db(report_time, "report_time", "sale_month", True)


report_stores = f.join(st, st.id == f.store_id) \
               .groupBy(st.name.alias("store_name"), st.city, st.country) \
               .agg(
                    F.sum(f.sale_total_price).alias("total_revenue"),
                    (F.sum(f.sale_total_price) / F.count(f.sale_id)).alias("store_average_check"))
write_to_db(report_stores, "report_stores", "store_name")


report_suppliers = f.join(sp, f.supplier_id == sp.id) \
                    .join(p, f.product_id == p.id) \
                    .groupBy(sp.name.alias("supplier_name"), sp.country) \
                    .agg(
                        F.sum(f.sale_total_price).alias("total_revenue_generated"),
                        F.avg(p.price).alias("avg_product_price"))
write_to_db(report_suppliers, "report_suppliers", "supplier_name")



report_quality = f.join(p, f.product_id == p.id) \
                .withColumn("product_name",
                        F.concat(F.col("brand"), F.lit(": "), F.col("name"))) \
                .groupBy("product_name", p.rating, p.reviews) \
                .agg(
                    F.sum(f.sale_quantity).alias("sales_volume"))

write_to_db(report_quality, "report_quality", "product_name")

