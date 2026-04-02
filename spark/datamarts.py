# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# %%
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])  # Replace with your Cassandra host IP(s)
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS my_keyspace
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.shutdown()
cluster.shutdown()

# %%
spark = SparkSession.builder \
    .appName("datamart") \
    .config("spark.sql.catalog.casscatalog", "com.datastax.spark.connector.datasource.CassandraCatalog") \
    .config("spark.sql.catalog.casscatalog.spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# %%


def read_db(table_name):
    return spark.read.jdbc(url="jdbc:postgresql://postgres:5432/mydatabase",
                           table=table_name,
                           properties={
                               "user": "user",
                               "password": "password",
                               "driver": "org.postgresql.Driver"
                           })


def write_clickhouse(df, table_name):
    df.write \
        .format("clickhouse") \
        .option("host", "clickhouse") \
        .option("port", "8123") \
        .option("database", "default") \
        .option("user", "user") \
        .option("password", "password") \
        .option("table", table_name) \
        .option("order_by", "_id") \
        .mode("overwrite") \
        .save()


def write_cassandra(df, table_name):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .mode("append") \
        .partitionBy("_id") \
        .saveAsTable(f"casscatalog.my_keyspace.{table_name}")

def write_mongodb(df, table_name):
    df.write \
        .format("mongodb") \
        .option("connection.uri", "mongodb://user:password@mongodb:27017") \
        .option("database", "mydatabase") \
        .option("collection", table_name) \
        .mode("overwrite") \
        .save()


def write_to_db(df, table_name, _main_col="_id"):
    df2 = df.withColumn("_id", f.monotonically_increasing_id())
    # this is to properly order rows
    write_clickhouse(df2, table_name)
    write_cassandra(df2, table_name)
    write_mongodb(df2, table_name)


# %%
dim_products = read_db("dim_products")
dim_customers = read_db("dim_customers")
dim_sellers = read_db("dim_sellers")
dim_suppliers = read_db("dim_suppliers")
dim_stores = read_db("dim_stores")
fact_sales = read_db("fact_sales")

# %%
df_product_sales = fact_sales.join(dim_products,
                                   fact_sales.product_id == dim_products.id,
                                   "inner")

# %%
products_top_10 = df_product_sales.groupBy("name", "brand") \
    .agg(
        f.sum("sale_quantity").alias("total_sold"),
        f.sum("sale_total_price").alias("total_revenue")
    ) \
    .orderBy(f.col("total_sold").desc()) \
    .limit(10) \

write_to_db(products_top_10, "mart_products_top_10")
products_top_10.toPandas()

# %%

products_revenue_by_category = df_product_sales.groupBy("category") \
    .agg(f.sum("sale_total_price").alias("total_revenue")) \
    .orderBy(f.desc("total_revenue"))

write_to_db(products_revenue_by_category, "mart_products_revenue",
            "total_revenue")
products_revenue_by_category.toPandas()

# %%
products_rating = df_product_sales.groupBy("name", "brand") \
    .agg(
        f.avg("rating").alias("average_rating"),
        f.sum("reviews").alias("total_reviews")
    ) \
    .orderBy(f.col("average_rating").desc())
write_to_db(products_rating, "mart_products_rating", "average_rating")
products_rating.toPandas()

# %%
df_customer_sales = fact_sales.join(dim_customers,
                                    fact_sales.customer_id == dim_customers.id,
                                    "inner")

# %%
customers_top_10 = df_customer_sales.groupBy("id","first_name", "last_name", "email") \
    .agg(f.sum("sale_total_price").alias("total_spent")) \
    .orderBy(f.desc("total_spent")) \
    .limit(10)
write_to_db(customers_top_10, "mart_customers_top_10", "total_spent")
customers_top_10.toPandas()

# %%
customers_by_country = dim_customers.groupBy("country") \
    .agg(f.count("id").alias("customer_count")) \
    .orderBy(f.desc("customer_count"))
write_to_db(customers_by_country, "mart_customers_by_country",
            "customer_count")
customers_by_country.toPandas()

# %%
customers_average_check = df_customer_sales.groupBy("id","first_name", "last_name", "email") \
    .agg(f.avg("sale_total_price").alias("avg_check_amount"))
write_to_db(customers_average_check, "mart_customers_average_check", "id")
customers_average_check.toPandas()

# %%
df_time_sales = fact_sales.withColumn("sale_year", f.year("sale_date")) \
                          .withColumn("sale_month", f.month("sale_date"))

# %%

time_trends = df_time_sales.groupBy("sale_year", "sale_month") \
    .agg(f.sum("sale_total_price").alias("total_revenue"), f.sum("sale_quantity").alias("total_items_sold")) \
    .orderBy("sale_year", "sale_month")
write_to_db(time_trends, "mart_time_monthly_yearly_trends", "sale_year")
time_trends.toPandas()

# %%

time_average_order_size = df_time_sales.groupBy("sale_year", "sale_month") \
    .agg(f.avg("sale_total_price").alias("avg_order_value")) \
    .orderBy("sale_year", "sale_month")
write_to_db(time_average_order_size, "mart_time_average_order_size",
            "sale_year")
time_average_order_size.toPandas()

# %%
df_store_sales = fact_sales.join(dim_stores,
                                 fact_sales.store_id == dim_stores.id, "left")

# %%
stores_top_5 = df_store_sales.groupBy("store_id", "name") \
    .agg(f.sum("sale_total_price").alias("total_revenue")) \
    .orderBy(f.desc("total_revenue")) \
    .limit(5)
write_to_db(stores_top_5, "mart_stores_top_5", "total_revenue")
stores_top_5.toPandas()

# %%
stores_location = df_store_sales.groupBy("country", "city") \
    .agg(f.sum("sale_total_price").alias("total_revenue"), f.count("sale_id").alias("transactions_count")) \
    .orderBy(f.desc("transactions_count"))
write_to_db(stores_location, "mart_stores_location", "transactions_count")
stores_location.toPandas()

# %%
stores_average_check = df_store_sales.groupBy("store_id", "name") \
    .agg(f.avg("sale_total_price").alias("avg_check"))
write_to_db(stores_average_check, "mart_stores_average_check", "store_id")
stores_average_check.toPandas()

# %%
df_supplier_sales = fact_sales.join(dim_suppliers, fact_sales.supplier_id == dim_suppliers.id, "inner") \
                              .join(dim_products, fact_sales.product_id == dim_products.id, "inner")
df_supplier_sales.columns

# %%
suppliers_top_5 = df_supplier_sales.groupBy(fact_sales.supplier_id, dim_suppliers.name, dim_suppliers.email) \
    .agg(f.sum("sale_total_price").alias("total_revenue")) \
    .orderBy(f.desc("total_revenue")) \
    .limit(5)
write_to_db(suppliers_top_5, "mart_suppliers_top_5", "total_revenue")
suppliers_top_5.toPandas()

# %%
suppliers_average_price = df_supplier_sales.groupBy(fact_sales.supplier_id, dim_suppliers.name, dim_suppliers.email) \
    .agg(f.avg("price").alias("avg_product_price"))
write_to_db(suppliers_average_price, "mart_suppliers_average_price",
            "supplier_id")
suppliers_average_price.toPandas()

# %%
suppliers_sale_by_country = df_supplier_sales.groupBy(dim_suppliers.country) \
    .agg(f.sum("sale_total_price").alias("total_revenue")) \
    .orderBy(f.desc("total_revenue"))
write_to_db(suppliers_sale_by_country, "mart_suppliers_sale_by_country",
            "total_revenue")
suppliers_sale_by_country.toPandas()

# %%
ratings_rating = products_rating.select("name", "brand", f.col("average_rating").alias("rating")) \
    .orderBy("average_rating")
least = ratings_rating.first()
most = ratings_rating.tail(1)[0]
ratings_rating_extremes = spark.createDataFrame([least, most])
write_to_db(ratings_rating_extremes, "mart_ratings_extremes", "rating")
ratings_rating_extremes.toPandas()

# %%
ratings_most_reviewed = products_rating.select(
    "brand", "name",
    f.col("total_reviews").alias("reviews")).orderBy(f.desc("reviews"))
write_to_db(ratings_most_reviewed, "mart_ratings_most_reviewed", "reviews")
ratings_most_reviewed.toPandas()

# %%

# Корреляция между рейтингом и объемом продаж
df_rating_sales = df_product_sales.groupBy("brand", "name") \
    .agg(
        f.avg("rating").alias("rating"),
        f.sum("sale_quantity").alias("sales")
    )
df_rating_sales.toPandas()

# %%

ratings_correlation = df_rating_sales.select(
    f.corr("rating", "sales").alias("rating_sales_correlation"))
write_to_db(ratings_correlation, "mart_ratings_rating_sales_correlation",
            "rating_sales_correlation")
ratings_correlation.toPandas()
