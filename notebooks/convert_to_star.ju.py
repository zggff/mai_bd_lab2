# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, expr

# %%
jdbc_url = "jdbc:postgresql://postgres:5432/mydatabase"
properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}


def read_db(table_name):
    return spark.read.jdbc(url=jdbc_url,
                           table=table_name,
                           properties=properties)


def write_to_db(df, table_name):
    df.write.jdbc(url=jdbc_url,
                  table=table_name,
                  mode="overwrite",
                  properties=properties)


# %%
spark = SparkSession.builder \
    .appName("JupyterToSpark") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# %%

df_raw = read_db("raw_data")

# %%
dim_suppliers = df_raw.select(
                            col("supplier_name").alias("name"),
                            col("supplier_contact").alias("contact"),
                            col("supplier_email").alias("email"),
                            col("supplier_phone").alias("phone"),
                            col("supplier_address").alias("address"),
                            col("supplier_city").alias("city"),
                            col("supplier_country").alias("country"),
                              ).distinct() \
                        .withColumn("id", monotonically_increasing_id())
write_to_db(dim_suppliers, "dim_suppliers")
dim_suppliers.head()

# %%
dim_stores = df_raw.select(
                            col("store_name").alias("name"),
                            col("store_location").alias("location"),
                            col("store_city").alias("city"),
                            col("store_state").alias("state"),
                            col("store_country").alias("country"),
                            col("store_phone").alias("phone"),
                            col("store_email").alias("email"),
                           ).distinct() \
                        .withColumn("id", monotonically_increasing_id())
write_to_db(dim_stores, "dim_stores")
dim_stores.head()

# %%
dim_products = df_raw.select(
                            col("sale_product_id").alias("id"),
                            col("product_brand").alias("brand"),
                            col("product_category").alias("category"),
                            col("product_color").alias("color"),
                            col("product_description").alias("description"),
                            col("product_material").alias("material"),
                            col("product_name").alias("name"),
                            col("product_price").alias("price"),
                            col("product_quantity").alias("quantity"),
                            col("product_size").alias("size"),
                            col("product_weight").alias("weight"),
                            col("product_release_date").alias("release_data"),
                            col("product_expiry_date").alias("expiry_date"),
                            col("product_reviews").alias("reviews"),
                            col("product_rating").alias("rating"),
                             ).distinct()
write_to_db(dim_products, "dim_products")
dim_products.head()

# %%
dim_sellers = df_raw.select(
                            col("sale_seller_id").alias("id"),
                            col("seller_first_name").alias("first_name"),
                            col("seller_last_name").alias("last_name"),
                            col("seller_email").alias("email"),
                            col("seller_country").alias("country"),
                            col("seller_postal_code").alias("postal_code"),
                             ).distinct() 
write_to_db(dim_sellers, "dim_sellers")
dim_sellers.head()

# %%
dim_customers = df_raw.select(
                            col("sale_customer_id").alias("id"),
                            col("customer_first_name").alias("first_name"),
                            col("customer_last_name").alias("last_name"),
                            col("customer_email").alias("email"),
                            col("customer_age").alias("age"),
                            col("customer_pet_type").alias("pet_type"),
                            col("customer_pet_breed").alias("pet_breed"),
                            col("customer_pet_name").alias("pet_name"),
                            col("customer_country").alias("country"),
                            col("customer_postal_code").alias("postal_code"),
                              ).distinct()
write_to_db(dim_customers, "dim_customers")
dim_customers.head()

# %%
stores = dim_stores.select("email", "id").alias("st")
suppliers = dim_suppliers.select("email", "id").alias("sp")

fact_sales = df_raw \
    .join(stores, col("store_email") == col("st.email"), "left") \
    .join(suppliers, col("supplier_email") == col("sp.email"), "left") \
    .select(
        col("sale_customer_id").alias("customer_id"),
        col("sale_seller_id").alias("seller_id"),
        col("st.id").alias("store_id"),
        col("sp.id").alias("supplier_id"),
        col("sale_product_id").alias("product_id"),
        "pet_category",
        "sale_date",
        "sale_quantity",
        "sale_total_price"
    ).withColumn("sale_id", monotonically_increasing_id())

write_to_db(fact_sales, "fact_sales")
fact_sales.head()
