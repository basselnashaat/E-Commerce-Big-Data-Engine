from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, size, array_contains, explode, lit
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("MarketBasketSQL") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


print("MARKET BASKET ANALYSIS — FINDING FREQUENTLY BOUGHT TOGETHER ITEMS")


# Load data
csv_path = "D:/Uni y3s2/BigData/Project 2/ecommerce_logs.csv"
df = spark.read.option("header", True).csv(csv_path)
print(f" Loaded {df.count():,} rows")

# Filter purchases
purchases = df.filter(col("event_type") == "purchase") \
              .select("session_id", "product_id") \
              .filter(col("product_id").isNotNull()) \
              .filter(col("product_id") != "")

print(f" Purchase events: {purchases.count():,}")

# Get sessions with multiple purchases
session_products = purchases.groupBy("session_id") \
    .agg(collect_set("product_id").alias("products")) \
    .filter(size("products") >= 2)

session_count = session_products.count()
print(f" Sessions with 2+ products: {session_count:,}")

if session_count == 0:
    print("No sessions with multiple purchases found!")
    spark.stop()
    exit(1)

# STEP 1: Find top products (instead of pairs)
print("\n Finding most popular products in multi-purchase sessions...")

# Explode products to get frequency
all_products = session_products.select(explode("products").alias("product_id"))
top_products = all_products.groupBy("product_id") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(20)

print("\n TOP 20 PRODUCTS IN MULTI-PURCHASE SESSIONS:")
top_products.show(20, truncate=False)

# STEP 2: Self-join approach (finds pairs without combinations)
print("\n Finding product pairs using self-join...")

# Create a dataframe with session and product pairs via self-join
# This is more memory-efficient than generating combinations
session_products_flat = session_products.select(
    col("session_id"),
    explode("products").alias("product_id")
)

# Self-join to create pairs (product1, product2) where product1 < product2
pairs_df = session_products_flat.alias("a") \
    .join(session_products_flat.alias("b"), 
          (col("a.session_id") == col("b.session_id")) & 
          (col("a.product_id") < col("b.product_id"))) \
    .select(
        col("a.product_id").alias("product_1"),
        col("b.product_id").alias("product_2")
    )

# Count pairs
pair_counts = pairs_df.groupBy("product_1", "product_2") \
    .count() \
    .orderBy(col("count").desc())

# Show top pairs
print("\n TOP 20 PRODUCT PAIRS:")
pair_counts.show(20, truncate=False)


import os
os.makedirs("output", exist_ok=True)

# Method 1: Save as CSV using pandas (works without Hadoop)
print("\n Saving results...")

# Convert Spark DataFrames to Pandas and save as CSV
top_products_pd = top_products.toPandas()
top_products_pd.to_csv("output/top_20_products.csv", index=False)

# Save top pairs
top_pairs_pd = pair_counts.limit(100).toPandas()  # Save top 100 pairs
top_pairs_pd.to_csv("output/top_100_product_pairs.csv", index=False)

print(f" Saved: output/top_20_products.csv")
print(f" Saved: output/top_100_product_pairs.csv")

# Method 2: Save as JSON if needed
top_products.coalesce(1).write.mode("overwrite").format("json").save("output/top_products_json")
top_pairs_pd.to_json("output/product_pairs.json", orient="records", indent=2)