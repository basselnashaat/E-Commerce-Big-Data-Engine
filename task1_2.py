# task1_2_fixed.py - WITH WORKING SAVE METHODS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, row_number, collect_list, struct, desc
from pyspark.sql.window import Window
import json

# Initialize Spark
spark = SparkSession.builder \
    .appName("UserAffinityAnalysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


print("PHASE 1.2: USER AFFINITY AGGREGATION")


# STEP 1: Load the data

csv_path = "D:/Uni y3s2/BigData/Project 2/ecommerce_logs.csv"
df = spark.read.option("header", True).csv(csv_path)
print(f" Loaded {df.count():,} rows")


# STEP 2: Assign weights to different event types

print("\n Assigning weights to events...")
df_weighted = df.withColumn(
    "weight",
    when(col("event_type") == "view", 1)
    .when(col("event_type") == "cart", 3)
    .when(col("event_type") == "purchase", 5)
    .otherwise(0)
)


# STEP 3: Filter valid events

print("\n Filtering valid events...")
valid_events = df_weighted.filter(col("weight") > 0) \
    .filter(col("user_id").isNotNull()) \
    .filter(col("product_id").isNotNull())

valid_count = valid_events.count()
print(f" Valid events: {valid_count:,} ({(valid_count/df.count()*100):.1f}% of total)")


# STEP 4: Aggregate scores per user per product


user_product_scores = valid_events.groupBy("user_id", "product_id") \
    .agg(spark_sum("weight").alias("score"))

user_product_scores = user_product_scores.cache()

total_combinations = user_product_scores.count()
unique_users = user_product_scores.select("user_id").distinct().count()
print(f" Total (user, product) combinations: {total_combinations:,}")
print(f" Unique users: {unique_users:,}")


# STEP 5: Rank products per user by score

print("\n Ranking products per user...")
window_spec = Window.partitionBy("user_id").orderBy(desc("score"))
user_ranked = user_product_scores.withColumn("rank", row_number().over(window_spec))


# STEP 6: Keep only top 5 for each user

print("\n Taking top 5 products per user...")
user_top5 = user_ranked.filter(col("rank") <= 5)

print("\n Sample user preferences:")
user_top5.select("user_id", "product_id", "score", "rank").show(10, truncate=False)


# STEP 7: Aggregate into compact format


user_profiles = user_top5.groupBy("user_id") \
    .agg(collect_list(struct("product_id", "score")).alias("top_products"))

user_count = user_profiles.count()
print(f" User profiles created: {user_count:,}")

print("\n Sample user profiles:")
user_profiles.show(5, truncate=False)

# STEP 8: Analyze the results

print("\n ANALYSIS RESULTS:")
avg_top_products = user_top5.groupBy("user_id").count().agg({"count": "avg"}).collect()[0][0]
print(f" Average top products per user: {avg_top_products:.2f}")

print("\n Most popular products in user top-5 lists:")
popular_products = user_top5.groupBy("product_id") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(10)
popular_products.show(10, truncate=False)

# STEP 9: Save results (FIXED - No winutils needed)

import os
os.makedirs("output", exist_ok=True)

print("\n Saving results...")

# METHOD 1: Save as CSV using pandas (ALWAYS WORKS)
print("   Method 1: Saving CSV files...")
user_profiles.limit(1000).toPandas().to_csv("output/user_profiles_sample.csv", index=False)
user_top5.limit(5000).toPandas().to_csv("output/user_top5_products.csv", index=False)
print("    CSV files saved!")

# METHOD 2: Save as JSON manually (without Spark's write)
print("   Method 2: Saving JSON files...")
user_profiles_pd = user_profiles.limit(5000).toPandas()
user_profiles_pd["top_products"] = user_profiles_pd["top_products"].apply(
    lambda rows: [{"product_id": r["product_id"], "score": int(r["score"])} for r in rows]
)
user_profiles_pd.to_json("output/user_profiles_sample.json", orient="records", indent=2)
print("   JSON file saved!")


# METHOD 4: Save as Parquet using local path (compatible)
print("   Method 4: Saving as Parquet...")
user_profiles.coalesce(4).write.mode("overwrite").parquet("output/user_profiles_parquet_local")



print(" ALL RESULTS SAVED SUCCESSFULLY!")




summary = {
    "total_rows_processed": 10831817,
    "valid_events": valid_count,
    "unique_users": unique_users,
    "total_user_product_pairs": total_combinations,
    "avg_top_products_per_user": float(avg_top_products),
    "top_10_products": [
        {"product_id": row["product_id"], "user_count": row["count"]}
        for row in popular_products.collect()
    ]
}

with open("output/phase1_2_summary.json", "w") as f:
    json.dump(summary, f, indent=2)

print(" Summary report saved: output/phase1_2_summary.json")




spark.stop()
print("\n Spark stopped. Phase 1.2 Complete!")