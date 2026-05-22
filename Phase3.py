from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql import functions as F
from pymongo import MongoClient
import pandas as pd

#STEP 1: Start Spark
spark = SparkSession.builder \
    .appName("Phase3_CartAbandonment") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")



# STEP 2: Load raw logs
DATA_PATH = r"D:\Uni y3s2\BigData\Project 2\ecommerce_logs.csv"
df = spark.read.option("header", True).csv(DATA_PATH)
print(f"\n Loaded {df.count():,} rows")

# STEP 3: Find cart abandoners
# Get all cart events
cart_events = df.filter(col("event_type") == "cart") \
                .select("session_id", "user_id", "product_id") \
                .dropna()

# Get all purchase events
purchase_events = df.filter(col("event_type") == "purchase") \
                    .select("session_id") \
                    .dropna() \
                    .distinct()

# Cart abandoners = sessions with cart but NO purchase
# Left anti join keeps only rows from left that have NO match in right
abandoners = cart_events.join(purchase_events, on="session_id", how="left_anti")

print(f" Cart events: {cart_events.count():,}")
print(f" Sessions with purchases: {purchase_events.count():,}")
print(f" Cart abandonment events: {abandoners.count():,}")
# STEP 4: Get unique abandoners
# One row per user-product combination
abandoners_unique = abandoners.select("user_id", "product_id").distinct()
print(f" Unique user-product abandonment pairs: {abandoners_unique.count():,}")

# Show sample
print("\n Sample cart abandoners:")
abandoners_unique.show(5, truncate=False)

# STEP 5: Query MongoDB for user profiles
print("\n Fetching user profiles from MongoDB...")
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce"]

# Fetch all user profiles into a dictionary for fast lookup
# { user_id: [product_id1, product_id2, ...] }
profiles_cursor = db.user_profiles.find({}, {"_id": 0, "user_id": 1, "top_products": 1})
user_top_products = {}
for doc in profiles_cursor:
    top = [p["product_id"] for p in doc.get("top_products", [])]
    user_top_products[doc["user_id"]] = top

print(f" Loaded {len(user_top_products):,} user profiles from MongoDB")
client.close()

# STEP 6: Classify abandoners
print("\n Classifying abandoners...")

# Convert abandoners to pandas for easy row-by-row processing
abandoners_pd = abandoners_unique.toPandas()

# Classify each abandoner
def classify(row):
    user_id = row["user_id"]
    product_id = row["product_id"]
    top_products = user_top_products.get(user_id, [])
    if product_id in top_products:
        return "High_Discount"
    else:
        return "Standard_Reminder"

abandoners_pd["campaign_flag"] = abandoners_pd.apply(classify, axis=1)  

# STEP 7: Show results
total = len(abandoners_pd)
high_discount = len(abandoners_pd[abandoners_pd["campaign_flag"] == "High_Discount"])
standard = len(abandoners_pd[abandoners_pd["campaign_flag"] == "Standard_Reminder"])

print(f"\n CAMPAIGN RESULTS:")
print(f"   Total abandoners:    {total:,}")
print(f"    High Discount:    {high_discount:,} ({high_discount/total*100:.1f}%)")
print(f"    Std Reminder:     {standard:,} ({standard/total*100:.1f}%)")

print("\n Sample results:")
print(abandoners_pd.head(10).to_string(index=False))

# STEP 8: Save results
import os
os.makedirs("output", exist_ok=True)

abandoners_pd.to_csv("output/phase3_campaign.csv", index=False)
print(f"\n Results saved to: output/phase3_campaign.csv")

# Save separately by flag
abandoners_pd[abandoners_pd["campaign_flag"] == "High_Discount"] \
    .to_csv("output/phase3_high_discount.csv", index=False)
abandoners_pd[abandoners_pd["campaign_flag"] == "Standard_Reminder"] \
    .to_csv("output/phase3_standard_reminder.csv", index=False)

print(f" High discount list saved to: output/phase3_high_discount.csv")
print(f" Standard reminder list saved: output/phase3_standard_reminder.csv")

spark.stop()
print("\n Phase 3 Complete!")