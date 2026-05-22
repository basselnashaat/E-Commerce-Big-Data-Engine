import json
import csv
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce"]

db.user_profiles.drop()
db.cooccurrence.drop()
print("Cleared old collections.")

# STEP 1: Load user profiles (handles both JSON Lines and single JSON array)
user_count = 0
with open("output/user_profiles_sample.json", "r", encoding="utf-8") as f:
    first_char = f.read(1)
    f.seek(0)
    if first_char == '[':
        # Single JSON array
        data = json.load(f)
        for doc in data:
            db.user_profiles.insert_one(doc)
            user_count += 1
    else:
        # JSON Lines (one object per line)
        for line in f:
            line = line.strip()
            if line:
                doc = json.loads(line)
                db.user_profiles.insert_one(doc)
                user_count += 1
print(f"Loaded {user_count} user profiles.")

# STEP 3: Load product pairs (CSV)
pair_count = 0
with open("output/top_100_product_pairs.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        item_a = row["product_1"]
        item_b = row["product_2"]
        count = int(row["count"])
        if item_a and item_b:  # skip empty rows
           db.cooccurrence.insert_one({"item_a": item_a, "item_b": item_b, "count": count})
        pair_count += 1
print(f"Loaded {pair_count} product pairs.")

# STEP 4: Create indexes
db.user_profiles.create_index("user_id", unique=True)
db.cooccurrence.create_index([("item_a", 1), ("item_b", 1)], unique=True)
db.cooccurrence.create_index([("count", -1)])
print("Phase 2 complete. Indexes created.")