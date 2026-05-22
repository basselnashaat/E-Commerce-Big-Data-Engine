from pymongo import MongoClient
import json

# ─── Connect to MongoDB ────────────────────────────────────────────────────
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce"]

print("="*60)
print("QUERY DEMO — E-Commerce Recommendation Engine")
print("="*60)

# ─── QUERY 1: Get recommendations for a user ──────────────────────────────
def get_user_recommendations(user_id):
    print(f"\n🔍 Query 1: Top product recommendations for '{user_id}'")
    user = db.user_profiles.find_one({"user_id": user_id}, {"_id": 0})
    if not user:
        print(f"   ❌ User '{user_id}' not found in database.")
        return []
    print(f"   ✅ Found profile for {user_id}:")
    for i, product in enumerate(user["top_products"], 1):
        print(f"   {i}. {product['product_id']}  (affinity score: {product['score']})")
    return [p["product_id"] for p in user["top_products"]]

# ─── QUERY 2: Check if an item should get high discount ───────────────────
def check_discount(user_id, item_id):
    print(f"\n🎯 Query 2: Should '{user_id}' get a discount for '{item_id}'?")
    user = db.user_profiles.find_one({"user_id": user_id}, {"_id": 0})
    if not user:
        print(f"   ❌ User not found.")
        return
    top_products = [p["product_id"] for p in user["top_products"]]
    if item_id in top_products:
        print(f"   🔴 HIGH DISCOUNT — '{item_id}' is in user's top affinity products!")
    else:
        print(f"   🔵 STANDARD REMINDER — '{item_id}' is not in user's top products.")
    print(f"   User's top products: {top_products}")

# ─── QUERY 3: Find frequently bought together items ───────────────────────
def get_frequently_bought_together(item_id):
    print(f"\n🛒 Query 3: Items frequently bought with '{item_id}'")
    pairs = db.cooccurrence.find(
        {"$or": [{"item_a": item_id}, {"item_b": item_id}]},
        {"_id": 0}
    ).sort("count", -1).limit(5)
    pairs_list = list(pairs)
    if not pairs_list:
        print(f"   ❌ No co-occurrence data found for '{item_id}'.")
        return
    for pair in pairs_list:
        other_item = pair["item_b"] if pair["item_a"] == item_id else pair["item_a"]
        print(f"   {other_item}  →  bought together {pair['count']} times")

# ─── QUERY 4: Find users who have a specific item in top 5 ────────────────
def find_users_interested_in(item_id):
    print(f"\n👥 Query 4: Users most interested in '{item_id}'")
    users = db.user_profiles.find(
        {"top_products.product_id": item_id},
        {"_id": 0, "user_id": 1, "top_products.$": 1}
    ).limit(5)
    users_list = list(users)
    if not users_list:
        print(f"   ❌ No users found with '{item_id}' in their top products.")
        return
    for u in users_list:
        score = u["top_products"][0]["score"]
        print(f"   {u['user_id']}  (score: {score})")

# ─── RUN ALL QUERIES ──────────────────────────────────────────────────────
# Change these values to test different users and items
TEST_USER = "User_10"
TEST_ITEM = "ITEM_1471"   # This is in User_10's top products → should get High Discount
TEST_ITEM_2 = "ITEM_9999" # This is NOT in top products → should get Standard Reminder

get_user_recommendations(TEST_USER)
check_discount(TEST_USER, TEST_ITEM)
check_discount(TEST_USER, TEST_ITEM_2)
get_frequently_bought_together("ITEM_1126")
find_users_interested_in("ITEM_4516")

print("\n" + "="*60)
print("✅ Query Demo Complete!")
print("="*60)

client.close()