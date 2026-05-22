# generate_data.py
import csv
import random
import os

# Create directory if it doesn't exist
os.makedirs("data/raw_logs", exist_ok=True)

print("Generating sample data...")

# Generate 500,000 rows (adjust as needed)
with open("data/raw_logs/sample_data.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["session_id", "user_id", "item_id", "event_type", "timestamp", "category"])
    
    for i in range(500000):  # 500k rows - good for testing
        session = f"session_{random.randint(1, 10000)}"
        user = f"user_{random.randint(1, 5000)}"
        item = f"item_{random.randint(1, 200)}"
        
        # Weighted events (view more common, purchase less common)
        event = random.choices(
            ["view", "view", "view", "cart", "purchase"],
            weights=[0.4, 0.4, 0.4, 0.15, 0.05]
        )[0]
        
        category = random.choice([
            "electronics", "clothing", "books", "home", 
            "toys", "sports", "beauty", "groceries"
        ])
        
        writer.writerow([session, user, item, event, "2026-01-01", category])
        
        # Progress indicator
        if i % 50000 == 0:
            print(f"  Generated {i:,} rows...")

print("✅ Done! Generated data/raw_logs/sample_data.csv")