# 🛒 E-Commerce Behavioral Analytics & Recommendation Engine

A big data engineering pipeline that processes 10M+ raw e-commerce event logs to build a personalized recommendation and cart abandonment recovery system using Apache Spark and MongoDB.

---

## 🧠 How It Works

The system is built across 3 phases:

### Phase 1 — Large-Scale Log Processing (Apache Spark)
- Ingests 10GB+ of raw e-commerce event logs (10M+ records)
- Processes data using PySpark with the MapReduce paradigm across all CPU cores
- Builds a **Market Basket Analysis** engine using self-join MapReduce to identify frequently co-purchased product pairs

### Phase 2 — User Affinity Scoring (MongoDB)
- Assigns weighted scores to behavioral events:
  - View = 1 | Add to Cart = 3 | Purchase = 5
- Builds per-user product affinity profiles for **25,000 users**
- Stores denormalized user profile documents in MongoDB optimized for sub-millisecond recommendation queries

### Phase 3 — Cart Abandonment Recovery
- Joins Spark DataFrames with MongoDB user profiles
- Classifies **1.4M+ cart abandoners** into targeted campaign segments:
  - 🔴 High Discount — high-affinity users who didn't convert
  - 🔵 Standard Reminder — lower-engagement abandoners
- Outputs segmented campaign CSVs ready for marketing pipeline ingestion

---

## 📊 Key Metrics

| Metric | Value |
|---|---|
| Raw event logs processed | **10M+** |
| Raw data size | **10GB+** |
| User profiles built | **25,000** |
| Cart abandoners classified | **1.4M+** |
| Processing paradigm | MapReduce across all CPU cores |
| Query optimization | Sub-millisecond (MongoDB denormalized docs) |

---

## 🛠️ Tech Stack

| Layer | Technologies |
|---|---|
| Big Data Processing | Apache Spark (PySpark), MapReduce |
| Database | MongoDB, PyMongo |
| Data Manipulation | Pandas, Python |
| Dev Environment | VS Code, Python venv |

---

## 🗂️ Project Structure

```
├── task1_1.py              # Phase 1 - Spark log ingestion & processing
├── task1_2.py              # Phase 1 - Market Basket Analysis (self-join MapReduce)
├── phase2_mongodb.py       # Phase 2 - User Affinity Scoring & MongoDB storage
├── Phase3.py               # Phase 3 - Cart abandonment classification pipeline
├── query_demo.py           # MongoDB query demonstrations
├── generate_data.py        # Synthetic data generator for testing
└── Architecture_Schema_Document.docx  # System architecture documentation
```

---

## ⚙️ Setup & Run

### Prerequisites
- Python 3.8+
- Apache Spark
- MongoDB (local or Atlas)
- Java 8 or 11 (required for Spark)

### Installation

```bash
# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Mac/Linux

# Install dependencies
pip install pyspark pymongo pandas
```

### Run

```bash
# Phase 1 - Process raw logs
python task1_1.py
python task1_2.py

# Phase 2 - Build user profiles in MongoDB
python phase2_mongodb.py

# Phase 3 - Cart abandonment segmentation
python Phase3.py
```

> **Note:** Raw event log data (`ecommerce_logs.csv`) is not included due to file size (10GB+). Use `generate_data.py` to generate a synthetic dataset for testing.

---

## 🔑 Key Engineering Highlights

- **MapReduce at scale** — self-join MapReduce on 10M+ records to surface co-purchased product pairs without external ML libraries
- **Weighted behavioral modeling** — custom affinity scoring formula balancing recency and intent signals across all user interactions
- **MongoDB schema design** — denormalized document structure chosen specifically for read-heavy recommendation queries at sub-millisecond latency
- **Segmentation logic** — rule-based classification combining affinity scores and abandonment signals to produce actionable marketing segments

---

## 👤 Author

**Bassel Nashaat**
- LinkedIn: [linkedin.com/in/bassel-nashaat](https://linkedin.com/in/bassel-nashaat)
- GitHub: [github.com/basselnashaat](https://github.com/basselnashaat)
