# 📊 BI Campaign Performance Pipeline

## 👤 Author

Neeraj Kumar

---

## 📌 Overview

This project builds an end-to-end BI data pipeline to transform raw advertising and order data into actionable campaign-level insights.

It simulates a real-world BI engineering workflow involving data ingestion, transformation, validation, and SQL-based analysis.

---

## 🎯 Business Objectives

* Track daily campaign performance
* Compute key marketing KPIs (CTR, CPA, ROAS)
* Analyze performance across time slots
* Detect campaigns exceeding impression caps
* Enable data-driven optimization

---

## 🛠️ Tech Stack

* Python (Pandas, NumPy)
* SQL
* Data Transformation & Validation

---

## 📁 Project Structure

```
data/        → Raw input datasets  
pipeline/    → Data pipeline script  
sql/         → Analytical SQL queries  
outputs/     → Final processed dataset  
docs/        → Assumptions & validation checks  
```

---

## ⚙️ Pipeline Output

Generated file:
`outputs/daily_campaign_metrics.csv`

### Metrics Included:

* Impressions, Clicks, CTR
* Orders (7-day rolling)
* Revenue, Spend
* CPA, ROAS
* Day of Week
* Campaign attributes (slot, rank, SKU)

---

## 🔄 Pipeline Logic

### 1. Data Cleaning & Validation

* Removed duplicate `event_id`
* Filtered invalid event types
* Ensured `impressions ≥ clicks`
* Removed negative spend values

### 2. Aggregation

* Daily aggregation at `date, campaign_id`
* Computed impressions, clicks, CTR

### 3. Orders & Revenue

* Revenue = `price × quantity`
* Orders = distinct `order_id`
* Aggregated at campaign-day level

### 4. Rolling Attribution

* 7-day rolling sum for:

  * Orders
  * Revenue

### 5. KPI Calculation

* CTR = clicks / impressions
* CPA = spend / orders (7-day)
* ROAS = revenue (7-day) / spend

### 6. Enrichment

* Joined campaign metadata (slot, rank, SKU)
* Added `day_of_week`

---

## 📊 SQL Analysis

### B1: ROAS by Time Slot

* Compared average ROAS across:

  * 08:00–10:00
  * 10:00–12:00
  * 12:00–14:00

### B2: Campaign Cap Breach Detection

* Identified campaigns exceeding impression caps
* Ranked top 5 by % overage
* Included troubleshooting steps:

  * Cap configuration issues
  * Targeting spikes
  * Logging inconsistencies

👉 Full queries available in: `sql/answers.sql`

---

## 🚀 How to Run

```bash
pip install -r requirements.txt
python pipeline/data_pipeline.py
```

---

## 💡 Key Assumptions

* Data is aggregated at `date, campaign_id` level
* Orders use 7-day rolling attribution
* CTR = clicks / impressions
* ROAS = revenue / spend
* CPA = spend / orders
* Campaign-level SKU derived from `campaigns.csv`
* Missing values handled as 0 where applicable

---

## ✅ Data Validation Checks

* No duplicate `event_id`
* Impressions always ≥ clicks
* No negative spend
* Proper date parsing
* Null handling applied

---

## 📌 Notes

* Raw `ad_events` and `campaign_caps` are used directly for cap analysis to avoid double aggregation
* Designed to reflect real-world BI pipeline practices and scalable data transformations

---
