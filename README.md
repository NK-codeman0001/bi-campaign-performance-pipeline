# 📊 BI Campaign Performance Pipeline

## 👤 Author
Neeraj Kumar

---

## 📌 Overview
This project builds an end-to-end BI data pipeline to analyze campaign performance using raw ad events, orders, and cost data.

---

## 🎯 Business Objective
- Measure campaign performance daily
- Calculate CTR, CPA, ROAS
- Identify inefficient campaigns
- Detect impression cap breaches

---

## 🛠️ Tech Stack
- Python (Pandas)
- SQL

---

## ⚙️ Pipeline Output
`outputs/daily_campaign_metrics.csv`

Metrics:
- impressions, clicks, CTR
- orders (7-day)
- revenue, spend
- CPA, ROAS

---

## 🚀 How to Run

```bash
pip install -r requirements.txt
python pipeline/data_pipeline.py
