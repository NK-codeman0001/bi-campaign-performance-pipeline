# Starter: implement pipeline here
# Submitted by: Neeraj Kumar (8882078050)


import pandas as pd
from pathlib import Path

# ----------------------------
# Paths
# ----------------------------
# ROOT_DIR = Path(r"C:\Users\HP\Downloads\takehome_assignment_bi")  # change only if you move the repo
ROOT_DIR = Path(__file__).resolve().parents[1] 
DATA_DIR = ROOT_DIR / "data"
OUT_DIR = ROOT_DIR / "pipeline"
OUT_DIR.mkdir(exist_ok=True, parents=True)

# ----------------------------
# Load input data
# ----------------------------
ad_events = pd.read_csv(DATA_DIR / "ad_events.csv")
orders = pd.read_csv(DATA_DIR / "orders.csv")
costs = pd.read_csv(DATA_DIR / "costs.csv")
campaigns = pd.read_csv(DATA_DIR / "campaigns.csv")

# ----------------------------
# Preprocess timestamps
# ----------------------------
ad_events["timestamp"] = pd.to_datetime(ad_events["timestamp"])
ad_events["date"] = ad_events["timestamp"].dt.date

orders["order_ts"] = pd.to_datetime(orders["order_ts"])
orders["date"] = orders["order_ts"].dt.date

costs["date"] = pd.to_datetime(costs["date"]).dt.date

# ----------------------------
# Data quality / cleanup
# ----------------------------
# Deduplicate events by event_id
ad_events = ad_events.drop_duplicates(subset=["event_id"])

# Only keep impression and click events
ad_events = ad_events[ad_events["event_type"].isin(["impression", "click"])]

# Remove negative spend rows
costs = costs[costs["spend"] >= 0]

# ----------------------------
# Aggregate ad_events -> daily impressions & clicks
# ----------------------------
daily_events = (
    ad_events
    .groupby(["date", "campaign_id"], as_index=False)
    .agg(
        impressions=("event_type", lambda x: (x == "impression").sum()),
        clicks=("event_type",      lambda x: (x == "click").sum())
    )
)

# Basic sanity: impressions >= clicks (drop if violated)
daily_events = daily_events[daily_events["impressions"] >= daily_events["clicks"]]

# CTR
daily_events["ctr"] = daily_events.apply(
    lambda r: r["clicks"] / r["impressions"] if r["impressions"] > 0 else 0,
    axis=1
)

# ----------------------------
# Aggregate orders -> daily orders & revenue
# ----------------------------
orders["revenue"] = orders["price"] * orders["quantity"]
daily_orders = (
    orders[orders["campaign_id"].notna()]
    .groupby(["date", "campaign_id"], as_index=False)
    .agg(
        orders=("order_id", "nunique"),
        revenue=("revenue", "sum")
    )
)

# ----------------------------
# Merge daily events + orders
# ----------------------------
daily = pd.merge(
    daily_events,
    daily_orders,
    on=["date", "campaign_id"],
    how="left"
)
daily[["orders", "revenue"]] = daily[["orders", "revenue"]].fillna(0)

# ----------------------------
# 7-day rolling orders & revenue per campaign
# ----------------------------
daily = daily.sort_values(["campaign_id", "date"])

daily["orders_7d"] = (
    daily
    .groupby("campaign_id")["orders"]
    .rolling(window=7, min_periods=1)
    .sum()
    .reset_index(level=0, drop=True)
)

daily["revenue_7d"] = (
    daily
    .groupby("campaign_id")["revenue"]
    .rolling(window=7, min_periods=1)
    .sum()
    .reset_index(level=0, drop=True)
)

# ----------------------------
# Join spend
# ----------------------------
daily = pd.merge(
    daily,
    costs,
    on=["date", "campaign_id"],
    how="left"
)
daily["spend"] = daily["spend"].fillna(0)

# ----------------------------
# CPA & ROAS
# ----------------------------
daily["cpa"] = daily.apply(
    lambda r: r["spend"] / r["orders_7d"] if r["orders_7d"] > 0 else 0,
    axis=1
)
daily["roas"] = daily.apply(
    lambda r: r["revenue_7d"] / r["spend"] if r["spend"] > 0 else 0,
    axis=1
)

# ----------------------------
# Add slot, rank, sku from campaigns
# ----------------------------
campaign_dim = campaigns[["campaign_id", "slot", "rank", "sku"]].drop_duplicates()

daily = pd.merge(
    daily,
    campaign_dim,
    on="campaign_id",
    how="left"
)

# ----------------------------
# Day of week
# ----------------------------
daily["date"] = pd.to_datetime(daily["date"])
daily["day_of_week"] = daily["date"].dt.day_name()

# ----------------------------
# Final selection / rename
# ----------------------------
final = daily[[
    "date",
    "campaign_id",
    "impressions",
    "clicks",
    "ctr",
    "orders_7d",
    "revenue_7d",
    "spend",
    "cpa",
    "roas",
    "day_of_week",
    "slot",
    "rank",
    "sku"
]].rename(columns={
    "orders_7d": "orders (7-day lookback)",
    "revenue_7d": "revenue"
})

final = final.sort_values(["date", "campaign_id"])

final["revenue"] = final["revenue"].round(2)
final["ctr"] = final["ctr"].round(4)
final["roas"] = final["roas"].round(4)

# ----------------------------
# Write output
# ----------------------------
out_path = OUT_DIR / "daily_campaign_metrics.csv"
final.to_csv(out_path, index=False)
print(f"Wrote {out_path}")
