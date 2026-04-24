
# BI Engineer — 2-hour Take-Home Assignment (Pipeline + Ad-hoc)

Estimated completion time: 2 hours.

Files provided in data/: ad_events.csv, orders.csv, campaigns.csv, costs.csv, campaign_caps.csv.

Deliverables:
1. pipeline/ script or notebook producing daily_campaign_metrics.csv with columns:
   date, campaign_id, impressions, clicks, ctr, orders (7-day lookback), revenue, spend, cpa, roas, day_of_week, slot, rank, sku
2. sql/answers.sql with queries for B1-B2 and short explanations.
3. README describing assumptions and run steps.

Tasks:
- Part A (pipeline): build transformations, basic validation checks (impressions >= clicks, no negative spend, dedupe).
- Part B (SQL): B1 compare avg ROAS across slots (08:00-10:00, 10:00-12:00, 12:00-14:00) for last 14 days; B2 find campaigns exceeding allocated_impression_cap (top 5 by pct overage) and give troubleshooting steps;

Timebox: 2 hours. Keep answers concise.
