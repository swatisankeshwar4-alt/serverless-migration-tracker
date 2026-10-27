# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Cost before vs after migration (L12)
# MAGIC Compares DBUs and list cost per job, before (classic) and after (serverless), with the team tag.
# MAGIC
# MAGIC * `cost_source = SAMPLE` uses `fixtures/sample_billing.json` (made-up numbers, for the demo).
# MAGIC * `cost_source = SYSTEM` reads `system.billing.usage` joined to `system.billing.list_prices`.
# MAGIC   Correction rows (RETRACTION / RESTATEMENT) are summed in, so the totals are net, as billed.
# MAGIC
# MAGIC DBUs and cost are shown side by side because serverless often uses fewer DBUs per run at a higher
# MAGIC price per DBU: the cost per run can rise even when DBUs fall. The tracker reports both.
# MAGIC Prices are **list** prices, not your negotiated rates.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "serverless_migration")
dbutils.widgets.dropdown("cost_source", "SAMPLE", ["SAMPLE", "SYSTEM"])
dbutils.widgets.text("tag_key", "team")
dbutils.widgets.text("lookback_days", "60")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
cost_source = dbutils.widgets.get("cost_source")
tag_key = dbutils.widgets.get("tag_key")
lookback_days = int(dbutils.widgets.get("lookback_days"))
fq = f"`{catalog}`.`{schema}`"

here = os.getcwd()
sys.path.insert(0, here)
from tracker import cost, tables  # noqa: E402

fixtures_dir = os.path.join(os.path.dirname(here), "fixtures")

# COMMAND ----------

if cost_source == "SAMPLE":
    rows = cost.load_sample(os.path.join(fixtures_dir, "sample_billing.json"))
else:
    if not cost.valid_tag_key(tag_key):
        raise ValueError("tag_key may only contain letters, digits, _ . -")
    if lookback_days < 1 or lookback_days > 730:
        raise ValueError("lookback_days must be between 1 and 730")
    try:
        spark.sql("SELECT 1 FROM system.billing.usage LIMIT 1").collect()
        spark.sql("SELECT 1 FROM system.billing.list_prices LIMIT 1").collect()
    except Exception as e:
        raise RuntimeError(
            "Cannot read system.billing tables. A metastore admin must grant SELECT on "
            "system.billing.usage and system.billing.list_prices. Original error: " + str(e)[:200])
    df = spark.sql(f"""
        WITH names AS (
          SELECT job_id, max_by(name, change_time) AS job_name
          FROM system.lakeflow.jobs GROUP BY job_id
        ),
        u AS (
          SELECT u.usage_date,
                 u.usage_metadata.job_id AS job_id,
                 u.custom_tags['{tag_key}'] AS team,
                 coalesce(u.product_features.is_serverless, false) AS is_serverless,
                 u.usage_quantity AS dbus,
                 u.usage_quantity * p.pricing.default AS list_cost,
                 CASE WHEN u.record_type = 'ORIGINAL' THEN u.usage_metadata.job_run_id END AS run_id
          FROM system.billing.usage u
          LEFT JOIN system.billing.list_prices p
            ON p.sku_name = u.sku_name AND p.cloud = u.cloud AND p.usage_unit = u.usage_unit
           AND u.usage_start_time >= p.price_start_time
           AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
          WHERE u.usage_metadata.job_id IS NOT NULL
            AND u.usage_unit = 'DBU'
            AND u.usage_date >= date_sub(current_date(), {lookback_days})
        )
        SELECT usage_date, u.job_id, any_value(n.job_name) AS job_name, team, is_serverless,
               sum(dbus) AS dbus, sum(list_cost) AS list_cost, count(DISTINCT run_id) AS runs
        FROM u LEFT JOIN names n ON n.job_id = u.job_id
        GROUP BY usage_date, u.job_id, team, is_serverless
    """)
    rows = [{**r.asDict(), "usage_date": str(r["usage_date"])} for r in df.collect()]

periods = cost.compare(rows)
summary = cost.job_summary(periods)
print(f"{len(rows)} daily rows -> {len(periods)} period rows -> {len(summary)} jobs")

# COMMAND ----------

now = datetime.now(timezone.utc)
for col, typ in [("job_name", "STRING"), ("runs", "BIGINT"), ("dbus_per_run", "DOUBLE"),
                 ("active_days", "INT"), ("cost_source", "STRING")]:
    tables.ensure_column(spark, f"{fq}.cost_comparison", col, typ)

spark.sql(f"DELETE FROM {fq}.cost_comparison")
if periods:
    spark.createDataFrame(
        [(p["job_id"], p["period"], p["dbus"], p["list_cost_usd"], p["team_tag"], now,
          p["job_name"], p["runs"], p["dbus_per_run"], p["active_days"], cost_source) for p in periods],
        "workload_id string, period string, dbus double, list_cost_usd double, team_tag string, "
        "computed_at timestamp, job_name string, runs bigint, dbus_per_run double, active_days int, "
        "cost_source string",
    ).write.mode("append").saveAsTable(f"{fq}.cost_comparison")

summary_schema = ("job_id string, job_name string, team_tag string, state string, before_dbus double, "
                  "after_dbus double, before_runs bigint, after_runs bigint, before_dbu_per_run double, "
                  "after_dbu_per_run double, dbu_per_run_change_pct double, before_cost_usd double, "
                  "after_cost_usd double, cost_per_run_change_pct double")
cols = [f.split()[0] for f in summary_schema.split(", ")]
spark.createDataFrame([tuple(s[c] for c in cols) for s in summary], summary_schema) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fq}.cost_summary")

# COMMAND ----------

display(spark.table(f"{fq}.cost_summary").orderBy("job_id"))
