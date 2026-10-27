# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Discovery
# MAGIC Builds the workspace inventory (L1) and stores the raw job/cluster definitions that
# MAGIC the readiness scan reads later (L2-L7, L11).
# MAGIC
# MAGIC * `mode = FIXTURE` reads the sample definitions in `fixtures/` (works on Free Edition).
# MAGIC * `mode = LIVE` reads every job and cluster of this workspace through the SDK.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

from pyspark.sql.types import (IntegerType, StringType, StructField, StructType,
                               TimestampType)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "serverless_migration")
dbutils.widgets.dropdown("mode", "FIXTURE", ["FIXTURE", "LIVE"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
mode = dbutils.widgets.get("mode")
fq = f"`{catalog}`.`{schema}`"

# The notebook lives in src/, so the tracker package and fixtures sit next to it.
here = os.getcwd()
sys.path.insert(0, here)
fixtures_dir = os.path.join(os.path.dirname(here), "fixtures")

from tracker import inventory  # noqa: E402

# COMMAND ----------

if mode == "FIXTURE":
    jobs, clusters = inventory.load_fixtures(fixtures_dir)
else:
    from databricks.sdk import WorkspaceClient
    try:
        jobs, clusters = inventory.load_live(WorkspaceClient())
    except Exception as e:  # readable failure instead of a raw stack trace
        raise RuntimeError(
            "LIVE discovery failed. The running user needs permission to list jobs "
            f"and clusters in this workspace. Original error: {e}"
        )

inv_rows, raw_rows = inventory.build_inventory(jobs, clusters, mode)
now = datetime.now(timezone.utc)
print(f"{mode}: {len(jobs)} jobs, {len(clusters)} clusters -> {len(inv_rows)} inventory rows")

# COMMAND ----------

# Raw definitions table (new in step 2). Created here so the notebook is self-contained.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.raw_definitions (
  workload_id     STRING,
  workload_type   STRING,
  definition_json STRING COMMENT 'full Jobs/Clusters API JSON, scanned in later steps',
  source          STRING,
  discovered_at   TIMESTAMP
) USING DELTA
""")

inv_schema = StructType([
    StructField("workload_id", StringType()),
    StructField("workload_type", StringType()),
    StructField("name", StringType()),
    StructField("owner", StringType()),
    StructField("task_count", IntegerType()),
    StructField("compute_kind", StringType()),
    StructField("runtime_version", StringType()),
    StructField("source", StringType()),
    StructField("discovered_at", TimestampType()),
])
raw_schema = StructType([
    StructField("workload_id", StringType()),
    StructField("workload_type", StringType()),
    StructField("definition_json", StringType()),
    StructField("source", StringType()),
    StructField("discovered_at", TimestampType()),
])

inv_df = spark.createDataFrame(
    [(r["workload_id"], r["workload_type"], r["name"], r["owner"], r["task_count"],
      r["compute_kind"], r["runtime_version"], r["source"], now) for r in inv_rows],
    inv_schema)
raw_df = spark.createDataFrame(
    [(r["workload_id"], r["workload_type"], r["definition_json"], mode, now) for r in raw_rows],
    raw_schema)

# COMMAND ----------

# Replace this mode's previous snapshot, keep the other mode's rows untouched.
spark.sql(f"DELETE FROM {fq}.inventory_workloads WHERE source = '{mode}'")
inv_df.write.mode("append").saveAsTable(f"{fq}.inventory_workloads")

spark.sql(f"DELETE FROM {fq}.raw_definitions WHERE source = '{mode}'")
raw_df.write.mode("append").saveAsTable(f"{fq}.raw_definitions")

# Every new workload starts as DISCOVERED in the status table; existing rows keep their status.
inv_df.select("workload_id").distinct().createOrReplaceTempView("_discovered_ids")
spark.sql(f"""
MERGE INTO {fq}.migration_status t
USING _discovered_ids s ON t.workload_id = s.workload_id
WHEN NOT MATCHED THEN INSERT (workload_id, readiness_score, status, wave, updated_at)
VALUES (s.workload_id, NULL, 'DISCOVERED', NULL, current_timestamp())
""")

# COMMAND ----------

display(spark.sql(f"""
SELECT workload_type, compute_kind, COUNT(*) AS workloads
FROM {fq}.inventory_workloads
WHERE source = '{mode}'
GROUP BY workload_type, compute_kind
ORDER BY workload_type, compute_kind
"""))
