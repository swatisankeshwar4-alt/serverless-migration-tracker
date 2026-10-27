# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Approval gate
# MAGIC Admins approve or reject migrations here. Rules (see `tracker/approvals.py`):
# MAGIC * only workspace **admins** can decide; the decider is the signed-in user, never typed in
# MAGIC * only classic jobs in **wave 1 or 2** can be approved (no manual blockers left)
# MAGIC * every decision is written to `approvals`; an approved job moves to status `APPROVED`
# MAGIC
# MAGIC `targets` is a comma list of job ids (for example `1009,1001`) or `wave:1` for a whole wave.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "serverless_migration")
dbutils.widgets.text("targets", "wave:1")
dbutils.widgets.dropdown("decision", "APPROVED", ["APPROVED", "REJECTED"])
dbutils.widgets.text("note", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
targets_spec = dbutils.widgets.get("targets")
decision = dbutils.widgets.get("decision")
note = dbutils.widgets.get("note")
fq = f"`{catalog}`.`{schema}`"

sys.path.insert(0, os.getcwd())
from tracker import approvals  # noqa: E402

# COMMAND ----------

from databricks.sdk import WorkspaceClient

is_admin, who = approvals.workspace_admin(WorkspaceClient())
if not who:
    who = spark.sql("SELECT current_user()").first()[0]
print(f"signed in as {who}; admin = {is_admin}")

jobs = [r.asDict() for r in spark.sql(f"""
  SELECT DISTINCT s.workload_id, s.status, s.wave, i.name
  FROM {fq}.migration_status s
  JOIN {fq}.inventory_workloads i ON i.workload_id = s.workload_id
  WHERE i.workload_type = 'JOB'
""").collect()]
by_id = {r["workload_id"]: r for r in jobs}

ids = approvals.resolve_targets(targets_spec, jobs)
if not ids:
    raise ValueError(f"No jobs matched targets={targets_spec!r}")

# COMMAND ----------

now = datetime.now(timezone.utc)
results, allowed_ids = [], []
for wid in ids:
    row = by_id.get(wid)
    if row is None:
        results.append((wid, None, "REFUSED", "not a known job"))
        continue
    ok, reason = approvals.check_approval(row["status"], row["wave"], is_admin, decision)
    results.append((wid, row["name"], "RECORDED" if ok else "REFUSED", reason))
    if ok:
        allowed_ids.append(wid)

if allowed_ids:
    spark.createDataFrame(
        [(w, decision, who, now, note or None) for w in allowed_ids],
        "workload_id STRING, decision STRING, decided_by STRING, decided_at TIMESTAMP, note STRING"
    ).write.mode("append").saveAsTable(f"{fq}.approvals")

    if decision == "APPROVED":
        spark.createDataFrame([(w,) for w in allowed_ids], "workload_id STRING").createOrReplaceTempView("_approved")
        spark.sql(f"""
          MERGE INTO {fq}.migration_status t USING _approved s ON t.workload_id = s.workload_id
          WHEN MATCHED THEN UPDATE SET t.status = 'APPROVED', t.updated_at = current_timestamp()
        """)

display(spark.createDataFrame(results, "workload_id STRING, name STRING, outcome STRING, reason STRING"))
