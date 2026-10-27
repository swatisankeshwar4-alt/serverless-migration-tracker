# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Readiness scan
# MAGIC Reads the raw job and cluster definitions saved by discovery, then writes:
# MAGIC * `readiness_findings`: one row per blocker or warning (L2, L3, L4, L5, L7 R libraries, L11)
# MAGIC * `migration_status`: readiness score and status (READY / BLOCKED / DONE) per workload
# MAGIC
# MAGIC Statuses that have moved past the scan (APPROVED, MIGRATING, VALIDATING, DONE, ROLLED_BACK)
# MAGIC are never overwritten.

# COMMAND ----------

import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql.types import (BooleanType, StringType, StructField, StructType,
                               TimestampType)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "serverless_migration")
dbutils.widgets.dropdown("mode", "FIXTURE", ["FIXTURE", "LIVE"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
mode = dbutils.widgets.get("mode")
fq = f"`{catalog}`.`{schema}`"

here = os.getcwd()
sys.path.insert(0, here)
from tracker import initscripts, readiness, tables, waves  # noqa: E402

# Tables created by an earlier version of step 1 may lack the auto_fixable column.
tables.ensure_column(spark, f"{fq}.readiness_findings", "auto_fixable", "BOOLEAN")

# COMMAND ----------

# Raw definitions are small JSON strings, so collecting them to the driver is fine up to a
# few thousand workloads. For very large workspaces this step would move to a Spark UDF.
rows = spark.sql(
    f"SELECT workload_type, definition_json FROM {fq}.raw_definitions WHERE source = '{mode}'"
).collect()
if not rows:
    raise RuntimeError(f"No raw definitions for mode {mode}. Run the discover job first.")

jobs = [json.loads(r.definition_json) for r in rows if r.workload_type == "JOB"]
clusters = [json.loads(r.definition_json) for r in rows if r.workload_type == "CLUSTER"]

# Notebook findings are written by 03_code_scan. They lower the score of the notebook and of the
# jobs that run it. If the code scan has not run yet there are simply none.
nb_ids = {r.workload_id for r in spark.sql(
    f"SELECT workload_id FROM {fq}.inventory_workloads "
    f"WHERE workload_type = 'NOTEBOOK' AND source = '{mode}'").collect()}
notebook_findings = {}
for r in spark.sql(f"SELECT workload_id, limitation_id, severity, detail, suggested_fix, auto_fixable "
                   f"FROM {fq}.readiness_findings").collect():
    if r.workload_id in nb_ids:
        notebook_findings.setdefault(r.workload_id, []).append(
            {"workload_id": r.workload_id, "limitation_id": r.limitation_id, "severity": r.severity,
             "detail": r.detail, "suggested_fix": r.suggested_fix,
             "auto_fixable": bool(r.auto_fixable)})

findings, status_rows = readiness.analyse_all(jobs, clusters, notebook_findings)
wave_plan = waves.plan_waves(jobs, clusters, notebook_findings)   # classic jobs only
for s in status_rows:
    s["wave"] = wave_plan.get(s["workload_id"], {}).get("wave")
now = datetime.now(timezone.utc)
print(f"{len(jobs)} jobs, {len(clusters)} clusters, {len(notebook_findings)} notebooks with code findings "
      f"-> {len(findings)} config findings, {len(status_rows)} status rows")

# COMMAND ----------

find_schema = StructType([StructField(n, StringType()) for n in
                          ("workload_id", "limitation_id", "severity", "detail", "suggested_fix")]
                         + [StructField("found_at", TimestampType()),
                            StructField("auto_fixable", BooleanType())])
find_df = spark.createDataFrame(
    [(f["workload_id"], f["limitation_id"], f["severity"], f["detail"], f["suggested_fix"], now,
      bool(f.get("auto_fixable"))) for f in findings],
    find_schema)

# Replace findings only for jobs and clusters. Notebook findings belong to 03_code_scan.
scanned = [s["workload_id"] for s in status_rows if s["workload_id"] not in nb_ids]
spark.createDataFrame([(w,) for w in scanned], "workload_id STRING").createOrReplaceTempView("_scanned_ids")
spark.sql(f"DELETE FROM {fq}.readiness_findings WHERE workload_id IN (SELECT workload_id FROM _scanned_ids)")
find_df.write.mode("append").saveAsTable(f"{fq}.readiness_findings")

# COMMAND ----------

spark.createDataFrame(
    [(s["workload_id"], int(s["readiness_score"]), s["status"], s["wave"]) for s in status_rows],
    "workload_id STRING, readiness_score INT, status STRING, wave INT").createOrReplaceTempView("_status_updates")

spark.sql(f"""
MERGE INTO {fq}.migration_status t
USING _status_updates s ON t.workload_id = s.workload_id
WHEN MATCHED AND t.status IN ('DISCOVERED', 'READY', 'BLOCKED') THEN UPDATE SET
  t.readiness_score = s.readiness_score, t.status = s.status, t.wave = s.wave,
  t.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (workload_id, readiness_score, status, wave, updated_at)
VALUES (s.workload_id, s.readiness_score, s.status, s.wave, current_timestamp())
""")

# COMMAND ----------

display(spark.sql(f"""
SELECT i.workload_type, i.name, s.readiness_score, s.status,
       (SELECT COUNT(*) FROM {fq}.readiness_findings f
        WHERE f.workload_id = i.workload_id AND f.severity = 'BLOCKER') AS blockers
FROM {fq}.inventory_workloads i
JOIN {fq}.migration_status s ON s.workload_id = i.workload_id
WHERE i.source = '{mode}' AND i.workload_type <> 'NOTEBOOK'
ORDER BY s.readiness_score, i.name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## L3 demo: translate an init script into a serverless environment spec
# MAGIC Reads the sample script in `fixtures/init_scripts/`. In a real workspace the script text
# MAGIC would be fetched from DBFS / the workspace first; S3 scripts need cloud access from the caller.

# COMMAND ----------

script_path = os.path.join(os.path.dirname(here), "fixtures", "init_scripts", "install.sh")
with open(script_path, encoding="utf-8") as f:
    translation = initscripts.translate_init_script(f.read())
print(json.dumps(translation, indent=2))

# COMMAND ----------

# Store the example so the app's Fixes tab can show the script next to the environment spec it becomes.
from tracker import fixes  # noqa: E402

with open(script_path, encoding="utf-8") as f:
    script_text = f.read()
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.init_script_translations (
  script_name      STRING,
  original_script  STRING COMMENT 'sample script only; real scripts are not stored because they can hold secrets',
  environment_yaml STRING COMMENT 'serverless environments block for the job, plus manual follow-ups',
  translation_json STRING,
  created_at       TIMESTAMP
) USING DELTA
""")
spark.sql(f"DELETE FROM {fq}.init_script_translations WHERE script_name = 'install.sh'")
spark.createDataFrame(
    [("install.sh", script_text, fixes.environment_spec_yaml(translation), json.dumps(translation),
      datetime.now(timezone.utc))],
    "script_name STRING, original_script STRING, environment_yaml STRING, translation_json STRING, created_at TIMESTAMP",
).write.mode("append").saveAsTable(f"{fq}.init_script_translations")
print(fixes.environment_spec_yaml(translation))
