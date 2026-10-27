# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Code scan
# MAGIC Reads the source of every discovered notebook and writes:
# MAGIC * `readiness_findings`: L6 (hard API blockers), L7 (Scala/R), L13 (streaming triggers, ANSI), L11 (mounts, DBFS)
# MAGIC * `proposed_rewrites`: automated rewrites (never applied to the original notebook) and
# MAGIC   keep-on-classic / rewrite decisions for Scala and R
# MAGIC
# MAGIC Notebook source is only stored in `proposed_rewrites` when `store_source` is true. It defaults to
# MAGIC true for FIXTURE data and false for LIVE, because real notebooks can contain secrets.

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
dbutils.widgets.dropdown("store_source", "auto", ["auto", "true", "false"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
mode = dbutils.widgets.get("mode")
store_source = {"true": True, "false": False}.get(dbutils.widgets.get("store_source"), mode == "FIXTURE")
fq = f"`{catalog}`.`{schema}`"

here = os.getcwd()
sys.path.insert(0, here)
fixtures_dir = os.path.join(os.path.dirname(here), "fixtures")
from tracker import codescan, tables  # noqa: E402

tables.ensure_column(spark, f"{fq}.readiness_findings", "auto_fixable", "BOOLEAN")

# COMMAND ----------

paths = [r.workload_id for r in spark.sql(
    f"SELECT workload_id FROM {fq}.inventory_workloads "
    f"WHERE workload_type = 'NOTEBOOK' AND source = '{mode}'").collect()]
if not paths:
    raise RuntimeError(f"No notebooks in the inventory for mode {mode}. Run the discover job first.")

if mode == "FIXTURE":
    sources, missing = codescan.load_fixture_sources(fixtures_dir, paths)
    errors = {p: "no fixture source" for p in missing}
else:
    from databricks.sdk import WorkspaceClient
    sources, errors = codescan.load_live_sources(WorkspaceClient(), paths)

print(f"{len(paths)} notebooks: {len(sources)} scanned, {len(errors)} without readable source")
if errors:
    print("Not scanned (reason):", dict(list(errors.items())[:10]), "..." if len(errors) > 10 else "")

# COMMAND ----------

now = datetime.now(timezone.utc)
findings, proposals = [], []

for path, (src, lang) in sources.items():
    findings.extend(codescan.scan_with_fixability(path, src, lang))

    if lang in ("SCALA", "R"):
        c = codescan.classify_jvm_or_r(src, lang)
        proposals.append((path, lang, c["decision"],
                          json.dumps({"effort": c["effort"], "lines": c["lines"], "reasons": c["reasons"]}),
                          None, False, now, None))
    else:
        new_src, changes = codescan.rewrite_notebook(src, lang)
        if changes:
            proposals.append((path, lang, "REWRITE_PROPOSED", json.dumps(changes),
                              new_src if store_source else None, False, now,
                              src if store_source else None))

print(f"{len(findings)} findings, {len(proposals)} proposals")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.proposed_rewrites (
  workload_id     STRING COMMENT 'notebook path',
  language        STRING,
  decision        STRING COMMENT 'REWRITE_PROPOSED | REWRITE_PYSPARK | KEEP_ON_CLASSIC',
  changes_json    STRING COMMENT 'rule names and counts, or classification reasons; never source text',
  proposed_source STRING COMMENT 'only stored when store_source is true',
  applied         BOOLEAN,
  created_at      TIMESTAMP,
  original_source STRING COMMENT 'only stored when store_source is true; shown as a before/after diff'
) USING DELTA
""")
tables.ensure_column(spark, f"{fq}.proposed_rewrites", "original_source", "STRING")

find_schema = StructType([StructField(n, StringType()) for n in
                          ("workload_id", "limitation_id", "severity", "detail", "suggested_fix")]
                         + [StructField("found_at", TimestampType()),
                            StructField("auto_fixable", BooleanType())])
prop_schema = StructType([StructField(n, StringType()) for n in
                          ("workload_id", "language", "decision", "changes_json", "proposed_source")]
                         + [StructField("applied", BooleanType()), StructField("created_at", TimestampType()),
                            StructField("original_source", StringType())])

# Replace results for every notebook of this mode (also clears stale findings for notebooks that
# can no longer be read).
spark.createDataFrame([(p,) for p in paths], "workload_id STRING").createOrReplaceTempView("_nb_ids")
spark.sql(f"DELETE FROM {fq}.readiness_findings WHERE workload_id IN (SELECT workload_id FROM _nb_ids)")
spark.sql(f"DELETE FROM {fq}.proposed_rewrites WHERE workload_id IN (SELECT workload_id FROM _nb_ids)")

spark.createDataFrame(
    [(f["workload_id"], f["limitation_id"], f["severity"], f["detail"], f["suggested_fix"], now,
      bool(f.get("auto_fixable"))) for f in findings],
    find_schema).write.mode("append").saveAsTable(f"{fq}.readiness_findings")
spark.createDataFrame(proposals, prop_schema).write.mode("append").option("mergeSchema", "true") \
    .saveAsTable(f"{fq}.proposed_rewrites")

# COMMAND ----------

display(spark.sql(f"""
SELECT limitation_id, severity, COUNT(*) AS findings, COUNT(DISTINCT workload_id) AS notebooks,
       SUM(CASE WHEN auto_fixable THEN 1 ELSE 0 END) AS auto_fixable
FROM {fq}.readiness_findings
WHERE workload_id IN (SELECT workload_id FROM _nb_ids)
GROUP BY limitation_id, severity
ORDER BY limitation_id, severity
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT workload_id, language, decision, changes_json
FROM {fq}.proposed_rewrites
ORDER BY decision, workload_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Next: run `02_readiness` again (or the `pipeline` job) so job and notebook scores include these code findings.
